import logging
import allure
import datetime
import pytest
import time
import random
import simplejson as json

import config
from common.bindings.databases.aerospike_base import AerospikeClient
from common.bindings.kafka.encoders import encode_thrift
from common.bindings.kafka.kafka_class_confluent import Kafka
from common.bindings.shrinker.shrinker_backend import filter_controller as filter_controller_base
from common.models.thrift.TCallModel.ttypes import TCallModel as TCallModelThrift
from common.models.thrift.TCiscoModel.ttypes import TCiscoModel as TCiscoModelThrift
from common.models.thrift.TGeoModel.ttypes import TGeoModel as TGeoModelThrift
from common.models.thrift.TSmsModel.ttypes import TSmsModel as TSmsModelThrift
from common.models.thrift.TVlrModel.ttypes import TVlrModel as TVlrModelThrift
from common.utils.non_blocking_asserts import compare_values
from common.utils.state import State
from common.utils import tags
from tests.shrinker_domain.smart_triggers.testutils import body_for_test, transform_for_domain_filter_smart_triggers


@allure.epic("Shrinker")
@allure.suite("Shrinker")
@allure.feature("Shrinker Domain")
@tags.regression
@tags.author("Кащеев Денис")
@tags.test_type("Интеграционное тестирование")
@tags.priority("Средний")
@pytest.mark.usefixtures("kerberos_setup", "postgres_setup", "datariver_api_setup", "check_token")
class TestShrinkerDomainSmartTriggers:

    @tags.smoke
    @allure.story("Smart Triggers: Aerospike")
    def test_shrinker_domain_smart_triggers_aerospike(self, data_test_smart_triggers_aerospike):

        with allure.step("Исходные параметры для теста"):
            allure.attach(json.dumps(
                data_test_smart_triggers_aerospike, indent=4, default=str), f"build_test_data.txt", allure.attachment_type.TEXT,
            )

        # Предварительные шаги
        test_filter = data_test_smart_triggers_aerospike["pre-create"]

        response = filter_controller_base.post_filter(
            token=State.datariver_token,
            data=test_filter
        )

        domain_filter = response.json()
        if response.status_code == 201:
            State.domain_filters.append(domain_filter["id"])

        assert 201 == response.status_code

        time.sleep(config.SHRINKER_DOMAIN_WAITER)

        source_broker = config.KAFKA_PREP1
        target_topic = data_test_smart_triggers_aerospike['kafka']['target_topic']

        with allure.step("Формирование тестовых данных"):
            for test_data in data_test_smart_triggers_aerospike['test_data']:

                if test_data["type"] == "call":
                    test_data["source_topic"] = "test1_dr_shrinker-engine_call"
                    test_data["encoded_message"] = encode_thrift(data=test_data["message"], model=TCallModelThrift)

                elif test_data["type"] == "sms":
                    test_data["source_topic"] = "test1_dr_shrinker-engine_sms"
                    test_data["encoded_message"] = encode_thrift(data=test_data["message"], model=TSmsModelThrift)

                elif test_data["type"] == "roaming":
                    test_data["source_topic"] = "test1_dr_shrinker-engine_roaming"
                    test_data["encoded_message"] = encode_thrift(data=test_data["message"], model=TVlrModelThrift)

                elif test_data["type"] == "geo":
                    test_data["source_topic"] = "test1_dr_shrinker-engine_geo"
                    test_data["encoded_message"] = encode_thrift(data=test_data["message"], model=TGeoModelThrift)

                elif test_data["type"] == "url":
                    test_data["source_topic"] = "test1_dr_shrinker-engine_clickstream"
                    test_data["encoded_message"] = encode_thrift(data=test_data["message"], model=TCiscoModelThrift)

                elif test_data["type"] == "geozone":
                    test_data["source_topic"] = "test1_dr_customer-geo_events"
                    test_data["encoded_message"] = json.dumps(test_data['message']).encode('utf-8')

                else:
                    raise ValueError(f"Нет известного типа потока: {test_data}")

            logger = logging.getLogger('тестовые данные')
            logger.setLevel(logging.INFO)
            logger.info(f"{data_test_smart_triggers_aerospike['test_data']}")


        with allure.step("Отправка сообщения в Кафку и поиск записей в Аэроспайке"):
            with Kafka(bootstrap_servers=source_broker) as kafka:
                kafka.create_producer()
                kafka.create_consumer(
                    topic=target_topic
                )

                for i, test_data in enumerate(data_test_smart_triggers_aerospike['test_data']):

                    message = test_data["message"]
                    source_topic = test_data["source_topic"]
                    encoded_message = test_data["encoded_message"]

                    if message.get("imsi") is not None:
                        message_key = message["imsi"]
                    elif message.get("imsiIn") is not None:
                        message_key = message["imsiIn"]
                    else:
                        raise KeyError(f"Нет ключа (imsi / imsiIn) в сообщении: {message}")

                    kafka.send_message(
                        topic=source_topic,
                        key=message_key,
                        message=encoded_message
                    )

                if data_test_smart_triggers_aerospike.get("latency") is not None or data_test_smart_triggers_aerospike["latency"] != 0:
                    time.sleep(data_test_smart_triggers_aerospike["latency"])

                with AerospikeClient() as aerospike:
                    records = aerospike.get_all_records(namespace="fct1", set="smartTrigger")
                    allure.attach(
                        json.dumps(records, indent=4, default=str), "aerospike_records", allure.attachment_type.TEXT
                    )

                # Точечный отбор записей
                aerospike_data = []
                for record in records:
                    data = record[2]
                    if data["imsi"] == data_test_smart_triggers_aerospike["expected_imsi"]:
                        aerospike_data.append(data)
                allure.attach(
                    json.dumps(aerospike_data, indent=4, default=str), "aerospike_target_data", allure.attachment_type.TEXT
                )


                if data_test_smart_triggers_aerospike["expected_result"]:
                    assert aerospike_data, f"Не найдены целевые записи в Аэроспайке"
                    flag = True
                    target_value = 1 if data_test_smart_triggers_aerospike["is_trigger"] == 0 else 1
                    for data in aerospike_data:
                        if data["isTrigger"] == target_value:
                            flag = False
                            break
                    assert flag, f"Не совпадает результат параметра isTrigger: {aerospike_data}"
                else:
                    assert not aerospike_data, f"Ожидалось, что записи в Аероспайке будут удалены по ttl: {aerospike_data}"

    @allure.story("Smart Triggers: Content")
    def test_shrinker_domain_smart_triggers_content(self, data_test_smart_triggers_content):

        with allure.step("Исходные параметры для теста"):
            allure.attach(json.dumps(
                data_test_smart_triggers_content, indent=4, default=str), f"build_test_data.txt", allure.attachment_type.TEXT,
            )

        # Предварительные шаги
        test_filter = data_test_smart_triggers_content["pre-create"]

        response = filter_controller_base.post_filter(
            token=State.datariver_token,
            data=test_filter
        )

        domain_filter = response.json()
        if response.status_code == 201:
            State.domain_filters.append(domain_filter["id"])

        assert 201 == response.status_code

        time.sleep(config.SHRINKER_DOMAIN_WAITER)

        # Основная логика теста
        test_data, messages = body_for_test(data_test_smart_triggers_content)

        # Когда ожидаем, что сообщение НЕ должно пройти фильтрацию
        if not data_test_smart_triggers_content['expected_result']:
            assert not messages

        # Когда ожидаем, что сообщение должно пройти фильтрацию
        else:

            with allure.step("Формирование ожидаемого результата"):
                expected_data = transform_for_domain_filter_smart_triggers(
                    source_data=data_test_smart_triggers_content
                )
                allure.attach(json.dumps(expected_data, indent=4, default=str), f"expected_data.txt",
                              allure.attachment_type.TEXT)

            assert messages, "Целевое сообщение не найдено"
            assert len(messages) == 1, "Обнаружен дубль"

            if data_test_smart_triggers_content["out_type"] == "CSV":
                expected_data = expected_data.encode('utf-8')
                assert expected_data == messages[0]

            else:
                #JSON/THRIFT
                compare_values(
                    expected=expected_data,
                    actual=messages[0],
                )


    @allure.story("Smart Triggers: Statuses (from run to pause)")
    def test_shrinker_domain_smart_triggers_from_run_to_pause(self, data_test_smart_triggers_from_run_to_pause):

        with allure.step("Исходные параметры для теста"):
            allure.attach(json.dumps(
                data_test_smart_triggers_from_run_to_pause, indent=4, default=str), f"build_test_data.txt", allure.attachment_type.TEXT,
            )

        """
        Предварительные шаги
        """
        # Создание фильтра
        test_filter = data_test_smart_triggers_from_run_to_pause["pre-create"]

        response = filter_controller_base.post_filter(
            token=State.datariver_token,
            data=test_filter
        )

        domain_filter = response.json()
        if response.status_code == 201:
            State.domain_filters.append(domain_filter["id"])

        assert 201 == response.status_code

        # Изменение статуса фильтра
        response = filter_controller_base.patch_filter_by_id(
            token=State.datariver_token,
            id=domain_filter["id"],
            status="PAUSE"
        )
        assert 200 == response.status_code

        time.sleep(config.SHRINKER_DOMAIN_WAITER)

        """
        Основная логика теста
        """
        test_data, messages = body_for_test(data_test_smart_triggers_from_run_to_pause)

        # Когда ожидаем, что сообщение НЕ должно пройти фильтрацию
        if not data_test_smart_triggers_from_run_to_pause['expected_result']:
            assert not messages

        # Когда ожидаем, что сообщение должно пройти фильтрацию
        else:

            with allure.step("Формирование ожидаемого результата"):
                expected_data = transform_for_domain_filter_smart_triggers(
                    source_data=data_test_smart_triggers_from_run_to_pause
                )
                allure.attach(json.dumps(expected_data, indent=4, default=str), f"expected_data.txt",
                              allure.attachment_type.TEXT)

            assert messages, "Целевое сообщение не найдено"
            assert len(messages) == 1, "Обнаружен дубль"

            if data_test_smart_triggers_from_run_to_pause["out_type"] == "CSV":
                expected_data = expected_data.encode('utf-8')
                assert expected_data == messages[0]

            else:
                #JSON/THRIFT
                compare_values(
                    expected=expected_data,
                    actual=messages[0],
                )


    @allure.story("Smart Triggers: Statuses (from pause to run)")
    def test_shrinker_domain_smart_triggers_from_pause_to_run(self, data_test_smart_triggers_from_pause_to_run):

        with allure.step("Исходные параметры для теста"):
            allure.attach(json.dumps(
                data_test_smart_triggers_from_pause_to_run, indent=4, default=str), f"build_test_data.txt", allure.attachment_type.TEXT,
            )

        """
        Предварительные шаги
        """
        # Создание фильтра
        test_filter = data_test_smart_triggers_from_pause_to_run["pre-create"]

        response = filter_controller_base.post_filter(
            token=State.datariver_token,
            data=test_filter
        )

        domain_filter = response.json()
        if response.status_code == 201:
            State.domain_filters.append(domain_filter["id"])

        assert 201 == response.status_code

        # Изменение статуса фильтра
        response = filter_controller_base.patch_filter_by_id(
            token=State.datariver_token,
            id=domain_filter["id"],
            status="PAUSE"
        )
        assert 200 == response.status_code

        # Изменение статуса фильтра
        response = filter_controller_base.patch_filter_by_id(
            token=State.datariver_token,
            id=domain_filter["id"],
            status="RUN"
        )
        assert 200 == response.status_code

        time.sleep(config.SHRINKER_DOMAIN_WAITER)

        """
        Основная логика теста
        """
        test_data, messages = body_for_test(data_test_smart_triggers_from_pause_to_run)

        # Когда ожидаем, что сообщение НЕ должно пройти фильтрацию
        if not data_test_smart_triggers_from_pause_to_run['expected_result']:
            assert not messages

        # Когда ожидаем, что сообщение должно пройти фильтрацию
        else:

            with allure.step("Формирование ожидаемого результата"):
                expected_data = transform_for_domain_filter_smart_triggers(
                    source_data=data_test_smart_triggers_from_pause_to_run
                )
                allure.attach(json.dumps(expected_data, indent=4, default=str), f"expected_data.txt",
                              allure.attachment_type.TEXT)

            assert messages, "Целевое сообщение не найдено"
            assert len(messages) == 1, "Обнаружен дубль"

            if data_test_smart_triggers_from_pause_to_run["out_type"] == "CSV":
                expected_data = expected_data.encode('utf-8')
                assert expected_data == messages[0]

            else:
                #JSON/THRIFT
                compare_values(
                    expected=expected_data,
                    actual=messages[0],
                )


    @allure.story("Smart Triggers: Statuses (stop by time)")
    def test_shrinker_domain_smart_triggers_stop_by_time(self, data_test_smart_triggers_stop_by_time):

        with allure.step("Исходные параметры для теста"):
            allure.attach(json.dumps(
                data_test_smart_triggers_stop_by_time, indent=4, default=str), f"build_test_data.txt",
                allure.attachment_type.TEXT,
            )

        """
        Предварительные шаги
        """
        # Создание фильтра
        test_filter = data_test_smart_triggers_stop_by_time["pre-create"]

        response = filter_controller_base.post_filter(
            token=State.datariver_token,
            data=test_filter
        )

        domain_filter = response.json()
        if response.status_code == 201:
            State.domain_filters.append(domain_filter["id"])

        assert 201 == response.status_code

        time.sleep(config.SHRINKER_DOMAIN_WAITER+105)

        """
        Основная логика теста
        """
        test_data, messages = body_for_test(data_test_smart_triggers_stop_by_time)

        # Когда ожидаем, что сообщение НЕ должно пройти фильтрацию
        if not data_test_smart_triggers_stop_by_time['expected_result']:
            assert not messages

        # Когда ожидаем, что сообщение должно пройти фильтрацию
        else:

            with allure.step("Формирование ожидаемого результата"):
                expected_data = transform_for_domain_filter_smart_triggers(
                    source_data=data_test_smart_triggers_stop_by_time
                )
                allure.attach(json.dumps(expected_data, indent=4, default=str), f"expected_data.txt",
                              allure.attachment_type.TEXT)

            assert messages, "Целевое сообщение не найдено"
            assert len(messages) == 1, "Обнаружен дубль"

            if data_test_smart_triggers_stop_by_time["out_type"] == "CSV":
                expected_data = expected_data.encode('utf-8')
                assert expected_data == messages[0]

            else:
                #JSON/THRIFT
                compare_values(
                    expected=expected_data,
                    actual=messages[0],
                )


    @allure.story("Smart Triggers: Statuses (run by time)")
    def test_shrinker_domain_smart_triggers_run_by_time(self, data_test_smart_triggers_run_by_time):

        with allure.step("Исходные параметры для теста"):
            allure.attach(json.dumps(
                data_test_smart_triggers_run_by_time, indent=4, default=str), f"build_test_data.txt",
                allure.attachment_type.TEXT,
            )

        """
        Предварительные шаги
        """
        # Создание фильтра
        test_filter = data_test_smart_triggers_run_by_time["pre-create"]

        response = filter_controller_base.post_filter(
            token=State.datariver_token,
            data=test_filter
        )

        domain_filter = response.json()
        if response.status_code == 201:
            State.domain_filters.append(domain_filter["id"])

        assert 201 == response.status_code

        time.sleep(config.SHRINKER_DOMAIN_WAITER+105)

        """
        Основная логика теста
        """
        test_data, messages = body_for_test(data_test_smart_triggers_run_by_time)

        # Когда ожидаем, что сообщение НЕ должно пройти фильтрацию
        if not data_test_smart_triggers_run_by_time['expected_result']:
            assert not messages

        # Когда ожидаем, что сообщение должно пройти фильтрацию
        else:

            with allure.step("Формирование ожидаемого результата"):
                expected_data = transform_for_domain_filter_smart_triggers(
                    source_data=data_test_smart_triggers_run_by_time
                )
                allure.attach(json.dumps(expected_data, indent=4, default=str), f"expected_data.txt",
                              allure.attachment_type.TEXT)

            assert messages, "Целевое сообщение не найдено"
            assert len(messages) == 1, "Обнаружен дубль"

            if data_test_smart_triggers_run_by_time["out_type"] == "CSV":
                expected_data = expected_data.encode('utf-8')
                assert expected_data == messages[0]

            else:
                #JSON/THRIFT
                compare_values(
                    expected=expected_data,
                    actual=messages[0],
                )


    @allure.story("Smart Triggers: Statuses (from stop to run)")
    def test_shrinker_domain_smart_triggers_from_stop_to_run(self, data_test_smart_triggers_from_stop_to_run):

        with allure.step("Исходные параметры для теста"):
            allure.attach(json.dumps(
                data_test_smart_triggers_from_stop_to_run, indent=4, default=str), f"build_test_data.txt",
                allure.attachment_type.TEXT,
            )

        """
        Предварительные шаги
        """

        # Создание фильтра
        test_filter = data_test_smart_triggers_from_stop_to_run["pre-create"]

        response = filter_controller_base.post_filter(
            token=State.datariver_token,
            data=test_filter
        )

        domain_filter = response.json()
        if response.status_code == 201:
            State.domain_filters.append(domain_filter["id"])

        assert 201 == response.status_code

        # Изменение фильтра
        test_filter["id"] = domain_filter["id"]
        test_filter["filters"][0]["id"] = domain_filter['filters'][0]["id"]
        test_filter['lastEdit'] = domain_filter['lastEdit']

        test_filter["range"] = [
            int((datetime.datetime.now() - datetime.timedelta(hours=random.randint(1, 12))).timestamp()) * 1000,
            int((datetime.datetime.now() + datetime.timedelta(hours=random.randint(1, 12))).timestamp()) * 1000
        ]

        # Отправляем PUT запрос на изменение фильтра
        response_put = filter_controller_base.put_filter_by_list(
            token=State.datariver_token,
            data=test_filter  # передаем тест данные
        )

        assert 200 == response_put.status_code

        time.sleep(config.SHRINKER_DOMAIN_WAITER)

        """
        Основная логика теста
        """
        test_data, messages = body_for_test(data_test_smart_triggers_from_stop_to_run)

        # Когда ожидаем, что сообщение НЕ должно пройти фильтрацию
        if not data_test_smart_triggers_from_stop_to_run['expected_result']:
            assert not messages

        # Когда ожидаем, что сообщение должно пройти фильтрацию
        else:

            with allure.step("Формирование ожидаемого результата"):
                expected_data = transform_for_domain_filter_smart_triggers(
                    source_data=data_test_smart_triggers_from_stop_to_run
                )
                allure.attach(json.dumps(expected_data, indent=4, default=str), f"expected_data.txt",
                              allure.attachment_type.TEXT)

            assert messages, "Целевое сообщение не найдено"
            assert len(messages) == 1, "Обнаружен дубль"

            if data_test_smart_triggers_from_stop_to_run["out_type"] == "CSV":
                expected_data = expected_data.encode('utf-8')
                assert expected_data == messages[0]

            else:
                #JSON/THRIFT
                compare_values(
                    expected=expected_data,
                    actual=messages[0],
                )
