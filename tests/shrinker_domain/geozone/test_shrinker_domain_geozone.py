import json
import random
import time
import datetime

import allure
import pytest

import config
from common.utils import tags
from common.bindings.shrinker.shrinker_backend import filter_controller
from common.utils.non_blocking_asserts import compare_values
from common.utils.state import State
from tests.shrinker_domain.geozone.testutils import body_for_test, transform_for_domain_filter_geozone


@allure.epic("Shrinker")
@allure.suite("Shrinker")
@allure.feature("Shrinker Domain")
@tags.regression
@tags.author("Кондрашов Владимир")
@tags.test_type("Интеграционное тестирование")
@tags.priority("Средний")
@pytest.mark.usefixtures("kerberos_setup", "postgres_setup", "datariver_api_setup", "check_token")
class TestShrinkerDomainGeozone:

    @tags.business_critical
    @tags.smoke
    @allure.story("Geozone: Content")
    def test_shrinker_domain_base(self, data_test_geozone_base):

        with allure.step("Исходные параметры для теста"):
            allure.attach(json.dumps(
                data_test_geozone_base, indent=4, default=str), f"test_data.txt", allure.attachment_type.TEXT
            )

        # Предварительные шаги
        test_filter = data_test_geozone_base["pre-create"]

        response = filter_controller.post_filter(
            token=State.datariver_token,
            data=test_filter
        )

        domain_filter = response.json()
        if response.status_code == 201:
            State.domain_filters.append(domain_filter["id"])

        assert 201 == response.status_code

        time.sleep(config.SHRINKER_DOMAIN_WAITER)

        # Основная логика теста
        test_data, messages = body_for_test(data_test_geozone_base)

        #todo: расскоментить

        # Когда ожидаем, что сообщение НЕ должно пройти фильтрацию
        if not data_test_geozone_base['expected_result']:
            assert not messages

        # Когда ожидаем, что сообщение должно пройти фильтрацию
        else:

            with allure.step("Формирование ожидаемого результата"):
                expected_data = transform_for_domain_filter_geozone(
                    test_data=test_data, out_type=data_test_geozone_base['out_type']
                )
                allure.attach(json.dumps(expected_data, indent=4, default=str), f"expected_data.txt",
                              allure.attachment_type.TEXT)

            assert messages, "Целевое сообщение не найдено"
            assert len(messages) == 1, "Обнаружен дубль"

            read_message = messages[0]

            # JSON / THRIFT
            if isinstance(expected_data, dict):
                compare_values(
                    expected=expected_data,
                    actual=read_message,
                    ignore_params=['region', 'kafkaTimestamp']
                )

            # CSV
            if isinstance(expected_data, str):
                assert expected_data.encode('utf-8') == read_message


    @allure.story("Geozone: Statuses (from run to pause)")
    def test_shrinker_domain_from_run_to_pause(self, data_test_geozone_from_run_to_pause):

        with allure.step("Исходные параметры для теста"):
            allure.attach(json.dumps(
                data_test_geozone_from_run_to_pause, indent=4, default=str), f"test_data.txt", allure.attachment_type.TEXT
            )

        """
        Предварительные шаги
        """
        # Создание фильтра
        test_filter = data_test_geozone_from_run_to_pause["pre-create"]

        response = filter_controller.post_filter(
            token=State.datariver_token,
            data=test_filter
        )

        domain_filter = response.json()
        if response.status_code == 201:
            State.domain_filters.append(domain_filter["id"])

        assert 201 == response.status_code

        time.sleep(config.SHRINKER_DOMAIN_WAITER)

        # Изменение статуса фильтра
        response = filter_controller.patch_filter_by_id(
            token=State.datariver_token,
            id=domain_filter["id"],
            status="PAUSE"
        )
        assert 200 == response.status_code

        time.sleep(config.SHRINKER_DOMAIN_WAITER)

        # Основная логика теста
        test_data, messages = body_for_test(data_test_geozone_from_run_to_pause)

        # Когда ожидаем, что сообщение НЕ должно пройти фильтрацию
        if not data_test_geozone_from_run_to_pause['expected_result']:
            assert not messages

        # Когда ожидаем, что сообщение должно пройти фильтрацию
        else:

            with allure.step("Формирование ожидаемого результата"):
                expected_data = transform_for_domain_filter_geozone(
                    test_data=test_data, out_type=data_test_geozone_from_run_to_pause['out_type']
                )
                allure.attach(json.dumps(expected_data, indent=4, default=str), f"expected_data.txt",
                              allure.attachment_type.TEXT)

            assert messages, "Целевое сообщение не найдено"
            assert len(messages) == 1, "Обнаружен дубль"

            read_message = messages[0]

            # JSON / THRIFT
            if isinstance(expected_data, dict):
                compare_values(
                    expected=expected_data,
                    actual=read_message,
                    ignore_params=['region', 'kafkaTimestamp']
                )

            # CSV
            if isinstance(expected_data, str):
                assert expected_data.encode('utf-8') == read_message

    @allure.story("Geozone: Statuses (from pause to run)")
    def test_shrinker_domain_from_pause_to_run(self, data_test_geozone_from_pause_to_run):

        with allure.step("Исходные параметры для теста"):
            allure.attach(json.dumps(
                data_test_geozone_from_pause_to_run, indent=4, default=str), f"test_data.txt", allure.attachment_type.TEXT
            )

        """
        Предварительные шаги
        """
        # Создание фильтра
        test_filter = data_test_geozone_from_pause_to_run["pre-create"]

        response = filter_controller.post_filter(
            token=State.datariver_token,
            data=test_filter
        )

        domain_filter = response.json()
        if response.status_code == 201:
            State.domain_filters.append(domain_filter["id"])

        assert 201 == response.status_code

        time.sleep(config.SHRINKER_DOMAIN_WAITER)

        # Изменение статуса фильтра
        response = filter_controller.patch_filter_by_id(
            token=State.datariver_token,
            id=domain_filter["id"],
            status="PAUSE"
        )
        assert 200 == response.status_code

        time.sleep(config.SHRINKER_DOMAIN_WAITER)

        # Изменение статуса фильтра
        response = filter_controller.patch_filter_by_id(
            token=State.datariver_token,
            id=domain_filter["id"],
            status="RUN"
        )
        assert 200 == response.status_code

        time.sleep(config.SHRINKER_DOMAIN_WAITER)

        # Основная логика теста
        test_data, messages = body_for_test(data_test_geozone_from_pause_to_run)

        # Когда ожидаем, что сообщение НЕ должно пройти фильтрацию
        if not data_test_geozone_from_pause_to_run['expected_result']:
            assert not messages

        # Когда ожидаем, что сообщение должно пройти фильтрацию
        else:

            with allure.step("Формирование ожидаемого результата"):
                expected_data = transform_for_domain_filter_geozone(
                    test_data=test_data, out_type=data_test_geozone_from_pause_to_run['out_type']
                )
                allure.attach(json.dumps(expected_data, indent=4, default=str), f"expected_data.txt",
                              allure.attachment_type.TEXT)

            assert messages, "Целевое сообщение не найдено"
            assert len(messages) == 1, "Обнаружен дубль"

            read_message = messages[0]

            # JSON / THRIFT
            if isinstance(expected_data, dict):
                compare_values(
                    expected=expected_data,
                    actual=read_message,
                    ignore_params=['region', 'kafkaTimestamp']
                )

            # CSV
            if isinstance(expected_data, str):
                assert expected_data.encode('utf-8') == read_message

    @allure.story("Geozone: Statuses (stop by time)")
    def test_shrinker_domain_to_stop_by_time(self, data_test_geozone_to_stop_by_time):

        with allure.step("Исходные параметры для теста"):
            allure.attach(json.dumps(
                data_test_geozone_to_stop_by_time, indent=4, default=str), f"test_data.txt", allure.attachment_type.TEXT
            )

        """
        Предварительные шаги
        """
        # Создание фильтра
        test_filter = data_test_geozone_to_stop_by_time["pre-create"]

        response = filter_controller.post_filter(
            token=State.datariver_token,
            data=test_filter
        )

        domain_filter = response.json()
        if response.status_code == 201:
            State.domain_filters.append(domain_filter["id"])

        assert 201 == response.status_code

        time.sleep(config.SHRINKER_DOMAIN_WAITER + 105)

        # Основная логика теста
        test_data, messages = body_for_test(data_test_geozone_to_stop_by_time)

        # Когда ожидаем, что сообщение НЕ должно пройти фильтрацию
        if not data_test_geozone_to_stop_by_time['expected_result']:
            assert not messages

        # Когда ожидаем, что сообщение должно пройти фильтрацию
        else:

            with allure.step("Формирование ожидаемого результата"):
                expected_data = transform_for_domain_filter_geozone(
                    test_data=test_data, out_type=data_test_geozone_to_stop_by_time['out_type']
                )
                allure.attach(json.dumps(expected_data, indent=4, default=str), f"expected_data.txt",
                              allure.attachment_type.TEXT)

            assert messages, "Целевое сообщение не найдено"
            assert len(messages) == 1, "Обнаружен дубль"

            read_message = messages[0]

            # JSON / THRIFT
            if isinstance(expected_data, dict):
                compare_values(
                    expected=expected_data,
                    actual=read_message,
                    ignore_params=['region', 'kafkaTimestamp']
                )

            # CSV
            if isinstance(expected_data, str):
                assert expected_data.encode('utf-8') == read_message

    @allure.story("Geozone: Statuses (run by time)")
    def test_shrinker_domain_to_run_by_time(self, data_test_geozone_to_run_by_time):

        with allure.step("Исходные параметры для теста"):
            allure.attach(json.dumps(
                data_test_geozone_to_run_by_time, indent=4, default=str), f"test_data.txt", allure.attachment_type.TEXT
            )

        """
        Предварительные шаги
        """
        # Создание фильтра
        test_filter = data_test_geozone_to_run_by_time["pre-create"]

        response = filter_controller.post_filter(
            token=State.datariver_token,
            data=test_filter
        )

        domain_filter = response.json()
        if response.status_code == 201:
            State.domain_filters.append(domain_filter["id"])

        assert 201 == response.status_code

        time.sleep(config.SHRINKER_DOMAIN_WAITER + 105)

        # Основная логика теста
        test_data, messages = body_for_test(data_test_geozone_to_run_by_time)

        # Когда ожидаем, что сообщение НЕ должно пройти фильтрацию
        if not data_test_geozone_to_run_by_time['expected_result']:
            assert not messages

        # Когда ожидаем, что сообщение должно пройти фильтрацию
        else:

            with allure.step("Формирование ожидаемого результата"):
                expected_data = transform_for_domain_filter_geozone(
                    test_data=test_data, out_type=data_test_geozone_to_run_by_time['out_type']
                )
                allure.attach(json.dumps(expected_data, indent=4, default=str), f"expected_data.txt",
                              allure.attachment_type.TEXT)

            assert messages, "Целевое сообщение не найдено"
            assert len(messages) == 1, "Обнаружен дубль"

            read_message = messages[0]

            # JSON / THRIFT
            if isinstance(expected_data, dict):
                compare_values(
                    expected=expected_data,
                    actual=read_message,
                    ignore_params=['region', 'kafkaTimestamp']
                )

            # CSV
            if isinstance(expected_data, str):
                assert expected_data.encode('utf-8') == read_message

    @allure.story("Geozone: Statuses (from stop to run)")
    def test_shrinker_domain_from_stop_to_run(self, data_test_geozone_from_stop_to_run):

        with allure.step("Исходные параметры для теста"):
            allure.attach(json.dumps(
                data_test_geozone_from_stop_to_run, indent=4, default=str), f"test_data.txt", allure.attachment_type.TEXT
            )

        """
        Предварительные шаги
        """
        # Создание фильтра
        test_filter = data_test_geozone_from_stop_to_run["pre-create"]

        response = filter_controller.post_filter(
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
        response_put = filter_controller.put_filter_by_list(
            token=State.datariver_token,
            data=test_filter  # передаем тест данные
        )

        assert 200 == response_put.status_code

        time.sleep(config.SHRINKER_DOMAIN_WAITER)

        # Основная логика теста
        test_data, messages = body_for_test(data_test_geozone_from_stop_to_run)

        # Когда ожидаем, что сообщение НЕ должно пройти фильтрацию
        if not data_test_geozone_from_stop_to_run['expected_result']:
            assert not messages

        # Когда ожидаем, что сообщение должно пройти фильтрацию
        else:

            with allure.step("Формирование ожидаемого результата"):
                expected_data = transform_for_domain_filter_geozone(
                    test_data=test_data, out_type=data_test_geozone_from_stop_to_run['out_type']
                )
                allure.attach(json.dumps(expected_data, indent=4, default=str), f"expected_data.txt",
                              allure.attachment_type.TEXT)

            assert messages, "Целевое сообщение не найдено"
            assert len(messages) == 1, "Обнаружен дубль"

            read_message = messages[0]

            # JSON / THRIFT
            if isinstance(expected_data, dict):
                compare_values(
                    expected=expected_data,
                    actual=read_message,
                    ignore_params=['region', 'kafkaTimestamp']
                )

            # CSV
            if isinstance(expected_data, str):
                assert expected_data.encode('utf-8') == read_message
