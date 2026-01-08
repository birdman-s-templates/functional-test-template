import allure
import datetime
import pytest
import random
import time
import simplejson as json

import config
from common.bindings.shrinker.shrinker_backend import filter_controller
from common.utils.state import State
from common.utils import tags
from tests.shrinker_domain.url.testutils import body_for_test, transform_for_domain_filter_url


@allure.epic("Shrinker")
@allure.suite("Shrinker")
@allure.feature("Shrinker Domain")
@tags.regression
@tags.author("Кащеев Денис")
@tags.test_type("Интеграционное тестирование")
@tags.priority("Средний")
@pytest.mark.usefixtures("kerberos_setup", "postgres_setup", "datariver_api_setup", "check_token")
class TestShrinkerDomainUrl:

    @tags.business_critical
    @tags.smoke
    @allure.story("URL: Content")
    def test_shrinker_domain_base(self, data_test_url_base):

        with allure.step("Исходные параметры для теста"):
            allure.attach(json.dumps(
                data_test_url_base, indent=4, default=str), f"test_data.txt", allure.attachment_type.TEXT,
            )

        # Предварительные шаги
        test_filter = data_test_url_base["pre-create"]

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
        test_data, messages = body_for_test(data_test_url_base)

        # Когда ожидаем, что сообщение НЕ должно пройти фильтрацию
        if not data_test_url_base['expected_result']:
            assert not messages

        # Когда ожидаем, что сообщение должно пройти фильтрацию
        else:

            with allure.step("Формирование ожидаемого результата"):
                expected_data = transform_for_domain_filter_url(
                    test_data=test_data, out_type=data_test_url_base['out_type']
                )
                allure.attach(json.dumps(expected_data, indent=4, default=str), f"expected_data.txt",
                              allure.attachment_type.TEXT)

            assert messages, "Целевое сообщение не найдено"
            assert len(messages) == 1, "Обнаружен дубль"

            read_message = messages[0]

            # JSON / THRIFT
            if isinstance(expected_data, dict):

                for field in expected_data.keys():

                    if field in ["kafkaTimestamp", "protocol"]:
                        continue

                    if data_test_url_base['out_type'] == 'THRIFT' and field in ['lac', 'cell']:
                        continue

                    assert expected_data[field] == read_message[field], f"Не совпадает значение в поле {field}"

            # CSV
            if isinstance(expected_data, str):
                assert expected_data.encode('utf-8') == read_message

    @allure.story("URL: Statuses (from run to pause)")
    def test_shrinker_domain_from_run_to_pause(self, data_test_urls_from_run_to_pause):

        with allure.step("Исходные параметры для теста"):
            allure.attach(json.dumps(
                data_test_urls_from_run_to_pause, indent=4, default=str), f"test_data.txt", allure.attachment_type.TEXT
            )

        """
        Предварительные шаги
        """
        # Создание фильтра
        test_filter = data_test_urls_from_run_to_pause["pre-create"]

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
        test_data, messages = body_for_test(data_test_urls_from_run_to_pause)

        # Когда ожидаем, что сообщение НЕ должно пройти фильтрацию
        if not data_test_urls_from_run_to_pause['expected_result']:
            assert not messages

        # Когда ожидаем, что сообщение должно пройти фильтрацию
        else:

            with allure.step("Формирование ожидаемого результата"):
                expected_data = transform_for_domain_filter_url(
                    test_data=test_data, out_type=data_test_urls_from_run_to_pause['out_type']
                )
                allure.attach(json.dumps(expected_data, indent=4, default=str), f"expected_data.txt",
                              allure.attachment_type.TEXT)

            assert messages, "Целевое сообщение не найдено"
            assert len(messages) == 1, "Обнаружен дубль"

            read_message = messages[0]

            # JSON / THRIFT
            if isinstance(expected_data, dict):
                for field in expected_data.keys():
                    if field in ["kafkaTimestamp", "protocol"]:
                        continue
                    assert expected_data[field] == read_message[field], f"Не совпадает значение в поле {field}"

            # CSV
            if isinstance(expected_data, str):
                assert expected_data.encode('utf-8') == read_message

    @allure.story("URL: Statuses (from pause to run)")
    def test_shrinker_domain_from_pause_to_run(self, data_test_urls_from_pause_to_run):

        with allure.step("Исходные параметры для теста"):
            allure.attach(json.dumps(
                data_test_urls_from_pause_to_run, indent=4, default=str), f"test_data.txt", allure.attachment_type.TEXT
            )

        """
        Предварительные шаги
        """
        # Создание фильтра
        test_filter = data_test_urls_from_pause_to_run["pre-create"]

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
        test_data, messages = body_for_test(data_test_urls_from_pause_to_run)

        # Когда ожидаем, что сообщение НЕ должно пройти фильтрацию
        if not data_test_urls_from_pause_to_run['expected_result']:
            assert not messages

        # Когда ожидаем, что сообщение должно пройти фильтрацию
        else:

            with allure.step("Формирование ожидаемого результата"):
                expected_data = transform_for_domain_filter_url(
                    test_data=test_data, out_type=data_test_urls_from_pause_to_run['out_type']
                )
                allure.attach(json.dumps(expected_data, indent=4, default=str), f"expected_data.txt",
                              allure.attachment_type.TEXT)

            assert messages, "Целевое сообщение не найдено"
            assert len(messages) == 1, "Обнаружен дубль"

            read_message = messages[0]

            # JSON / THRIFT
            if isinstance(expected_data, dict):
                for field in expected_data.keys():
                    if field in ["kafkaTimestamp", "protocol"]:
                        continue
                    assert expected_data[field] == read_message[field], f"Не совпадает значение в поле {field}"

            # CSV
            if isinstance(expected_data, str):
                assert expected_data.encode('utf-8') == read_message

    @allure.story("URL: Statuses (stop by time)")
    def test_shrinker_domain_to_stop_by_time(self, data_test_urls_to_stop_by_time):

        with allure.step("Исходные параметры для теста"):
            allure.attach(json.dumps(
                data_test_urls_to_stop_by_time, indent=4, default=str), f"test_data.txt", allure.attachment_type.TEXT
            )

        """
        Предварительные шаги
        """
        # Создание фильтра
        test_filter = data_test_urls_to_stop_by_time["pre-create"]

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
        test_data, messages = body_for_test(data_test_urls_to_stop_by_time)

        # Когда ожидаем, что сообщение НЕ должно пройти фильтрацию
        if not data_test_urls_to_stop_by_time['expected_result']:
            assert not messages

        # Когда ожидаем, что сообщение должно пройти фильтрацию
        else:

            with allure.step("Формирование ожидаемого результата"):
                expected_data = transform_for_domain_filter_url(
                    test_data=test_data, out_type=data_test_urls_to_stop_by_time['out_type']
                )
                allure.attach(json.dumps(expected_data, indent=4, default=str), f"expected_data.txt",
                              allure.attachment_type.TEXT)

            assert messages, "Целевое сообщение не найдено"
            assert len(messages) == 1, "Обнаружен дубль"

            read_message = messages[0]

            # JSON / THRIFT
            if isinstance(expected_data, dict):
                for field in expected_data.keys():
                    if field in ["kafkaTimestamp", "protocol"]:
                        continue
                    assert expected_data[field] == read_message[field], f"Не совпадает значение в поле {field}"

            # CSV
            if isinstance(expected_data, str):
                assert expected_data.encode('utf-8') == read_message

    @allure.story("URL: Statuses (run by time)")
    def test_shrinker_domain_to_run_by_time(self, data_test_urls_to_run_by_time):

        with allure.step("Исходные параметры для теста"):
            allure.attach(json.dumps(
                data_test_urls_to_run_by_time, indent=4, default=str), f"test_data.txt", allure.attachment_type.TEXT
            )

        """
        Предварительные шаги
        """
        # Создание фильтра
        test_filter = data_test_urls_to_run_by_time["pre-create"]

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
        test_data, messages = body_for_test(data_test_urls_to_run_by_time)

        # Когда ожидаем, что сообщение НЕ должно пройти фильтрацию
        if not data_test_urls_to_run_by_time['expected_result']:
            assert not messages

        # Когда ожидаем, что сообщение должно пройти фильтрацию
        else:

            with allure.step("Формирование ожидаемого результата"):
                expected_data = transform_for_domain_filter_url(
                    test_data=test_data, out_type=data_test_urls_to_run_by_time['out_type']
                )
                allure.attach(json.dumps(expected_data, indent=4, default=str), f"expected_data.txt",
                              allure.attachment_type.TEXT)

            assert messages, "Целевое сообщение не найдено"
            assert len(messages) == 1, "Обнаружен дубль"

            read_message = messages[0]

            # JSON / THRIFT
            if isinstance(expected_data, dict):
                for field in expected_data.keys():
                    if field in ["kafkaTimestamp", "protocol"]:
                        continue
                    assert expected_data[field] == read_message[field], f"Не совпадает значение в поле {field}"

            # CSV
            if isinstance(expected_data, str):
                assert expected_data.encode('utf-8') == read_message

    @allure.story("URL: Statuses (from stop to run)")
    def test_shrinker_domain_from_stop_to_run(self, data_test_urls_from_stop_to_run):

        with allure.step("Исходные параметры для теста"):
            allure.attach(json.dumps(
                data_test_urls_from_stop_to_run, indent=4, default=str), f"test_data.txt", allure.attachment_type.TEXT
            )

        """
        Предварительные шаги
        """
        # Создание фильтра
        test_filter = data_test_urls_from_stop_to_run["pre-create"]

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
        test_data, messages = body_for_test(data_test_urls_from_stop_to_run)

        # Когда ожидаем, что сообщение НЕ должно пройти фильтрацию
        if not data_test_urls_from_stop_to_run['expected_result']:
            assert not messages

        # Когда ожидаем, что сообщение должно пройти фильтрацию
        else:

            with allure.step("Формирование ожидаемого результата"):
                expected_data = transform_for_domain_filter_url(
                    test_data=test_data, out_type=data_test_urls_from_stop_to_run['out_type']
                )
                allure.attach(json.dumps(expected_data, indent=4, default=str), f"expected_data.txt",
                              allure.attachment_type.TEXT)

            assert messages, "Целевое сообщение не найдено"
            assert len(messages) == 1, "Обнаружен дубль"

            read_message = messages[0]

            # JSON / THRIFT
            if isinstance(expected_data, dict):
                for field in expected_data.keys():
                    if field in ["kafkaTimestamp", "protocol"]:
                        continue
                    assert expected_data[field] == read_message[field], f"Не совпадает значение в поле {field}"

            # CSV
            if isinstance(expected_data, str):
                assert expected_data.encode('utf-8') == read_message
