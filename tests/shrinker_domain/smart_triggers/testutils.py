import logging
import time
import pprint

import allure
import simplejson as json

import config
from common.bindings.databases.aerospike_base import AerospikeClient
from common.bindings.kafka.encoders import encode_thrift
from common.bindings.kafka.kafka_class_confluent import Kafka
from common.models.dataclasses.TCallModel import t_call_generator
from common.models.dataclasses.TGeofenceEvent import t_geofence_event_generator
from common.models.dataclasses.TGeozoneModel import TGeozoneModel
from common.models.thrift.TCallModel.ttypes import TCallModel as TCallModelThrift
from common.models.dataclasses.TCiscoModel import t_url_generator
from common.models.thrift.TCiscoModel.ttypes import TCiscoModel as TCiscoModelThrift
from common.models.dataclasses.TGeoModel import t_geo_generator
from common.models.thrift.TGeoModel.ttypes import TGeoModel as TGeoModelThrift
from common.models.dataclasses.TSmsModel import t_sms_generator
from common.models.thrift.TGeofenceEvent.ttypes import TGeofenceEvent
from common.models.thrift.TSmsModel.ttypes import TSmsModel as TSmsModelThrift
from common.models.dataclasses.TVlrModel import t_vlr_generator
from common.models.thrift.TVlrModel.ttypes import TVlrModel as TVlrModelThrift
from common.models.thrift.TSmTriggerModel.ttypes import TSmTriggerModel as TSmTriggerModelThrift
from common.utils import format_utils
from common.utils.shrinker.shrinker_backend import filter_controller


def body_for_test(source_data):

    source_broker = config.KAFKA_PREP1
    target_topic = source_data['kafka']['target_topic']

    with allure.step("Формирование тестовых данных"):
        for test_data in source_data['test_data']:

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
                test_data["source_topic"] = "test1_dr_customer-geo_events" # для геозон передаем json
                test_data["encoded_message"] = json.dumps(test_data['message']).encode('utf-8')

            else:
                raise ValueError(f"Нет известного типа потока: {test_data}")


        logger = logging.getLogger('body_for_test')
        logger.setLevel(logging.INFO)
        logger.info(f"{source_data['test_data']}")


    decoding_model = None
    if source_data["out_type"] == "THRIFT":
        decoding_model = TSmTriggerModelThrift

        #info: если значение geozone есть в значениях в filters то берем ее модель для декодирования
        if any("GEOZONE" in item.values() for item in source_data['pre-create']['filters']):
            decoding_model = TGeozoneModel

    with allure.step("Отправка сообщения в Кафку и поиск сообщений в выходном топике"):
        with Kafka(bootstrap_servers=source_broker) as kafka:
            kafka.create_producer()
            kafka.create_consumer(
                topic=target_topic
            )

            for i, test_data in enumerate(source_data['test_data']):

                if i == 1 and source_data.get("latency_after_first_message") is not None:
                    time.sleep(source_data["latency_after_first_message"])

                message = test_data["message"]
                source_topic = test_data["source_topic"]
                encoded_message = test_data["encoded_message"]

                if message.get("imsi") is not None:
                    message_key = message["imsi"]
                elif message.get("imsiIn") is not None:
                    message_key = message["imsiIn"]
                else:
                    raise KeyError(f"Нет ключа (imsi / imsiIn) в сообщении: {message}")

                if i != len(source_data['test_data']) - 1:
                    kafka.send_message(
                        topic=source_topic,
                        key=message_key,
                        message=encoded_message
                    )

                    with AerospikeClient() as aerospike:
                        records = aerospike.get_all_records(namespace="fct1", set="smartTrigger")
                        allure.attach(
                            json.dumps(records, indent=4, default=str), f"aerospike_records_{i+1}", allure.attachment_type.TEXT #записывает триггеры кроме последнего
                        )

                else:

                    messages = kafka.find_message_by_key(
                        message_key=message_key,
                        search_depth=1000,
                        return_all=True,
                        max_wait_time=120,
                        timeout_sec=2,
                        decoding_model=decoding_model,
                        send_function=kafka.send_message,
                        message=encoded_message,
                        topic=source_topic
                    )

            allure.attach(json.dumps(messages, indent=4, default=str), f"message_from_kafka.txt",
                          allure.attachment_type.TEXT)

    return source_data['test_data'], messages


def transform_for_domain_filter_smart_triggers(source_data: dict):

    if source_data["out_type"] == "THRIFT":

        result = dict()

        result["imsi"] = source_data["expected_imsi"]
        result["msisdn"] = source_data["expected_msisdn"]
        result["timestamp"] = source_data["expected_timestamp"]
        result["totalSum"] = source_data["expected_sum"]
        result["imei"] = source_data["expected_imei"]

        return result

    elif source_data["out_type"] == "CSV":
        csv_string = (f"{source_data['expected_imsi']},{source_data['expected_msisdn']},smart,"
                      f"{source_data['expected_timestamp']},{source_data['expected_sum']},{source_data['expected_imei']}")
        return csv_string

    elif source_data["out_type"] == "JSON":

        result = dict()

        result["imsi"] = source_data["expected_imsi"]
        result["msisdn"] = source_data["expected_msisdn"]
        result["eventType"] = "smart"
        result["topicProtocol"] = source_data["expected_protocol"]
        result["timestamp"] = source_data["expected_timestamp"]
        result["totalSum"] = source_data["expected_sum"]
        result["imei"] = source_data["expected_imei"]

        return result

    elif source_data["out_type"] == "JSON_RTM":

        result = dict()

        name = str(source_data["domain_filter_name"]).upper()
        description = str(source_data["domain_filter_description"]).upper()

        result["CONDITION_GROUP"] = f"{name}_DATARIVER_RTM_SMART_PREPROD"
        result["CAMPAIGN_ID"] = f"{name}"
        result["FLOW_TYPE"] = "DATARIVER_RTM_SMART_PREPROD"
        result["PARAM1"] = description
        result["PARAM2"] = ""
        result["PARAM3"] = ""
        result["PARAM3"] = ""
        result["EVENT_TYPE"] = "SMART"
        result["IMSI"] = source_data["expected_imsi"]
        result["MSISDN"] = source_data["expected_msisdn"]
        result["TOTAL_SUM"] = str(source_data["expected_sum"])
        result["IMEI"] = source_data["expected_imei"]
        result["TIMESTAMP"] = format_utils.unix_to_iso8601(
                    unixtime=source_data["expected_timestamp"]
                )
        result["TIMESTAMP_UNIX"] = source_data["expected_timestamp"]

        return result

    else:
        raise ValueError(f"Передан неизвестный выходной формат: {source_data['out_type']}")


def test_data_generator(
        target_topic: str = None,
        target_broker: str = None,

        is_call: bool = None,
        is_sms: bool = None,
        is_roaming: bool = None,
        is_geo: bool = None,
        is_clickstream: bool = None,
        is_geozone: bool = None,

        first_message: str = None,
        second_message: str = None,
        third_message: str = None,
        fourth_message: str = None,
        fifth_message: str = None,

        call_m_count: int = 1,
        sms_m_count: int = 1,
        roaming_m_count: int = 1,
        geo_m_count: int = 1,
        clickstream_m_count: int = 1,
        geozone_m_count: int = 1,

        trigger_message_type: str = None,

        description: str = None,

        filter_out_type: str = None,
        filter_range: list = None,
        filter_regions: list = None,
        filter_total_weight: int = None,
        filter_trigger_time: int = None,
        filter_is_smart_trigger: bool = None,

        call_f_event_type: list = None,
        call_f_inner_filter_type: str = None,
        call_f_msisdns: list = None,
        call_f_weight: int = None,
        call_f_ttl: int = None,
        call_m_imsi: int = None,
        call_m_imei: str = None,
        call_m_msisdn_in: int = None,
        call_m_msisdn_out: int = None,
        call_m_start_time: int = None,
        call_m_region: str = None,
        call_m_protocol: str = None,
        call_m_start_dttm: int = None,
        call_m_end_dttm: int = None,
        call_m_status: str = None,

        sms_f_inner_filter_type: str = None,
        sms_f_msisdns: list = None,
        sms_f_weight: int = None,
        sms_f_ttl: int = None,
        sms_m_imsi: int = None,
        sms_m_imei: str = None,
        sms_m_msisdn_in: str = None,
        sms_m_msisdn_out: str = None,
        sms_m_start_time: int = None,
        sms_m_region: str = None,
        sms_m_protocol: str = None,
        sms_m_start_dttm: int = None,
        sms_m_end_dttm: int = None,

        vlr_f_msisdns: list = None,
        vlr_f_weight: int = None,
        vlr_f_ttl: int = None,
        vlr_m_imsi: int = None,
        vlr_m_imei: str = None,
        vlr_m_vlr: int = None,
        vlr_m_msisdn: int = None,
        vlr_m_timestamp: int = None,
        vlr_m_region: str = None,
        vlr_m_protocol: str = None,

        geo_f_lac_cells: list = None,
        geo_f_weight: int = None,
        geo_f_ttl: int = None,
        geo_m_imsi: int = None,
        geo_m_imei: str = None,
        geo_m_msisdn: int = None,
        geo_m_lac: int = None,
        geo_m_cell: int = None,
        geo_m_timestamp: int = None,
        geo_m_region: str = None,
        geo_m_protocol: str = None,

        url_f_urls: list = None,
        url_f_protocols: list = None,
        url_f_weight: int = None,
        url_f_ttl: int = None,
        url_m_imsi: int = None,
        url_m_imei: str = None,
        url_m_msisdn: int = None,
        url_m_host: str = None,
        url_m_timestamp: int = None,
        url_m_region: str = None,
        url_m_protocol: str = None,

        #info: для домена Geozone
        geozone_f_weight: int = None,
        geozone_f_ttl: int = None,
        geozone_f_inner_filter_type: list = None,
        geozone_f_file_content_event_type: list = None,
        geozone_f_file_content_geofence_type: list = None,
        geozone_f_file_content_zone_id: list = None,
        geozone_m_event_type: str = None,
        geozone_m_geofence_type: str = None,
        geozone_m_zone_id: str = None,
        geozone_m_imsi: int = None,
        geozone_m_msisdn: int = None,
        geozone_m_timestamp: int = None,
        geozone_m_kafka_insert_time: int = None,
        geozone_m_system_id: str = None,
        geozone_m_author_role: str = None,

        latency_after_first_message: int = None,

        latency: int = None,
        is_trigger: int = None,

        expected_imsi: int = None,
        expected_msisdn: int = None,
        expected_sum: int = None,

        expected_result: bool = None,

        xfail: str = None
):

    """
    Генерирует тестовые данные для проверки работы фильтров и триггеров в системе обработки событий (звонки, SMS, роуминг, геоданные, кликстримы).
    Формирует структуру с сообщениями, настройками фильтров, ожидаемыми результатами и метаданными для интеграционных тестов.

    Основные настройки
    target_topic (str, optional): Название топика Kafka для отправки данных. По умолчанию: "test1_dr_shrinker-domain_out".
    target_broker (str, optional): Адрес брокера Kafka. По умолчанию: значение из config.KAFKA_PREP1.

    Типы генерируемых данных (флаги)
    is_call (bool): Генерировать данные о звонках.
    is_sms (bool): Генерировать данные о SMS.
    is_roaming (bool): Генерировать данные о роуминге.
    is_geo (bool): Генерировать геоданные.
    is_clickstream (bool): Генерировать кликстримы.

    Порядок сообщений
    first_message (str): Тип первого сообщения (значения: "call", "sms", "roaming", "geo", "clickstream").
    second_message (str): Тип второго сообщения.
    third_message (str): Тип третьего сообщения.
    fourth_message (str): Тип четвертого сообщения.
    fifth_message (str): Тип пятого сообщения.

    Количество сообщений
    call_m_count (int): Количество сообщений о звонках. По умолчанию: 1.
    sms_m_count (int): Количество SMS-сообщений. По умолчанию: 1.
    roaming_m_count (int): Количество сообщений о роуминге. По умолчанию: 1.
    geo_m_count (int): Количество геосообщений. По умолчанию: 1.
    clickstream_m_count (int): Количество кликстримов. По умолчанию: 1.

    Настройки триггера
    trigger_message_type (str): Тип сообщения, которое должно вызвать триггер (значения: "call", "sms", "roaming", "geo", "clickstream").

    Общие параметры фильтрации
    filter_out_type (str): Формат выходных данных фильтра (например, "THRIFT").Если указан "THRIFT", обязателен параметр expected_sum.
    filter_range (list): Временной диапазон фильтрации.
    filter_regions (list): Регионы для фильтрации.
    filter_total_weight (int): Общий вес фильтра.
    filter_trigger_time (int): Время срабатывания триггера (в секундах).
    filter_is_smart_trigger (bool): Использовать "умные" триггеры.

    Параметры фильтров для звонков (префикс call_f_*)
    call_f_event_type (list): Типы событий (например, ["INCOMING", "OUTGOING"]).
    call_f_inner_filter_type (str): Тип внутреннего фильтра ("incoming" или "outgoing").
    call_f_msisdns (list): Список номеров MSISDN для фильтрации.
    call_f_weight (int): Вес фильтра.
    call_f_ttl (int): Время жизни фильтра.

    Параметры сообщений о звонках (префикс call_m_*)
    Поля: imsi, imei, msisdn_in, msisdn_out, start_time, region, protocol, start_dttm, end_dttm, status.

    Аналогичные параметры для SMS, роуминга, геоданных, кликстримов
    Префиксы: sms_f_*, sms_m_*, vlr_f_*, vlr_m_*, geo_f_*, geo_m_*, url_f_*, url_m_*.

    Задержки и ожидания
    latency_after_first_message (int): Задержка после первого сообщения (в секундах). Для получения данных из
    Аэроспайка, задержка нужна, чтобы записи успели появиться.
    latency (int): Задержка для тестов интеграции с Аэроспайком.

    Ожидаемые результаты
    is_trigger (int): Ожидаемое состояние триггера (1 — сработал, 0 — нет).
    description (str): Пробрасывается для выходных сообщений формата JSON_RTM
    expected_imsi (int): Ожидаемый IMSI.
    expected_msisdn (int): Ожидаемый MSISDN.
    expected_sum (int): Ожидаемая сумма (обязателен при filter_out_type="THRIFT").
    expected_result (bool): Ожидаемый результат теста (True/False).

    Ошибки
    xfail (str): Ссылка на задачу в Jira, если тест помечен как ожидаемо падающий.

    Возвращаемое значение в виде словаря:

    description (str): Описание теста.
    kafka (dict): Настройки Kafka (target_topic, target_broker).
    out_type (str): Тип выходных данных фильтра.
    pre-create (dict): Сконфигурированный фильтр.
    test_data (list): Сгенерированные сообщения.
    domain_filter_name (str): Название фильтра.
    domain_filter_description (str): Описание фильтра.
    latency*, expected_*: Настройки задержек и ожидаемых результатов.
    xfail (dict): Информация об ожидаемо падающем тесте.
    """

    if filter_out_type == "THRIFT" and expected_sum is None:
        raise ValueError("При выходном формате THRIFT нужно передать параметр expected_sum")

    target_topic = target_topic if target_topic is not None else "test1_dr_shrinker-domain_out_st"
    target_broker = target_broker if target_broker is not None else config.KAFKA_PREP1

    filters = []

    message_call = None
    if is_call:

        # Фильтр
        filter = filter_controller.generate_filter(
            filter_type="CALLS",
            event_type=call_f_event_type,
            inner_filter_type=call_f_inner_filter_type,
            file_content=call_f_msisdns,
            weight=call_f_weight,
            ttl=call_f_ttl
        )
        filters.append(filter)

        if call_f_inner_filter_type == "incoming":
            call_m_inner_filter_type = "IN"
        else:
            call_m_inner_filter_type = "OUT"

        # Сообщение
        message_call = {
            "type": "call",
            "message": t_call_generator(
                imsi=call_m_imsi,
                msisdn_in=call_m_msisdn_in,
                msisdn_out=call_m_msisdn_out,
                imei=call_m_imei,
                type=call_m_inner_filter_type,
                start_time=call_m_start_time,
                region=call_m_region,
                protocol=call_m_protocol,
                start_dttm=call_m_start_dttm,
                end_dttm=call_m_end_dttm,
                status=call_m_status,
            )
        }

    message_sms = None
    if is_sms:

        # Фильтр
        filter = filter_controller.generate_filter(
            filter_type="SMS",
            inner_filter_type=sms_f_inner_filter_type,
            file_content=sms_f_msisdns,
            weight=sms_f_weight,
            ttl=sms_f_ttl
        )
        filters.append(filter)

        if sms_f_inner_filter_type == "incoming":
            sms_m_inner_filter_type = "IN"
        else:
            sms_m_inner_filter_type = "OUT"

        # Сообщение
        message_sms = {
            "type": "sms",
            "message": t_sms_generator(
                imsi=sms_m_imsi,
                msisdn_in=sms_m_msisdn_in,
                msisdn_out=sms_m_msisdn_out,
                imei=sms_m_imei,
                type=sms_m_inner_filter_type,
                start_time=sms_m_start_time,
                region=sms_m_region,
                protocol=sms_m_protocol,
                start_dttm=sms_m_start_dttm,
                end_dttm=sms_m_end_dttm,
            )
        }

    message_roaming = None
    if is_roaming:

        # Фильтр
        filter = filter_controller.generate_filter(
            filter_type="ROAMING",
            file_content=vlr_f_msisdns,
            weight=vlr_f_weight,
            ttl=vlr_f_ttl
        )
        filters.append(filter)

        # Сообщение
        message_roaming = {
            "type": "roaming",
            "message": t_vlr_generator(
                imsi=vlr_m_imsi,
                vlr=vlr_m_vlr,
                msisdn=vlr_m_msisdn,
                imei=vlr_m_imei,
                timestamp=vlr_m_timestamp,
                region=vlr_m_region,
                protocol=vlr_m_protocol,
            )
        }

    message_geo = None
    if is_geo:

        # Фильтр
        filter = filter_controller.generate_filter(
            filter_type="GEO",
            event_type=["Entry", "Standing"],
            file_content=geo_f_lac_cells,
            weight=geo_f_weight,
            ttl=geo_f_ttl
        )
        filters.append(filter)

        # Сообщение
        message_geo = {
            "type": "geo",
            "message": t_geo_generator(
                imsi=geo_m_imsi,
                msisdn=geo_m_msisdn,
                imei=geo_m_imei,
                lac=geo_m_lac,
                cell=geo_m_cell,
                timestamp=geo_m_timestamp,
                region=geo_m_region,
                protocol=geo_m_protocol,
                msisdn_raw=geo_m_msisdn
            )
        }

    message_clickstream = None
    if is_clickstream:

        # Фильтр
        filter = filter_controller.generate_filter(
            filter_type="URL_CISCO",
            protocol=url_f_protocols,
            file_content=url_f_urls,
            weight=url_f_weight,
            ttl=url_f_ttl
        )
        filters.append(filter)

        # Сообщение
        message_clickstream = {
            "type": "url",
            "message": t_url_generator(
                imsi=url_m_imsi,
                msisdn=url_m_msisdn,
                imei=url_m_imei,
                host=url_m_host,
                timestamp=url_m_timestamp,
                region=url_m_region,
                protocol=url_m_protocol,
            )
        }

    message_geozone = None
    if is_geozone:
        domain_filter = filter_controller.create_default_geozone_filter(
            inner_filter_type=geozone_f_inner_filter_type,
            file_content_event_type=geozone_f_file_content_event_type,
            file_content_geofence_type=geozone_f_file_content_geofence_type,
            file_content_zone_id=geozone_f_file_content_zone_id,
            weight=geozone_f_weight,
            ttl=geozone_f_ttl
        )
        # забираем сам фильтр - filters - по умолчанию генерится 1
        geozone_filter = domain_filter['filters'][0]
        # добавляем в список
        filters.append(geozone_filter)

        message_geozone = {
            "type": "geozone",
            "message": t_geofence_event_generator(
                imsi=geozone_m_imsi,
                msisdn=geozone_m_msisdn,
                timestamp=geozone_m_timestamp,
                kafka_insert_time=geozone_m_kafka_insert_time,
                system_id=geozone_m_system_id,
                author_role=geozone_m_author_role,
                event_type=geozone_m_event_type,
                geofence_type=geozone_m_geofence_type,
                zone_id=geozone_m_zone_id
            )
        }

    # Словарь для сопоставления значений параметров с переменными
    message_mapping = {
        "call": message_call,
        "sms": message_sms,
        "roaming": message_roaming,
        "geo": message_geo,
        "clickstream": message_clickstream,
        "geozone": message_geozone
    }

    # Собираем переданные параметры в порядке first -> fifth, исключая None
    message_params_order = [first_message, second_message, third_message, fourth_message, fifth_message]
    message_valid_params = [param for param in message_params_order if param is not None]

    # Собираем сообщения в нужном порядке
    messages = [message_mapping[param] for param in message_valid_params]

    # Если каких-то сообщений должно быть несколько, добавляем
    temp_messages = []
    for i, message in enumerate(messages):

        temp_messages.append(message)

        if call_m_count > 1:
            for _ in range(call_m_count-1):
                temp_messages.append(message)

        if sms_m_count > 1:
            for _ in range(sms_m_count - 1):
                temp_messages.append(message)

        if roaming_m_count > 1:
            for _ in range(roaming_m_count - 1):
                temp_messages.append(message)

        if geo_m_count > 1:
            for _ in range(geo_m_count - 1):
                temp_messages.append(message)

        if clickstream_m_count > 1:
            for _ in range(clickstream_m_count - 1):
                temp_messages.append(message)

        if geozone_m_count > 1:
            for _ in range(geozone_m_count - 1):
                temp_messages.append(message)

    messages = temp_messages

    # Определяем выходные протокол и временную метку
    if trigger_message_type is not None:

        if trigger_message_type == "call":
            expected_imei = message_call["message"]["imei"]
            expected_protocol = message_call["message"]["protocol"]
            expected_timestamp = message_call["message"]["starttime"]
        elif trigger_message_type == "sms":
            expected_imei = message_sms["message"]["imei"]
            expected_protocol = message_sms["message"]["protocol"]
            expected_timestamp = message_sms["message"]["starttime"]
        elif trigger_message_type == "roaming":
            expected_imei = message_roaming["message"]["imei"]
            expected_protocol = message_roaming["message"]["protocol"]
            expected_timestamp = message_roaming["message"]["timestamp"]
        elif trigger_message_type == "geo":
            expected_imei = message_geo["message"]["imei"]
            expected_protocol = message_geo["message"]["protocol"]
            expected_timestamp = message_geo["message"]["timestamp"]
        elif trigger_message_type == "clickstream":
            expected_imei = message_clickstream["message"]["imei"]
            expected_protocol = message_clickstream["message"]["protocol"]
            expected_timestamp = message_clickstream["message"]["timestamp"]

        elif trigger_message_type == "geozone":
            expected_imei = "" #тк при None будет в вых сообщение пустая строка
            expected_protocol = "geozone"
            expected_timestamp = message_geozone["message"]["timestamp"]

        else:
            raise ValueError(f"Передан неизвестный trigger_message_type: {trigger_message_type}")

    else:
        expected_imei = None
        expected_protocol = None
        expected_timestamp = None

    domain_filter = filter_controller.create_default_smart_triggers_filter(
        record_format=filter_out_type,
        out_topic=target_topic,
        regions=filter_regions,
        range=filter_range,
        filters=filters,
        total_weight=filter_total_weight,
        trigger_time=filter_trigger_time,
        is_smart_trigger=filter_is_smart_trigger,
    )

    result = {
        "description": description,
        "kafka": {
            "target_topic": target_topic,
            "target_broker": target_broker,
        },
        "out_type": filter_out_type,
        "pre-create": domain_filter,
        "test_data": messages,
        "domain_filter_name": domain_filter["name"],
        "domain_filter_description": domain_filter["description"],
        "latency_after_first_message": latency_after_first_message,
        "latency": latency,
        "is_trigger": is_trigger,
        "expected_imsi": expected_imsi,
        "expected_msisdn": expected_msisdn,
        "expected_imei": expected_imei,
        "expected_sum": expected_sum,
        "expected_protocol": expected_protocol,
        "expected_timestamp": expected_timestamp,
        "expected_result": expected_result,
    }

    if xfail is not None:
        result["xfail"] = {
            "jira": xfail
        }

    return result
