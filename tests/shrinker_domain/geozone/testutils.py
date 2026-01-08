import json
import logging
import pprint

import allure
import config

from common.bindings.kafka.kafka_class_confluent import Kafka
from common.models.dataclasses.TGeofenceEvent import t_geofence_event_generator
from common.models.thrift.TGeozoneModel.ttypes import TGeozoneModel as TGeozoneModelThrift
from common.utils.shrinker.shrinker_backend import filter_controller


def body_for_test(source_data):
    logger = logging.getLogger('geozone_body_for_test')
    logger.setLevel(logging.INFO)

    source_broker = source_data['kafka']['source_broker']
    source_topic = source_data['kafka']['source_topic']

    target_topic = source_data['kafka']['target_topic']

    with allure.step("Формирование тестовых данных"):
        test_data = source_data["test_data"]

        logger.info(f"{test_data}")

    with allure.step("Сериализация тестовых данных"):
        encoded_test_data = json.dumps(test_data).encode('utf-8')
        allure.attach(encoded_test_data, f"encoded_test_data.txt", allure.attachment_type.TEXT)

    decoding_model = None
    if source_data["out_type"] == "THRIFT":
        decoding_model = TGeozoneModelThrift

    with allure.step("Отправка сообщения в Кафку и поиск сообщений в выходном топике"):
        with Kafka(bootstrap_servers=source_broker) as kafka:
            kafka.create_producer()
            kafka.create_consumer(
                topic=target_topic
            )

            messages = kafka.find_message_by_key(
                message_key=source_data['test_data'].get("imsi"),
                search_depth=1000,
                return_all=True,
                max_wait_time=120, #todo: вернуть 120
                timeout_sec=2,
                decoding_model=decoding_model,
                send_function=kafka.send_message,
                message=encoded_test_data,
                topic=source_topic
            )

        allure.attach(json.dumps(messages, indent=4, default=str), f"message_from_kafka.txt",
                          allure.attachment_type.TEXT)

    return test_data, messages


def transform_for_domain_filter_geozone(
        test_data: dict = None,
        out_type: str = "JSON") -> dict | str:

    if out_type == "THRIFT":
        result = dict()
        result["imsi"] = int(test_data["imsi"])
        result["msisdn"] = int(test_data["msisdn"])
        result["eventType"] = test_data["eventType"]
        result["geofenceType"] = test_data["geofenceType"]
        result["zoneId"] = test_data["zoneId"]
        result["timestamp"] = test_data["timestamp"]
        result["protocol"] = 'geozone'  # по-умолчанию
        return result

    if out_type == "CSV":
        csv_string = (f"{test_data['imsi']},"
                      f"{test_data['msisdn']},"
                      f"{test_data['eventType']},"
                      f"{test_data['geofenceType']},"
                      f"{test_data['zoneId']},"
                      f"{test_data['timestamp']},geozone")
        return csv_string

    result = dict()
    result["imsi"] = int(test_data["imsi"])
    result["msisdn"] = int(test_data["msisdn"])
    result["eventType"] = test_data["eventType"]
    result["geofenceType"] = test_data["geofenceType"]
    result["zoneId"] = test_data["zoneId"]
    result["timestamp"] = test_data["timestamp"]
    result["topicProtocol"] = 'geozone' #по-умолчанию
    return result

def test_data_generator_geozone_base(
        source_topic: str = None,
        source_broker: str = None,
        target_topic: str = None,
        target_broker: str = None,
        description: str = None,
        out_type: str = None,
        expected_result: bool = None,

        #параметры для доменного потока
        filter_range: list = None,
        filter_regions: list = None,
        inner_filter_type: list = None,
        file_content_event_type: list = None,
        file_content_geofence_type: list = None,
        file_content_zone_id: list = None,

        #параметры для сообщения
        m_timestamp: int = None,
        m_kafka_insert_time: int = None,
        m_system_id: str = None,
        m_zone_id: str = None,
        m_imsi: str = None,
        m_msisdn: str = None,
        m_event_type: str = None,
        m_geofence_type: str = None,
        m_author_role: str = None,
):

    source_topic = source_topic if source_topic is not None else "test1_dr_customer-geo_events"
    source_broker = source_broker if source_broker is not None else config.KAFKA_PREP1
    target_topic = target_topic if target_topic is not None else "test1_dr_shrinker-domain_out_geozone"
    target_broker = target_broker if target_broker is not None else config.KAFKA_PREP1


    result = {
        "description": description,
        "kafka": {
            "source_topic": source_topic,
            "source_broker": source_broker,
            "target_topic": target_topic,
            "target_broker": target_broker,
        },
        "out_type": out_type,
        "pre-create": filter_controller.create_default_geozone_filter(
            record_format=out_type,
            topic=target_topic,
            regions=filter_regions,
            range=filter_range,
            inner_filter_type=inner_filter_type,
            file_content_event_type=file_content_event_type,
            file_content_geofence_type=file_content_geofence_type,
            file_content_zone_id=file_content_zone_id,
        ),
        "test_data": t_geofence_event_generator(
            timestamp=m_timestamp,
            kafka_insert_time=m_kafka_insert_time,
            system_id=m_system_id,
            imsi=m_imsi,
            msisdn=m_msisdn,
            event_type=m_event_type,
            geofence_type=m_geofence_type,
            zone_id=m_zone_id,
            author_role=m_author_role
        ),
        "expected_result": expected_result
    }

    return result
