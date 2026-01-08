import allure
import simplejson as json

import config
from common.bindings.kafka.encoders import encode_thrift
from common.bindings.kafka.kafka_class_confluent import Kafka
from common.models.dataclasses.TVlrModel import t_vlr_generator
from common.models.thrift.TVlrModel.ttypes import TVlrModel as TVlrModelThrift
from common.utils.shrinker.shrinker_backend import filter_controller


def body_for_test(source_data):
    source_broker = source_data['kafka']['source_broker']
    source_topic = source_data['kafka']['source_topic']

    target_topic = source_data['kafka']['target_topic']

    with allure.step("Формирование тестовых данных"):
        test_data = source_data["test_data"]
        allure.attach(json.dumps(
            test_data, indent=4, default=str), f"built_test_data.txt", allure.attachment_type.TEXT
        )

    with allure.step("Сериализация тестовых данных"):
        encoded_test_data = encode_thrift(data=test_data, model=TVlrModelThrift)
        allure.attach(encoded_test_data, f"encoded_test_data.txt", allure.attachment_type.TEXT)

    decoding_model = None
    if source_data["out_type"] == "THRIFT":
        decoding_model = TVlrModelThrift

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
                max_wait_time=120,
                timeout_sec=2,
                decoding_model=decoding_model,
                send_function=kafka.send_message,
                message=encoded_test_data,
                topic=source_topic
            )

        allure.attach(json.dumps(messages, indent=4, default=str), f"message_from_kafka.txt",
                      allure.attachment_type.TEXT)

    return test_data, messages


def transform_for_domain_filter_roaming(test_data: dict = None, out_type: str = "JSON") -> dict | str:

    if out_type == "THRIFT":
        return test_data

    elif out_type == "CSV":
        csv_string = (f"{test_data['imsi']},{test_data['msisdn']},{test_data['imei']},"
                      f"{test_data['vlr']},{test_data['callType']},{test_data['region']},"
                      f"{test_data['timestamp']}")
        return csv_string

    result = dict()
    result["imsi"] = test_data["imsi"]
    result["msisdn"] = test_data["msisdn"]
    result["vlr"] = test_data["vlr"]
    result["imei"] = test_data["imei"]
    result["callType"] = test_data["callType"]
    result["originatingTid"] = test_data["originatingTid"]
    result["timestamp"] = test_data["timestamp"]
    result["region"] = test_data["region"]
    result["topicProtocol"] = test_data["protocol"]
    return result


def test_data_generator(
        source_topic: str = None,
        source_broker: str = None,
        target_topic: str = None,
        target_broker: str = None,
        description: str = None,
        out_type: str = None,
        filter_range: list = None,
        filter_regions: list = None,
        filter_vlrs: list = None,
        msisdn: int = None,
        vlr: int = None,
        call_type: int = None,
        imsi: int = None,
        timestamp: int = None,
        originating_tid: int = None,
        region: str = None,
        protocol: str = None,
        imei: int = None,
        network_generation: int = None,
        expected_result: bool = None
):

    source_topic = source_topic if source_topic is not None else "test1_dr_shrinker-engine_roaming"
    source_broker = source_broker if source_broker is not None else config.KAFKA_PREP1
    target_topic = target_topic if target_topic is not None else "test1_dr_shrinker-domain_out_roaming"
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
        "pre-create": filter_controller.create_default_roaming_filter(
            record_format=out_type,
            topic=target_topic,
            vlrs=filter_vlrs,
            regions=filter_regions,
            range=filter_range
        ),
        "test_data": t_vlr_generator(
            imsi=imsi,
            msisdn=msisdn,
            vlr=vlr,
            call_type=call_type,
            timestamp=timestamp,
            originating_tid=originating_tid,
            region=region,
            protocol=protocol,
            imei=imei,
            network_generation=network_generation,
        ),
        "expected_result": expected_result
    }

    return result
