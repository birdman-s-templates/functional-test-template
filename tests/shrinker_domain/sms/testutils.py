import allure
import simplejson as json

import config
from common.bindings.kafka.encoders import encode_thrift
from common.bindings.kafka.kafka_class_confluent import Kafka
from common.models.dataclasses.TSmsModel import t_sms_generator
from common.models.thrift.TSmsModel.ttypes import TSmsModel as TSmsModelThrift
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
        encoded_test_data = encode_thrift(data=test_data, model=TSmsModelThrift)
        allure.attach(encoded_test_data, f"encoded_test_data.txt", allure.attachment_type.TEXT)

    decoding_model = None
    if source_data["out_type"] == "THRIFT":
        decoding_model = TSmsModelThrift

    with allure.step("Отправка сообщения в Кафку и поиск сообщений в выходном топике"):
        with Kafka(bootstrap_servers=source_broker) as kafka:
            kafka.create_producer()
            kafka.create_consumer(
                topic=target_topic
            )

            messages = kafka.find_message_by_key(
                message_key=source_data['test_data'].get("imsiIn"),
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


def transform_for_domain_filter_sms(test_data: dict = None, out_type: str = "JSON",
                                    filter_type: str = "IN") -> dict | str:

    if out_type == "THRIFT":
        return test_data

    elif out_type == "CSV":
        test_data['msisdn'] = test_data["msisdnOut"] if filter_type == "IN" else test_data["msisdnIn"]
        csv_string = (f"{test_data['imsiIn']},{test_data['msisdn']},{test_data['imei']},{test_data['msisdnIn']},"
                      f"{test_data['msisdnOut']},{str(test_data['type']).lower()},{test_data['region']},"
                      f"{test_data['protocol']},{test_data['starttime']}")
        return csv_string

    result = dict()
    result["imsi"] = test_data["imsiIn"]
    result["msisdnA"] = str(test_data["msisdnIn"])
    result["msisdnB"] = str(test_data["msisdnOut"])
    result["callType"] = str(test_data["type"]).lower()
    result["timestamp"] = test_data["starttime"]
    result["region"] = test_data["region"]
    result["topicProtocol"] = test_data["protocol"]
    result["startDttm"] = test_data["startDttm"]
    result["endDttm"] = test_data["endDttm"]
    result["imei"] = test_data["imei"]
    return result


def test_data_generator(
        source_topic: str = None,
        source_broker: str = None,
        target_topic: str = None,
        target_broker: str = None,
        description: str = None,
        out_type: str = None,
        filter_type: str = None,
        filter_event_type: list = None,
        filter_range: list = None,
        filter_regions: list = None,
        filter_msisdns: list = None,
        msisdn_in: str = None,
        msisdn_out: str = None,
        imsi: int = None,
        start_time: int = None,
        region: str = None,
        protocol: str = None,
        start_dttm: int = None,
        end_dttm: int = None,
        imei: int = None,
        kafka_timestamp: int = None,
        expected_result: bool = None
):
    if filter_type == "IN":
        filter = "incoming"
    else:
        filter = "outgoing"

    source_topic = source_topic if source_topic is not None else "test1_dr_shrinker-engine_sms"
    source_broker = source_broker if source_broker is not None else config.KAFKA_PREP1
    target_topic = target_topic if target_topic is not None else "test1_dr_shrinker-domain_out_sms"
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
        "filter_type": filter_type,
        "pre-create": filter_controller.create_default_sms_filter(
            record_format=out_type,
            topic=target_topic,
            filter_type=filter,
            msisdns=filter_msisdns,
            regions=filter_regions,
            range=filter_range
        ),
        "test_data": t_sms_generator(
            imsi=imsi,
            msisdn_in=msisdn_in,
            msisdn_out=msisdn_out,
            type=filter_type,
            start_time=start_time,
            region=region,
            protocol=protocol,
            start_dttm=start_dttm,
            end_dttm=end_dttm,
            imei=imei,
            kafka_timestamp=kafka_timestamp,
        ),
        "expected_result": expected_result
    }

    return result
