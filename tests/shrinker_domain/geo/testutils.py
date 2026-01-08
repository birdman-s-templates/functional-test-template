import allure
import random
import simplejson as json

import config
from common.bindings.kafka.encoders import encode_thrift
from common.bindings.kafka.kafka_class_confluent import Kafka
from common.models.dataclasses.TGeoModel import t_geo_generator
from common.models.thrift.TGeoModel.ttypes import TGeoModel as TGeoModelThrift
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
        encoded_test_data = encode_thrift(data=test_data, model=TGeoModelThrift)
        allure.attach(encoded_test_data, f"encoded_test_data.txt", allure.attachment_type.TEXT)

    decoding_model = None
    if source_data["out_type"] == "THRIFT":
        decoding_model = TGeoModelThrift

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


def transform_for_domain_filter_geo(test_data: dict = None, out_type: str = "JSON") -> dict | str:

    if out_type == "THRIFT":
        return test_data

    elif out_type == "CSV":
        csv_string = (f"{test_data['imsi']},{test_data['msisdn']},{test_data['imei']},{test_data['lac']},"
                      f"{test_data['cell']},{test_data['region']},{test_data['networkGeneration'].lower()},"
                      f"{test_data['timestamp']},{test_data['protocol']},{test_data['tz']}")
        return csv_string

    result = dict()
    result["imsi"] = test_data["imsi"]
    result["imei"] = test_data["imei"]
    result["msisdn"] = test_data["msisdn"]
    result["lac"] = test_data["lac"]
    result["cell"] = test_data["cell"]
    result["region"] = test_data["region"]
    result["topicProtocol"] = test_data["protocol"]
    result["generation"] = str(test_data["networkGeneration"]).lower()
    result["timestamp"] = test_data["timestamp"]
    result["msisdnRaw"] = test_data["msisdnRaw"]
    result['tz'] = test_data["tz"]
    return result


def test_data_generator(
        source_topic: str = None,
        source_broker: str = None,
        target_topic: str = None,
        target_broker: str = None,
        description: str = None,
        out_type: str = None,
        filter_event_type: list = None,
        filter_range: list = None,
        filter_regions: list = None,
        file_content: list = None,
        msisdn: int = None,
        imsi: int = None,
        timestamp: int = None,
        region: str = None,
        protocol: str = None,
        imei: int = None,
        network_generation: int = None,
        kafka_timestamp: int = None,
        lac: int = None,
        cell: int = None,
        expected_result: bool = None
):

    source_topic = source_topic if source_topic is not None else "test1_dr_shrinker-engine_geo"
    source_broker = source_broker if source_broker is not None else config.KAFKA_PREP1
    target_topic = target_topic if target_topic is not None else "test1_dr_shrinker-domain_out_geo"
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
        "pre-create": filter_controller.create_default_geo_filter(
            record_format=out_type,
            topic=target_topic,
            event_type=filter_event_type,
            regions=filter_regions,
            range=filter_range,
            file_content=file_content
        ),
        "test_data": t_geo_generator(
            imsi=imsi,
            msisdn=msisdn,
            lac=lac,
            cell=cell,
            timestamp=timestamp,
            region=region,
            protocol=protocol,
            imei=imei,
            network_generation=network_generation,
            kafka_timestamp=kafka_timestamp,
            msisdn_raw=msisdn
        ),
        "expected_result": expected_result
    }

    return result


def generate_lac_cell_list():
    return [f"{random.randint(1, 10000)}:{random.randint(1, 10000)}" for _ in range(5)]


def extract_lac(lac_cell: str) -> int:
    return int(lac_cell.split(":")[0])


def extract_cell(lac_cell: str) -> int:
    return int(lac_cell.split(":")[1])
