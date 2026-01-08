import allure
import datetime
import random
import simplejson as json

import config
from common.bindings.kafka.encoders import encode_thrift
from common.bindings.kafka.kafka_class_confluent import Kafka
from common.models.dataclasses.TCiscoModel import TCiscoModel as TCiscoModelDataclass
from common.models.dataclasses.TCiscoModel import t_url_generator
from common.models.thrift.TCiscoModel.ttypes import TCiscoModel as TCiscoModelThrift
from common.utils import format_utils
from common.utils import random_utils
from common.utils.shrinker.shrinker_backend import filter_controller


def body_for_test(source_data):
    source_broker = source_data['kafka']['source_broker']
    source_topic = source_data['kafka']['source_topic']

    target_topic = source_data['kafka']['target_topic']

    with allure.step("Формирование тестовых данных"):
        test_data = TCiscoModelDataclass(
            imsi=source_data['test_data'].get("imsi"),
            msisdn=source_data['test_data'].get("msisdn"),
            host=source_data['test_data'].get("host"),
            timestamp=source_data['test_data'].get("timestamp"),
            region=source_data['test_data'].get("region"),
            protocol=source_data['test_data'].get("protocol"),
            lac=source_data['test_data'].get("lac"),
            cell=source_data['test_data'].get("cell"),
            imei=source_data['test_data'].get("imei"),
            networkGeneration=source_data['test_data'].get("networkGeneration"),
            kafkaTimestamp=source_data['test_data'].get("kafkaTimestamp"),
            duration=source_data['test_data'].get("duration"),
            downloadDataVol=source_data['test_data'].get("downloadDataVol"),
            uploadDataVol=source_data['test_data'].get("uploadDataVol"),
            ip=source_data['test_data'].get("ip"),
        )

        test_data = format_utils.dataclass_to_dict(test_data)
        allure.attach(json.dumps(
            test_data, indent=4, default=str), f"built_test_data.txt", allure.attachment_type.TEXT
        )

    with allure.step("Сериализация тестовых данных"):
        encoded_test_data = encode_thrift(data=test_data, model=TCiscoModelThrift)
        allure.attach(encoded_test_data, f"encoded_test_data.txt", allure.attachment_type.TEXT)

    decoding_model = None
    if source_data["out_type"] == "THRIFT":
        decoding_model = TCiscoModelThrift

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


def transform_for_domain_filter_url(test_data: dict = None, out_type: str = "JSON") -> dict | str:

    result = dict()

    result["topicProtocol"] = test_data['protocol']
    if "cisco_flow_" in result["topicProtocol"] or "huawei_" in result["topicProtocol"]:
        test_data['protocol'] = "https"
    else:
        test_data['protocol'] = "http"

    if out_type == "THRIFT":
        return test_data

    elif out_type == "CSV":
        csv_string = (f"{test_data['imsi']},{test_data['msisdn']},{test_data['imei']},{test_data['host']},"
                      f"{test_data['region']},{test_data['protocol']},{test_data['timestamp']},"
                      f"{result['topicProtocol']},{test_data['lac']},{test_data['cell']}")
        return csv_string

    result["imsi"] = test_data["imsi"]
    result["msisdn"] = test_data["msisdn"]
    result["host"] = test_data["host"]
    result["imei"] = test_data["imei"]
    result["lac"] = test_data["lac"]
    result["cell"] = test_data["cell"]
    result["timestamp"] = test_data["timestamp"]
    result["region"] = test_data["region"]
    result["durationSec"] = test_data["duration"]
    result["downloadDataVolBytes"] = test_data["downloadDataVol"]
    result["uploadDataVolBytes"] = test_data["uploadDataVol"]
    result["ip"] = test_data["ip"]
    result["protocol"] = test_data['protocol']
    return result


def test_data_generator(
        source_topic: str = None,
        source_broker: str = None,
        target_topic: str = None,
        target_broker: str = None,
        description: str = None,
        out_type: str = None,
        filter_protocols: list = None,
        filter_range: list = None,
        filter_regions: list = None,
        filter_urls: list = None,
        msisdn: int = None,
        host: str = None,
        imsi: int = None,
        timestamp: int = None,
        region: str = None,
        protocol: str = None,
        duration: int = None,
        imei: int = None,
        network_generation: int = None,
        kafka_timestamp: int = None,
        lac: int = None,
        cell: int = None,
        download_data_vol: int = None,
        upload_data_vol: int = None,
        ip: str = None,
        expected_result: bool = None
):

    source_topic = source_topic if source_topic is not None else "test1_dr_shrinker-engine_clickstream"
    source_broker = source_broker if source_broker is not None else config.KAFKA_PREP1
    target_topic = target_topic if target_topic is not None else "test1_dr_shrinker-domain_out_url"
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
        "pre-create": filter_controller.create_default_url_filter(
            record_format=out_type,
            topic=target_topic,
            regions=filter_regions,
            range=filter_range,
            protocols=filter_protocols,
            urls=filter_urls
        ),
        "test_data": t_url_generator(
            imsi=imsi,
            msisdn=msisdn,
            host=host,
            timestamp=timestamp,
            region=region,
            protocol=protocol,
            lac=lac,
            cell=cell,
            imei=imei,
            network_generation=network_generation,
            kafka_timestamp=kafka_timestamp,
            duration=duration,
            download_data_vol=download_data_vol,
            upload_data_vol=upload_data_vol,
            ip=ip,
        ),
        "expected_result": expected_result
    }

    return result
