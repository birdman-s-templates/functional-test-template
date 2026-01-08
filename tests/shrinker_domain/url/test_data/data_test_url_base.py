import datetime
import pytz
import random
from faker import Faker

from common.utils import random_utils
from tests.shrinker_domain.url.testutils import test_data_generator

# - - - - - - - - - - - - - - - - - - - - - - - - - -
# константы

fake = Faker()

def generate_urls():
    return [random_utils.generate_random_url() for _ in range(5)]

urls_lists = [generate_urls() for _ in range(8)]

timezone = pytz.timezone("Europe/Moscow")
current_time = int((datetime.datetime.now(timezone) - datetime.timedelta(seconds=random.randint(90, 300))).timestamp())

# - - - - - - - - - - - - - - - - - - - - - - - - - -
# тестовые данные

test_data = [

    test_data_generator(
        description="Позитивная проверка (по точному совпадению): URL_CISCO, протоколы: all, JSON",
        out_type="JSON",
        filter_urls=urls_lists[0],
        host=random.choice(urls_lists[0]),
        expected_result=True
    ),

    test_data_generator(
        description="Позитивная проверка (по точному совпадению): URL_CISCO, протоколы: http, JSON",
        out_type="JSON",
        filter_urls=urls_lists[1],
        host=random.choice(urls_lists[1]),
        protocol=random.choice(["cisco_http_http_host", "cisco_http_dns_query_name"]),
        expected_result=True
    ),

    test_data_generator(
        description="Позитивная проверка (по точному совпадению): URL_CISCO, протоколы: https, JSON",
        out_type="JSON",
        filter_urls=urls_lists[2],
        host=random.choice(urls_lists[2]),
        protocol=random.choice([
            'huawei_host', 'huawei_https_host', 'huawei_quic_sni', 'huawei_tls_cname', 'huawei_tls_oname',
            'huawei_dns_request_host', "cisco_flow_sni", "cisco_flow_quic_sni", "cisco_flow_tls_cname"
        ]),
        expected_result=True
    ),

    test_data_generator(
        description="Позитивная проверка (по точному совпадению): URL_CISCO, протоколы: all, CSV",
        out_type="CSV",
        filter_urls=urls_lists[3],
        host=random.choice(urls_lists[3]),
        expected_result=True
    ),

    test_data_generator(
        description="Позитивная проверка (по точному совпадению): URL_CISCO, протоколы: all, THRIFT",
        out_type="THRIFT",
        filter_urls=urls_lists[4],
        host=random.choice(urls_lists[4]),
        expected_result=True
    ),

    test_data_generator(
        description="Негативная проверка (нерелевантный host): URL_CISCO, протоколы: all, JSON",
        out_type="JSON",
        filter_urls=urls_lists[5],
        host=random_utils.generate_random_url(),
        expected_result=False
    ),

    test_data_generator(
        description="Позитивная проверка (по более низкому домену): URL_CISCO, протоколы: all, JSON",
        out_type="JSON",
        filter_urls=urls_lists[6],
        host=fake.word() + "." + random.choice(urls_lists[6]),
        expected_result=True
    ),

    test_data_generator(
        description="Негативная проверка (отличается корневой домен): URL_CISCO, протоколы: all, JSON",
        out_type="JSON",
        filter_urls=urls_lists[7],
        host=".".join(random.choice(urls_lists[7]).split('.')[:-1]) + ".test",
        expected_result=False
    ),

]
