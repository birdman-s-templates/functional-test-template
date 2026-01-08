import datetime
import pytz
import random

from common.utils import random_utils
from tests.shrinker_domain.sms.testutils import test_data_generator

# - - - - - - - - - - - - - - - - - - - - - - - - - -
# константы

def generate_msisdns():
    return [random_utils.generate_phone() for _ in range(5)]

msisdns_lists = [generate_msisdns() for _ in range(8)]

timezone = pytz.timezone("Europe/Moscow")
current_time = int((datetime.datetime.now(timezone) - datetime.timedelta(seconds=random.randint(90, 300))).timestamp())

# - - - - - - - - - - - - - - - - - - - - - - - - - -
# тестовые данные

test_data = [

    test_data_generator(
        description="Позитивная проверка: SMS (in), JSON",
        out_type="JSON",
        filter_type="IN",
        filter_msisdns=list(map(str, msisdns_lists[0])),
        msisdn_in=random.choice(msisdns_lists[0]),
        msisdn_out=random_utils.generate_phone(),
        expected_result=True
    ),

    test_data_generator(
        description="Позитивная проверка: SMS (out), JSON",
        out_type="JSON",
        filter_type="OUT",
        filter_msisdns=list(map(str, msisdns_lists[1])),
        msisdn_in=random_utils.generate_phone(),
        msisdn_out=random.choice(msisdns_lists[1]),
        expected_result=True
    ),

    test_data_generator(
        description="Негативная проверка (фильтр по другому номеру): SMS (in), JSON",
        out_type="JSON",
        filter_type="IN",
        filter_msisdns=list(map(str, msisdns_lists[2])),
        msisdn_in=random_utils.generate_phone(),
        msisdn_out=random.choice(msisdns_lists[2]),
        expected_result=False
    ),

    test_data_generator(
        description="Негативная проверка (фильтр по другому номеру): SMS (out), JSON",
        out_type="JSON",
        filter_type="OUT",
        filter_msisdns=list(map(str, msisdns_lists[3])),
        msisdn_in=random.choice(msisdns_lists[3]),
        msisdn_out=random_utils.generate_phone(),
        expected_result=False
    ),

    test_data_generator(
        description="Позитивная проверка: SMS (in), CSV",
        out_type="CSV",
        filter_type="IN",
        filter_msisdns=list(map(str, msisdns_lists[4])),
        msisdn_in=random.choice(msisdns_lists[4]),
        msisdn_out=random_utils.generate_phone(),
        expected_result=True
    ),

    test_data_generator(
        description="Позитивная проверка: SMS (out), THRIFT",
        out_type="THRIFT",
        filter_type="OUT",
        filter_msisdns=list(map(str, msisdns_lists[5])),
        msisdn_in=random_utils.generate_phone(),
        msisdn_out=random.choice(msisdns_lists[5]),
        expected_result=True
    ),

    test_data_generator(
        description="Негативная проверка (нерелевантное время в сообщении): SMS (in), JSON",
        out_type="JSON",
        filter_type="IN",
        filter_msisdns=list(map(str, msisdns_lists[6])),
        msisdn_in=random.choice(msisdns_lists[6]),
        msisdn_out=random_utils.generate_phone(),
        start_time=current_time - 48 * 60,
        start_dttm=current_time - 48 * 60,
        end_dttm=current_time - 48 * 60,
        expected_result=False
    ),

    test_data_generator(
        description="Негативная проверка (нерелевантный регион в сообщении): SMS (in), JSON",
        out_type="JSON",
        filter_type="IN",
        filter_msisdns=list(map(str, msisdns_lists[7])),
        msisdn_in=random.choice(msisdns_lists[7]),
        msisdn_out=random_utils.generate_phone(),
        filter_regions=["nw", "ug"],
        region="msk",
        expected_result=False
    ),

]
