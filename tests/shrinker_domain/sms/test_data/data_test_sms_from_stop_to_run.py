import datetime
import pytz
import random

from common.utils import random_utils
from tests.shrinker_domain.sms.testutils import test_data_generator

# - - - - - - - - - - - - - - - - - - - - - - - - - -
# константы

def generate_msisdns():
    return [random_utils.generate_phone() for _ in range(5)]

msisdns_lists = [generate_msisdns() for _ in range(2)]

timezone = pytz.timezone("Europe/Moscow")
current_time = int((datetime.datetime.now(timezone) - datetime.timedelta(seconds=random.randint(90, 300))).timestamp())

# - - - - - - - - - - - - - - - - - - - - - - - - - -
# тестовые данные

test_data = [

    test_data_generator(
        description="Позитивная проверка: SMS (in), JSON, from STOP to RUN",
        out_type="JSON",
        filter_type="IN",
        filter_msisdns=list(map(str, msisdns_lists[0])),
        msisdn_in=random.choice(msisdns_lists[0]),
        msisdn_out=random_utils.generate_phone(),
        filter_range=[
            int((datetime.datetime.now() - datetime.timedelta(hours=random.randint(1, 12))).timestamp()) * 1000,
            int((datetime.datetime.now() + datetime.timedelta(seconds=5)).timestamp()) * 1000,
        ],
        expected_result=True,
    ),

    test_data_generator(
        description="Позитивная проверка: SMS (out), JSON, from STOP to RUN",
        out_type="JSON",
        filter_type="OUT",
        filter_msisdns=list(map(str, msisdns_lists[1])),
        msisdn_in=random_utils.generate_phone(),
        msisdn_out=random.choice(msisdns_lists[1]),
        filter_range=[
            int((datetime.datetime.now() - datetime.timedelta(hours=random.randint(1, 12))).timestamp()) * 1000,
            int((datetime.datetime.now() + datetime.timedelta(seconds=5)).timestamp()) * 1000,
        ],
        expected_result=True,
    ),

]
