import datetime
import pytz
import random

from common.utils import random_utils
from tests.shrinker_domain.calls.testutils import test_data_generator

# - - - - - - - - - - - - - - - - - - - - - - - - - -
# константы

def generate_msisdns():
    return [int(random_utils.generate_phone()) for _ in range(5)]

msisdns_lists = [generate_msisdns() for _ in range(2)]

timezone = pytz.timezone("Europe/Moscow")
current_time = int((datetime.datetime.now(timezone) - datetime.timedelta(seconds=random.randint(90, 300))).timestamp())

# - - - - - - - - - - - - - - - - - - - - - - - - - -
# тестовые данные

test_data = [

    test_data_generator(
        description="Негативная проверка: CALLS (in), статус: all, JSON, from RUN to PAUSE",
        out_type="JSON",
        filter_type="IN",
        filter_msisdns=list(map(str, msisdns_lists[0])),
        msisdn_in=random.choice(msisdns_lists[0]),
        msisdn_out=int(random_utils.generate_phone()),
        expected_result=False
    ),

    test_data_generator(
        description="Негативная проверка: CALLS (out), статус: all, JSON, from RUN to PAUSE",
        out_type="JSON",
        filter_type="OUT",
        filter_msisdns=list(map(str, msisdns_lists[1])),
        msisdn_in=int(random_utils.generate_phone()),
        msisdn_out=random.choice(msisdns_lists[1]),
        expected_result=False
    ),

]
