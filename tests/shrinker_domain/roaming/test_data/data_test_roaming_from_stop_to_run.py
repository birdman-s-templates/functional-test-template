import datetime
import pytz
import random

from common.utils import random_utils
from tests.shrinker_domain.roaming.testutils import test_data_generator

# - - - - - - - - - - - - - - - - - - - - - - - - - -
# константы

def generate_msisdns():
    return [int(random_utils.generate_phone()) for _ in range(5)]

vlrs_lists = [generate_msisdns() for _ in range(1)]

timezone = pytz.timezone("Europe/Moscow")
current_time = int((datetime.datetime.now(timezone) - datetime.timedelta(seconds=random.randint(90, 300))).timestamp())

# - - - - - - - - - - - - - - - - - - - - - - - - - -
# тестовые данные

test_data = [

    test_data_generator(
        description="Позитивная проверка: ROAMING, JSON, from STOP to RUN",
        out_type="JSON",
        filter_vlrs=list(map(str, vlrs_lists[0])),
        vlr=random.choice(vlrs_lists[0]),
        filter_range=[
            int((datetime.datetime.now() - datetime.timedelta(hours=random.randint(1, 12))).timestamp()) * 1000,
            int((datetime.datetime.now() + datetime.timedelta(seconds=5)).timestamp()) * 1000,
        ],
        expected_result=True
    ),

]
