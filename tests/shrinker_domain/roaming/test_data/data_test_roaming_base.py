import datetime
import pytz
import random

from common.utils import random_utils
from tests.shrinker_domain.roaming.testutils import test_data_generator

# - - - - - - - - - - - - - - - - - - - - - - - - - -
# константы

def generate_msisdns():
    return [int(random_utils.generate_phone()) for _ in range(5)]

vlrs_lists = [generate_msisdns() for _ in range(6)]

timezone = pytz.timezone("Europe/Moscow")
current_time = int((datetime.datetime.now(timezone) - datetime.timedelta(seconds=random.randint(90, 300))).timestamp())

# - - - - - - - - - - - - - - - - - - - - - - - - - -
# тестовые данные

test_data = [

    test_data_generator(
        description="Позитивная проверка: ROAMING, JSON",
        out_type="JSON",
        filter_vlrs=list(map(str, vlrs_lists[0])),
        vlr=random.choice(vlrs_lists[0]),
        expected_result=True
    ),

    test_data_generator(
        description="Негативная проверка (фильтр по другому номеру): ROAMING, JSON",
        out_type="JSON",
        filter_vlrs=list(map(str, vlrs_lists[1])),
        vlr=int(random_utils.generate_phone()),
        expected_result=False
    ),

    test_data_generator(
        description="Позитивная проверка: ROAMING, CSV",
        out_type="CSV",
        filter_vlrs=list(map(str, vlrs_lists[2])),
        vlr=random.choice(vlrs_lists[2]),
        expected_result=True
    ),

    test_data_generator(
        description="Позитивная проверка: ROAMING, THRIFT",
        out_type="THRIFT",
        filter_vlrs=list(map(str, vlrs_lists[3])),
        vlr=random.choice(vlrs_lists[3]),
        expected_result=True
    ),

    test_data_generator(
        description="Негативная проверка (нерелевантное время в сообщении): ROAMING, JSON",
        out_type="JSON",
        filter_vlrs=list(map(str, vlrs_lists[4])),
        vlr=random.choice(vlrs_lists[4]),
        timestamp=current_time - 48 * 60,
        expected_result=False
    ),

    test_data_generator(
        description="Негативная проверка (нерелевантный регион в сообщении): ROAMING, JSON",
        out_type="JSON",
        filter_vlrs=list(map(str, vlrs_lists[5])),
        vlr=random.choice(vlrs_lists[5]),
        filter_regions=["nw", "ug"],
        region="msk",
        expected_result=False
    ),

]
