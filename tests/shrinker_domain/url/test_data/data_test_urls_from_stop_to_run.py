import datetime
import pytz
import random

from common.utils import random_utils
from tests.shrinker_domain.url.testutils import test_data_generator

# - - - - - - - - - - - - - - - - - - - - - - - - - -
# константы

def generate_urls():
    return [random_utils.generate_random_url() for _ in range(5)]

urls_lists = [generate_urls() for _ in range(1)]

timezone = pytz.timezone("Europe/Moscow")
current_time = int((datetime.datetime.now(timezone) - datetime.timedelta(seconds=random.randint(90, 300))).timestamp())

# - - - - - - - - - - - - - - - - - - - - - - - - - -
# тестовые данные

test_data = [

    test_data_generator(
        description="Позитивная проверка: URL_CISCO, протоколы: all, JSON, from STOP to RUN",
        out_type="JSON",
        filter_urls=urls_lists[0],
        filter_range=[
            int((datetime.datetime.now(timezone) - datetime.timedelta(hours=random.randint(1, 12))).timestamp()) * 1000,
            int((datetime.datetime.now(timezone) + datetime.timedelta(seconds=5)).timestamp()) * 1000,
        ],
        host=random.choice(urls_lists[0]),
        expected_result=True,
    ),

]
