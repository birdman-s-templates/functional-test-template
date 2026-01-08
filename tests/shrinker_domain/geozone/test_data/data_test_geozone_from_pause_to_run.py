import datetime
import random
from pprint import pprint

import pytz
from faker import Faker

from tests.shrinker_domain.geozone.testutils import test_data_generator_geozone_base

# - - - - - - - - - - - - - - - - - - - - - - - - - -
# константы
fake = Faker()
fake_ru = Faker('ru_RU')

timezone = pytz.timezone("Europe/Moscow")
current_time = int((datetime.datetime.now(timezone) - datetime.timedelta(seconds=random.randint(90, 300))).timestamp())

test_counter = 7

# генерим контент для саб-фильтров потока = генерим по 7 списков по 6 слов
content_event_type_list = [[f"{fake.word()}{random.randint(0,9)}" for _ in range(6)]
                           for _ in range(test_counter)]
content_geofence_type_list = [[f"{fake.word()}{random.randint(0,9)}" for _ in range(6)]
                              for _ in range(test_counter)]
content_zone_id_list = [[f"{random.choice([fake.word(), fake_ru.word()])}{random.randint(0,9)}" for _ in range(6)]
                              for _ in range(test_counter)]

random_indexes_event_type = [random.randint(0, 4) for _ in range(len(content_event_type_list))]
random_indexes_geofence_type = [random.randint(0, 4) for _ in range(len(content_geofence_type_list))]
random_indexes_zone_id = [random.randint(0, 4) for _ in range(len(content_zone_id_list))]


# - - - - - - - - - - - - - - - - - - - - - - - - - -
# тестовые данные

test_data = [
    test_data_generator_geozone_base(
        description="Позитивная проверка: GEOZONE, all sub-filters, JSON, from PAUSE to RUN",
        out_type="JSON",
        file_content_event_type=content_event_type_list[0],
        file_content_geofence_type=content_geofence_type_list[0],
        file_content_zone_id=content_zone_id_list[0],
        m_event_type=content_event_type_list[0][random_indexes_event_type[0]], # рандомный индекс
        m_geofence_type=content_geofence_type_list[0][random_indexes_geofence_type[0]],
        m_zone_id=content_zone_id_list[0][random_indexes_zone_id[0]],
        inner_filter_type=['eventType', 'geofenceType', 'zoneId'],
        expected_result=True
    ),

]