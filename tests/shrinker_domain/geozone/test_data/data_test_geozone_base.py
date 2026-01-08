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

test_counter = 9

# генерим контент для саб-фильтров потока = генерим по 7 списков по 6 слов
content_event_type_list = [[f"{fake.word()}{random.randint(0,9)}" for _ in range(10)]
                           for _ in range(test_counter)]
content_geofence_type_list = [[f"{fake.word()}{random.randint(0,9)}" for _ in range(10)]
                              for _ in range(test_counter)]
content_zone_id_list = [[f"{random.choice([fake.word(), fake_ru.word()])}{random.randint(0,9)}" for _ in range(10)]
                              for _ in range(test_counter)]

random_indexes_event_type = [random.randint(0, 4) for _ in range(len(content_event_type_list))]
random_indexes_geofence_type = [random.randint(0, 4) for _ in range(len(content_geofence_type_list))]
random_indexes_zone_id = [random.randint(0, 4) for _ in range(len(content_zone_id_list))]


# - - - - - - - - - - - - - - - - - - - - - - - - - -
# тестовые данные

test_data = [
    test_data_generator_geozone_base(
        description="Позитивная проверка: GEOZONE, all sub-filters, JSON",
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

    test_data_generator_geozone_base(
        description="Негативная проверка: GEOZONE, eventType & geofenceType (без zoneId в сообщении), JSON",
        out_type="JSON",
        file_content_event_type=content_event_type_list[1],
        file_content_geofence_type=content_geofence_type_list[1],
        m_event_type=content_event_type_list[1][random_indexes_event_type[1]],
        m_geofence_type=content_geofence_type_list[1][random_indexes_geofence_type[1]],
        inner_filter_type=['eventType', 'geofenceType'],
        expected_result=False
    ),

    test_data_generator_geozone_base(
        description="Позитивная проверка: GEOZONE, all sub-filters, CSV",
        out_type="CSV",
        file_content_event_type=content_event_type_list[2],
        file_content_geofence_type=content_geofence_type_list[2],
        file_content_zone_id=content_zone_id_list[2],
        m_event_type=content_event_type_list[2][random_indexes_event_type[2]],
        m_geofence_type=content_geofence_type_list[2][random_indexes_geofence_type[2]],
        m_zone_id=content_zone_id_list[2][random_indexes_zone_id[2]],
        inner_filter_type=['eventType', 'geofenceType', 'zoneId'],
        expected_result=True
    ),

    test_data_generator_geozone_base(
        description="Негативная проверка: GEOZONE, в сообщении контент нерелевантный, JSON",
        out_type="JSON",
        file_content_event_type=content_event_type_list[3],
        file_content_geofence_type=content_geofence_type_list[3],
        file_content_zone_id=content_zone_id_list[3],
        m_event_type=fake.word(),
        m_geofence_type=fake.word(),
        m_zone_id=fake.word(),
        inner_filter_type=['eventType', 'geofenceType', 'zoneId'],
        expected_result=False
    ),

    test_data_generator_geozone_base(
        description="Позитивная проверка: GEOZONE, all sub-filters, THRIFT",
        out_type="THRIFT",
        file_content_event_type=content_event_type_list[4],
        file_content_geofence_type=content_geofence_type_list[4],
        file_content_zone_id=content_zone_id_list[4],
        m_event_type=content_event_type_list[4][random_indexes_event_type[4]],
        m_geofence_type=content_geofence_type_list[4][random_indexes_geofence_type[4]],
        m_zone_id=content_zone_id_list[4][random_indexes_zone_id[4]],
        inner_filter_type=['eventType', 'geofenceType', 'zoneId'],
        expected_result=True
    ),

    # добавлены новые проверки фильтрации
    test_data_generator_geozone_base(
        description="Негативная проверка: GEOZONE, в сообщении в geofenceType нерелевантный контент, JSON",
        out_type="JSON",
        file_content_event_type=content_event_type_list[5],
        file_content_geofence_type=content_geofence_type_list[5],
        file_content_zone_id=content_zone_id_list[5],
        m_event_type=content_event_type_list[1][random_indexes_event_type[5]],
        m_geofence_type=fake.word(),
        m_zone_id=content_zone_id_list[5][random_indexes_zone_id[5]],
        inner_filter_type=['eventType', 'geofenceType', 'zoneId'],
        expected_result=False
    ),

    test_data_generator_geozone_base(
        description="Негативная проверка: GEOZONE, в сообщении в eventType нерелевантный контент, JSON",
        out_type="JSON",
        file_content_event_type=content_event_type_list[6],
        file_content_geofence_type=content_geofence_type_list[6],
        file_content_zone_id=content_zone_id_list[6],
        m_event_type=fake.word(),
        m_geofence_type=content_geofence_type_list[6][random_indexes_geofence_type[6]],
        m_zone_id=content_zone_id_list[6][random_indexes_zone_id[6]],
        inner_filter_type=['eventType', 'geofenceType', 'zoneId'],
        expected_result=False
    ),

    test_data_generator_geozone_base(
        description="Негативная проверка: GEOZONE, в сообщении в zoneId нерелевантный контент, JSON",
        out_type="JSON",
        file_content_event_type=content_event_type_list[7],
        file_content_geofence_type=content_geofence_type_list[7],
        file_content_zone_id=content_zone_id_list[7],
        m_event_type=content_event_type_list[7][random_indexes_event_type[7]],
        m_geofence_type=content_geofence_type_list[7][random_indexes_geofence_type[7]],
        m_zone_id=fake.word(),
        inner_filter_type=['eventType', 'geofenceType', 'zoneId'],
        expected_result=False
    ),

    test_data_generator_geozone_base(
        description="Позитивная проверка: GEOZONE, в потоке нет фильтрации по zoneId, JSON",
        out_type="JSON",
        file_content_event_type=content_event_type_list[8],
        file_content_geofence_type=content_geofence_type_list[8],
        #file_content_zone_id=content_zone_id_list[7],
        m_event_type=content_event_type_list[8][random_indexes_event_type[8]],
        m_geofence_type=content_geofence_type_list[8][random_indexes_geofence_type[8]],
        m_zone_id=fake.word(),
        inner_filter_type=['eventType', 'geofenceType'],
        expected_result=True
    ),


]


if __name__ == '__main__':
    import pprint
    # pprint.pp(content_event_type_list)
    # pprint.pp(content_geofence_type_list)
    # pprint.pp(content_zone_id_list)

    pprint.pp(test_data[1])
    print(fake.word())

