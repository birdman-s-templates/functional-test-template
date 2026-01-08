import datetime
import pytz
import random

from faker import Faker

from common.utils import random_utils
from tests.shrinker_domain.smart_triggers.testutils import test_data_generator
from tests.shrinker_domain.geo.testutils import generate_lac_cell_list, extract_lac, extract_cell

# - - - - - - - - - - - - - - - - - - - - - - - - - -
# константы
fake = Faker()
fake_ru = Faker('ru_RU')

test_counter = 7

# common_data
imsi_list = [random_utils.generate_imsi() for _ in range(test_counter)]
msisdn_list = [(random_utils.generate_phone(), random_utils.generate_phone()) for _ in range(test_counter)]

# vlr
def generate_vlrs():
    return [random_utils.generate_phone() for _ in range(5)]
vlrs_lists = [generate_vlrs() for _ in range(test_counter)]

# geo
lac_cell_lists = [generate_lac_cell_list() for _ in range(test_counter)]
random_indexes = [random.randint(0, 4) for _ in range(len(lac_cell_lists))]

# urls
def generate_urls():
    return [random_utils.generate_random_url() for _ in range(5)]
urls_lists = [generate_urls() for _ in range(test_counter)]

timezone = pytz.timezone("Europe/Moscow")
current_time = int((datetime.datetime.now(timezone) - datetime.timedelta(seconds=random.randint(90, 300))).timestamp())

# контент для Geozone
content_event_type_list = [[f"{fake.word()}{random.randint(0,9)}" for _ in range(6)]
                           for _ in range(5)]
content_geofence_type_list = [[f"{fake.word()}{random.randint(0,9)}" for _ in range(6)]
                              for _ in range(5)]
content_zone_id_list = [[f"{random.choice([fake.word(), fake_ru.word()])}{random.randint(0,9)}" for _ in range(6)]
                              for _ in range(5)]

random_indexes_event_type = [random.randint(0, 4) for _ in range(len(content_event_type_list))]
random_indexes_geofence_type = [random.randint(0, 4) for _ in range(len(content_geofence_type_list))]
random_indexes_zone_id = [random.randint(0, 4) for _ in range(len(content_zone_id_list))]

# - - - - - - - - - - - - - - - - - - - - - - - - - -
# тестовые данные

test_data = [

    test_data_generator(
        description="Негативная проверка. Filters: SMS (in) w=10, Geo w=10. Messages: SMS. Trigger message: -. JSON. "
                    "Total filter weight: 20. Total messages weight: 10.",
        filter_out_type="JSON",
        filter_total_weight=20,
        filter_is_smart_trigger=True,

        first_message="sms",

        sms_m_count=1,

        is_sms=True,
        sms_f_inner_filter_type="incoming",
        sms_f_msisdns=[msisdn_list[0][0]],
        sms_f_weight=10,
        sms_f_ttl=1,
        sms_m_imsi=imsi_list[0],
        sms_m_msisdn_in=msisdn_list[0][0],
        sms_m_msisdn_out=msisdn_list[0][1],

        latency=65,

        expected_imsi=imsi_list[0],
        expected_result=False,

    ),

    test_data_generator(
        description="Позитивная проверка. Filters: SMS (in) w=10, Geo w=10. Messages: SMS. JSON. "
                    "Total filter weight: 20. Total messages weight: 10.",
        filter_out_type="JSON",
        filter_total_weight=20,
        filter_is_smart_trigger=True,

        first_message="sms",

        sms_m_count=1,

        is_sms=True,
        sms_f_inner_filter_type="incoming",
        sms_f_msisdns=[msisdn_list[1][0]],
        sms_f_weight=10,
        sms_f_ttl=1,
        sms_m_imsi=imsi_list[1],
        sms_m_msisdn_in=msisdn_list[1][0],
        sms_m_msisdn_out=msisdn_list[1][1],

        latency=55,
        is_trigger=0,

        expected_imsi=imsi_list[1],
        expected_result=True,

    ),

    test_data_generator(
        description="Негативная проверка. Filters: SMS (in) w=10, Geo w=10. Messages: SMS, Geo. Trigger message: Geo. "
                    "JSON. Total filter weight: 20. Total messages weight: 20.",
        filter_out_type="JSON",
        filter_total_weight=20,
        filter_is_smart_trigger=True,

        first_message="sms",
        second_message="geo",

        sms_m_count=1,
        geo_m_count=1,

        trigger_message_type="geo",

        is_sms=True,
        sms_f_inner_filter_type="incoming",
        sms_f_msisdns=[msisdn_list[2][0]],
        sms_f_weight=10,
        sms_f_ttl=1,
        sms_m_imsi=imsi_list[2],
        sms_m_msisdn_in=msisdn_list[2][0],
        sms_m_msisdn_out=msisdn_list[2][1],

        is_geo=True,
        geo_f_lac_cells=lac_cell_lists[2],
        geo_f_weight=10,
        geo_f_ttl=1,
        geo_m_imsi=imsi_list[2],
        geo_m_msisdn=int(msisdn_list[2][1]),
        geo_m_lac=extract_lac(lac_cell_lists[2][random_indexes[2]]),
        geo_m_cell=extract_cell(lac_cell_lists[2][random_indexes[2]]),

        latency=5,

        expected_imsi=imsi_list[2],
        expected_result=False,

    ),

    test_data_generator(
        description="Негативная проверка. Filters: SMS (in) w=10, Geo w=10, Call w=10. Messages: SMS, Geo, Call. "
                    "Trigger message: -. JSON. Total filter weight: 50. Total messages weight: 30.",
        filter_out_type="JSON",
        filter_total_weight=50,
        filter_is_smart_trigger=True,

        first_message="sms",
        second_message="geo",
        third_message="call",

        sms_m_count=1,
        geo_m_count=1,
        call_m_count=1,

        is_sms=True,
        sms_f_inner_filter_type="incoming",
        sms_f_msisdns=[msisdn_list[3][0]],
        sms_f_weight=10,
        sms_f_ttl=1,
        sms_m_imsi=imsi_list[3],
        sms_m_msisdn_in=msisdn_list[3][0],
        sms_m_msisdn_out=msisdn_list[3][1],

        is_geo=True,
        geo_f_lac_cells=lac_cell_lists[3],
        geo_f_weight=10,
        geo_f_ttl=1,
        geo_m_imsi=imsi_list[3],
        geo_m_msisdn=int(msisdn_list[3][1]),
        geo_m_lac=extract_lac(lac_cell_lists[3][random_indexes[3]]),
        geo_m_cell=extract_cell(lac_cell_lists[3][random_indexes[3]]),

        is_call=True,
        call_f_inner_filter_type="incoming",
        call_f_msisdns=[msisdn_list[3][0]],
        call_f_weight=10,
        call_f_ttl=1,
        call_m_imsi=imsi_list[3],
        call_m_msisdn_in=int(msisdn_list[3][0]),
        call_m_msisdn_out=int(msisdn_list[3][1]),

        latency=5,
        is_trigger=0,

        expected_imsi=imsi_list[3],
        expected_result=True,

    ),

    test_data_generator(
        description="Позитивная проверка (два абонента). Filters: SMS (in) w=5, Geo w=20, Call w=5. "
                    "Messages: SMS (абонент А), Geo (абонент Б), Call (абонент А). Trigger message: Geo. JSON. "
                    "Total filter weight: 20. Total messages weight: 20.",
        filter_out_type="JSON",
        filter_total_weight=20,
        filter_is_smart_trigger=True,

        first_message="sms",
        second_message="geo",
        third_message="call",

        sms_m_count=1,
        geo_m_count=1,
        call_m_count=1,

        is_sms=True,
        sms_f_inner_filter_type="incoming",
        sms_f_msisdns=[msisdn_list[4][0]],
        sms_f_weight=5,
        sms_f_ttl=1,
        sms_m_imsi=imsi_list[4],
        sms_m_msisdn_in=msisdn_list[4][0],
        sms_m_msisdn_out=msisdn_list[4][1],

        is_geo=True,
        geo_f_lac_cells=lac_cell_lists[4],
        geo_f_weight=20,
        geo_f_ttl=1,
        geo_m_imsi=random_utils.generate_imsi(),
        geo_m_msisdn=int(random_utils.generate_phone()),
        geo_m_lac=extract_lac(lac_cell_lists[4][random_indexes[4]]),
        geo_m_cell=extract_cell(lac_cell_lists[4][random_indexes[4]]),

        is_call=True,
        call_f_inner_filter_type="incoming",
        call_f_msisdns=[msisdn_list[4][0]],
        call_f_weight=5,
        call_f_ttl=1,
        call_m_imsi=imsi_list[4],
        call_m_msisdn_in=int(msisdn_list[4][0]),
        call_m_msisdn_out=int(msisdn_list[4][1]),

        latency=5,
        is_trigger=0,

        expected_imsi=imsi_list[4],
        expected_result=True,

    ),


    #todo: дебаг проверки
    test_data_generator(
        description="Позитивная проверка. Filters: Geo w=10, Geozone w=10. "
                    "Messages: SMS , Geozone. Trigger message: Geozone. JSON. "
                    "Total filter weight: 20. Total messages weight: 20.",
        filter_out_type="JSON",
        filter_total_weight=20,
        filter_is_smart_trigger=True,

        first_message="geo",
        second_message="geozone",

        geo_m_count=1,
        geozone_m_count=1,

        is_geo=True,
        geo_f_lac_cells=lac_cell_lists[5],
        geo_f_weight=20,
        geo_f_ttl=1,
        geo_m_imsi=random_utils.generate_imsi(),
        geo_m_msisdn=int(random_utils.generate_phone()),
        geo_m_lac=extract_lac(lac_cell_lists[5][random_indexes[5]]),
        geo_m_cell=extract_cell(lac_cell_lists[5][random_indexes[5]]),

        is_geozone=True,
        geozone_f_inner_filter_type = ['eventType', 'geofenceType', 'zoneId'],
        geozone_f_weight=10,
        geozone_f_ttl=1,
        geozone_m_msisdn=int(msisdn_list[5][1]),
        geozone_m_imsi=imsi_list[5],
        geozone_f_file_content_event_type=content_event_type_list[0],
        geozone_f_file_content_geofence_type=content_geofence_type_list[0],
        geozone_f_file_content_zone_id=content_zone_id_list[0],
        geozone_m_event_type=content_event_type_list[0][random_indexes_event_type[0]],  # рандомный индекс
        geozone_m_geofence_type=content_geofence_type_list[0][random_indexes_geofence_type[0]],
        geozone_m_zone_id=content_zone_id_list[0][random_indexes_zone_id[0]],

        latency=5,
        is_trigger=1,

        #expected_msisdn=int(msisdn_list[5][1]),
        expected_imsi=imsi_list[5],
        expected_result=True,
    ),

]
