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

test_counter = 15 #увеличиваем если добавляются проверки

# common_data
imsi_list = [random_utils.generate_imsi() for _ in range(test_counter)]
imei_list = [str(random_utils.generate_imei()) for _ in range(test_counter)]
msisdn_list = [(random_utils.generate_phone(), random_utils.generate_phone()) for _ in range(test_counter)]

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

# - - - - - - - - - - - - - - - - - - - - - - - - - -
# тестовые данные

test_data = [

    test_data_generator(
        description="Позитивная проверка. Filters: CALLS (out) w=10, Roaming w=10. Trigger message: Roaming. JSON. "
                    "Total filter weight: 20. Total messages weight: 20.",
        filter_out_type="JSON",
        filter_total_weight=20,
        filter_is_smart_trigger=True,

        first_message="call",
        second_message="roaming",

        call_m_count=1,
        roaming_m_count=1,

        trigger_message_type="roaming",

        is_call=True,
        call_f_inner_filter_type="outgoing",
        call_f_msisdns=[msisdn_list[0][1]],
        call_f_weight=10,
        call_f_ttl=5,
        call_m_imsi=imsi_list[0],
        call_m_msisdn_in=int(msisdn_list[0][0]),
        call_m_msisdn_out=int(msisdn_list[0][1]),

        is_roaming=True,
        vlr_f_msisdns=vlrs_lists[0],
        vlr_f_weight=10,
        vlr_f_ttl=5,
        vlr_m_imsi=imsi_list[0],
        vlr_m_vlr=int(random.choice(vlrs_lists[0])),
        vlr_m_msisdn=int(msisdn_list[0][0]),

        expected_imsi=imsi_list[0],
        expected_msisdn=int(msisdn_list[0][0]),
        expected_result=True,
        expected_sum=20,
    ),

    test_data_generator(
        description="Позитивная проверка. Filters: SMS (in) w=10, Geo w=10. Trigger message: Geo. JSON. "
                    "Total filter weight: 20. Total messages weight: 20.",
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
        sms_f_msisdns=[msisdn_list[1][0]],
        sms_f_weight=10,
        sms_f_ttl=5,
        sms_m_imsi=imsi_list[1],
        sms_m_msisdn_in=msisdn_list[1][0],
        sms_m_msisdn_out=msisdn_list[1][1],

        is_geo=True,
        geo_f_lac_cells=lac_cell_lists[1],
        geo_f_weight=10,
        geo_f_ttl=5,
        geo_m_imsi=imsi_list[1],
        geo_m_msisdn=int(msisdn_list[1][1]),
        geo_m_lac=extract_lac(lac_cell_lists[1][random_indexes[1]]),
        geo_m_cell=extract_cell(lac_cell_lists[1][random_indexes[1]]),

        expected_imsi=imsi_list[1],
        expected_msisdn=int(msisdn_list[1][1]),
        expected_result=True,
        expected_sum=20,
    ),

    test_data_generator(
        description="Позитивная проверка. Filters: CALLS (in) w=10, Clickstream w=10. Trigger message: Clickstream. JSON. "
                    "Total filter weight: 50. Total messages weight: 20.",
        filter_out_type="JSON",
        filter_total_weight=20,
        filter_is_smart_trigger=True,

        first_message="call",
        second_message="clickstream",

        call_m_count=1,
        clickstream_m_count=1,

        trigger_message_type="clickstream",

        is_call=True,
        call_f_inner_filter_type="incoming",
        call_f_msisdns=[msisdn_list[2][0]],
        call_f_weight=10,
        call_f_ttl=5,
        call_m_imsi=imsi_list[2],
        call_m_msisdn_in=int(msisdn_list[2][0]),
        call_m_msisdn_out=int(msisdn_list[2][1]),

        is_clickstream=True,
        url_f_urls=urls_lists[2],
        url_f_weight=10,
        url_f_ttl=5,
        url_m_imsi=imsi_list[2],
        url_m_msisdn=int(msisdn_list[2][1]),
        url_m_host=random.choice(urls_lists[2]),

        expected_imsi=imsi_list[2],
        expected_msisdn=int(msisdn_list[2][1]),
        expected_result=True,
        expected_sum=20,
    ),

    test_data_generator(
        description="Негативная проверка. Filters: CALLS (in), SMS (in). JSON. Total filter weight: 50. "
                    "Total messages weight: 48.",
        filter_out_type="JSON",
        filter_total_weight=50,
        filter_is_smart_trigger=True,

        first_message="call",
        second_message="sms",

        call_m_count=1,
        sms_m_count=1,

        trigger_message_type=None,

        is_call=True,
        call_f_inner_filter_type="incoming",
        call_f_msisdns=[msisdn_list[3][0]],
        call_f_weight=24,
        call_f_ttl=5,
        call_m_imsi=imsi_list[3],
        call_m_msisdn_in=int(msisdn_list[3][0]),
        call_m_msisdn_out=int(msisdn_list[3][1]),

        is_sms=True,
        sms_f_inner_filter_type="incoming",
        sms_f_msisdns=[msisdn_list[3][0]],
        sms_f_weight=24,
        sms_f_ttl=5,
        sms_m_imsi=imsi_list[3],
        sms_m_msisdn_in=msisdn_list[3][0],
        sms_m_msisdn_out=msisdn_list[3][1],

        expected_result=False
    ),

    test_data_generator(
        description="Негативная проверка. Filters: CALLS (in), SMS (in). Messages order: Call, Call. JSON. "
                    "Total filter weight: 50. Total messages weight: 50.",
        filter_out_type="JSON",
        filter_total_weight=50,
        filter_is_smart_trigger=True,

        first_message="call",

        call_m_count=2,

        trigger_message_type=None,

        is_call=True,
        call_f_inner_filter_type="incoming",
        call_f_msisdns=[msisdn_list[4][0]],
        call_f_weight=25,
        call_f_ttl=5,
        call_m_imsi=imsi_list[4],
        call_m_msisdn_in=int(msisdn_list[4][0]),
        call_m_msisdn_out=int(msisdn_list[4][1]),

        expected_result=False
    ),

    test_data_generator(
        description="Позитивная проверка. Filters: SMS (in) w=10, Geo w=10. Trigger message: Geo. CSV. "
                    "Total filter weight: 20. Total messages weight: 20.",
        filter_out_type="CSV",
        filter_total_weight=20,
        filter_is_smart_trigger=True,

        first_message="sms",
        second_message="geo",

        sms_m_count=1,
        geo_m_count=1,

        trigger_message_type="geo",

        is_sms=True,
        sms_f_inner_filter_type="incoming",
        sms_f_msisdns=[msisdn_list[5][0]],
        sms_f_weight=10,
        sms_f_ttl=5,
        sms_m_imsi=imsi_list[5],
        sms_m_msisdn_in=msisdn_list[5][0],
        sms_m_msisdn_out=msisdn_list[5][1],

        is_geo=True,
        geo_f_lac_cells=lac_cell_lists[5],
        geo_f_weight=10,
        geo_f_ttl=5,
        geo_m_imsi=imsi_list[5],
        geo_m_msisdn=int(msisdn_list[5][1]),
        geo_m_lac=extract_lac(lac_cell_lists[5][random_indexes[5]]),
        geo_m_cell=extract_cell(lac_cell_lists[5][random_indexes[5]]),

        expected_imsi=imsi_list[5],
        expected_msisdn=int(msisdn_list[5][1]),
        expected_result=True,
        expected_sum=20,
    ),

    test_data_generator(
        description="Позитивная проверка. Filters: SMS (in) w=10, Geo w=10. Trigger message: Geo. THRIFT. "
                    "Total filter weight: 20. Total messages weight: 20.",
        filter_out_type="THRIFT",
        filter_total_weight=20,
        filter_is_smart_trigger=True,

        first_message="sms",
        second_message="geo",

        sms_m_count=1,
        geo_m_count=1,

        trigger_message_type="geo",

        is_sms=True,
        sms_f_inner_filter_type="incoming",
        sms_f_msisdns=[msisdn_list[6][0]],
        sms_f_weight=10,
        sms_f_ttl=1,
        sms_m_imsi=imsi_list[6],
        sms_m_msisdn_in=msisdn_list[6][0],
        sms_m_msisdn_out=msisdn_list[6][1],

        is_geo=True,
        geo_f_lac_cells=lac_cell_lists[6],
        geo_f_weight=10,
        geo_f_ttl=1,
        geo_m_imsi=imsi_list[6],
        geo_m_msisdn=int(msisdn_list[6][1]),
        geo_m_lac=extract_lac(lac_cell_lists[6][random_indexes[6]]),
        geo_m_cell=extract_cell(lac_cell_lists[6][random_indexes[6]]),

        expected_imsi=imsi_list[6],
        expected_msisdn=int(msisdn_list[6][1]),
        expected_sum=20,
        expected_result=True,

    ),

    test_data_generator(
        description="Позитивная проверка. Filters: SMS (in) w=10, Geo w=10. Trigger message: Geo. JSON_RTM. "
                    "Total filter weight: 20. Total messages weight: 20.",
        filter_out_type="JSON_RTM",
        filter_total_weight=20,
        filter_is_smart_trigger=True,

        first_message="sms",
        second_message="geo",

        sms_m_count=1,
        geo_m_count=1,

        trigger_message_type="geo",

        is_sms=True,
        sms_f_inner_filter_type="incoming",
        sms_f_msisdns=[msisdn_list[7][0]],
        sms_f_weight=10,
        sms_f_ttl=5,
        sms_m_imsi=imsi_list[7],
        sms_m_msisdn_in=msisdn_list[7][0],
        sms_m_msisdn_out=msisdn_list[7][1],

        is_geo=True,
        geo_f_lac_cells=lac_cell_lists[7],
        geo_f_weight=10,
        geo_f_ttl=5,
        geo_m_imsi=imsi_list[7],
        geo_m_msisdn=int(msisdn_list[7][1]),
        geo_m_lac=extract_lac(lac_cell_lists[7][random_indexes[7]]),
        geo_m_cell=extract_cell(lac_cell_lists[7][random_indexes[7]]),

        expected_imsi=imsi_list[7],
        expected_msisdn=int(msisdn_list[7][1]),
        expected_result=True,
        expected_sum=20,
    ),

    test_data_generator(
        description="Позитивная проверка. Filters: CALLS (out) w=10, Roaming w=10. Trigger message: Call "
                    "(разные msisdn / imei). JSON. Total filter weight: 20. Total messages weight: 20.",
        filter_out_type="JSON",
        filter_total_weight=20,
        filter_is_smart_trigger=True,

        first_message="roaming",
        second_message="call",

        call_m_count=1,
        roaming_m_count=1,

        trigger_message_type="call",

        is_call=True,
        call_f_inner_filter_type="outgoing",
        call_f_msisdns=[msisdn_list[8][1]],
        call_f_weight=10,
        call_f_ttl=5,
        call_m_imsi=imsi_list[8],
        call_m_imei=imei_list[8],
        call_m_msisdn_in=int(msisdn_list[8][0]),
        call_m_msisdn_out=int(msisdn_list[8][1]),

        is_roaming=True,
        vlr_f_msisdns=vlrs_lists[8],
        vlr_f_weight=10,
        vlr_f_ttl=5,
        vlr_m_imsi=imsi_list[8],
        vlr_m_imei=str(random_utils.generate_imei()),
        vlr_m_vlr=int(random.choice(vlrs_lists[8])),
        vlr_m_msisdn=int(random_utils.generate_phone()),

        expected_imsi=imsi_list[8],
        expected_msisdn=int(msisdn_list[8][0]),
        expected_result=True,
        expected_sum=20,
    ),

    test_data_generator(
        description="Позитивная проверка (два абонента). Filters: SMS (in) w=10, Roaming w=20, Geo w=10. "
                    "Messages: SMS (абонент А), Roaming (абонент Б), Geo (абонент А). Trigger message: Geo. JSON. "
                    "Total filter weight: 20. Total messages weight: 20.",
        filter_out_type="JSON",
        filter_total_weight=20,
        filter_is_smart_trigger=True,

        first_message="sms",
        second_message="roaming",
        third_message="geo",

        sms_m_count=1,
        geo_m_count=1,
        roaming_m_count=1,

        trigger_message_type="geo",

        is_sms=True,
        sms_f_inner_filter_type="incoming",
        sms_f_msisdns=[msisdn_list[9][0]],
        sms_f_weight=10,
        sms_f_ttl=5,
        sms_m_imsi=imsi_list[9],
        sms_m_msisdn_in=msisdn_list[9][0],
        sms_m_msisdn_out=msisdn_list[9][1],

        is_roaming=True,
        vlr_f_msisdns=vlrs_lists[9],
        vlr_f_weight=20,
        vlr_f_ttl=5,
        vlr_m_imsi=random_utils.generate_imsi(),
        vlr_m_vlr=int(random.choice(vlrs_lists[9])),
        vlr_m_msisdn=int(random_utils.generate_phone()),

        is_geo=True,
        geo_f_lac_cells=lac_cell_lists[9],
        geo_f_weight=10,
        geo_f_ttl=5,
        geo_m_imsi=imsi_list[9],
        geo_m_msisdn=int(msisdn_list[9][1]),
        geo_m_lac=extract_lac(lac_cell_lists[9][random_indexes[9]]),
        geo_m_cell=extract_cell(lac_cell_lists[9][random_indexes[9]]),

        expected_imsi=imsi_list[9],
        expected_msisdn=int(msisdn_list[9][1]),
        expected_result=True,
        expected_sum=20,
    ),

    # todo: пока скрыты тесты, тк нужно править текущую реализацию - https://jira.mts.ru/browse/DR-9894
    # test_data_generator(
    #     description="Негативная проверка (два триггера по одному абоненту, без задержки). Filters: SMS (in) w=10, Roaming w=20, "
    #                 "Geo w=10. Messages: Roaming, SMS, Geo. Trigger message: Geo. JSON. Total filter weight: 20. "
    #                 "Total messages weight: 20.",
    #     filter_out_type="JSON",
    #     filter_total_weight=20,
    #     filter_is_smart_trigger=True,
    #
    #     first_message="roaming",
    #     second_message="sms",
    #     third_message="geo",
    #
    #     sms_m_count=1,
    #     geo_m_count=1,
    #     roaming_m_count=1,
    #
    #     trigger_message_type="geo",
    #
    #     is_roaming=True,
    #     vlr_f_msisdns=vlrs_lists[10],
    #     vlr_f_weight=20,
    #     vlr_f_ttl=5,
    #     vlr_m_imsi=imsi_list[10],
    #     vlr_m_vlr=int(random.choice(vlrs_lists[10])),
    #     vlr_m_msisdn=int(msisdn_list[10][1]),
    #
    #     is_sms=True,
    #     sms_f_inner_filter_type="incoming",
    #     sms_f_msisdns=[msisdn_list[10][0]],
    #     sms_f_weight=10,
    #     sms_f_ttl=5,
    #     sms_m_imsi=imsi_list[10],
    #     sms_m_msisdn_in=msisdn_list[10][0],
    #     sms_m_msisdn_out=msisdn_list[10][1],
    #
    #     is_geo=True,
    #     geo_f_lac_cells=lac_cell_lists[10],
    #     geo_f_weight=10,
    #     geo_f_ttl=5,
    #     geo_m_imsi=imsi_list[10],
    #     geo_m_msisdn=int(msisdn_list[10][1]),
    #     geo_m_lac=extract_lac(lac_cell_lists[10][random_indexes[10]]),
    #     geo_m_cell=extract_cell(lac_cell_lists[10][random_indexes[10]]),
    #
    #     expected_imsi=imsi_list[10],
    #     expected_msisdn=int(msisdn_list[10][1]),
    #     expected_result=False,
    #
    # ),

    #пока скрыто, тк нужно править текущую реализацию - https://jira.mts.ru/browse/DR-9894
    # test_data_generator(
    #     description="Позитивная проверка (два триггера по одному абоненту с задержкой). Filters: SMS (in) w=10, Roaming w=20, "
    #                 "Geo w=10. Messages: Roaming, SMS, Geo. Trigger message: Geo. JSON. Total filter weight: 20. "
    #                 "Total messages weight: 20.",
    #     filter_out_type="JSON",
    #     filter_total_weight=20,
    #     filter_is_smart_trigger=True,
    #
    #     first_message="roaming",
    #     second_message="sms",
    #     third_message="geo",
    #
    #     sms_m_count=1,
    #     geo_m_count=1,
    #     roaming_m_count=1,
    #
    #     trigger_message_type="geo",
    #
    #     is_roaming=True,
    #     vlr_f_msisdns=vlrs_lists[11],
    #     vlr_f_weight=20,
    #     vlr_f_ttl=5,
    #     vlr_m_imsi=imsi_list[11],
    #     vlr_m_vlr=int(random.choice(vlrs_lists[11])),
    #     vlr_m_msisdn=int(msisdn_list[11][1]),
    #
    #     is_sms=True,
    #     sms_f_inner_filter_type="incoming",
    #     sms_f_msisdns=[msisdn_list[11][0]],
    #     sms_f_weight=10,
    #     sms_f_ttl=5,
    #     sms_m_imsi=imsi_list[11],
    #     sms_m_msisdn_in=msisdn_list[11][0],
    #     sms_m_msisdn_out=msisdn_list[11][1],
    #
    #     is_geo=True,
    #     geo_f_lac_cells=lac_cell_lists[11],
    #     geo_f_weight=10,
    #     geo_f_ttl=5,
    #     geo_m_imsi=imsi_list[11],
    #     geo_m_msisdn=int(msisdn_list[11][1]),
    #     geo_m_lac=extract_lac(lac_cell_lists[11][random_indexes[11]]),
    #     geo_m_cell=extract_cell(lac_cell_lists[11][random_indexes[11]]),
    #
    #     latency_after_first_message=5,
    #
    #     expected_imsi=imsi_list[11],
    #     expected_msisdn=int(msisdn_list[11][1]),
    #     expected_result=True,
    #     expected_sum=20,
    # ),

    #проверки GEOZONE
    test_data_generator(
        description="Позитивная проверка. Filters: Geozone w=10. Trigger message: -. JSON. "
                    "Total filter weight: 20. Total messages weight: 20.",
        filter_out_type="JSON",
        filter_total_weight=20,
        filter_is_smart_trigger=True,

        first_message="geozone",
        geozone_m_count=2,

        trigger_message_type=None,

        is_geozone=True,
        geozone_f_inner_filter_type=['eventType', 'geofenceType', 'zoneId'],
        geozone_f_weight=10,
        geozone_f_ttl=5,
        geozone_m_msisdn=int(msisdn_list[12][0]),
        geozone_m_imsi=imsi_list[12],
        geozone_f_file_content_event_type=content_event_type_list[0],
        geozone_f_file_content_geofence_type=content_geofence_type_list[0],
        geozone_f_file_content_zone_id=content_zone_id_list[0],
        geozone_m_event_type=content_event_type_list[0][random_indexes_event_type[0]],  # рандомный индекс
        geozone_m_geofence_type=content_geofence_type_list[0][random_indexes_geofence_type[0]],
        geozone_m_zone_id=content_zone_id_list[0][random_indexes_zone_id[0]],

        expected_result=False,
    ),

    test_data_generator(
        description="Позитивная проверка. Filters: Clickstream w=10, Geozone w=10. Trigger message: Geozone. JSON. "
                    "Total filter weight: 20. Total messages weight: 20.",
        filter_out_type="JSON",
        filter_total_weight=20,
        filter_is_smart_trigger=True,

        first_message="clickstream",
        second_message="geozone",

        clickstream_m_count=1,
        geozone_m_count=1,

        trigger_message_type="geozone",

        is_clickstream=True,
        url_f_urls=urls_lists[3],
        url_f_weight=10,
        url_f_ttl=5,
        url_m_imsi=imsi_list[13],
        url_m_msisdn=int(msisdn_list[13][1]),
        url_m_host=random.choice(urls_lists[3]),

        is_geozone=True,
        geozone_f_inner_filter_type=['eventType', 'geofenceType', 'zoneId'],
        geozone_f_weight=10,
        geozone_f_ttl=5,
        geozone_m_msisdn=int(msisdn_list[13][0]),
        geozone_m_imsi=imsi_list[13],
        geozone_f_file_content_event_type=content_event_type_list[1],
        geozone_f_file_content_geofence_type=content_geofence_type_list[1],
        geozone_f_file_content_zone_id=content_zone_id_list[1],
        geozone_m_event_type=content_event_type_list[1][random_indexes_event_type[1]],  # рандомный индекс
        geozone_m_geofence_type=content_geofence_type_list[1][random_indexes_geofence_type[1]],
        geozone_m_zone_id=content_zone_id_list[1][random_indexes_zone_id[1]],

        expected_imsi=imsi_list[13],
        expected_msisdn=int(msisdn_list[13][0]),
        expected_result=True,
        expected_sum=20,
    ),

]
