import datetime
import pytz
import random

from common.utils import random_utils
from tests.shrinker_domain.smart_triggers.testutils import test_data_generator
from tests.shrinker_domain.geo.testutils import generate_lac_cell_list, extract_lac, extract_cell

# - - - - - - - - - - - - - - - - - - - - - - - - - -
# константы

test_counter = 2

# common_data
imsi_list = [random_utils.generate_imsi() for _ in range(test_counter)]
imei_list = [str(random_utils.generate_imei()) for _ in range(test_counter)]
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

# - - - - - - - - - - - - - - - - - - - - - - - - - -
# тестовые данные

test_data = [

    test_data_generator(
        description="Позитивная проверка (from STOP to RUN). Filters: SMS (in) w=10, Geo w=10. Trigger message: Geo. "
                    "JSON. Total filter weight: 20. Total messages weight: 20.",
        filter_out_type="JSON",
        filter_total_weight=20,
        filter_is_smart_trigger=True,
        filter_range=[
            int((datetime.datetime.now() - datetime.timedelta(hours=random.randint(1, 12))).timestamp()) * 1000,
            int((datetime.datetime.now() + datetime.timedelta(seconds=5)).timestamp()) * 1000,
        ],

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
        expected_sum=20
    ),

]
