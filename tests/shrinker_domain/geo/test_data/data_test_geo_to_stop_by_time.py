import datetime
import pytz
import random

from tests.shrinker_domain.geo.testutils import test_data_generator, generate_lac_cell_list, extract_lac, extract_cell

# - - - - - - - - - - - - - - - - - - - - - - - - - -
# константы

lac_cell_lists = [generate_lac_cell_list() for _ in range(6)]

random_indexes = [random.randint(0, 4) for _ in range(len(lac_cell_lists))]

timezone = pytz.timezone("Europe/Moscow")
current_time = int((datetime.datetime.now(timezone) - datetime.timedelta(seconds=random.randint(90, 300))).timestamp())

# - - - - - - - - - - - - - - - - - - - - - - - - - -
# тестовые данные

test_data = [

    test_data_generator(
        description="Негативная проверка: GEO, статус: all, JSON, to STOP by time",
        out_type="JSON",
        filter_event_type=["Entry", "Standing"],
        file_content=lac_cell_lists[0],
        lac=extract_lac(lac_cell_lists[0][random_indexes[0]]),
        cell=extract_cell(lac_cell_lists[0][random_indexes[0]]),
        filter_range=[
            int((datetime.datetime.now() - datetime.timedelta(hours=random.randint(1, 12))).timestamp()) * 1000,
            int((datetime.datetime.now() + datetime.timedelta(seconds=100)).timestamp()) * 1000,
        ],
        expected_result=False
    ),

]
