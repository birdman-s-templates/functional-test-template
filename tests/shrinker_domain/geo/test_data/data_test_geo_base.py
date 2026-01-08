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
        description="Позитивная проверка: GEO, статус: all, JSON",
        out_type="JSON",
        filter_event_type=["Entry", "Standing"],
        file_content=lac_cell_lists[0],
        lac=extract_lac(lac_cell_lists[0][random_indexes[0]]),
        cell=extract_cell(lac_cell_lists[0][random_indexes[0]]),
        expected_result=True
    ),

    test_data_generator(
        description="Позитивная проверка: GEO, статус: all, CSV",
        out_type="CSV",
        filter_event_type=["Entry", "Standing"],
        file_content=lac_cell_lists[1],
        lac=extract_lac(lac_cell_lists[1][random_indexes[1]]),
        cell=extract_cell(lac_cell_lists[1][random_indexes[1]]),
        expected_result=True
    ),

    test_data_generator(
        description="Позитивная проверка: GEO, статус: all, THRIFT",
        out_type="THRIFT",
        filter_event_type=["Entry", "Standing"],
        file_content=lac_cell_lists[2],
        lac=extract_lac(lac_cell_lists[2][random_indexes[2]]),
        cell=extract_cell(lac_cell_lists[2][random_indexes[2]]),
        expected_result=True
    ),

    test_data_generator(
        description="Позитивная проверка: GEO, статус: Entry, JSON",
        out_type="JSON",
        filter_event_type=["Entry"],
        file_content=lac_cell_lists[3],
        lac=extract_lac(lac_cell_lists[3][random_indexes[3]]),
        cell=extract_cell(lac_cell_lists[3][random_indexes[3]]),
        expected_result=True
    ),

    test_data_generator(
        description="Позитивная проверка: GEO, статус: Standing, JSON",
        out_type="JSON",
        filter_event_type=["Standing"],
        file_content=lac_cell_lists[4],
        lac=extract_lac(lac_cell_lists[4][random_indexes[4]]),
        cell=extract_cell(lac_cell_lists[4][random_indexes[4]]),
        expected_result=True
    ),

    test_data_generator(
        description="Негативная проверка: GEO (нерелевантные координаты), статус: all, JSON",
        out_type="JSON",
        filter_event_type=["Entry", "Standing"],
        file_content=lac_cell_lists[5],
        lac=random.randint(1, 1000),
        cell=random.randint(1, 1000),
        expected_result=False
    ),

]
