import logging
import pytest

from common.bindings.databases import postgres
from common.bindings.shrinker.shrinker_backend import filter_controller
from common.utils.state import State


@pytest.fixture(scope="function", autouse=True)
def clear_test_data():

    logger = logging.getLogger('clear_test_data')
    logger.setLevel(logging.INFO)

    yield

    if State.domain_filters:

        logger.info(f"Список фильтров на удаление: {State.domain_filters}")

        for domain_filters_id in State.domain_filters:
            response = filter_controller.delete_filter(
                token=State.datariver_token,
                id=domain_filters_id
            )
            logger.info(f"response for deleting: {response.json()}")

            # Удаляем записи из таблицы campaigns
            postgres.crud_query(
                text_query=f"delete from app.filter_stream fs2 where fs2.filter_hash='{domain_filters_id}'",
                host=State.shrinker_backend_db_host,
                port=State.shrinker_backend_db_port,
                database_name=State.shrinker_backend_db_name,
                user=State.shrinker_backend_db_user,
                password=State.shrinker_backend_db_password
            )

        logger.info(f"Список фильтров перед обнулением: {State.domain_filters}")
        State.domain_filters = []
