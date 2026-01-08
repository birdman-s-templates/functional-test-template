import allure
import datetime
import logging
import pytest
import time

import config
from common.bindings.auth import auth
from common.bindings.kerberos import Kerberos
from common.bindings.vault import Vault
from common.configs.vault_config import VaultCorp
from common.utils.checks import is_gitlab_runner
from common.utils.state import State
from common.utils.tags import MARKER_SPECS


# Фикстуры
@pytest.fixture(scope="session", autouse=True)
def vault_setup():
    if State.vault is None:
        vault = Vault()
        State.vault = vault


@pytest.fixture(scope="session", autouse=True)
def state_setup(vault_setup):

    # sa0000datariveradmin
    State.admin_user = State.vault.get_secret_value(
        path=VaultCorp.sa0000datariveradmin,
        key="user"
    )
    State.admin_password = State.vault.get_secret_value(
        path=VaultCorp.sa0000datariveradmin,
        key="pass"
    )

    if config.env == 'test1':

        # sa0000datarivertest
        State.test_user = State.vault.get_secret_value(
            path=VaultCorp.sa0000datarivertest,
            key="user"
        )
        State.test_password = State.vault.get_secret_value(
            path=VaultCorp.sa0000datarivertest,
            key="pass"
        )

    elif config.env == 'dev1':
        State.client_id = State.vault.get_secret_value(
            path=VaultCorp.dr_sa_dev_isso,
            key="client_id"
        )
        State.client_secret = State.vault.get_secret_value(
            path=VaultCorp.dr_sa_dev_isso,
            key="client_secret"
        )

    elif config.env == 'prep1':
        State.client_id = State.vault.get_secret_value(
            path=VaultCorp.keycloak_sa_prep,
            key="iam_client_sa_id"
        )
        State.client_secret = State.vault.get_secret_value(
            path=VaultCorp.keycloak_sa_prep,
            key="iam_client_sa_secret"
        )

    else:
        raise ValueError("Отсутствует значение окружения в config")

    State.is_gitlab_runner = is_gitlab_runner()


@pytest.fixture(scope="session", autouse=False)
def kerberos_setup(state_setup):
    kerberos = None

    if config.env == 'prep1':
        kerberos = Kerberos(keytab_base64=State.keytab_base64_prep)
        kerberos.kinit()

    if config.env == 'test1':
        kerberos = Kerberos(
            user="sa0000datarivertest",
            keytab_base64=State.keytab_base64_test)
        kerberos.kinit()

    yield

    if not State.is_gitlab_runner:
        # При параллельном запуске уничтожение случится, как закончится первый тест финальной партии, оттого задержка
        time.sleep(10)
        kerberos.kdestroy()


@pytest.fixture(scope="session", autouse=False)
def postgres_setup(vault_setup):
    State.shrinker_backend_db_host = config.POSTGRES_HOST
    State.shrinker_backend_db_name = config.POSTGRES_SHRINKER_BACKEND_DB
    State.shrinker_backend_db_port = config.POSTGRES_SHRINKER_BACKEND_PORT
    State.shrinker_backend_db_user = State.admin_user
    State.shrinker_backend_db_password = State.admin_password


@pytest.fixture(scope="module", autouse=False)
def datariver_api_setup():
    logger = logging.getLogger('datariver_api_setup')
    logger.setLevel(logging.INFO)

    State.datariver_token = auth(client_id=State.client_id, client_secret=State.client_secret)
    State.datariver_token_expires_in = datetime.datetime.now() + datetime.timedelta(seconds=600)
    logger.info(f"State.datariver_token_expires_in: {State.datariver_token_expires_in}")


@pytest.fixture(scope="function", autouse=False)
def check_token():

    logger = logging.getLogger('check_token')
    logger.setLevel(logging.INFO)

    now = datetime.datetime.now()

    logger.info(f"Текущее время: {now}")
    logger.info(f"State.datariver_token_expires_in: {State.datariver_token_expires_in}")

    if now >= State.datariver_token_expires_in:

        logger.info(f"Время обновления токена")

        State.datariver_token = auth(client_id=State.client_id, client_secret=State.client_secret)
        State.datariver_token_expires_in = datetime.datetime.now() + datetime.timedelta(seconds=600)

    yield


# Хуки
def pytest_assertrepr_compare(op, left, right):
    """
    Хук pytest, который переопределяет вывод всех asserts
    """

    if op == "==":
        return [f"Сравнение равенства объектов. Ожидаемый рез-т: {left} ({type(left)}), фактический рез-т: {right} ({type(left)})"]

    elif op == "!=":
        return [f"Сравнение неравенства объектов. Ожидаемый рез-т: {left} ({type(left)}), фактический рез-т: {right} ({type(left)})"]

    elif isinstance(right, list) and op == "in":
        return [f"Проверка наличия целевого значения в списке. Искомое значение: {left}, список: {right}"]

    elif isinstance(right, list) and op == "not in":
        return [f"Проверка отсутствия целевого значения в списке. Искомое значение: {left}, список: {right}"]

    else:
        return None


def pytest_configure(config):
    config.addinivalue_line("markers", "allure_id(id): External ID for TestOps")
    for m in MARKER_SPECS:
        config.addinivalue_line("markers", f"{m}(value): custom marker for TestOps field {m}")


def pytest_collection_modifyitems(session, config, items):
    for item in items:

        # Убираем usefixtures с тестового узла
        item.own_markers[:] = [
            mark for mark in item.own_markers
            if mark.name != "usefixtures"
        ]
        # Если usefixtures ставился на уровне класса, то и у родителя тоже:
        parent = getattr(item, "parent", None)
        if parent is not None and hasattr(parent, "own_markers"):
            parent.own_markers[:] = [
                mark for mark in parent.own_markers
                if mark.name != "usefixtures"
            ]


def pytest_runtest_call(item):
    if hasattr(item, "callspec"):

        # Allure Title
        for data in item.callspec.params.values():
            if isinstance(data, dict) and "description" in data:
                allure.dynamic.title(data["description"])
                break

        # Кастомные поля для Allure
        for name, spec in MARKER_SPECS.items():
            marker = item.get_closest_marker(name)
            value = marker.args[0] if (marker and marker.args) else spec.get("default")
            if value:
                allowed = spec.get("allowed")
                if allowed and value not in allowed:
                    raise ValueError(
                        f"[Allure Label Error] Некорректное значение для '{name}': '{value}'. "
                        f"Допустимые: {allowed}"
                    )
                allure.dynamic.label(name, value)
