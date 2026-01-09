from common.utils.fixtures_and_hooks import load_tests, xfail_handler, tags_handler


def pytest_generate_tests(metafunc):
    """
    Хук pytest, который при инициализации фикстур, смотрит есть ли у теста фикстуры, начинающиеся с data_, если
    находит - импортирует фикстуру, как пакет
    """

    path = "tests/shrinker_domain/roaming/test_data"

    # Обработка тестовых данных
    for fixture in metafunc.fixturenames:
        if fixture.startswith('data_'):

            # Передаем название параметра в функцию, которая парсит файл
            tests = load_tests(name_of_data_file=fixture, path=path)

            # Обрабатываем xfails
            tests = xfail_handler(tests=tests)

            # Обрабатываем tags и custom labels для Allure
            tests = tags_handler(tests=tests)

            metafunc.parametrize(fixture, tests)
