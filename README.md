## Обзор

Этот репозиторий содержит пример структурированного функционального тестирования (domain layer).

- __Modular Test Architecture__ - разделение на домены (calls, geo, roaming, sms, url)
- __Data-Driven Testing__ - использование параметризованных тестов с разделенными test data
- __Clean Code__ - утилиты для тестирования и фикстуры в отдельных модулях

## Архитектура проекта

### Структура

```javascript
tests/
└── shrinker_domain/
    ├── calls/
    ├── geo/
    ├── roaming/
    ├── sms/
    └── url/
```

### Каждый домен содержит:

- __test_*.py__ - основной файл с test cases
- __conftest.py__ - fixtures и конфигурация для домена (содержит только путь к тестовым данным, все остальные фикстуры и хуки являются общими и находятся в conftest-файле из корня проекта)
- __testutils.py__ - утилиты и helper функции
- __test_data/__ - параметризованные данные для тестов

### Стек:
- Python 3.x
- pytest
- PostrgeSQL
- Apache Kafka

## Подход к тестированию

### 1. __Организация test data__

Test данные разделены по сценариям в отдельные модули:

- `data_test_*_base.py` - базовые сценарии (контентные проверки)
- `data_test_*_from_pause_to_run.py` - активация
- `data_test_*_from_run_to_pause.py` - деактивация 
- `data_test_*_from_stop_to_run.py` - новый запуск после завершения
- `data_test_*_to_run_by_time.py` - временные триггеры запуска
- `data_test_*_to_stop_by_time.py` - временные триггеры остановки

### 2. __Параметризованное тестирование__

Параметризация через хук pytest_generate_tests:
```javascript
def pytest_generate_tests(metafunc):
    """
    Универсальный хук для обработки тестовых данных по всем доменам.
    Автоматически определяет путь на основе расположения модуля теста.
    """

    # Определяем путь на основе директории текущего модуля
    test_module_dir = os.path.dirname(os.path.abspath(metafunc.module.__file__))
    domain_name = os.path.basename(test_module_dir)
    path = f"tests/shrinker_domain/{domain_name}/test_data"

    for fixture in metafunc.fixturenames:
        if fixture.startswith('data_'):
            tests = load_tests(name_of_data_file=fixture, path=path)
            tests = xfail_handler(tests=tests)
            tests = tags_handler(tests=tests)
            metafunc.parametrize(fixture, tests)
```

### 3. __Утилиты и вспомогательные функции__

Каждый домен имеет `testutils.py` для:

- Подготовки test data
- Выполнения утверждений
- Работы с доменоспецифичными операциями

### 4. __Модульные fixtures__

Использование `conftest.py` для:

- Инициализации shared ресурсов
- Setup/teardown операций
- Доменоспецифичных конфигураций


## Структура тест-кейса

Типичный тест имеет структуру:

1. __Setup__ - подготовка данных (через test data и fixtures)
2. __Execution__ - вызов тестируемой функции
3. __Assertion__ - проверка результата (через testutils)
4. __Cleanup__ - очистка (в fixtures teardown)

## Практики, используемые в проекте

1. __Data-Driven Tests__ - разделение logic от data
2. __DRY Principle__ - переиспользование через utilities и fixtures
3. __Test Independence__ - каждый тест может выполняться независимо
4. __Descriptive Naming__ - понятные названия тестов и файлов
5. __Configuration Management__ - конфигурация через conftest.py
