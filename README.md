# effective-palm-tree

Четыре независимые задачи:

- `task1_fastapi` — FastAPI-эндпоинт `/db/version` с пулом `asyncpg` и зависимостью `get_pg_connection`.
- `task2_scraper` — асинхронный GitHub-скраппер с лимитами MCR/RPS и подсчётом коммитов авторов.
- `task3_clickhouse` — сохранение метрик из скраппера в ClickHouse батчами через `aiochclient`.
- `task4_sql` — запрос ClickHouse для почасовых просмотров по фразам.

## Использование

- Требуется Python 3.12+ и менеджер пакетов `uv`.
- Установка зависимостей задачи: `uv pip install -e ".[task1]"` (аналогично `task2`, `task3`).
- Линтер: `uv run ruff check`.

## Конфигурация

Переменные окружения перечислены в коде каждой задачи (`settings.py`, `config.py` и т.п.). Секреты и параметры читаются через `pydantic-settings`, поддерживается `.env`.
