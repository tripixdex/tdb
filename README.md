# TDB — Terminal Database Builder + GUI (DuckDB)

Проект: импорт CSV → построение БД DuckDB → проверка PK/FK → SQL-запросы  
Платформа: macOS, Python, DuckDB, SwiftUI GUI.

## Что умеет
- CLI: `build`, `validate`, `sql`, `tables`, `describe`, `import-csv`
- GUI (SwiftUI): таблицы слева, вкладки Preview / Schema / SQL / Validate, пагинация Preview, индикатор Busy
- Контроль качества данных: PK уникальность + FK orphan checks
- CI: GitHub Actions выполняет build + validate

## Быстрый старт (CLI)
```bash
python -m pip install -e .
python -m tdb build data/raw --db build/school.duckdb --profile .tdb_profile.json
python -m tdb validate --db build/school.duckdb --profile .tdb_profile.json
python -m tdb sql "SELECT COUNT(*) FROM customer" --db build/school.duckdb
Быстрый старт (GUI)
Открыть gui/TDBGUI.xcodeproj

Run

Нажать Project… и выбрать папку репозитория tdb

Нажать Build, затем Validate

Профиль схемы (.tdb_profile.json)
Профиль описывает:

pk: колонки первичного ключа

fks: связи (cols → ref_table/ref_cols)

По профилю выполняются build и validate.

Демо-сценарий для защиты
Build (создание БД)

Validate (PK/FK, orphan=0)

Preview (пагинация 50/200/1000)

Schema (структура таблицы)

SQL (запросы на JOIN и агрегаты)
