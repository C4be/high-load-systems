# Лабораторная работа №4 - Trino

## Цель работы

Поработать с инструментом `Trino`

## Ход работы

### 1. Postgres

Проверка загрузки данных

![pg_connect](materials/imgs/pg_connect.png)

Посчитаем кол-во строк, которые получились

```sql
SELECT count(*) FROM mock_data;
```

Получаем `5к` строк, ровно половина из всех файлов.

![cnt](materials/imgs/pg_cnt.png)

✅ Данные успешно загружены!

### 2. ClickHouse

Проверим состояние загрузки в ClickHouse

![ch_data](materials/imgs/ch_data.png)

✅ Как видно, остальные `5k` строк успешно загружены!

![tables](materials/imgs/ch_tables.png)

### 3. Trino

Запуск скриптов

```bash
docker exec -it trino_service /bin/bash -c 'trino --server http://localhost:8080 -f "/scripts/snowflake.sql" && trino --server http://localhost:8080 -f "/scripts/reports.sql"'
```






