# Fusion → Oracle ATP ETL Pipeline — Design Document

## Обзор

ETL-пайплайн для инкрементальной загрузки данных из Oracle Fusion в Oracle Autonomous Database (ATP).
Источник читается через ofjdbc JDBC-драйвер, приёмник — Oracle ATP через UCP с wallet (mTLS).

## Архитектура

```
┌──────────────┐     ofjdbc/JDBC      ┌──────────────────────────────┐
│ Oracle Fusion│ ◄───SELECT WHERE───  │         ETL Pipeline         │
│  (источник)  │    wm > :last_wm     │                              │
└──────────────┘                      │  1. Read watermark           │
                                      │  2. SELECT from Fusion       │
┌──────────────┐     Oracle JDBC      │  3. Batch INSERT → STG       │
│  Oracle ATP  │ ◄───MERGE INTO───── │  4. MERGE STG → Target       │
│  (приёмник)  │                      │  5. Update watermark         │
└──────────────┘                      └──────────────────────────────┘
```

## Модель данных

### ETL_WATERMARK — управляющая таблица

Хранит состояние каждой загрузки. Одна строка на каждый target.

| Колонка | Тип | Описание |
|---------|-----|----------|
| TARGET_NAME | VARCHAR2(128) PK | Имя target (например `orders`) |
| WM_COLUMN | VARCHAR2(128) | Колонка watermark в источнике (например `UPDATED_AT`) |
| LAST_WM | VARCHAR2(64) | Значение watermark после последнего успешного запуска |
| LAST_RUN_STARTED | TIMESTAMP | Время начала последнего запуска |
| LAST_RUN_FINISHED | TIMESTAMP | Время завершения последнего запуска |
| LAST_STATUS | VARCHAR2(16) | `RUNNING` / `OK` / `FAILED` |
| ROWS_LOADED | NUMBER | Количество загруженных строк |
| ERROR_MESSAGE | VARCHAR2(4000) | Текст ошибки (при FAILED) |

### Staging tables (STG_*)

- Отдельная staging-таблица на каждый target
- Структура колонок совпадает с target-таблицей
- Без PK, FK, индексов — буферная таблица
- TRUNCATE перед каждой загрузкой

### Target tables (T_*)

- Финальные таблицы с PK и constraints
- Обновляются через MERGE из staging

## Flow загрузки одного target

```
1. SELECT last_wm FROM ETL_WATERMARK WHERE target_name = ?
2. MERGE INTO ETL_WATERMARK: status = 'RUNNING'
3. COMMIT (фиксируем статус RUNNING)

4. TRUNCATE TABLE STG_<target>   (Oracle DDL — implicit commit)
5. SELECT ... FROM <fusion_table> WHERE <wm_column> > :last_wm
6. Batch INSERT в STG_<target>   (порциями по 5000 строк)
7. COMMIT

8. SELECT MAX(<wm_column>) FROM STG_<target>   → new_wm
9. MERGE INTO T_<target> USING STG_<target> ON (key_columns)
     WHEN MATCHED THEN UPDATE ...
     WHEN NOT MATCHED THEN INSERT ...
10. UPDATE ETL_WATERMARK: last_wm = new_wm, status = 'OK', rows_loaded = N
11. COMMIT

При ошибке на шагах 4-11:
  - ROLLBACK текущей транзакции
  - UPDATE ETL_WATERMARK: status = 'FAILED', error_message = ...
    (в отдельном соединении, т.к. текущее в broken state)
```

## Watermark-стратегия

### Принцип работы

Watermark — значение колонки (обычно `LAST_UPDATE_DATE` или `UPDATED_AT`),
которое отмечает «до куда мы уже загрузили». Каждый следующий запуск
читает только строки с watermark > last_wm.

### Первый запуск (full load)

При первом запуске в ETL_WATERMARK нет записи для target.
Используется `initialWm` из конфига (например `1970-01-01T00:00:00`),
что фактически загружает все данные.

### Инкрементальные запуски

Каждый последующий запуск читает только новые/обновлённые строки.
MERGE гарантирует идемпотентность — повторная загрузка тех же строк
просто обновит их.

### Lookback window (планируется)

Для компенсации задержек в источнике (batch jobs, timezone drift, длинные транзакции)
watermark сдвигается назад на `lookback_hours`:

```
effective_wm = last_wm - lookback_interval
```

Это безопасно благодаря MERGE — дубликаты просто перезаписываются.
Рекомендуемые значения: 2-4 часа, для payroll/HR до 24 часов.

## Конфигурация

### targets.json

```json
{
  "targets": [
    {
      "name": "orders",
      "stagingTable": "STG_ORDERS",
      "targetTable": "T_ORDERS",
      "watermarkColumn": "UPDATED_AT",
      "watermarkType": "TIMESTAMP",
      "keyColumns": ["ORDER_ID"],
      "columns": ["ORDER_ID", "CUSTOMER_ID", "AMOUNT", "UPDATED_AT"],
      "sourceQuery": "SELECT ORDER_ID, CUSTOMER_ID, AMOUNT, UPDATED_AT FROM ORDERS WHERE UPDATED_AT > ?",
      "initialWm": "1970-01-01T00:00:00"
    }
  ]
}
```

| Поле | Описание |
|------|----------|
| name | Уникальный идентификатор target, используется как ключ в ETL_WATERMARK |
| stagingTable | Имя staging-таблицы в ATP |
| targetTable | Имя финальной таблицы в ATP |
| watermarkColumn | Колонка watermark (должна быть и в source, и в staging/target) |
| watermarkType | `TIMESTAMP` или `NUMBER` |
| keyColumns | Ключевые колонки для MERGE ON clause |
| columns | Полный список колонок (порядок должен совпадать с SELECT в sourceQuery) |
| sourceQuery | SQL-запрос к Fusion с `?` placeholder для watermark |
| initialWm | Начальное значение watermark для первого запуска |

### Переменные окружения

| Переменная | Обязательная | Описание |
|------------|:---:|----------|
| DB_USER | да | Oracle ATP username |
| DB_PASSWORD | да | Oracle ATP password |
| DB_CONNECT_STRING | да | TNS alias (например `ubeb4ocafu6uaz8o_medium`) или full connect string |
| TNS_ADMIN | нет | Путь к распакованному wallet (для mTLS) |
| SOURCE_DRIVER_CLASS | да | FQCN JDBC-драйвера источника (например `my.jdbc.wsdl_driver.WsdlDriver`) |
| SOURCE_URL | да | JDBC URL источника |
| SOURCE_USER | нет | Username для источника |
| SOURCE_PASSWORD | нет | Password для источника |

## Структура проекта

```
src/main/kotlin/app/
├── Main.kt                    — entry point, загрузка конфига, запуск pipeline
├── config/
│   └── PipelineConfig.kt      — @Serializable data classes + JSON loader
├── db/
│   ├── OracleDs.kt            — UCP connection pool к Oracle ATP (singleton)
│   └── SourceDs.kt            — DriverManager wrapper для ofjdbc
└── etl/
    ├── Pipeline.kt            — оркестрация: итерация по targets, error handling
    ├── StagingLoader.kt       — TRUNCATE staging → batch INSERT из source
    ├── TargetMerger.kt        — генерация и выполнение MERGE SQL
    └── WatermarkStore.kt      — read/beginRun/finishRun/failRun для ETL_WATERMARK

src/main/resources/
├── targets.json               — конфигурация targets
└── ddl/
    ├── 00_etl_watermark.sql   — DDL управляющей таблицы
    ├── 10_stg_orders.sql      — DDL staging (пример)
    └── 20_t_orders.sql        — DDL target (пример)

src/test/kotlin/app/etl/
└── TargetMergerTest.kt        — unit-тест генерации MERGE SQL
```

## Сборка и запуск

```bash
# Сборка + тесты
./gradlew build

# Запуск
DB_USER="..." \
DB_PASSWORD="..." \
DB_CONNECT_STRING="ubeb4ocafu6uaz8o_medium" \
TNS_ADMIN="/path/to/wallet" \
SOURCE_DRIVER_CLASS="my.jdbc.wsdl_driver.WsdlDriver" \
SOURCE_URL="jdbc:wsdl://..." \
./gradlew run
```

## DDL: подготовка базы

Перед первым запуском выполнить DDL-скрипты из `src/main/resources/ddl/` в Oracle ATP:

1. `00_etl_watermark.sql` — управляющая таблица (один раз)
2. `10_stg_orders.sql` — staging-таблица для каждого target
3. `20_t_orders.sql` — target-таблица для каждого target

## Добавление нового target

1. Создать DDL для `STG_<NAME>` и `T_<NAME>` в ATP
2. Добавить запись в `targets.json`
3. Перезапустить приложение

Watermark-запись в ETL_WATERMARK создаётся автоматически при первом запуске (через MERGE).

## Мониторинг

Текущий статус всех загрузок:

```sql
SELECT TARGET_NAME, LAST_STATUS, ROWS_LOADED,
       LAST_RUN_STARTED, LAST_RUN_FINISHED,
       LAST_WM, ERROR_MESSAGE
FROM ETL_WATERMARK
ORDER BY TARGET_NAME;
```

Проблемные загрузки:

```sql
SELECT * FROM ETL_WATERMARK
WHERE LAST_STATUS = 'FAILED'
   OR LAST_RUN_STARTED < SYSTIMESTAMP - INTERVAL '2' HOUR;
```

## Планируемые улучшения

- **REST API** — HTTP-сервер для запуска загрузок по REST (для OIC-интеграции)
- **Health endpoint** — GET /health для OCI health probes
- **Lookback window** — настраиваемый lookback per target в конфиге
- **Параллелизм** — параллельная загрузка нескольких targets
- **Логирование** — SLF4J вместо println для структурированных логов
