# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

**ckh_dw** — ETL-пайплайн для инкрементальной загрузки данных из Oracle Fusion в Oracle ATP.
Источник — ofjdbc JDBC-драйвер (проект `/Users/sergeyrudenko_1_2/projects/ofjdbc`).
Приёмник — Oracle Autonomous Database через UCP с wallet (mTLS).

Подробный design document: `docs/design.md`

## Build & Run

```bash
./gradlew build                          # сборка + тесты
./gradlew test                           # только тесты
./gradlew test --tests "*TargetMerger*"  # один тест
./gradlew run                            # запуск (нужны env vars)
```

Kotlin 2.2.20, kotlinx-serialization, Gradle (Kotlin DSL), JUnit 5.

## Architecture

```
app/Main.kt              → entry point, загрузка targets.json
app/config/               → @Serializable data classes для конфига
app/db/OracleDs.kt        → UCP pool к Oracle ATP (singleton)
app/db/SourceDs.kt        → DriverManager wrapper для ofjdbc
app/etl/Pipeline.kt       → оркестрация per target
app/etl/StagingLoader.kt  → TRUNCATE STG → batch INSERT из source
app/etl/TargetMerger.kt   → MERGE STG → target
app/etl/WatermarkStore.kt → CRUD по ETL_WATERMARK
```

**ETL flow per target:** read watermark → SELECT from Fusion (with wm bind) → batch INSERT into STG_* → MERGE STG → T_* → update watermark.

## Configuration

- **targets.json** (`src/main/resources/`) — список targets с sourceQuery, keyColumns, watermark config
- **Env vars:** `DB_USER`, `DB_PASSWORD`, `DB_CONNECT_STRING`, `TNS_ADMIN`, `SOURCE_DRIVER_CLASS`, `SOURCE_URL`, `SOURCE_USER`, `SOURCE_PASSWORD`
- **DDL скрипты** в `src/main/resources/ddl/` — применить в ATP перед первым запуском

## Key Design Decisions

- Staging table per target (не общая) — независимость перезапусков, параллелизм
- Watermark хранится в ATP (таблица ETL_WATERMARK), не в файлах
- MERGE гарантирует идемпотентность — безопасно перезапускать
- TRUNCATE staging перед каждой загрузкой (DDL, implicit commit)
- Ошибки пишутся в ETL_WATERMARK через отдельное соединение
