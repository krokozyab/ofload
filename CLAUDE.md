# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

**ckh_dw** — Kotlin/JVM проект для работы с Oracle Database через JDBC и UCP (Universal Connection Pool). Используется Oracle Autonomous Database (ADB) с TLS-подключением.

## Build & Run

- **Сборка:** `./gradlew build`
- **Тесты:** `./gradlew test` (JUnit 5 через `useJUnitPlatform()`)
- **Запуск:** `./gradlew run` (если настроен application plugin) или через IDE — точка входа: `app.UCPDataSource.main()`

## Architecture

- Kotlin 2.2.20, Gradle (Kotlin DSL), стиль кода — official
- Единственный модуль, исходники в `src/main/kotlin/`
- `UCPDataSource` — основной класс, управляет пулом соединений Oracle UCP и выполняет тестовое подключение
- Подключение к Oracle ADB через THIN драйвер (TCPS, порт 1522). Для THICK режима с wallet нужно раскомментировать альтернативный URL и настроить `TNS_ADMIN`

## Dependencies

- Oracle JDBC + UCP (пока не добавлены в `build.gradle.kts` — потребуется добавить `oracle-jdbc` и `oracle-ucp` зависимости)
- JUnit 5 для тестов

## Notes

- Учётные данные БД захардкожены как placeholder'ы (`USER_NAME`/`PASSWORD`) — при реальном использовании вынести в переменные окружения или конфигурацию
- Пакет объявлен как `app`