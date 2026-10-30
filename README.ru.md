# Коннекторы Trino для Greenplum и Apache Doris

Два плагина-коннектора для Trino 470:

- `trino-greenplum` — Greenplum 6 и 7. Построен на PostgreSQL-коннекторе Trino (маппинг типов, pushdown,
  статистика) и добавляет DDL и метаданные, специфичные для Greenplum.
- `trino-doris` — Apache Doris 2.1+ (проверен на 3.0). Работает через MySQL-протокол frontend-узла Doris,
  со своим маппингом типов и DDL.

## Сборка

Нужны JDK 23 или новее, Maven 3.9 и Docker (только для интеграционных тестов).

```bash
mvn clean package -DskipTests
```

Архивы плагинов: `trino-greenplum/target/trino-greenplum-470.zip` и `trino-doris/target/trino-doris-470.zip`.
Распакуйте архив в каталог `plugin` сервера Trino 470 и перезапустите сервер.

Плагины собраны против SPI Trino 470: версия плагина должна совпадать с версией сервера. Для другого релиза
Trino поменяйте версию родителя и версию проекта в корневом `pom.xml`.

## Greenplum

`etc/catalog/greenplum.properties`:

```properties
connector.name=greenplum
connection-url=jdbc:postgresql://gp-coordinator:5432/database
connection-user=trino
connection-password=secret
```

Все настройки PostgreSQL-коннектора (`postgresql.*`, например `postgresql.array-mapping`) и его сессионные
свойства тоже действуют. Как и PostgreSQL-коннектору, серверу нужен плагин Trino `geospatial` (он входит в
стандартную поставку Trino).

| Свойство | Описание | По умолчанию |
|---|---|---|
| `greenplum.include-partition-children` | Показывать листовые партиции партиционированных таблиц как отдельные таблицы. Скрытая партиция по-прежнему доступна по имени | `false` |

### Свойства таблиц

| Свойство | Описание |
|---|---|
| `distribution_policy` | `HASH`, `RANDOM` или `REPLICATED`. Если не задано, действует умолчание Greenplum |
| `distributed_by` | Колонки ключа распределения, подразумевает `HASH` |
| `appendoptimized` | Создать append-optimized таблицу |
| `orientation` | `ROW` или `COLUMN`, требует `appendoptimized = true` |
| `compresstype` | Тип сжатия, например `zstd` или `zlib`, требует `appendoptimized = true` |
| `compresslevel` | Уровень сжатия, требует `appendoptimized = true` |

```sql
CREATE TABLE greenplum.public.orders_ao
WITH (
    distributed_by = ARRAY['orderkey'],
    appendoptimized = true,
    orientation = 'COLUMN',
    compresstype = 'zstd',
    compresslevel = 5)
AS SELECT * FROM tpch.tiny.orders;
```

Свойства существующих таблиц читаются из `gp_distribution_policy` и `pg_class` и видны в `SHOW CREATE TABLE`.
`INSERT` пишет через временную таблицу с тем же ключом распределения, что у целевой, поэтому финальный
`INSERT ... SELECT` не перераспределяет строки между сегментами.

## Apache Doris

`etc/catalog/doris.properties`:

```properties
connector.name=doris
# Query-порт frontend-узла; база данных в URL не указывается
connection-url=jdbc:mysql://doris-fe:9030/
connection-user=root
connection-password=
```

| Свойство | Описание | По умолчанию |
|---|---|---|
| `doris.default-replication-num` | `replication_num` для таблиц, которые создаёт Trino, если свойство таблицы не задано | умолчание Doris (3) |

Базы данных Doris видны в Trino как схемы. Доступен только внутренний каталог Doris.

### Маппинг типов

Типы колонок читаются из `information_schema.columns`: метаданные JDBC-драйвера искажают часть типов Doris
(например, `LARGEINT` и `ARRAY` выглядят как `INT`).

| Doris | Trino |
|---|---|
| `BOOLEAN` | `boolean` |
| `TINYINT`, `SMALLINT`, `INT`, `BIGINT` | `tinyint`, `smallint`, `integer`, `bigint` |
| `LARGEINT` | `decimal(38, 0)`; значения длиннее 38 цифр не читаются — такие колонки отображайте через `jdbc-types-mapped-to-varchar=largeint` |
| `FLOAT`, `DOUBLE` | `real`, `double` |
| `DECIMAL(p, s)` | `decimal(p, s)` |
| `DATE` | `date` |
| `DATETIME(p)` | `timestamp(p)` |
| `CHAR(n)`, `VARCHAR(n)`, `STRING` | `char(n)`, `varchar(n)`, `varchar` |
| `JSON`, `VARIANT` | `json` |
| `ARRAY`, `MAP`, `STRUCT`, `IPV4`, `IPV6` | `varchar` (текстовое представление, только чтение, без pushdown предикатов) |
| `BITMAP`, `HLL`, `QUANTILE_STATE`, `AGG_STATE` | не показываются |

Когда таблицу создаёт Trino, `varchar(n)` и `char(n)` превращаются в `VARCHAR(4 * n)`: длины в Doris считаются
в байтах, а символ UTF-8 занимает до 4 байт; более длинные и неограниченные значения становятся `STRING`.
`timestamp(p)` превращается в `DATETIME(min(p, 6))`. Типы `time`, `timestamp with time zone` и `varbinary`
записать нельзя.

### Свойства таблиц

| Свойство | Описание |
|---|---|
| `table_model` | `DUPLICATE` (по умолчанию) или `UNIQUE` |
| `key_columns` | Ключевые колонки, должны идти первыми в таблице. Обязательно для `UNIQUE` |
| `distributed_by` | Колонки hash-распределения. Если пусто — случайное распределение (для `UNIQUE` — ключевые колонки) |
| `buckets` | Число бакетов, если не задано — `AUTO` |
| `replication_num` | Число реплик |
| `extra_properties` | Словарь дополнительных `PROPERTIES` Doris |

```sql
CREATE TABLE doris.demo.users (id bigint NOT NULL, name varchar(100), updated_at timestamp(3))
WITH (table_model = 'UNIQUE', key_columns = ARRAY['id'], buckets = 8, replication_num = 3);
```

Таблица `DUPLICATE` без `key_columns` создаётся без ключей сортировки
(`enable_duplicate_without_keys_by_default`), поэтому первой может идти колонка любого типа. `UPDATE` работает
только для таблиц `UNIQUE`; `DELETE` подчиняется правилам Doris для модели таблицы. `MERGE` не поддерживается.

Запись идёт пакетными `INSERT` через MySQL-протокол. Для умеренных объёмов этого достаточно; массовую загрузку
лучше делать через Stream Load или коннекторы Doris для Spark/Flink.

### Pushdown

В Doris передаются предикаты по числовым, датам, времени и строковым колонкам, `LIMIT`, `ORDER BY ... LIMIT`,
агрегации (`count`, `min`, `max`, `sum`, `avg`, `stddev`, `variance`) и join. `LIKE` передаётся только с явным
`ESCAPE`: Doris считает обратный слэш в шаблоне экранирующим символом.

## Тесты

```bash
mvn test
```

Интеграционные тесты через Testcontainers запускают Docker-контейнеры `woblerr/greenplum:6.27.1` и
`apache/doris:3.0.8-all`. Другие образы задаются через `-Dtesting.greenplum-image-name=...` и
`-Dtesting.doris-image-name=...`. Тесты Greenplum проходят и на Greenplum 7:

```bash
mvn test -pl trino-greenplum -Dtesting.greenplum-image-name=woblerr/greenplum:7.1.0
```
