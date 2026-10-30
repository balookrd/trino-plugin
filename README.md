# Trino connectors for Greenplum and Apache Doris

Two Trino connector plugins built against Trino 470:

- `trino-greenplum` — Greenplum 6 and 7. Builds on the Trino PostgreSQL connector (type mapping, pushdown,
  statistics) and adds Greenplum-specific DDL and metadata.
- `trino-doris` — Apache Doris 2.1+ (tested with 3.0). Uses the MySQL protocol of the Doris frontend, with a
  Doris-specific type mapping and DDL.

## Build

Requirements: JDK 23 or newer, Maven 3.9, Docker (integration tests only).

```bash
mvn clean package -DskipTests
```

The plugin archives are in `trino-greenplum/target/trino-greenplum-470.zip` and
`trino-doris/target/trino-doris-470.zip`. Unpack an archive into the `plugin` directory of a Trino 470 server
and restart it.

The plugins depend on Trino SPI 470: the plugin version must match the server version. To target another
Trino release, change the parent version and the project version in the root `pom.xml`.

## Greenplum

`etc/catalog/greenplum.properties`:

```properties
connector.name=greenplum
connection-url=jdbc:postgresql://gp-coordinator:5432/database
connection-user=trino
connection-password=secret
```

All PostgreSQL connector settings (`postgresql.*`, for example `postgresql.array-mapping`) and session
properties also apply. The Greenplum server needs the `geospatial` Trino plugin (it is part of the standard Trino
distribution), same as the PostgreSQL connector.

| Property | Description | Default |
|---|---|---|
| `greenplum.include-partition-children` | List leaf partitions of partitioned tables as separate tables. A hidden partition is still accessible by its name | `false` |

### Table properties

| Property | Description |
|---|---|
| `distribution_policy` | `HASH`, `RANDOM` or `REPLICATED`. The Greenplum default is used when not set |
| `distributed_by` | Distribution key columns, implies `HASH` |
| `appendoptimized` | Create an append-optimized table |
| `orientation` | `ROW` or `COLUMN`, requires `appendoptimized = true` |
| `compresstype` | Compression type, for example `zstd` or `zlib`, requires `appendoptimized = true` |
| `compresslevel` | Compression level, requires `appendoptimized = true` |

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

The properties of existing tables are read back from `gp_distribution_policy` and `pg_class` and shown by
`SHOW CREATE TABLE`. `INSERT` writes through a temporary table that keeps the distribution key of the target
table, so the final `INSERT ... SELECT` does not redistribute rows between segments.

## Apache Doris

`etc/catalog/doris.properties`:

```properties
connector.name=doris
# Query port of a frontend; the URL must not contain a database
connection-url=jdbc:mysql://doris-fe:9030/
connection-user=root
connection-password=
```

| Property | Description | Default |
|---|---|---|
| `doris.default-replication-num` | `replication_num` for tables created by Trino when the table property is not set | Doris default (3) |

Doris databases are exposed as Trino schemas. Only the internal Doris catalog is accessible.

### Type mapping

Column types are read from `information_schema.columns`, because the JDBC driver metadata reports some Doris
types incorrectly (for example `LARGEINT` and `ARRAY` as `INT`).

| Doris | Trino |
|---|---|
| `BOOLEAN` | `boolean` |
| `TINYINT`, `SMALLINT`, `INT`, `BIGINT` | `tinyint`, `smallint`, `integer`, `bigint` |
| `LARGEINT` | `decimal(38, 0)`; values longer than 38 digits fail on read, map such columns with `jdbc-types-mapped-to-varchar=largeint` |
| `FLOAT`, `DOUBLE` | `real`, `double` |
| `DECIMAL(p, s)` | `decimal(p, s)` |
| `DATE` | `date` |
| `DATETIME(p)` | `timestamp(p)` |
| `CHAR(n)`, `VARCHAR(n)`, `STRING` | `char(n)`, `varchar(n)`, `varchar` |
| `JSON`, `VARIANT` | `json` |
| `ARRAY`, `MAP`, `STRUCT`, `IPV4`, `IPV6` | `varchar` (text representation, read only, no predicate pushdown) |
| `BITMAP`, `HLL`, `QUANTILE_STATE`, `AGG_STATE` | not exposed |

When Trino creates a table, `varchar(n)` and `char(n)` become `VARCHAR(4 * n)`, because Doris lengths are in
bytes and a UTF-8 character takes up to 4 bytes; longer and unbounded values become `STRING`. `timestamp(p)`
becomes `DATETIME(min(p, 6))`. `time`, `timestamp with time zone` and `varbinary` cannot be written.

### Table properties

| Property | Description |
|---|---|
| `table_model` | `DUPLICATE` (default) or `UNIQUE` |
| `key_columns` | Key columns, must be the leading columns of the table. Required for `UNIQUE` |
| `distributed_by` | Hash distribution columns. Random distribution is used when empty (for `UNIQUE` the key columns) |
| `buckets` | Number of buckets, `AUTO` when not set |
| `replication_num` | Number of replicas |
| `extra_properties` | Map of additional Doris `PROPERTIES` |

```sql
CREATE TABLE doris.demo.users (id bigint NOT NULL, name varchar(100), updated_at timestamp(3))
WITH (table_model = 'UNIQUE', key_columns = ARRAY['id'], buckets = 8, replication_num = 3);
```

A `DUPLICATE` table without `key_columns` is created without sort keys
(`enable_duplicate_without_keys_by_default`), so any column type can come first. `UPDATE` works for `UNIQUE`
tables only; `DELETE` follows the Doris rules for the table model. `MERGE` is not supported.

Writes use batched `INSERT` statements through the MySQL protocol. This is fine for moderate volumes; for bulk
loading prefer Doris Stream Load or the Doris Spark/Flink connectors.

### Pushdown

Predicates on numeric, date, time and string columns, `LIMIT`, `ORDER BY ... LIMIT`, aggregations (`count`,
`min`, `max`, `sum`, `avg`, `stddev`, `variance`) and joins are pushed down. `LIKE` is pushed down only with an
explicit `ESCAPE`, because Doris treats a backslash in a pattern as an escape character.

## Tests

```bash
mvn test
```

Integration tests start Docker containers `woblerr/greenplum:6.27.1` and `apache/doris:3.0.8-all` with
Testcontainers. Other images can be set with `-Dtesting.greenplum-image-name=...` and
`-Dtesting.doris-image-name=...`. The Greenplum tests also pass on Greenplum 7:

```bash
mvn test -pl trino-greenplum -Dtesting.greenplum-image-name=woblerr/greenplum:7.1.0
```

## License

Apache License 2.0, see [LICENSE](LICENSE). Parts of the connectors are based on the Trino PostgreSQL and MySQL connectors.
