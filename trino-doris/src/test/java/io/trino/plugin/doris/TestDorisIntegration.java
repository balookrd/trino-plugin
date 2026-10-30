/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.plugin.doris;

import io.trino.Session;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;
import org.junit.jupiter.api.Test;

import java.util.List;

import static io.trino.tpch.TpchTable.NATION;
import static io.trino.tpch.TpchTable.ORDERS;
import static io.trino.tpch.TpchTable.REGION;
import static org.assertj.core.api.Assertions.assertThat;

final class TestDorisIntegration
        extends AbstractTestQueryFramework
{
    private TestingDorisServer server;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        server = closeAfterClass(new TestingDorisServer());
        return DorisQueryRunner.builder(server)
                .setInitialTables(List.of(NATION, REGION, ORDERS))
                .build();
    }

    @Test
    void testReadNativeTypes()
    {
        server.execute("CREATE TABLE tpch.test_types (" +
                "k INT, c_bool BOOLEAN, c_tiny TINYINT, c_small SMALLINT, c_int INT, c_big BIGINT, c_large LARGEINT, " +
                "c_float FLOAT, c_double DOUBLE, c_dec DECIMAL(10,2), c_dec38 DECIMAL(38,10), c_date DATE, " +
                "c_dt DATETIME, c_dt3 DATETIME(3), c_dt6 DATETIME(6), c_char CHAR(10), c_varchar VARCHAR(100), c_string STRING, " +
                "c_json JSON, c_arr ARRAY<INT>, c_map MAP<STRING,INT>, c_ipv4 IPV4) " +
                "DUPLICATE KEY(k) DISTRIBUTED BY HASH(k) BUCKETS 1 PROPERTIES ('replication_num' = '1')");
        try {
            server.execute("INSERT INTO tpch.test_types VALUES (1, true, 1, 2, 3, 4, 12345678901234567890, 1.5, 2.5, 12.34, 1234567890.0123456789, " +
                    "'2024-01-02', '2024-01-02 03:04:05', '2024-01-02 03:04:05.123', '2024-01-02 03:04:05.123456', 'abc', 'привет', 'str', " +
                    "'{\"a\":1}', [1,2], {'x':1}, '1.2.3.4')");
            assertThat(computeActual("SELECT column_name, data_type FROM information_schema.columns WHERE table_name = 'test_types' ORDER BY ordinal_position").getMaterializedRows())
                    .extracting(row -> row.getField(0) + " " + row.getField(1))
                    .containsExactly(
                            "k integer",
                            "c_bool boolean",
                            "c_tiny tinyint",
                            "c_small smallint",
                            "c_int integer",
                            "c_big bigint",
                            "c_large decimal(38,0)",
                            "c_float real",
                            "c_double double",
                            "c_dec decimal(10,2)",
                            "c_dec38 decimal(38,10)",
                            "c_date date",
                            "c_dt timestamp(0)",
                            "c_dt3 timestamp(3)",
                            "c_dt6 timestamp(6)",
                            "c_char char(10)",
                            "c_varchar varchar(100)",
                            "c_string varchar",
                            "c_json json",
                            "c_arr varchar",
                            "c_map varchar",
                            "c_ipv4 varchar");
            assertThat(query("SELECT c_bool, c_tiny, c_small, c_int, c_big, c_large, c_float, c_double, c_dec, c_dec38, c_date, c_dt, c_dt3, c_dt6, c_char, c_varchar, c_string, json_format(c_json), c_arr, c_ipv4 FROM test_types"))
                    .matches("VALUES (true, TINYINT '1', SMALLINT '2', 3, BIGINT '4', CAST(DECIMAL '12345678901234567890' AS decimal(38,0)), REAL '1.5', DOUBLE '2.5', " +
                            "CAST(12.34 AS decimal(10,2)), CAST(1234567890.0123456789 AS decimal(38,10)), DATE '2024-01-02', TIMESTAMP '2024-01-02 03:04:05', " +
                            "TIMESTAMP '2024-01-02 03:04:05.123', TIMESTAMP '2024-01-02 03:04:05.123456', CAST('abc' AS char(10)), CAST('привет' AS varchar(100)), " +
                            "VARCHAR 'str', VARCHAR '{\"a\":1}', VARCHAR '[1, 2]', VARCHAR '1.2.3.4')");
            assertThat(query("SELECT k FROM test_types WHERE c_large = DECIMAL '12345678901234567890' AND c_dt6 = TIMESTAMP '2024-01-02 03:04:05.123456' AND c_bool"))
                    .isFullyPushedDown();
        }
        finally {
            server.execute("DROP TABLE tpch.test_types");
        }
    }

    @Test
    void testUnreadableColumnsAreHidden()
    {
        server.execute("CREATE TABLE tpch.test_bitmap (k INT, b BITMAP BITMAP_UNION, h HLL HLL_UNION) AGGREGATE KEY(k) " +
                "DISTRIBUTED BY HASH(k) BUCKETS 1 PROPERTIES ('replication_num' = '1')");
        try {
            server.execute("INSERT INTO tpch.test_bitmap VALUES (1, to_bitmap(1), hll_hash(1))");
            assertQuery("SELECT * FROM test_bitmap", "VALUES 1");
        }
        finally {
            server.execute("DROP TABLE tpch.test_bitmap");
        }
    }

    @Test
    void testWriteTypes()
    {
        assertUpdate("CREATE TABLE test_write_types AS SELECT " +
                "true c_bool, TINYINT '1' c_tiny, SMALLINT '2' c_small, 3 c_int, BIGINT '4' c_big, REAL '1.5' c_real, DOUBLE '2.5' c_double, " +
                "DECIMAL '12.34' c_dec, CAST(1 AS decimal(38,5)) c_dec38, DATE '2024-01-02' c_date, TIMESTAMP '2024-01-02 03:04:05.123456' c_ts6, " +
                "TIMESTAMP '2024-01-02 03:04:05.123456789' c_ts9, CAST('привет' AS varchar(6)) c_varchar, VARCHAR 'unbounded' c_unbounded, JSON '{\"a\":1}' c_json", 1);
        assertQuery("SELECT c_bool, c_tiny, c_small, c_int, c_big, c_real, c_double, c_dec, c_dec38, c_date, c_varchar, c_unbounded, json_format(c_json) FROM test_write_types",
                "VALUES (true, 1, 2, 3, 4, 1.5, 2.5, 12.34, 1, DATE '2024-01-02', 'привет', 'unbounded', '{\"a\":1}')");
        assertThat(query("SELECT c_ts6, c_ts9 FROM test_write_types"))
                .matches("VALUES (TIMESTAMP '2024-01-02 03:04:05.123456', TIMESTAMP '2024-01-02 03:04:05.123457')");
        // Doris VARCHAR length is in bytes, the column is sized for 4 bytes per character
        assertQuery("SELECT data_type FROM information_schema.columns WHERE table_name = 'test_write_types' AND column_name = 'c_varchar'", "VALUES 'varchar(24)'");
        assertUpdate("DROP TABLE test_write_types");
    }

    @Test
    void testUniqueModel()
    {
        assertUpdate("CREATE TABLE test_unique (id bigint NOT NULL, name varchar(10), value double) " +
                "WITH (table_model = 'UNIQUE', key_columns = ARRAY['id'], buckets = 2, replication_num = 1)");
        assertThat((String) computeScalar("SHOW CREATE TABLE test_unique"))
                .contains("table_model = 'UNIQUE'")
                .contains("key_columns = ARRAY['id']")
                .contains("distributed_by = ARRAY['id']")
                .contains("buckets = 2")
                .contains("replication_num = 1");
        assertUpdate("INSERT INTO test_unique VALUES (1, 'a', 1.0), (2, 'b', 2.0)", 2);
        // Rows with the same key replace each other
        assertUpdate("INSERT INTO test_unique VALUES (1, 'c', 3.0)", 1);
        assertQuery("SELECT * FROM test_unique", "VALUES (1, 'c', 3.0), (2, 'b', 2.0)");
        assertUpdate("UPDATE test_unique SET value = 10 WHERE id = 2", 1);
        assertUpdate("DELETE FROM test_unique WHERE id = 1", 1);
        assertQuery("SELECT * FROM test_unique", "VALUES (2, 'b', 10.0)");
        assertUpdate("DROP TABLE test_unique");
    }

    @Test
    void testDuplicateModel()
    {
        // Without key columns a duplicate table without sort keys is created, so leading double or string columns are allowed
        assertUpdate("CREATE TABLE test_duplicate_nokeys AS SELECT totalprice, comment, orderkey FROM orders", "SELECT count(*) FROM orders");
        assertThat((String) computeScalar("SHOW CREATE TABLE test_duplicate_nokeys"))
                .contains("table_model = 'DUPLICATE'")
                .doesNotContain("key_columns");
        assertQuery("SELECT sum(totalprice) FROM test_duplicate_nokeys", "SELECT sum(totalprice) FROM orders");
        assertUpdate("DROP TABLE test_duplicate_nokeys");

        assertUpdate("CREATE TABLE test_duplicate (orderkey bigint, orderdate date, totalprice double) " +
                "WITH (key_columns = ARRAY['orderkey', 'orderdate'], distributed_by = ARRAY['orderkey'], buckets = 3)");
        assertThat((String) computeScalar("SHOW CREATE TABLE test_duplicate"))
                .contains("key_columns = ARRAY['orderkey','orderdate']")
                .contains("distributed_by = ARRAY['orderkey']")
                .contains("buckets = 3");
        assertUpdate("INSERT INTO test_duplicate SELECT orderkey, orderdate, totalprice FROM orders", "SELECT count(*) FROM orders");
        assertUpdate("INSERT INTO test_duplicate SELECT orderkey, orderdate, totalprice FROM orders WHERE orderkey < 10", "SELECT count(*) FROM orders WHERE orderkey < 10");
        assertQuery("SELECT count(*) FROM test_duplicate WHERE orderkey < 10", "SELECT 2 * count(*) FROM orders WHERE orderkey < 10");
        // Doris does not report the number of rows deleted from a duplicate model table
        assertUpdate("DELETE FROM test_duplicate WHERE orderkey < 10");
        assertQuery("SELECT count(*) FROM test_duplicate WHERE orderkey < 10", "VALUES 0");
        assertUpdate("DROP TABLE test_duplicate");
    }

    @Test
    void testInvalidTableProperties()
    {
        assertQueryFails(
                "CREATE TABLE test_invalid (a bigint, b bigint) WITH (key_columns = ARRAY['b'])",
                "Columns in 'key_columns' must be the leading columns of the table in the same order: \\[b]");
        assertQueryFails(
                "CREATE TABLE test_invalid (a bigint) WITH (table_model = 'UNIQUE')",
                "Table model UNIQUE requires the 'key_columns' property");
        assertQueryFails(
                "CREATE TABLE test_invalid (a bigint) WITH (distributed_by = ARRAY['missing'])",
                "Column 'missing' specified in property 'distributed_by' doesn't exist in table");
    }

    @Test
    void testPushdown()
    {
        assertThat(query("SELECT regionkey, count(*), sum(nationkey), avg(nationkey) FROM nation GROUP BY regionkey")).isFullyPushedDown();
        assertThat(query("SELECT name FROM nation WHERE name = 'ALGERIA'")).isFullyPushedDown();
        assertThat(query("SELECT name FROM nation WHERE name > 'M'")).isFullyPushedDown();
        assertThat(query("SELECT orderkey FROM orders ORDER BY totalprice DESC NULLS FIRST, orderkey LIMIT 5")).isFullyPushedDown();
        assertThat(query("SELECT name FROM nation WHERE name LIKE 'A%' ESCAPE '\\'")).isFullyPushedDown();
        Session eagerJoinPushdown = Session.builder(getSession())
                .setCatalogSessionProperty("doris", "join_pushdown_strategy", "EAGER")
                .build();
        assertThat(query(eagerJoinPushdown, "SELECT n.name, r.name FROM nation n JOIN region r ON n.regionkey = r.regionkey")).isFullyPushedDown();
    }

    @Test
    void testSchemaAndTableManagement()
    {
        assertUpdate("CREATE SCHEMA test_schema_doris");
        assertUpdate("CREATE TABLE test_schema_doris.t (a bigint, b varchar) COMMENT 'table comment'");
        assertQuery("SELECT comment FROM system.metadata.table_comments WHERE catalog_name = 'doris' AND schema_name = 'test_schema_doris' AND table_name = 't'", "VALUES 'table comment'");
        assertUpdate("COMMENT ON COLUMN test_schema_doris.t.b IS 'column comment'");
        assertQuery("SELECT comment FROM information_schema.columns WHERE table_schema = 'test_schema_doris' AND table_name = 't' AND column_name = 'b'", "VALUES 'column comment'");
        assertUpdate("ALTER TABLE test_schema_doris.t ADD COLUMN c double");
        assertUpdate("ALTER TABLE test_schema_doris.t RENAME COLUMN c TO d");
        assertUpdate("ALTER TABLE test_schema_doris.t DROP COLUMN d");
        assertUpdate("ALTER TABLE test_schema_doris.t RENAME TO test_schema_doris.t2");
        assertQuery("SHOW TABLES FROM test_schema_doris", "VALUES 't2'");
        assertQueryFails("DROP SCHEMA test_schema_doris", ".*Cannot drop non-empty schema 'test_schema_doris'");
        assertUpdate("DROP TABLE test_schema_doris.t2");
        assertUpdate("DROP SCHEMA test_schema_doris");
    }
}
