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
package io.trino.plugin.greenplum;

import io.trino.Session;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;
import org.junit.jupiter.api.Test;

import java.util.List;

import static io.trino.tpch.TpchTable.NATION;
import static io.trino.tpch.TpchTable.ORDERS;
import static io.trino.tpch.TpchTable.REGION;
import static org.assertj.core.api.Assertions.assertThat;

final class TestGreenplumIntegration
        extends AbstractTestQueryFramework
{
    private TestingGreenplumServer server;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        server = closeAfterClass(new TestingGreenplumServer());
        return GreenplumQueryRunner.builder(server)
                .setInitialTables(List.of(NATION, REGION, ORDERS))
                .build();
    }

    @Test
    void testCreateTableWithHashDistribution()
    {
        assertUpdate("CREATE TABLE test_hash (a bigint, b varchar, c date) WITH (distributed_by = ARRAY['c', 'a'])");
        assertThat((String) computeScalar("SHOW CREATE TABLE test_hash"))
                .contains("distributed_by = ARRAY['c','a']")
                .contains("distribution_policy = 'HASH'");
        assertUpdate("INSERT INTO test_hash VALUES (1, 'x', DATE '2024-01-01'), (2, 'y', DATE '2024-01-02')", 2);
        assertQuery("SELECT a, b FROM test_hash WHERE c = DATE '2024-01-02'", "VALUES (2, 'y')");
        assertUpdate("DROP TABLE test_hash");
    }

    @Test
    void testCreateTableWithRandomAndReplicatedDistribution()
    {
        assertUpdate("CREATE TABLE test_random (a bigint) WITH (distribution_policy = 'RANDOM')");
        assertThat((String) computeScalar("SHOW CREATE TABLE test_random"))
                .contains("distribution_policy = 'RANDOM'")
                .doesNotContain("distributed_by");
        assertUpdate("DROP TABLE test_random");

        assertUpdate("CREATE TABLE test_replicated WITH (distribution_policy = 'REPLICATED') AS SELECT * FROM nation", 25);
        assertThat((String) computeScalar("SHOW CREATE TABLE test_replicated"))
                .contains("distribution_policy = 'REPLICATED'");
        assertQuery("SELECT count(*) FROM test_replicated", "VALUES 25");
        assertUpdate("DROP TABLE test_replicated");
    }

    @Test
    void testInvalidDistribution()
    {
        assertQueryFails(
                "CREATE TABLE test_invalid (a bigint) WITH (distributed_by = ARRAY['missing'])",
                "Column 'missing' specified in property 'distributed_by' doesn't exist in table");
        assertQueryFails(
                "CREATE TABLE test_invalid (a bigint) WITH (distribution_policy = 'RANDOM', distributed_by = ARRAY['a'])",
                "Property 'distributed_by' can only be used with distribution policy HASH");
        assertQueryFails(
                "CREATE TABLE test_invalid (a bigint) WITH (orientation = 'COLUMN')",
                ".*require 'appendoptimized' = true");
    }

    @Test
    void testAppendOptimizedTable()
    {
        assertUpdate(
                "CREATE TABLE test_ao WITH (appendoptimized = true, orientation = 'COLUMN', compresstype = 'zlib', compresslevel = 5, distributed_by = ARRAY['orderkey']) " +
                        "AS SELECT orderkey, custkey, totalprice FROM orders",
                "SELECT count(*) FROM orders");
        assertThat((String) computeScalar("SHOW CREATE TABLE test_ao"))
                .contains("appendoptimized = true")
                .contains("orientation = 'COLUMN'")
                .contains("compresstype = 'zlib'")
                .contains("compresslevel = 5")
                .contains("distributed_by = ARRAY['orderkey']");
        assertQuery("SELECT sum(totalprice) FROM test_ao", "SELECT sum(totalprice) FROM orders");
        // Insert goes through a temporary table that keeps the distribution of the target table
        assertUpdate("INSERT INTO test_ao SELECT orderkey, custkey, totalprice FROM orders WHERE orderkey < 100", "SELECT count(*) FROM orders WHERE orderkey < 100");
        assertUpdate("DROP TABLE test_ao");

        assertUpdate("CREATE TABLE test_ao_uncompressed (a bigint) WITH (appendoptimized = true)");
        assertThat((String) computeScalar("SHOW CREATE TABLE test_ao_uncompressed"))
                .contains("appendoptimized = true")
                .contains("orientation = 'ROW'")
                .doesNotContain("compresstype")
                .doesNotContain("compresslevel");
        assertUpdate("DROP TABLE test_ao_uncompressed");
    }

    @Test
    void testPartitionChildrenHidden()
    {
        server.execute("CREATE TABLE tpch.test_partitioned (id int, d date) DISTRIBUTED BY (id) " +
                "PARTITION BY RANGE (d) (START (date '2024-01-01') INCLUSIVE END (date '2024-03-01') EXCLUSIVE EVERY (INTERVAL '1 month'))");
        try {
            server.execute("INSERT INTO tpch.test_partitioned VALUES (1, date '2024-01-15'), (2, date '2024-02-15')");
            assertThat(computeActual("SHOW TABLES LIKE 'test_partitioned%'").getOnlyColumnAsSet())
                    .containsExactly("test_partitioned");
            assertQuery("SELECT count(*) FROM information_schema.columns WHERE table_name LIKE 'test_partitioned%'", "VALUES 2");
            assertQuery("SELECT id FROM test_partitioned WHERE d > DATE '2024-02-01'", "VALUES 2");
            // A leaf partition stays accessible when referenced explicitly
            assertQuery("SELECT id FROM test_partitioned_1_prt_1", "VALUES 1");
        }
        finally {
            server.execute("DROP TABLE tpch.test_partitioned");
        }
    }

    @Test
    void testPushdown()
    {
        assertThat(query("SELECT regionkey, count(*) FROM nation GROUP BY regionkey")).isFullyPushedDown();
        assertThat(query("SELECT name FROM nation WHERE nationkey = 3")).isFullyPushedDown();
        assertThat(query("SELECT orderkey FROM orders ORDER BY totalprice DESC LIMIT 5")).isFullyPushedDown();
        Session eagerJoinPushdown = Session.builder(getSession())
                .setCatalogSessionProperty("greenplum", "join_pushdown_strategy", "EAGER")
                .build();
        assertThat(query(eagerJoinPushdown, "SELECT n.name, r.name FROM nation n JOIN region r ON n.regionkey = r.regionkey")).isFullyPushedDown();
    }

    @Test
    void testMergeWithPrimaryKey()
    {
        server.execute("CREATE TABLE tpch.test_merge (id bigint PRIMARY KEY, v varchar(10)) DISTRIBUTED BY (id)");
        try {
            Session session = Session.builder(getSession())
                    .setCatalogSessionProperty("greenplum", "non_transactional_merge", "true")
                    .build();
            assertUpdate(session, "INSERT INTO test_merge VALUES (1, 'a'), (2, 'b')", 2);
            assertUpdate(session, "UPDATE test_merge SET v = 'z' WHERE id = 1", 1);
            assertUpdate(session,
                    "MERGE INTO test_merge t USING (VALUES (2, 'y'), (3, 'c')) s(id, v) ON t.id = s.id " +
                            "WHEN MATCHED THEN UPDATE SET v = s.v WHEN NOT MATCHED THEN INSERT VALUES (s.id, s.v)",
                    2);
            assertQuery("SELECT * FROM test_merge", "VALUES (1, 'z'), (2, 'y'), (3, 'c')");
        }
        finally {
            server.execute("DROP TABLE tpch.test_merge");
        }
    }

    @Test
    void testTableComment()
    {
        assertUpdate("CREATE TABLE test_comment (a bigint) COMMENT 'table comment' WITH (distribution_policy = 'RANDOM')");
        assertQuery("SELECT comment FROM system.metadata.table_comments WHERE catalog_name = 'greenplum' AND schema_name = 'tpch' AND table_name = 'test_comment'", "VALUES 'table comment'");
        assertUpdate("DROP TABLE test_comment");
    }
}
