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

import io.trino.plugin.jdbc.BaseJdbcConnectorSmokeTest;
import io.trino.testing.QueryRunner;
import io.trino.testing.TestingConnectorBehavior;
import io.trino.testing.sql.TestTable;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class TestDorisConnectorSmokeTest
        extends BaseJdbcConnectorSmokeTest
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        TestingDorisServer server = closeAfterClass(new TestingDorisServer());
        return DorisQueryRunner.builder(server)
                .setInitialTables(REQUIRED_TPCH_TABLES)
                .build();
    }

    @Override
    protected boolean hasBehavior(TestingConnectorBehavior connectorBehavior)
    {
        return switch (connectorBehavior) {
            case SUPPORTS_RENAME_TABLE_ACROSS_SCHEMAS,
                    SUPPORTS_MERGE -> false;
            default -> super.hasBehavior(connectorBehavior);
        };
    }

    @Test
    @Override
    public void testUpdate()
    {
        // Tables created from TPC-H use the duplicate model, Doris updates only unique model tables (covered in TestDorisIntegration)
        assertQueryFails("UPDATE nation SET nationkey = 100 WHERE regionkey = 2", ".*Only unique table could be updated.*");
    }

    @Test
    @Override
    public void testRowLevelDelete()
    {
        try (TestTable table = newTrinoTable("test_row_level_delete", "AS SELECT * FROM region")) {
            // Doris does not report the number of rows deleted from a duplicate model table
            assertUpdate("DELETE FROM " + table.getName() + " WHERE regionkey = 2");
            assertThat(query("SELECT cast(regionkey AS integer) FROM " + table.getName()))
                    .skippingTypesCheck()
                    .matches("VALUES 0, 1, 3, 4");
        }
    }

    @Test
    @Override
    public void testShowCreateTable()
    {
        // Doris lengths are in bytes, so varchar(n) is created as VARCHAR(4 * n)
        assertThat((String) computeScalar("SHOW CREATE TABLE region"))
                .startsWith("""
                        CREATE TABLE doris.tpch.region (
                           regionkey bigint,
                           name varchar(100),
                           comment varchar(608)
                        )""")
                .contains("table_model = 'DUPLICATE'");
    }

    @Override
    protected String getCreateTableDefaultDefinition()
    {
        // UPDATE requires the unique key model
        return "(a bigint NOT NULL, b double) WITH (table_model = 'UNIQUE', key_columns = ARRAY['a'])";
    }
}
