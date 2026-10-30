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

import io.trino.plugin.jdbc.BaseJdbcConnectorSmokeTest;
import io.trino.testing.QueryRunner;
import io.trino.testing.TestingConnectorBehavior;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class TestGreenplumConnectorSmokeTest
        extends BaseJdbcConnectorSmokeTest
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        TestingGreenplumServer server = closeAfterClass(new TestingGreenplumServer());
        return GreenplumQueryRunner.builder(server)
                .setInitialTables(REQUIRED_TPCH_TABLES)
                .build();
    }

    // MERGE, and UPDATE implemented with it, works only for tables with a primary key, which Trino cannot create,
    // and only with non_transactional_merge enabled; it is covered in TestGreenplumIntegration
    private static final String MERGE_DISABLED = "Non-transactional MERGE is disabled";

    @Test
    @Override
    public void testMerge()
    {
        assertQueryFails("MERGE INTO nation n USING region r ON r.regionkey = n.regionkey WHEN MATCHED THEN DELETE", MERGE_DISABLED);
    }

    @Test
    @Override
    public void testRowLevelUpdate()
    {
        assertQueryFails("UPDATE nation SET nationkey = nationkey + regionkey WHERE regionkey < 1", MERGE_DISABLED);
    }

    @Test
    @Override
    public void verifySupportsRowLevelUpdateDeclaration()
    {
        assertQueryFails("UPDATE nation SET nationkey = nationkey + regionkey WHERE regionkey < 1", MERGE_DISABLED);
    }

    @Test
    @Override
    public void testShowCreateTable()
    {
        assertThat((String) computeScalar("SHOW CREATE TABLE region"))
                .startsWith("""
                        CREATE TABLE greenplum.tpch.region (
                           regionkey bigint,
                           name varchar(25),
                           comment varchar(152)
                        )""")
                // Distribution policy of a table created by CTAS without properties is the Greenplum default
                .contains("distribution_policy = ");
    }

    @Override
    protected boolean hasBehavior(TestingConnectorBehavior connectorBehavior)
    {
        return switch (connectorBehavior) {
            case SUPPORTS_RENAME_TABLE_ACROSS_SCHEMAS -> false;
            default -> super.hasBehavior(connectorBehavior);
        };
    }
}
