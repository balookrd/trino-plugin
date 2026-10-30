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

import com.google.common.collect.ImmutableList;
import io.trino.plugin.geospatial.GeoPlugin;
import io.trino.plugin.tpch.TpchPlugin;
import io.trino.testing.DistributedQueryRunner;
import io.trino.tpch.TpchTable;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static io.trino.plugin.tpch.TpchMetadata.TINY_SCHEMA_NAME;
import static io.trino.testing.QueryAssertions.copyTpchTables;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static java.util.Objects.requireNonNull;

public final class GreenplumQueryRunner
{
    private GreenplumQueryRunner() {}

    private static final String TPCH_SCHEMA = "tpch";

    public static Builder builder(TestingGreenplumServer server)
    {
        return new Builder()
                .addConnectorProperties(Map.of(
                        "connection-url", server.getJdbcUrl(),
                        "connection-user", server.getUser(),
                        "connection-password", server.getPassword()));
    }

    public static final class Builder
            extends DistributedQueryRunner.Builder<Builder>
    {
        private final Map<String, String> connectorProperties = new HashMap<>();
        private List<TpchTable<?>> initialTables = ImmutableList.of();

        private Builder()
        {
            super(testSessionBuilder()
                    .setCatalog("greenplum")
                    .setSchema(TPCH_SCHEMA)
                    .build());
        }

        public Builder addConnectorProperties(Map<String, String> connectorProperties)
        {
            this.connectorProperties.putAll(connectorProperties);
            return this;
        }

        public Builder setInitialTables(Iterable<TpchTable<?>> initialTables)
        {
            this.initialTables = ImmutableList.copyOf(requireNonNull(initialTables, "initialTables is null"));
            return this;
        }

        @Override
        public DistributedQueryRunner build()
                throws Exception
        {
            super.setAdditionalSetup(runner -> {
                // The PostgreSQL type mapping used by the connector requires the geometry type
                runner.installPlugin(new GeoPlugin());

                runner.installPlugin(new TpchPlugin());
                runner.createCatalog("tpch", "tpch");

                runner.installPlugin(new GreenplumPlugin());
                runner.createCatalog("greenplum", "greenplum", connectorProperties);

                copyTpchTables(runner, "tpch", TINY_SCHEMA_NAME, initialTables);
            });
            return super.build();
        }
    }
}
