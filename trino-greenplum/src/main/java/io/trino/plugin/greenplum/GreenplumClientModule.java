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

import com.google.inject.Binder;
import com.google.inject.Scopes;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.trino.plugin.jdbc.DecimalModule;
import io.trino.plugin.jdbc.ForBaseJdbc;
import io.trino.plugin.jdbc.JdbcClient;
import io.trino.plugin.jdbc.JdbcJoinPushdownSupportModule;
import io.trino.plugin.jdbc.JdbcStatisticsConfig;
import io.trino.plugin.jdbc.QueryBuilder;
import io.trino.plugin.jdbc.TimestampTimeZoneDomain;
import io.trino.plugin.jdbc.ptf.Query;
import io.trino.plugin.postgresql.CollationAwareQueryBuilder;
import io.trino.plugin.postgresql.PostgreSqlConfig;
import io.trino.plugin.postgresql.PostgreSqlSessionProperties;
import io.trino.spi.function.table.ConnectorTableFunction;

import static com.google.inject.multibindings.Multibinder.newSetBinder;
import static com.google.inject.multibindings.OptionalBinder.newOptionalBinder;
import static io.airlift.configuration.ConfigBinder.configBinder;
import static io.trino.plugin.jdbc.JdbcModule.bindSessionPropertiesProvider;
import static io.trino.plugin.jdbc.JdbcModule.bindTablePropertiesProvider;

public class GreenplumClientModule
        extends AbstractConfigurationAwareModule
{
    @Override
    public void setup(Binder binder)
    {
        binder.bind(JdbcClient.class).annotatedWith(ForBaseJdbc.class).to(GreenplumClient.class).in(Scopes.SINGLETON);
        newOptionalBinder(binder, TimestampTimeZoneDomain.class).setBinding().toInstance(TimestampTimeZoneDomain.UTC_ONLY);
        // PostgreSQL connector settings (postgresql.*) apply to Greenplum as well
        configBinder(binder).bindConfig(PostgreSqlConfig.class);
        configBinder(binder).bindConfig(GreenplumConfig.class);
        configBinder(binder).bindConfig(JdbcStatisticsConfig.class);
        bindSessionPropertiesProvider(binder, PostgreSqlSessionProperties.class);
        bindTablePropertiesProvider(binder, GreenplumTableProperties.class);
        newOptionalBinder(binder, QueryBuilder.class).setBinding().to(CollationAwareQueryBuilder.class).in(Scopes.SINGLETON);
        install(new DecimalModule());
        install(new JdbcJoinPushdownSupportModule());
        newSetBinder(binder, ConnectorTableFunction.class).addBinding().toProvider(Query.class).in(Scopes.SINGLETON);
    }
}
