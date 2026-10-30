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
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Inject;
import io.trino.plugin.base.mapping.IdentifierMapping;
import io.trino.plugin.greenplum.GreenplumTableProperties.DistributionPolicy;
import io.trino.plugin.greenplum.GreenplumTableProperties.Orientation;
import io.trino.plugin.jdbc.BaseJdbcConfig;
import io.trino.plugin.jdbc.ConnectionFactory;
import io.trino.plugin.jdbc.JdbcStatisticsConfig;
import io.trino.plugin.jdbc.JdbcTableHandle;
import io.trino.plugin.jdbc.QueryBuilder;
import io.trino.plugin.jdbc.RemoteTableName;
import io.trino.plugin.jdbc.logging.RemoteQueryModifier;
import io.trino.plugin.postgresql.PostgreSqlClient;
import io.trino.plugin.postgresql.PostgreSqlConfig;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.type.TypeManager;

import java.sql.Array;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.trino.plugin.greenplum.GreenplumTableProperties.APPENDOPTIMIZED;
import static io.trino.plugin.greenplum.GreenplumTableProperties.COMPRESSLEVEL;
import static io.trino.plugin.greenplum.GreenplumTableProperties.COMPRESSTYPE;
import static io.trino.plugin.greenplum.GreenplumTableProperties.DISTRIBUTED_BY;
import static io.trino.plugin.greenplum.GreenplumTableProperties.DISTRIBUTION_POLICY;
import static io.trino.plugin.greenplum.GreenplumTableProperties.ORIENTATION;
import static io.trino.plugin.greenplum.GreenplumTableProperties.getAppendOptimized;
import static io.trino.plugin.greenplum.GreenplumTableProperties.getCompressLevel;
import static io.trino.plugin.greenplum.GreenplumTableProperties.getCompressType;
import static io.trino.plugin.greenplum.GreenplumTableProperties.getDistributedBy;
import static io.trino.plugin.greenplum.GreenplumTableProperties.getDistributionPolicy;
import static io.trino.plugin.greenplum.GreenplumTableProperties.getOrientation;
import static io.trino.plugin.jdbc.JdbcErrorCode.JDBC_ERROR;
import static io.trino.spi.StandardErrorCode.INVALID_TABLE_PROPERTY;
import static java.lang.String.format;
import static java.lang.String.join;
import static java.util.Locale.ENGLISH;
import static java.util.stream.Collectors.joining;

/**
 * Greenplum is a massively parallel database built on top of PostgreSQL, so the PostgreSQL
 * connector logic (type mapping, pushdown, statistics) is reused. This class adds what is
 * specific to Greenplum: distribution policy and append-optimized storage on table creation,
 * reading them back as table properties, and hiding leaf partitions from table listings.
 */
public class GreenplumClient
        extends PostgreSqlClient
{
    // GP7 (PostgreSQL 12 based) exposes declarative partitions via pg_class.relispartition,
    // GP6 (PostgreSQL 9.4 based) uses its own legacy partitioning catalog
    private static final String GP7_PARTITION_CHILDREN_QUERY = "" +
            "SELECT n.nspname, c.relname FROM pg_catalog.pg_class c " +
            "JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace " +
            "WHERE c.relispartition";
    private static final String GP6_PARTITION_CHILDREN_QUERY = "" +
            "SELECT n.nspname, c.relname FROM pg_catalog.pg_partition_rule r " +
            "JOIN pg_catalog.pg_class c ON c.oid = r.parchildrelid " +
            "JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace";

    private final boolean includePartitionChildren;

    @Inject
    public GreenplumClient(
            BaseJdbcConfig config,
            PostgreSqlConfig postgreSqlConfig,
            GreenplumConfig greenplumConfig,
            JdbcStatisticsConfig statisticsConfig,
            ConnectionFactory connectionFactory,
            QueryBuilder queryBuilder,
            TypeManager typeManager,
            IdentifierMapping identifierMapping,
            RemoteQueryModifier queryModifier)
    {
        super(config, postgreSqlConfig, statisticsConfig, connectionFactory, queryBuilder, typeManager, identifierMapping, queryModifier);
        this.includePartitionChildren = greenplumConfig.isIncludePartitionChildren();
    }

    @Override
    protected List<String> createTableSqls(RemoteTableName remoteTableName, List<String> columns, ConnectorTableMetadata tableMetadata)
    {
        Map<String, Object> properties = tableMetadata.getProperties();
        ImmutableList.Builder<String> sqls = ImmutableList.builder();
        sqls.add(format("CREATE TABLE %s (%s)%s%s",
                quoted(remoteTableName),
                join(", ", columns),
                storageClause(properties),
                distributionClause(properties, tableMetadata)));
        tableMetadata.getComment().ifPresent(comment -> sqls.add(format(
                "COMMENT ON TABLE %s IS %s",
                quoted(remoteTableName),
                varcharLiteral(comment))));
        return sqls.build();
    }

    private static String storageClause(Map<String, Object> properties)
    {
        boolean appendOptimized = getAppendOptimized(properties).orElse(false);
        if (!appendOptimized && (getOrientation(properties).isPresent() || getCompressType(properties).isPresent() || getCompressLevel(properties).isPresent())) {
            throw new TrinoException(INVALID_TABLE_PROPERTY, "Properties '%s', '%s' and '%s' require '%s' = true".formatted(ORIENTATION, COMPRESSTYPE, COMPRESSLEVEL, APPENDOPTIMIZED));
        }

        List<String> options = new ArrayList<>();
        getAppendOptimized(properties).ifPresent(value -> options.add("appendoptimized=" + value));
        getOrientation(properties).ifPresent(value -> options.add("orientation=" + value.name().toLowerCase(ENGLISH)));
        getCompressType(properties).ifPresent(value -> {
            // The value is inlined into DDL, so allow plain identifiers only
            if (!value.matches("[A-Za-z0-9_]+")) {
                throw new TrinoException(INVALID_TABLE_PROPERTY, "Invalid value for '%s': %s".formatted(COMPRESSTYPE, value));
            }
            options.add("compresstype=" + value.toLowerCase(ENGLISH));
        });
        getCompressLevel(properties).ifPresent(value -> options.add("compresslevel=" + value));
        if (options.isEmpty()) {
            return "";
        }
        return " WITH (" + join(", ", options) + ")";
    }

    private String distributionClause(Map<String, Object> properties, ConnectorTableMetadata tableMetadata)
    {
        Optional<DistributionPolicy> policy = getDistributionPolicy(properties);
        if (policy.isEmpty()) {
            return "";
        }
        return switch (policy.get()) {
            case RANDOM -> " DISTRIBUTED RANDOMLY";
            case REPLICATED -> " DISTRIBUTED REPLICATED";
            case HASH -> {
                Set<String> columnNames = tableMetadata.getColumns().stream()
                        .map(column -> column.getName().toLowerCase(ENGLISH))
                        .collect(toImmutableSet());
                List<String> distributedBy = getDistributedBy(properties);
                for (String column : distributedBy) {
                    if (!columnNames.contains(column.toLowerCase(ENGLISH))) {
                        throw new TrinoException(INVALID_TABLE_PROPERTY, "Column '%s' specified in property '%s' doesn't exist in table".formatted(column, DISTRIBUTED_BY));
                    }
                }
                yield distributedBy.stream()
                        // Trino column names are lower-cased, and so are remote names created by Trino
                        .map(column -> quoted(column.toLowerCase(ENGLISH)))
                        .collect(joining(", ", " DISTRIBUTED BY (", ")"));
            }
        };
    }

    @Override
    protected void copyTableSchema(ConnectorSession session, Connection connection, String catalogName, String schemaName, String tableName, String newTableName, List<String> columnNames)
    {
        // Keep the distribution of the target table for the temporary table used by INSERT,
        // so that the final INSERT ... SELECT does not need to redistribute rows between segments
        String distribution = " DISTRIBUTED RANDOMLY";
        try {
            TableDistribution tableDistribution = readDistribution(connection, quoted(catalogName, schemaName, tableName));
            if (tableDistribution.policy() == DistributionPolicy.HASH && columnNames.containsAll(tableDistribution.columns())) {
                distribution = tableDistribution.columns().stream()
                        .map(this::quoted)
                        .collect(joining(", ", " DISTRIBUTED BY (", ")"));
            }
        }
        catch (SQLException e) {
            throw new TrinoException(JDBC_ERROR, e);
        }

        String sql = format(
                "CREATE TABLE %s AS SELECT %s FROM %s WHERE 0 = 1%s",
                quoted(catalogName, schemaName, newTableName),
                columnNames.stream()
                        .map(this::quoted)
                        .collect(joining(", ")),
                quoted(catalogName, schemaName, tableName),
                distribution);
        try {
            execute(session, connection, sql);
        }
        catch (SQLException e) {
            throw new TrinoException(JDBC_ERROR, e);
        }
    }

    @Override
    public Map<String, Object> getTableProperties(ConnectorSession session, JdbcTableHandle tableHandle)
    {
        checkArgument(tableHandle.isNamedRelation(), "Not a named relation: %s", tableHandle);
        String table = quoted(tableHandle.asPlainTable().getRemoteTableName());
        try (Connection connection = connectionFactory.openConnection(session)) {
            ImmutableMap.Builder<String, Object> properties = ImmutableMap.builder();

            TableDistribution distribution = readDistribution(connection, table);
            if (distribution.policy() != null) {
                properties.put(DISTRIBUTION_POLICY, distribution.policy());
                if (!distribution.columns().isEmpty()) {
                    properties.put(DISTRIBUTED_BY, distribution.columns());
                }
            }

            try (PreparedStatement statement = connection.prepareStatement("" +
                    "SELECT c.reloptions, am.amname FROM pg_catalog.pg_class c " +
                    "LEFT JOIN pg_catalog.pg_am am ON am.oid = c.relam " +
                    "WHERE c.oid = CAST(? AS regclass)")) {
                statement.setString(1, table);
                try (ResultSet resultSet = statement.executeQuery()) {
                    if (resultSet.next()) {
                        properties.putAll(storageProperties(resultSet.getArray(1), resultSet.getString(2)));
                    }
                }
            }
            return properties.buildOrThrow();
        }
        catch (SQLException e) {
            throw new TrinoException(JDBC_ERROR, e);
        }
    }

    static Map<String, Object> storageProperties(Array relOptionsArray, String accessMethod)
            throws SQLException
    {
        Map<String, String> options = new HashMap<>();
        if (relOptionsArray != null) {
            for (Object option : (Object[]) relOptionsArray.getArray()) {
                String[] keyValue = option.toString().split("=", 2);
                if (keyValue.length == 2) {
                    options.put(keyValue[0].toLowerCase(ENGLISH), keyValue[1]);
                }
            }
        }

        boolean appendOptimized = "true".equalsIgnoreCase(options.getOrDefault("appendonly", options.get("appendoptimized")));
        String orientation = options.get("orientation");
        // GP7 stores append-optimized tables as table access methods instead of reloptions
        if ("ao_row".equals(accessMethod)) {
            appendOptimized = true;
            orientation = "row";
        }
        else if ("ao_column".equals(accessMethod)) {
            appendOptimized = true;
            orientation = "column";
        }

        ImmutableMap.Builder<String, Object> properties = ImmutableMap.builder();
        if (!appendOptimized) {
            return properties.buildOrThrow();
        }
        properties.put(APPENDOPTIMIZED, true);
        properties.put(ORIENTATION, orientation == null ? Orientation.ROW : Orientation.valueOf(orientation.toUpperCase(ENGLISH)));
        String compressType = options.get("compresstype");
        // GP7 stores compresstype=none and compresslevel=0 for uncompressed tables
        if (compressType != null && !compressType.equalsIgnoreCase("none")) {
            properties.put(COMPRESSTYPE, compressType);
            String compressLevel = options.get("compresslevel");
            if (compressLevel != null) {
                properties.put(COMPRESSLEVEL, Integer.parseInt(compressLevel));
            }
        }
        return properties.buildOrThrow();
    }

    private static TableDistribution readDistribution(Connection connection, String quotedTableName)
            throws SQLException
    {
        // gp_distribution_policy has the same shape in GP6 and GP7: policytype is 'p' (partitioned/hash) or 'r' (replicated),
        // and an empty distkey with policytype 'p' means DISTRIBUTED RANDOMLY
        try (PreparedStatement statement = connection.prepareStatement("" +
                "SELECT p.policytype, " +
                "  ARRAY(SELECT a.attname FROM pg_catalog.pg_attribute a " +
                "        JOIN generate_series(0, array_upper(CAST(p.distkey AS int2[]), 1)) i ON a.attnum = (CAST(p.distkey AS int2[]))[i] " +
                "        WHERE a.attrelid = p.localoid ORDER BY i) " +
                "FROM pg_catalog.gp_distribution_policy p " +
                "WHERE p.localoid = CAST(? AS regclass)")) {
            statement.setString(1, quotedTableName);
            try (ResultSet resultSet = statement.executeQuery()) {
                if (!resultSet.next()) {
                    // Views, external and foreign tables have no distribution policy
                    return new TableDistribution(null, ImmutableList.of());
                }
                String policyType = resultSet.getString(1);
                ImmutableList.Builder<String> columns = ImmutableList.builder();
                for (Object column : (Object[]) resultSet.getArray(2).getArray()) {
                    columns.add(column.toString());
                }
                List<String> distributionColumns = columns.build();
                if ("r".equals(policyType)) {
                    return new TableDistribution(DistributionPolicy.REPLICATED, ImmutableList.of());
                }
                return new TableDistribution(distributionColumns.isEmpty() ? DistributionPolicy.RANDOM : DistributionPolicy.HASH, distributionColumns);
            }
        }
    }

    private record TableDistribution(DistributionPolicy policy, List<String> columns) {}

    @Override
    public ResultSet getTables(Connection connection, Optional<String> remoteSchemaName, Optional<String> remoteTableName)
            throws SQLException
    {
        ResultSet tables = super.getTables(connection, remoteSchemaName, remoteTableName);
        if (includePartitionChildren || remoteTableName.isPresent()) {
            // Explicitly referenced partitions stay accessible
            return tables;
        }
        Set<SchemaTableName> partitionChildren = getPartitionChildren(connection);
        if (partitionChildren.isEmpty()) {
            return tables;
        }
        return FilteringResultSet.filter(tables, resultSet -> !partitionChildren.contains(new SchemaTableName(
                resultSet.getString("TABLE_SCHEM"),
                resultSet.getString("TABLE_NAME"))));
    }

    private static Set<SchemaTableName> getPartitionChildren(Connection connection)
            throws SQLException
    {
        // Probe the catalog instead of catching errors, as a failed statement would abort an open transaction
        if (queryBoolean(connection, "SELECT EXISTS (SELECT 1 FROM pg_catalog.pg_attribute WHERE attrelid = CAST('pg_catalog.pg_class' AS regclass) AND attname = 'relispartition')")) {
            return readPartitionChildren(connection, GP7_PARTITION_CHILDREN_QUERY);
        }
        if (queryBoolean(connection, "SELECT to_regclass('pg_catalog.pg_partition_rule') IS NOT NULL")) {
            return readPartitionChildren(connection, GP6_PARTITION_CHILDREN_QUERY);
        }
        return ImmutableSet.of();
    }

    private static boolean queryBoolean(Connection connection, String sql)
            throws SQLException
    {
        try (PreparedStatement statement = connection.prepareStatement(sql);
                ResultSet resultSet = statement.executeQuery()) {
            return resultSet.next() && resultSet.getBoolean(1);
        }
    }

    private static Set<SchemaTableName> readPartitionChildren(Connection connection, String sql)
            throws SQLException
    {
        // SchemaTableName lower-cases names, the same is done when filtering
        ImmutableSet.Builder<SchemaTableName> children = ImmutableSet.builder();
        try (PreparedStatement statement = connection.prepareStatement(sql);
                ResultSet resultSet = statement.executeQuery()) {
            while (resultSet.next()) {
                children.add(new SchemaTableName(resultSet.getString(1), resultSet.getString(2)));
            }
        }
        return children.build();
    }
}
