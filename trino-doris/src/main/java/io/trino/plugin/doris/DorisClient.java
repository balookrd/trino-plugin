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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Inject;
import com.mysql.cj.jdbc.JdbcStatement;
import io.trino.plugin.base.aggregation.AggregateFunctionRewriter;
import io.trino.plugin.base.aggregation.AggregateFunctionRule;
import io.trino.plugin.base.expression.ConnectorExpressionRewriter;
import io.trino.plugin.base.mapping.IdentifierMapping;
import io.trino.plugin.doris.DorisTableProperties.TableModel;
import io.trino.plugin.jdbc.BaseJdbcClient;
import io.trino.plugin.jdbc.BaseJdbcConfig;
import io.trino.plugin.jdbc.ColumnMapping;
import io.trino.plugin.jdbc.ConnectionFactory;
import io.trino.plugin.jdbc.JdbcColumnHandle;
import io.trino.plugin.jdbc.JdbcExpression;
import io.trino.plugin.jdbc.JdbcJoinCondition;
import io.trino.plugin.jdbc.JdbcSortItem;
import io.trino.plugin.jdbc.JdbcTableHandle;
import io.trino.plugin.jdbc.JdbcTypeHandle;
import io.trino.plugin.jdbc.ObjectReadFunction;
import io.trino.plugin.jdbc.ObjectWriteFunction;
import io.trino.plugin.jdbc.QueryBuilder;
import io.trino.plugin.jdbc.RemoteTableName;
import io.trino.plugin.jdbc.WriteMapping;
import io.trino.plugin.jdbc.aggregation.ImplementAvgDecimal;
import io.trino.plugin.jdbc.aggregation.ImplementAvgFloatingPoint;
import io.trino.plugin.jdbc.aggregation.ImplementCount;
import io.trino.plugin.jdbc.aggregation.ImplementCountAll;
import io.trino.plugin.jdbc.aggregation.ImplementCountDistinct;
import io.trino.plugin.jdbc.aggregation.ImplementMinMax;
import io.trino.plugin.jdbc.aggregation.ImplementStddevPop;
import io.trino.plugin.jdbc.aggregation.ImplementStddevSamp;
import io.trino.plugin.jdbc.aggregation.ImplementSum;
import io.trino.plugin.jdbc.aggregation.ImplementVariancePop;
import io.trino.plugin.jdbc.aggregation.ImplementVarianceSamp;
import io.trino.plugin.jdbc.expression.JdbcConnectorExpressionRewriterBuilder;
import io.trino.plugin.jdbc.expression.ParameterizedExpression;
import io.trino.plugin.jdbc.logging.RemoteQueryModifier;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.AggregateFunction;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.JoinCondition;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.expression.ConnectorExpression;
import io.trino.spi.type.CharType;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.Decimals;
import io.trino.spi.type.Int128;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeManager;
import io.trino.spi.type.TypeSignature;
import io.trino.spi.type.VarcharType;

import java.math.BigInteger;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.OptionalLong;
import java.util.function.BiFunction;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Strings.emptyToNull;
import static com.google.common.base.Strings.isNullOrEmpty;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.plugin.base.util.JsonTypeUtil.jsonParse;
import static io.trino.plugin.doris.DorisTableProperties.BUCKETS;
import static io.trino.plugin.doris.DorisTableProperties.DISTRIBUTED_BY;
import static io.trino.plugin.doris.DorisTableProperties.KEY_COLUMNS;
import static io.trino.plugin.doris.DorisTableProperties.REPLICATION_NUM;
import static io.trino.plugin.doris.DorisTableProperties.TABLE_MODEL;
import static io.trino.plugin.doris.DorisTableProperties.getBuckets;
import static io.trino.plugin.doris.DorisTableProperties.getDistributedBy;
import static io.trino.plugin.doris.DorisTableProperties.getExtraProperties;
import static io.trino.plugin.doris.DorisTableProperties.getKeyColumns;
import static io.trino.plugin.doris.DorisTableProperties.getReplicationNum;
import static io.trino.plugin.doris.DorisTableProperties.getTableModel;
import static io.trino.plugin.jdbc.DecimalConfig.DecimalMapping.ALLOW_OVERFLOW;
import static io.trino.plugin.jdbc.DecimalSessionSessionProperties.getDecimalDefaultScale;
import static io.trino.plugin.jdbc.DecimalSessionSessionProperties.getDecimalRounding;
import static io.trino.plugin.jdbc.DecimalSessionSessionProperties.getDecimalRoundingMode;
import static io.trino.plugin.jdbc.JdbcErrorCode.JDBC_ERROR;
import static io.trino.plugin.jdbc.PredicatePushdownController.DISABLE_PUSHDOWN;
import static io.trino.plugin.jdbc.StandardColumnMappings.bigintColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.bigintWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.booleanColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.booleanWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.charColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.charWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.dateColumnMappingUsingLocalDate;
import static io.trino.plugin.jdbc.StandardColumnMappings.dateWriteFunctionUsingLocalDate;
import static io.trino.plugin.jdbc.StandardColumnMappings.decimalColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.doubleColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.doubleWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.integerColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.integerWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.longDecimalWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.longTimestampWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.realWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.shortDecimalWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.smallintColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.smallintWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.timestampColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.timestampWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.tinyintColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.tinyintWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.varcharColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.varcharReadFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.varcharWriteFunction;
import static io.trino.plugin.jdbc.TypeHandlingJdbcSessionProperties.getUnsupportedTypeHandling;
import static io.trino.plugin.jdbc.UnsupportedTypeHandling.CONVERT_TO_VARCHAR;
import static io.trino.spi.StandardErrorCode.ALREADY_EXISTS;
import static io.trino.spi.StandardErrorCode.INVALID_TABLE_PROPERTY;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.StandardErrorCode.NUMERIC_VALUE_OUT_OF_RANGE;
import static io.trino.spi.StandardErrorCode.SCHEMA_NOT_EMPTY;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.DecimalType.createDecimalType;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.StandardTypes.JSON;
import static io.trino.spi.type.TimestampType.createTimestampType;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.spi.type.VarcharType.createUnboundedVarcharType;
import static io.trino.spi.type.VarcharType.createVarcharType;
import static java.lang.Float.floatToRawIntBits;
import static java.lang.Math.max;
import static java.lang.Math.min;
import static java.lang.String.format;
import static java.lang.String.join;
import static java.util.Locale.ENGLISH;
import static java.util.stream.Collectors.joining;

/**
 * Apache Doris client. Doris frontends implement the MySQL protocol, but the type system and DDL differ
 * from MySQL, so column types are resolved from {@code information_schema.columns.column_type}, which,
 * unlike the JDBC driver metadata, reports native Doris types (e.g. LARGEINT, ARRAY, BOOLEAN as tinyint(1)).
 */
public class DorisClient
        extends BaseJdbcClient
{
    // Doris VARCHAR(n) and CHAR(n) lengths are in bytes; UTF-8 needs up to 4 bytes per character
    private static final int MAX_BYTES_PER_CHARACTER = 4;
    private static final int MAX_VARCHAR_LENGTH = 65533;
    private static final int MAX_DATETIME_PRECISION = 6;
    private static final int MAX_DECIMAL_PRECISION = 38;
    private static final String NO_COMMENT = "";

    private static final Pattern TYPE_NAME_BASE = Pattern.compile("^([a-z_0-9]+)");
    private static final Pattern KEYS_CLAUSE = Pattern.compile("^(DUPLICATE|UNIQUE|AGGREGATE) KEY\\((.*)\\)$", Pattern.MULTILINE);
    private static final Pattern DISTRIBUTION_CLAUSE = Pattern.compile("^DISTRIBUTED BY (?:HASH\\((.*)\\)|RANDOM) BUCKETS (\\d+|AUTO)$", Pattern.MULTILINE);
    private static final Pattern REPLICATION_ALLOCATION = Pattern.compile("\"replication_allocation\" = \"tag\\.location\\.default: (\\d+)\"");

    // Columns are listed with the shape expected from DatabaseMetaData.getColumns(). DATA_TYPE is not meaningful,
    // the mapping relies on TYPE_NAME which carries the native Doris column type.
    private static final String COLUMNS_QUERY = "" +
            "SELECT table_schema AS TABLE_CAT, NULL AS TABLE_SCHEM, table_name AS TABLE_NAME, column_name AS COLUMN_NAME, " +
            Types.OTHER + " AS DATA_TYPE, column_type AS TYPE_NAME, " +
            "COALESCE(character_maximum_length, numeric_precision, datetime_precision) AS COLUMN_SIZE, " +
            "numeric_scale AS DECIMAL_DIGITS, " +
            "CASE is_nullable WHEN 'NO' THEN " + DatabaseMetaData.columnNoNulls + " ELSE " + DatabaseMetaData.columnNullable + " END AS NULLABLE, " +
            "column_comment AS REMARKS " +
            "FROM information_schema.columns ";

    private final Type jsonType;
    private final Optional<Integer> defaultReplicationNum;
    private final ConnectorExpressionRewriter<ParameterizedExpression> connectorExpressionRewriter;
    private final AggregateFunctionRewriter<JdbcExpression, ?> aggregateFunctionRewriter;

    @Inject
    public DorisClient(
            BaseJdbcConfig config,
            DorisConfig dorisConfig,
            ConnectionFactory connectionFactory,
            QueryBuilder queryBuilder,
            TypeManager typeManager,
            IdentifierMapping identifierMapping,
            RemoteQueryModifier queryModifier)
    {
        super("`", connectionFactory, queryBuilder, config.getJdbcTypesMappedToVarchar(), identifierMapping, queryModifier, false);
        this.jsonType = typeManager.getType(new TypeSignature(JSON));
        this.defaultReplicationNum = dorisConfig.getDefaultReplicationNum();

        this.connectorExpressionRewriter = JdbcConnectorExpressionRewriterBuilder.newBuilder()
                .addStandardRules(this::quoted)
                // No "real" on the list; pushdown on REAL is disabled also in toColumnMapping
                .withTypeClass("numeric_type", ImmutableSet.of("tinyint", "smallint", "integer", "bigint", "decimal", "double"))
                .map("$equal(left: numeric_type, right: numeric_type)").to("left = right")
                .map("$not_equal(left: numeric_type, right: numeric_type)").to("left <> right")
                .map("$less_than(left: numeric_type, right: numeric_type)").to("left < right")
                .map("$less_than_or_equal(left: numeric_type, right: numeric_type)").to("left <= right")
                .map("$greater_than(left: numeric_type, right: numeric_type)").to("left > right")
                .map("$greater_than_or_equal(left: numeric_type, right: numeric_type)").to("left >= right")
                // Doris treats a backslash in LIKE patterns as an escape character by default, unlike Trino,
                // so only patterns with an explicit escape character are pushed down
                .map("$like(value: varchar, pattern: varchar, escape: varchar(1)): boolean").to("value LIKE pattern ESCAPE escape")
                .map("$not($is_null(value))").to("value IS NOT NULL")
                .map("$is_null(value)").to("value IS NULL")
                .build();

        JdbcTypeHandle bigintTypeHandle = new JdbcTypeHandle(Types.BIGINT, Optional.of("bigint"), Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty());
        this.aggregateFunctionRewriter = new AggregateFunctionRewriter<>(
                this.connectorExpressionRewriter,
                ImmutableSet.<AggregateFunctionRule<JdbcExpression, ParameterizedExpression>>builder()
                        .add(new ImplementCountAll(bigintTypeHandle))
                        .add(new ImplementCount(bigintTypeHandle))
                        .add(new ImplementCountDistinct(bigintTypeHandle, false))
                        .add(new ImplementMinMax(false))
                        .add(new ImplementSum(DorisClient::toTypeHandle))
                        .add(new ImplementAvgFloatingPoint())
                        .add(new ImplementAvgDecimal())
                        .add(new ImplementAvgBigint())
                        .add(new ImplementStddevSamp())
                        .add(new ImplementStddevPop())
                        .add(new ImplementVarianceSamp())
                        .add(new ImplementVariancePop())
                        .build());
    }

    private static Optional<JdbcTypeHandle> toTypeHandle(DecimalType decimalType)
    {
        return Optional.of(new JdbcTypeHandle(Types.DECIMAL, Optional.of("decimal"), Optional.of(decimalType.getPrecision()), Optional.of(decimalType.getScale()), Optional.empty(), Optional.empty()));
    }

    @Override
    public Optional<ParameterizedExpression> convertPredicate(ConnectorSession session, ConnectorExpression expression, Map<String, ColumnHandle> assignments)
    {
        return connectorExpressionRewriter.rewrite(session, expression, assignments);
    }

    @Override
    public Optional<JdbcExpression> implementAggregation(ConnectorSession session, AggregateFunction aggregate, Map<String, ColumnHandle> assignments)
    {
        return aggregateFunctionRewriter.rewrite(session, aggregate, assignments);
    }

    @Override
    public boolean supportsAggregationPushdown(ConnectorSession session, JdbcTableHandle table, List<AggregateFunction> aggregates, Map<String, ColumnHandle> assignments, List<List<ColumnHandle>> groupingSets)
    {
        // Doris compares strings by their bytes, like Trino, so grouping by textual columns is safe
        return true;
    }

    // Doris databases are exposed as JDBC catalogs, like in MySQL

    @Override
    public Collection<String> listSchemas(Connection connection)
    {
        try (ResultSet resultSet = connection.getMetaData().getCatalogs()) {
            ImmutableSet.Builder<String> schemaNames = ImmutableSet.builder();
            while (resultSet.next()) {
                String schemaName = resultSet.getString("TABLE_CAT");
                if (filterSchema(schemaName)) {
                    schemaNames.add(schemaName);
                }
            }
            return schemaNames.build();
        }
        catch (SQLException e) {
            throw new TrinoException(JDBC_ERROR, e);
        }
    }

    @Override
    protected boolean filterSchema(String schemaName)
    {
        if (schemaName.equalsIgnoreCase("mysql") || schemaName.equalsIgnoreCase("__internal_schema")) {
            return false;
        }
        return super.filterSchema(schemaName);
    }

    @Override
    public ResultSet getTables(Connection connection, Optional<String> schemaName, Optional<String> tableName)
            throws SQLException
    {
        // The driver does not report table comments for Doris, information_schema does
        StringBuilder sql = new StringBuilder("" +
                "SELECT table_schema AS TABLE_CAT, NULL AS TABLE_SCHEM, table_name AS TABLE_NAME, " +
                "CASE table_type WHEN 'VIEW' THEN 'VIEW' ELSE 'TABLE' END AS TABLE_TYPE, table_comment AS REMARKS " +
                "FROM information_schema.tables " +
                "WHERE table_type IN ('BASE TABLE', 'VIEW')");
        if (schemaName.isPresent()) {
            sql.append(" AND table_schema = ?");
        }
        if (tableName.isPresent()) {
            sql.append(" AND table_name = ?");
        }
        sql.append(" ORDER BY table_schema, table_name");
        PreparedStatement statement = connection.prepareStatement(sql.toString());
        int index = 1;
        if (schemaName.isPresent()) {
            statement.setString(index++, schemaName.get());
        }
        if (tableName.isPresent()) {
            statement.setString(index, tableName.get());
        }
        statement.closeOnCompletion();
        return statement.executeQuery();
    }

    @Override
    protected String getTableSchemaName(ResultSet resultSet)
            throws SQLException
    {
        return resultSet.getString("TABLE_CAT");
    }

    @Override
    public Optional<String> getTableComment(ResultSet resultSet)
            throws SQLException
    {
        return Optional.ofNullable(emptyToNull(resultSet.getString("REMARKS")));
    }

    @Override
    protected ResultSet getColumns(RemoteTableName remoteTableName, DatabaseMetaData metadata)
            throws SQLException
    {
        PreparedStatement statement = metadata.getConnection().prepareStatement(COLUMNS_QUERY +
                "WHERE table_schema = ? AND table_name = ? " +
                "ORDER BY ordinal_position");
        statement.setString(1, remoteTableName.getCatalogName().orElseThrow());
        statement.setString(2, remoteTableName.getTableName());
        statement.closeOnCompletion();
        return statement.executeQuery();
    }

    @Override
    protected ResultSet getAllTableColumns(Connection connection, Optional<String> remoteSchemaName)
            throws SQLException
    {
        PreparedStatement statement;
        if (remoteSchemaName.isPresent()) {
            statement = connection.prepareStatement(COLUMNS_QUERY +
                    "WHERE table_schema = ? " +
                    "ORDER BY table_schema, table_name, ordinal_position");
            statement.setString(1, remoteSchemaName.get());
        }
        else {
            statement = connection.prepareStatement(COLUMNS_QUERY +
                    "ORDER BY table_schema, table_name, ordinal_position");
        }
        statement.closeOnCompletion();
        return statement.executeQuery();
    }

    @Override
    public void abortReadConnection(Connection connection, ResultSet resultSet)
            throws SQLException
    {
        if (!resultSet.isAfterLast()) {
            // Abort connection before closing. Without this, the MySQL driver
            // attempts to drain the connection by reading all the results.
            connection.abort(directExecutor());
        }
    }

    @Override
    public PreparedStatement getPreparedStatement(Connection connection, String sql, Optional<Integer> columnCount)
            throws SQLException
    {
        PreparedStatement statement = connection.prepareStatement(sql);
        if (statement.isWrapperFor(JdbcStatement.class)) {
            statement.unwrap(JdbcStatement.class).enableStreamingResults();
        }
        return statement;
    }

    @Override
    public Optional<ColumnMapping> toColumnMapping(ConnectorSession session, Connection connection, JdbcTypeHandle typeHandle)
    {
        Optional<ColumnMapping> mapping = getForcedMappingToVarchar(typeHandle);
        if (mapping.isPresent()) {
            return mapping;
        }

        String typeName = typeHandle.jdbcTypeName()
                .orElseThrow(() -> new TrinoException(JDBC_ERROR, "Type name is missing: " + typeHandle))
                .toLowerCase(ENGLISH);
        // Type handles come either from information_schema (native Doris type names such as "decimalv3(10, 2)")
        // or from result set metadata of a query passed through the query table function (MySQL type names)
        boolean nativeType = typeHandle.jdbcType() == Types.OTHER;

        switch (baseTypeName(typeName)) {
            case "boolean":
                return Optional.of(booleanColumnMapping());
            case "tinyint":
                // information_schema reports BOOLEAN as tinyint(1), Doris TINYINT is always tinyint(4)
                return Optional.of(typeName.equals("tinyint(1)") ? booleanColumnMapping() : tinyintColumnMapping());
            case "smallint":
                return Optional.of(smallintColumnMapping());
            case "int":
            case "integer":
                return Optional.of(integerColumnMapping());
            case "bigint":
                return Optional.of(bigintColumnMapping());
            case "largeint":
                return Optional.of(largeintColumnMapping());
            case "float":
                // Disable pushdown because floating-point values are approximate
                return Optional.of(ColumnMapping.longMapping(
                        REAL,
                        (resultSet, columnIndex) -> floatToRawIntBits(resultSet.getFloat(columnIndex)),
                        realWriteFunction(),
                        DISABLE_PUSHDOWN));
            case "double":
                return Optional.of(doubleColumnMapping());
            case "decimal":
            case "decimalv2":
            case "decimalv3":
            case "decimal32":
            case "decimal64":
            case "decimal128":
            case "decimal128i":
            case "decimal256":
                return decimalMapping(session, typeHandle);
            case "date":
            case "datev2":
                return Optional.of(dateColumnMappingUsingLocalDate());
            case "datetime":
            case "datetimev2": {
                TimestampType timestampType = createTimestampType(min(typeHandle.columnSize().orElse(MAX_DATETIME_PRECISION), MAX_DATETIME_PRECISION));
                return Optional.of(timestampColumnMapping(timestampType));
            }
            case "char":
                if (nativeType) {
                    return Optional.of(charColumnMapping(CharType.createCharType(typeHandle.requiredColumnSize()), true));
                }
                // MySQL protocol result metadata reports most textual values as CHAR without a meaningful length
                return Optional.of(varcharColumnMapping(createUnboundedVarcharType(), true));
            case "varchar":
                if (nativeType && typeHandle.requiredColumnSize() <= VarcharType.MAX_LENGTH) {
                    return Optional.of(varcharColumnMapping(createVarcharType(typeHandle.requiredColumnSize()), true));
                }
                return Optional.of(varcharColumnMapping(createUnboundedVarcharType(), true));
            case "string":
            case "text":
            case "tinytext":
            case "mediumtext":
            case "longtext":
                return Optional.of(varcharColumnMapping(createUnboundedVarcharType(), true));
            case "json":
            case "jsonb":
            case "variant":
                return Optional.of(jsonColumnMapping());
            case "ipv4":
            case "ipv6":
            case "array":
            case "map":
            case "struct":
                // Exposed as their textual representation; comparison semantics differ from text, so no pushdown
                return Optional.of(ColumnMapping.sliceMapping(
                        createUnboundedVarcharType(),
                        varcharReadFunction(createUnboundedVarcharType()),
                        varcharWriteFunction(),
                        DISABLE_PUSHDOWN));
            case "hll":
            case "bitmap":
            case "quantile_state":
            case "agg_state":
                // These columns cannot be read directly, only through their aggregate functions
                return Optional.empty();
            default:
                break;
        }

        if (getUnsupportedTypeHandling(session) == CONVERT_TO_VARCHAR) {
            return mapToUnboundedVarchar(typeHandle);
        }
        return Optional.empty();
    }

    private static String baseTypeName(String typeName)
    {
        Matcher matcher = TYPE_NAME_BASE.matcher(typeName);
        return matcher.find() ? matcher.group(1) : typeName;
    }

    private static Optional<ColumnMapping> decimalMapping(ConnectorSession session, JdbcTypeHandle typeHandle)
    {
        int precision = typeHandle.requiredColumnSize();
        int scale = typeHandle.decimalDigits().orElse(0);
        if (precision > Decimals.MAX_PRECISION) {
            if (getDecimalRounding(session) == ALLOW_OVERFLOW) {
                return Optional.of(decimalColumnMapping(createDecimalType(Decimals.MAX_PRECISION, min(scale, getDecimalDefaultScale(session))), getDecimalRoundingMode(session)));
            }
            return Optional.empty();
        }
        return Optional.of(decimalColumnMapping(createDecimalType(precision, max(scale, 0))));
    }

    private static ColumnMapping largeintColumnMapping()
    {
        // LARGEINT is a 128-bit integer (up to 39 digits), mapped to the widest Trino decimal.
        // Values exceeding 38 digits fail on read; such columns can be mapped to varchar with jdbc-types-mapped-to-varchar=largeint
        DecimalType decimalType = createDecimalType(MAX_DECIMAL_PRECISION, 0);
        ObjectReadFunction readFunction = ObjectReadFunction.of(Int128.class, (resultSet, columnIndex) -> {
            BigInteger value = new BigInteger(resultSet.getString(columnIndex));
            if (Decimals.overflows(value, MAX_DECIMAL_PRECISION)) {
                throw new TrinoException(NUMERIC_VALUE_OUT_OF_RANGE, "LARGEINT value out of range of decimal(38, 0): " + value);
            }
            return Int128.valueOf(value);
        });
        return ColumnMapping.objectMapping(decimalType, readFunction, longDecimalWriteFunction(decimalType));
    }

    private ColumnMapping jsonColumnMapping()
    {
        return ColumnMapping.sliceMapping(
                jsonType,
                (resultSet, columnIndex) -> jsonParse(utf8Slice(resultSet.getString(columnIndex))),
                varcharWriteFunction(),
                DISABLE_PUSHDOWN);
    }

    @Override
    public WriteMapping toWriteMapping(ConnectorSession session, Type type)
    {
        if (type == BOOLEAN) {
            return WriteMapping.booleanMapping("boolean", booleanWriteFunction());
        }
        if (type == TINYINT) {
            return WriteMapping.longMapping("tinyint", tinyintWriteFunction());
        }
        if (type == SMALLINT) {
            return WriteMapping.longMapping("smallint", smallintWriteFunction());
        }
        if (type == INTEGER) {
            return WriteMapping.longMapping("int", integerWriteFunction());
        }
        if (type == BIGINT) {
            return WriteMapping.longMapping("bigint", bigintWriteFunction());
        }
        if (type == REAL) {
            return WriteMapping.longMapping("float", realWriteFunction());
        }
        if (type == DOUBLE) {
            return WriteMapping.doubleMapping("double", doubleWriteFunction());
        }
        if (type instanceof DecimalType decimalType) {
            String dataType = format("decimal(%s, %s)", decimalType.getPrecision(), decimalType.getScale());
            if (decimalType.isShort()) {
                return WriteMapping.longMapping(dataType, shortDecimalWriteFunction(decimalType));
            }
            return WriteMapping.objectMapping(dataType, longDecimalWriteFunction(decimalType));
        }
        if (type == DATE) {
            return WriteMapping.longMapping("date", dateWriteFunctionUsingLocalDate());
        }
        if (type instanceof TimestampType timestampType) {
            if (timestampType.getPrecision() <= MAX_DATETIME_PRECISION) {
                return WriteMapping.longMapping(format("datetime(%s)", timestampType.getPrecision()), timestampWriteFunction(timestampType));
            }
            ObjectWriteFunction writeFunction = longTimestampWriteFunction(timestampType, MAX_DATETIME_PRECISION);
            return WriteMapping.objectMapping(format("datetime(%s)", MAX_DATETIME_PRECISION), writeFunction);
        }
        if (type instanceof CharType charType) {
            return WriteMapping.sliceMapping(textualDataType(charType.getLength()), charWriteFunction());
        }
        if (type instanceof VarcharType varcharType) {
            String dataType = varcharType.getLength().isPresent() ? textualDataType(varcharType.getBoundedLength()) : "string";
            return WriteMapping.sliceMapping(dataType, varcharWriteFunction());
        }
        if (type.equals(jsonType)) {
            return WriteMapping.sliceMapping("json", varcharWriteFunction());
        }
        throw new TrinoException(NOT_SUPPORTED, "Unsupported column type: " + type.getDisplayName());
    }

    private static String textualDataType(long characters)
    {
        long bytes = characters * MAX_BYTES_PER_CHARACTER;
        if (bytes > MAX_VARCHAR_LENGTH) {
            return "string";
        }
        return "varchar(" + bytes + ")";
    }

    @Override
    public void createTable(ConnectorSession session, ConnectorTableMetadata tableMetadata)
    {
        try {
            createTable(session, tableMetadata, tableMetadata.getTable().getTableName());
        }
        catch (SQLException e) {
            if (isTableExistsError(e)) {
                throw new TrinoException(ALREADY_EXISTS, e);
            }
            throw new TrinoException(JDBC_ERROR, e);
        }
    }

    private static boolean isTableExistsError(SQLException e)
    {
        return e.getMessage() != null && e.getMessage().matches("(?s).*Table '.*' already exists.*");
    }

    @Override
    protected List<String> createTableSqls(RemoteTableName remoteTableName, List<String> columns, ConnectorTableMetadata tableMetadata)
    {
        Map<String, Object> properties = tableMetadata.getProperties();
        List<String> columnNames = tableMetadata.getColumns().stream()
                .map(column -> column.getName().toLowerCase(ENGLISH))
                .collect(toImmutableList());
        TableModel model = getTableModel(properties).orElse(TableModel.DUPLICATE);
        List<String> keyColumns = normalizeColumns(getKeyColumns(properties), columnNames, KEY_COLUMNS);
        List<String> distributedBy = normalizeColumns(getDistributedBy(properties), columnNames, DISTRIBUTED_BY);

        if (!keyColumns.equals(columnNames.subList(0, min(keyColumns.size(), columnNames.size())))) {
            throw new TrinoException(INVALID_TABLE_PROPERTY, "Columns in '%s' must be the leading columns of the table in the same order: %s".formatted(KEY_COLUMNS, keyColumns));
        }

        StringBuilder sql = new StringBuilder()
                .append("CREATE TABLE ").append(quoted(remoteTableName))
                .append(" (").append(join(", ", columns)).append(")");

        ImmutableMap.Builder<String, String> tableProperties = ImmutableMap.builder();
        switch (model) {
            case DUPLICATE -> {
                if (keyColumns.isEmpty()) {
                    // Without explicit keys Doris would use the leading columns as the sort key, which fails for e.g. string or double columns
                    tableProperties.put("enable_duplicate_without_keys_by_default", "true");
                }
                else {
                    sql.append(" DUPLICATE KEY(").append(quotedColumns(keyColumns)).append(")");
                }
            }
            case UNIQUE -> {
                if (keyColumns.isEmpty()) {
                    throw new TrinoException(INVALID_TABLE_PROPERTY, "Table model %s requires the '%s' property".formatted(TableModel.UNIQUE, KEY_COLUMNS));
                }
                sql.append(" UNIQUE KEY(").append(quotedColumns(keyColumns)).append(")");
                if (distributedBy.isEmpty()) {
                    // Random distribution is not allowed for the unique model
                    distributedBy = keyColumns;
                }
            }
        }

        tableMetadata.getComment().ifPresent(comment -> sql.append(" COMMENT ").append(dorisStringLiteral(comment)));

        sql.append(" DISTRIBUTED BY ");
        if (distributedBy.isEmpty()) {
            sql.append("RANDOM");
        }
        else {
            sql.append("HASH(").append(quotedColumns(distributedBy)).append(")");
        }
        sql.append(" BUCKETS ").append(getBuckets(properties).map(String::valueOf).orElse("AUTO"));

        getReplicationNum(properties).or(() -> defaultReplicationNum)
                .ifPresent(replicationNum -> tableProperties.put("replication_num", String.valueOf(replicationNum)));
        tableProperties.putAll(getExtraProperties(properties));
        Map<String, String> allProperties = tableProperties.buildKeepingLast();
        if (!allProperties.isEmpty()) {
            sql.append(allProperties.entrySet().stream()
                    .map(entry -> dorisStringLiteral(entry.getKey()) + " = " + dorisStringLiteral(entry.getValue()))
                    .collect(joining(", ", " PROPERTIES (", ")")));
        }
        return ImmutableList.of(sql.toString());
    }

    private static List<String> normalizeColumns(List<String> columns, List<String> tableColumns, String property)
    {
        ImmutableList.Builder<String> normalized = ImmutableList.builder();
        for (String column : columns) {
            String name = column.toLowerCase(ENGLISH);
            if (!tableColumns.contains(name)) {
                throw new TrinoException(INVALID_TABLE_PROPERTY, "Column '%s' specified in property '%s' doesn't exist in table".formatted(column, property));
            }
            normalized.add(name);
        }
        return normalized.build();
    }

    private String quotedColumns(List<String> columns)
    {
        return columns.stream().map(this::quoted).collect(joining(", "));
    }

    @Override
    protected String getColumnDefinitionSql(ConnectorSession session, ColumnMetadata column, String columnName)
    {
        StringBuilder sql = new StringBuilder()
                .append(quoted(columnName))
                .append(" ")
                .append(toWriteMapping(session, column.getType()).getDataType())
                .append(column.isNullable() ? " NULL" : " NOT NULL");
        if (column.getComment() != null) {
            sql.append(" COMMENT ").append(dorisStringLiteral(column.getComment()));
        }
        return sql.toString();
    }

    private static String dorisStringLiteral(String value)
    {
        return "'" + value.replace("\\", "\\\\").replace("'", "\\'") + "'";
    }

    @Override
    public Map<String, Object> getTableProperties(ConnectorSession session, JdbcTableHandle tableHandle)
    {
        verify(tableHandle.getAuthorization().isEmpty(), "Unexpected authorization is required for table: %s", tableHandle);
        return readTableProperties(session, tableHandle.getRequiredNamedRelation().getRemoteTableName());
    }

    private Map<String, Object> readTableProperties(ConnectorSession session, RemoteTableName remoteTableName)
    {
        try (Connection connection = connectionFactory.openConnection(session);
                PreparedStatement statement = connection.prepareStatement("SHOW CREATE TABLE " + quoted(remoteTableName));
                ResultSet resultSet = statement.executeQuery()) {
            if (!resultSet.next() || resultSet.getMetaData().getColumnCount() < 2) {
                return ImmutableMap.of();
            }
            return parseTableProperties(resultSet.getString(2));
        }
        catch (SQLException e) {
            throw new TrinoException(JDBC_ERROR, e);
        }
    }

    static Map<String, Object> parseTableProperties(String createTableSql)
    {
        ImmutableMap.Builder<String, Object> properties = ImmutableMap.builder();
        Matcher keys = KEYS_CLAUSE.matcher(createTableSql);
        if (keys.find()) {
            String model = keys.group(1);
            // AGGREGATE tables cannot be created through Trino, so the model is not exposed for them
            if (!model.equals("AGGREGATE")) {
                properties.put(TABLE_MODEL, TableModel.valueOf(model));
            }
            properties.put(KEY_COLUMNS, parseColumnList(keys.group(2)));
        }
        else if (createTableSql.contains("ENGINE=OLAP")) {
            properties.put(TABLE_MODEL, TableModel.DUPLICATE);
        }
        Matcher distribution = DISTRIBUTION_CLAUSE.matcher(createTableSql);
        if (distribution.find()) {
            if (distribution.group(1) != null) {
                properties.put(DISTRIBUTED_BY, parseColumnList(distribution.group(1)));
            }
            if (!distribution.group(2).equals("AUTO")) {
                properties.put(BUCKETS, Integer.parseInt(distribution.group(2)));
            }
        }
        Matcher replication = REPLICATION_ALLOCATION.matcher(createTableSql);
        if (replication.find()) {
            properties.put(REPLICATION_NUM, Integer.parseInt(replication.group(1)));
        }
        return properties.buildOrThrow();
    }

    private static List<String> parseColumnList(String columns)
    {
        List<String> result = new ArrayList<>();
        for (String column : columns.split(",")) {
            String name = column.trim();
            if (name.startsWith("`") && name.endsWith("`") && name.length() >= 2) {
                name = name.substring(1, name.length() - 1).replace("``", "`");
            }
            result.add(name);
        }
        return ImmutableList.copyOf(result);
    }

    @Override
    public OptionalLong delete(ConnectorSession session, JdbcTableHandle handle)
    {
        RemoteTableName remoteTableName = handle.getRequiredNamedRelation().getRemoteTableName();
        if (handle.getConstraint().isAll() && handle.getConstraintExpressions().isEmpty()) {
            // Doris rejects DELETE without a filter
            execute(session, "TRUNCATE TABLE " + quoted(remoteTableName));
            return OptionalLong.empty();
        }
        OptionalLong deletedRows = super.delete(session, handle);
        // DELETE on a duplicate model table adds a delete predicate and always reports 0 affected rows
        if (readTableProperties(session, remoteTableName).get(TABLE_MODEL) == TableModel.DUPLICATE) {
            return OptionalLong.empty();
        }
        return deletedRows;
    }

    @Override
    public void setTableComment(ConnectorSession session, JdbcTableHandle handle, Optional<String> comment)
    {
        execute(session, format(
                "ALTER TABLE %s MODIFY COMMENT %s",
                quoted(handle.asPlainTable().getRemoteTableName()),
                dorisStringLiteral(comment.orElse(NO_COMMENT))));
    }

    @Override
    public void setColumnComment(ConnectorSession session, JdbcTableHandle handle, JdbcColumnHandle column, Optional<String> comment)
    {
        execute(session, format(
                "ALTER TABLE %s MODIFY COLUMN %s COMMENT %s",
                quoted(handle.asPlainTable().getRemoteTableName()),
                quoted(column.getColumnName()),
                dorisStringLiteral(comment.orElse(NO_COMMENT))));
    }

    @Override
    protected void addColumn(ConnectorSession session, Connection connection, RemoteTableName table, ColumnMetadata column)
            throws SQLException
    {
        String columnName = column.getName();
        verifyColumnName(connection.getMetaData(), columnName);
        String remoteColumnName = getIdentifierMapping().toRemoteColumnName(getRemoteIdentifiers(connection), columnName);
        execute(session, connection, format(
                "ALTER TABLE %s ADD COLUMN %s",
                quoted(table),
                getColumnDefinitionSql(session, column, remoteColumnName)));
    }

    @Override
    protected void renameColumn(ConnectorSession session, Connection connection, RemoteTableName remoteTableName, String remoteColumnName, String newRemoteColumnName)
            throws SQLException
    {
        execute(session, connection, format(
                "ALTER TABLE %s RENAME COLUMN %s %s",
                quoted(remoteTableName),
                quoted(remoteColumnName),
                quoted(newRemoteColumnName)));
    }

    @Override
    public void setColumnType(ConnectorSession session, JdbcTableHandle handle, JdbcColumnHandle column, Type type)
    {
        throw new TrinoException(NOT_SUPPORTED, "This connector does not support setting column types");
    }

    @Override
    public void dropNotNullConstraint(ConnectorSession session, JdbcTableHandle handle, JdbcColumnHandle column)
    {
        throw new TrinoException(NOT_SUPPORTED, "This connector does not support dropping a not null constraint");
    }

    @Override
    protected void copyTableSchema(ConnectorSession session, Connection connection, String catalogName, String schemaName, String tableName, String newTableName, List<String> columnNames)
    {
        // CREATE TABLE ... LIKE keeps the data model, distribution and properties (e.g. replication) of the target table
        try {
            execute(session, connection, format(
                    "CREATE TABLE %s LIKE %s",
                    quoted(catalogName, schemaName, newTableName),
                    quoted(catalogName, schemaName, tableName)));
        }
        catch (SQLException e) {
            throw new TrinoException(JDBC_ERROR, e);
        }
    }

    @Override
    public void renameTable(ConnectorSession session, JdbcTableHandle handle, SchemaTableName newTableName)
    {
        // Existing tables are identified by catalog (the Doris database), with no schema
        RemoteTableName remoteTableName = handle.asPlainTable().getRemoteTableName();
        verify(remoteTableName.getSchemaName().isEmpty());
        renameTable(session, null, remoteTableName.getCatalogName().orElse(null), remoteTableName.getTableName(), newTableName);
    }

    @Override
    protected void renameTable(ConnectorSession session, Connection connection, String catalogName, String remoteSchemaName, String remoteTableName, String newRemoteSchemaName, String newRemoteTableName)
            throws SQLException
    {
        // The MySQL driver reports an empty catalog when the URL has no database
        checkArgument(isNullOrEmpty(catalogName), "Unexpected catalog name: %s", catalogName);
        if (!remoteSchemaName.equals(newRemoteSchemaName)) {
            throw new TrinoException(NOT_SUPPORTED, "This connector does not support renaming tables across schemas");
        }
        execute(session, connection, format(
                "ALTER TABLE %s RENAME %s",
                quoted(null, remoteSchemaName, remoteTableName),
                quoted(newRemoteTableName)));
    }

    @Override
    protected void renameSchema(ConnectorSession session, Connection connection, String remoteSchemaName, String newRemoteSchemaName)
            throws SQLException
    {
        execute(session, connection, "ALTER DATABASE " + quoted(remoteSchemaName) + " RENAME " + quoted(newRemoteSchemaName));
    }

    @Override
    protected void dropSchema(ConnectorSession session, Connection connection, String remoteSchemaName, boolean cascade)
            throws SQLException
    {
        // DROP DATABASE always deletes all tables inside the database
        if (!cascade) {
            try (ResultSet tables = getTables(connection, Optional.of(remoteSchemaName), Optional.empty())) {
                if (tables.next()) {
                    throw new TrinoException(SCHEMA_NOT_EMPTY, "Cannot drop non-empty schema '%s'".formatted(remoteSchemaName));
                }
            }
        }
        execute(session, connection, "DROP DATABASE " + quoted(remoteSchemaName));
    }

    @Override
    public OptionalInt getMaxColumnNameLength(ConnectorSession session)
    {
        return getMaxColumnNameLengthFromDatabaseMetaData(session);
    }

    @Override
    protected Optional<BiFunction<String, Long, String>> limitFunction()
    {
        return Optional.of((sql, limit) -> sql + " LIMIT " + limit);
    }

    @Override
    public boolean isLimitGuaranteed(ConnectorSession session)
    {
        return true;
    }

    @Override
    public boolean supportsTopN(ConnectorSession session, JdbcTableHandle handle, List<JdbcSortItem> sortOrder)
    {
        return true;
    }

    @Override
    protected Optional<TopNFunction> topNFunction()
    {
        return Optional.of((query, sortItems, limit) -> {
            String orderBy = sortItems.stream()
                    .map(sortItem -> {
                        String ordering = sortItem.sortOrder().isAscending() ? "ASC" : "DESC";
                        String nullsHandling = sortItem.sortOrder().isNullsFirst() ? "NULLS FIRST" : "NULLS LAST";
                        return format("%s %s %s", quoted(sortItem.column().getColumnName()), ordering, nullsHandling);
                    })
                    .collect(joining(", "));
            return format("%s ORDER BY %s LIMIT %s", query, orderBy, limit);
        });
    }

    @Override
    public boolean isTopNGuaranteed(ConnectorSession session)
    {
        return true;
    }

    @Override
    protected boolean isSupportedJoinCondition(ConnectorSession session, JdbcJoinCondition joinCondition)
    {
        // Doris has no IS NOT DISTINCT FROM
        if (joinCondition.getOperator() == JoinCondition.Operator.IDENTICAL) {
            return false;
        }
        return Stream.of(joinCondition.getLeftColumn(), joinCondition.getRightColumn())
                .map(JdbcColumnHandle::getColumnType)
                .noneMatch(type -> type instanceof CharType);
    }
}
