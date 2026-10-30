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
import com.google.inject.Inject;
import io.trino.plugin.jdbc.TablePropertiesProvider;
import io.trino.spi.TrinoException;
import io.trino.spi.session.PropertyMetadata;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.MapType;
import io.trino.spi.type.TypeManager;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static io.trino.spi.StandardErrorCode.INVALID_TABLE_PROPERTY;
import static io.trino.spi.session.PropertyMetadata.enumProperty;
import static io.trino.spi.session.PropertyMetadata.integerProperty;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.util.Objects.requireNonNull;

/**
 * Doris specific table properties, used when creating a table:
 * <pre>
 * CREATE TABLE t (id bigint NOT NULL, name varchar)
 * WITH (table_model = 'UNIQUE', key_columns = ARRAY['id'], distributed_by = ARRAY['id'], buckets = 8, replication_num = 3)
 * </pre>
 */
public final class DorisTableProperties
        implements TablePropertiesProvider
{
    public static final String TABLE_MODEL = "table_model";
    public static final String KEY_COLUMNS = "key_columns";
    public static final String DISTRIBUTED_BY = "distributed_by";
    public static final String BUCKETS = "buckets";
    public static final String REPLICATION_NUM = "replication_num";
    public static final String EXTRA_PROPERTIES = "extra_properties";

    public enum TableModel
    {
        DUPLICATE,
        UNIQUE,
    }

    private final List<PropertyMetadata<?>> tableProperties;

    @Inject
    public DorisTableProperties(TypeManager typeManager)
    {
        tableProperties = ImmutableList.of(
                enumProperty(
                        TABLE_MODEL,
                        "Doris data model: DUPLICATE (default) or UNIQUE",
                        TableModel.class,
                        null,
                        false),
                new PropertyMetadata<>(
                        KEY_COLUMNS,
                        "Key columns of the data model, must be the leading columns of the table. Required for UNIQUE model",
                        new ArrayType(VARCHAR),
                        List.class,
                        ImmutableList.of(),
                        false,
                        value -> ImmutableList.copyOf((List<?>) value),
                        value -> value),
                new PropertyMetadata<>(
                        DISTRIBUTED_BY,
                        "Hash distribution columns (DISTRIBUTED BY HASH). Random distribution is used when empty",
                        new ArrayType(VARCHAR),
                        List.class,
                        ImmutableList.of(),
                        false,
                        value -> ImmutableList.copyOf((List<?>) value),
                        value -> value),
                integerProperty(
                        BUCKETS,
                        "Number of buckets. Automatic bucketing is used when not set",
                        null,
                        value -> {
                            if (value != null && value < 1) {
                                throw new TrinoException(INVALID_TABLE_PROPERTY, "'%s' must be positive".formatted(BUCKETS));
                            }
                        },
                        false),
                integerProperty(
                        REPLICATION_NUM,
                        "Number of tablet replicas",
                        null,
                        value -> {
                            if (value != null && value < 1) {
                                throw new TrinoException(INVALID_TABLE_PROPERTY, "'%s' must be positive".formatted(REPLICATION_NUM));
                            }
                        },
                        false),
                new PropertyMetadata<>(
                        EXTRA_PROPERTIES,
                        "Additional Doris table PROPERTIES",
                        new MapType(VARCHAR, VARCHAR, typeManager.getTypeOperators()),
                        Map.class,
                        ImmutableMap.of(),
                        true,
                        value -> {
                            @SuppressWarnings("unchecked")
                            Map<String, String> properties = (Map<String, String>) value;
                            if (properties.containsKey(null) || properties.containsValue(null)) {
                                throw new TrinoException(INVALID_TABLE_PROPERTY, "'%s' keys and values cannot be null".formatted(EXTRA_PROPERTIES));
                            }
                            return ImmutableMap.copyOf(properties);
                        },
                        value -> value));
    }

    @Override
    public List<PropertyMetadata<?>> getTableProperties()
    {
        return tableProperties;
    }

    public static Optional<TableModel> getTableModel(Map<String, Object> tableProperties)
    {
        requireNonNull(tableProperties, "tableProperties is null");
        return Optional.ofNullable((TableModel) tableProperties.get(TABLE_MODEL));
    }

    public static List<String> getKeyColumns(Map<String, Object> tableProperties)
    {
        return getStringList(tableProperties, KEY_COLUMNS);
    }

    public static List<String> getDistributedBy(Map<String, Object> tableProperties)
    {
        return getStringList(tableProperties, DISTRIBUTED_BY);
    }

    public static Optional<Integer> getBuckets(Map<String, Object> tableProperties)
    {
        return Optional.ofNullable((Integer) tableProperties.get(BUCKETS));
    }

    public static Optional<Integer> getReplicationNum(Map<String, Object> tableProperties)
    {
        return Optional.ofNullable((Integer) tableProperties.get(REPLICATION_NUM));
    }

    @SuppressWarnings("unchecked")
    public static Map<String, String> getExtraProperties(Map<String, Object> tableProperties)
    {
        Map<String, String> properties = (Map<String, String>) tableProperties.get(EXTRA_PROPERTIES);
        return properties == null ? ImmutableMap.of() : properties;
    }

    @SuppressWarnings("unchecked")
    private static List<String> getStringList(Map<String, Object> tableProperties, String name)
    {
        requireNonNull(tableProperties, "tableProperties is null");
        List<String> values = (List<String>) tableProperties.get(name);
        return values == null ? ImmutableList.of() : values;
    }
}
