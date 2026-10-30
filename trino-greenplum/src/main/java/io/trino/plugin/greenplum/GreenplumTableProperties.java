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
import com.google.inject.Inject;
import io.trino.plugin.jdbc.TablePropertiesProvider;
import io.trino.spi.TrinoException;
import io.trino.spi.session.PropertyMetadata;
import io.trino.spi.type.ArrayType;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static io.trino.spi.StandardErrorCode.INVALID_TABLE_PROPERTY;
import static io.trino.spi.session.PropertyMetadata.booleanProperty;
import static io.trino.spi.session.PropertyMetadata.enumProperty;
import static io.trino.spi.session.PropertyMetadata.integerProperty;
import static io.trino.spi.session.PropertyMetadata.stringProperty;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.util.Objects.requireNonNull;

/**
 * Greenplum specific table properties, used when creating a table:
 * <pre>CREATE TABLE t (a bigint, b varchar) WITH (distributed_by = ARRAY['a'], appendoptimized = true, orientation = 'COLUMN')</pre>
 */
public final class GreenplumTableProperties
        implements TablePropertiesProvider
{
    public static final String DISTRIBUTION_POLICY = "distribution_policy";
    public static final String DISTRIBUTED_BY = "distributed_by";
    public static final String APPENDOPTIMIZED = "appendoptimized";
    public static final String ORIENTATION = "orientation";
    public static final String COMPRESSTYPE = "compresstype";
    public static final String COMPRESSLEVEL = "compresslevel";

    public enum DistributionPolicy
    {
        HASH,
        RANDOM,
        REPLICATED,
    }

    public enum Orientation
    {
        ROW,
        COLUMN,
    }

    private final List<PropertyMetadata<?>> tableProperties;

    @Inject
    public GreenplumTableProperties()
    {
        tableProperties = ImmutableList.of(
                enumProperty(
                        DISTRIBUTION_POLICY,
                        "Distribution policy: HASH (requires distributed_by), RANDOM or REPLICATED. Greenplum default is used when not set",
                        DistributionPolicy.class,
                        null,
                        false),
                new PropertyMetadata<>(
                        DISTRIBUTED_BY,
                        "Distribution key columns (DISTRIBUTED BY)",
                        new ArrayType(VARCHAR),
                        List.class,
                        ImmutableList.of(),
                        false,
                        value -> ImmutableList.copyOf((List<?>) value),
                        value -> value),
                booleanProperty(
                        APPENDOPTIMIZED,
                        "Create an append-optimized table",
                        null,
                        false),
                enumProperty(
                        ORIENTATION,
                        "Storage orientation of an append-optimized table",
                        Orientation.class,
                        null,
                        false),
                stringProperty(
                        COMPRESSTYPE,
                        "Compression type of an append-optimized table, e.g. zstd, zlib",
                        null,
                        false),
                integerProperty(
                        COMPRESSLEVEL,
                        "Compression level of an append-optimized table",
                        null,
                        false));
    }

    @Override
    public List<PropertyMetadata<?>> getTableProperties()
    {
        return tableProperties;
    }

    public static Optional<DistributionPolicy> getDistributionPolicy(Map<String, Object> tableProperties)
    {
        requireNonNull(tableProperties, "tableProperties is null");
        DistributionPolicy policy = (DistributionPolicy) tableProperties.get(DISTRIBUTION_POLICY);
        List<String> distributedBy = getDistributedBy(tableProperties);
        if (policy == null) {
            return distributedBy.isEmpty() ? Optional.empty() : Optional.of(DistributionPolicy.HASH);
        }
        if (policy == DistributionPolicy.HASH && distributedBy.isEmpty()) {
            throw new TrinoException(INVALID_TABLE_PROPERTY, "Distribution policy HASH requires the '%s' property".formatted(DISTRIBUTED_BY));
        }
        if (policy != DistributionPolicy.HASH && !distributedBy.isEmpty()) {
            throw new TrinoException(INVALID_TABLE_PROPERTY, "Property '%s' can only be used with distribution policy HASH".formatted(DISTRIBUTED_BY));
        }
        return Optional.of(policy);
    }

    @SuppressWarnings("unchecked")
    public static List<String> getDistributedBy(Map<String, Object> tableProperties)
    {
        requireNonNull(tableProperties, "tableProperties is null");
        List<String> distributedBy = (List<String>) tableProperties.get(DISTRIBUTED_BY);
        return distributedBy == null ? ImmutableList.of() : distributedBy;
    }

    public static Optional<Boolean> getAppendOptimized(Map<String, Object> tableProperties)
    {
        return Optional.ofNullable((Boolean) tableProperties.get(APPENDOPTIMIZED));
    }

    public static Optional<Orientation> getOrientation(Map<String, Object> tableProperties)
    {
        return Optional.ofNullable((Orientation) tableProperties.get(ORIENTATION));
    }

    public static Optional<String> getCompressType(Map<String, Object> tableProperties)
    {
        return Optional.ofNullable((String) tableProperties.get(COMPRESSTYPE));
    }

    public static Optional<Integer> getCompressLevel(Map<String, Object> tableProperties)
    {
        return Optional.ofNullable((Integer) tableProperties.get(COMPRESSLEVEL));
    }
}
