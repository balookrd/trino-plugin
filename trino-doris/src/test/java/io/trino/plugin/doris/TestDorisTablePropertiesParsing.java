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
import io.trino.plugin.doris.DorisTableProperties.TableModel;
import org.junit.jupiter.api.Test;

import static io.trino.plugin.doris.DorisClient.parseTableProperties;
import static org.assertj.core.api.Assertions.assertThat;

final class TestDorisTablePropertiesParsing
{
    @Test
    void testUniqueTable()
    {
        String ddl = """
                CREATE TABLE `users` (
                  `id` bigint NOT NULL,
                  `name` varchar(400) NULL
                ) ENGINE=OLAP
                UNIQUE KEY(`id`)
                DISTRIBUTED BY HASH(`id`) BUCKETS 8
                PROPERTIES (
                "replication_allocation" = "tag.location.default: 3",
                "enable_unique_key_merge_on_write" = "true"
                );""";
        assertThat(parseTableProperties(ddl)).isEqualTo(ImmutableMap.of(
                "table_model", TableModel.UNIQUE,
                "key_columns", ImmutableList.of("id"),
                "distributed_by", ImmutableList.of("id"),
                "buckets", 8,
                "replication_num", 3));
    }

    @Test
    void testDuplicateTableWithoutKeys()
    {
        String ddl = """
                CREATE TABLE `t` (
                  `a` text NULL
                ) ENGINE=OLAP
                DISTRIBUTED BY RANDOM BUCKETS AUTO
                PROPERTIES (
                "replication_allocation" = "tag.location.default: 1",
                "enable_duplicate_without_keys_by_default" = "true"
                );""";
        assertThat(parseTableProperties(ddl)).isEqualTo(ImmutableMap.of(
                "table_model", TableModel.DUPLICATE,
                "replication_num", 1));
    }

    @Test
    void testDuplicateTableWithMultipleKeys()
    {
        String ddl = """
                CREATE TABLE `t` (
                  `a` bigint NULL,
                  `b` date NULL,
                  `c` double NULL
                ) ENGINE=OLAP
                DUPLICATE KEY(`a`, `b`)
                COMMENT 'x'
                DISTRIBUTED BY HASH(`a`, `b`) BUCKETS 3
                PROPERTIES (
                "replication_allocation" = "tag.location.default: 1"
                );""";
        assertThat(parseTableProperties(ddl)).isEqualTo(ImmutableMap.of(
                "table_model", TableModel.DUPLICATE,
                "key_columns", ImmutableList.of("a", "b"),
                "distributed_by", ImmutableList.of("a", "b"),
                "buckets", 3,
                "replication_num", 1));
    }

    @Test
    void testView()
    {
        assertThat(parseTableProperties("CREATE VIEW `v` AS SELECT 1")).isEmpty();
    }
}
