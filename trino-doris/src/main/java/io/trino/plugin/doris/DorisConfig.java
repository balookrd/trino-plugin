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

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import jakarta.validation.constraints.Min;

import java.util.Optional;

public class DorisConfig
{
    private Integer defaultReplicationNum;

    public Optional<@Min(1) Integer> getDefaultReplicationNum()
    {
        return Optional.ofNullable(defaultReplicationNum);
    }

    @Config("doris.default-replication-num")
    @ConfigDescription("Replication number for tables created by Trino when the replication_num table property is not set. Doris default is used when empty")
    public DorisConfig setDefaultReplicationNum(Integer defaultReplicationNum)
    {
        this.defaultReplicationNum = defaultReplicationNum;
        return this;
    }
}
