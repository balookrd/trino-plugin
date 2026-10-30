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

import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;

import static java.lang.String.format;

public class TestingDorisServer
        implements AutoCloseable
{
    public static final DockerImageName DEFAULT_IMAGE_NAME = DockerImageName.parse("apache/doris:3.0.8-all");

    private static final int QUERY_PORT = 9030;
    private static final String USER = "root";
    private static final String PASSWORD = "";

    private final GenericContainer<?> dockerContainer;

    public TestingDorisServer()
    {
        this(DockerImageName.parse(System.getProperty("testing.doris-image-name", DEFAULT_IMAGE_NAME.asCanonicalNameString())));
    }

    public TestingDorisServer(DockerImageName imageName)
    {
        dockerContainer = new GenericContainer<>(imageName)
                // The backend refuses to start when vm.max_map_count is low, which is the case in Docker Desktop VMs
                .withEnv("SKIP_CHECK_ULIMIT", "true")
                .withExposedPorts(QUERY_PORT)
                // The entrypoint logs "Start Successfully" only when the backend exits, so wait for the registration instead
                .waitingFor(Wait.forLogMessage(".*BE successfully registered to FE.*", 1)
                        .withStartupTimeout(Duration.ofMinutes(5)));
        dockerContainer.start();
        waitForBackend();
        execute("CREATE DATABASE IF NOT EXISTS tpch");
    }

    private void waitForBackend()
    {
        long deadline = System.nanoTime() + Duration.ofMinutes(3).toNanos();
        while (true) {
            try (Connection connection = openConnection();
                    Statement statement = connection.createStatement();
                    ResultSet backends = statement.executeQuery("SHOW BACKENDS")) {
                while (backends.next()) {
                    if (backends.getBoolean("Alive")) {
                        return;
                    }
                }
            }
            catch (SQLException ignored) {
                // not ready yet
            }
            if (System.nanoTime() > deadline) {
                throw new RuntimeException("Doris backend did not become alive");
            }
            try {
                Thread.sleep(1000);
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(e);
            }
        }
    }

    public void execute(String sql)
    {
        try (Connection connection = openConnection();
                Statement statement = connection.createStatement()) {
            statement.execute(sql);
        }
        catch (SQLException e) {
            throw new RuntimeException("Failed to execute statement: " + sql, e);
        }
    }

    private Connection openConnection()
            throws SQLException
    {
        return DriverManager.getConnection(getJdbcUrl(), USER, PASSWORD);
    }

    public String getUser()
    {
        return USER;
    }

    public String getPassword()
    {
        return PASSWORD;
    }

    public String getJdbcUrl()
    {
        return format("jdbc:mysql://%s:%s/", dockerContainer.getHost(), dockerContainer.getMappedPort(QUERY_PORT));
    }

    @Override
    public void close()
    {
        dockerContainer.close();
    }
}
