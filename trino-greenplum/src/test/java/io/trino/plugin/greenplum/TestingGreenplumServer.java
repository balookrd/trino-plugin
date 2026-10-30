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

import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.Properties;

import static java.lang.String.format;

public class TestingGreenplumServer
        implements AutoCloseable
{
    public static final DockerImageName DEFAULT_IMAGE_NAME = DockerImageName.parse("woblerr/greenplum:6.27.1");

    private static final int PORT = 5432;
    private static final String USER = "gpadmin";
    private static final String PASSWORD = "gpadmin";
    private static final String DATABASE = "tpch";

    private final GenericContainer<?> dockerContainer;

    public TestingGreenplumServer()
    {
        this(DockerImageName.parse(System.getProperty("testing.greenplum-image-name", DEFAULT_IMAGE_NAME.asCanonicalNameString())));
    }

    public TestingGreenplumServer(DockerImageName imageName)
    {
        dockerContainer = new GenericContainer<>(imageName)
                .withEnv("GREENPLUM_PASSWORD", PASSWORD)
                .withEnv("GREENPLUM_DATABASE_NAME", DATABASE)
                .withExposedPorts(PORT)
                // The cluster is restarted during initialization, so the open port alone is not a readiness signal.
                // This message is logged by the image entrypoint once the final restart is done.
                .waitingFor(Wait.forLogMessage(".*INFO - Execute custom init scripts.*", 1)
                        .withStartupTimeout(Duration.ofMinutes(5)));
        dockerContainer.start();
        waitForConnection();
        execute("CREATE SCHEMA IF NOT EXISTS tpch");
    }

    private void waitForConnection()
    {
        long deadline = System.nanoTime() + Duration.ofMinutes(3).toNanos();
        while (true) {
            try (Connection connection = openConnection()) {
                connection.createStatement().execute("SELECT 1");
                return;
            }
            catch (SQLException e) {
                if (System.nanoTime() > deadline) {
                    throw new RuntimeException("Greenplum did not become available", e);
                }
                try {
                    Thread.sleep(1000);
                }
                catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException(interrupted);
                }
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
        Properties properties = new Properties();
        properties.setProperty("user", USER);
        properties.setProperty("password", PASSWORD);
        return DriverManager.getConnection(getJdbcUrl(), properties);
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
        return format("jdbc:postgresql://%s:%s/%s", dockerContainer.getHost(), dockerContainer.getMappedPort(PORT), DATABASE);
    }

    @Override
    public void close()
    {
        dockerContainer.close();
    }
}
