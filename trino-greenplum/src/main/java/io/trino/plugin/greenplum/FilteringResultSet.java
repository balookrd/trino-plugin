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

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.sql.ResultSet;
import java.sql.SQLException;

import static java.util.Objects.requireNonNull;

/**
 * Forward-only view of a {@link ResultSet} that skips rows not matching a predicate.
 * Only {@code next()} is intercepted, all other calls go to the current row of the delegate.
 */
final class FilteringResultSet
{
    private FilteringResultSet() {}

    @FunctionalInterface
    interface RowPredicate
    {
        boolean test(ResultSet resultSet)
                throws SQLException;
    }

    static ResultSet filter(ResultSet delegate, RowPredicate predicate)
    {
        requireNonNull(delegate, "delegate is null");
        requireNonNull(predicate, "predicate is null");
        return (ResultSet) Proxy.newProxyInstance(
                FilteringResultSet.class.getClassLoader(),
                new Class<?>[] {ResultSet.class},
                (_, method, args) -> {
                    if (method.getName().equals("next") && method.getParameterCount() == 0) {
                        while (delegate.next()) {
                            if (predicate.test(delegate)) {
                                return true;
                            }
                        }
                        return false;
                    }
                    return invoke(delegate, method, args);
                });
    }

    private static Object invoke(ResultSet delegate, Method method, Object[] args)
            throws Throwable
    {
        try {
            return method.invoke(delegate, args);
        }
        catch (InvocationTargetException e) {
            throw e.getCause();
        }
    }
}
