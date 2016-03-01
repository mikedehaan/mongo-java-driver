/*
 * Copyright (c) 2008-2014 MongoDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.mongodb.connection;

public class ThreadAffinityConnectionFactory implements ConnectionFactory {

    private static final ThreadLocal<ThreadAffinityServerConnection> CONNECTION_THREAD_LOCAL = new ThreadLocal<ThreadAffinityServerConnection>();

    @Override
    public Connection create(final InternalConnection internalConnection, final ProtocolExecutor executor,
                             final ClusterConnectionMode clusterConnectionMode) {

        ThreadAffinityServerConnection connection = CONNECTION_THREAD_LOCAL.get();
        if (connection == null) {
            connection = new ThreadAffinityServerConnection(internalConnection, executor, clusterConnectionMode);
            CONNECTION_THREAD_LOCAL.set(connection);
        }

        return connection;
    }

    @Override
    public AsyncConnection createAsync(final InternalConnection internalConnection, final ProtocolExecutor executor,
                                       final ClusterConnectionMode clusterConnectionMode) {

        ThreadAffinityServerConnection connection = CONNECTION_THREAD_LOCAL.get();
        if (connection == null) {
            connection = new ThreadAffinityServerConnection(internalConnection, executor, clusterConnectionMode);
            CONNECTION_THREAD_LOCAL.set(connection);
        }

        return connection;
    }

    public static void destroy() {
        ThreadAffinityServerConnection connection = CONNECTION_THREAD_LOCAL.get();
        if (connection != null) {
            connection.destroy();
        }

        CONNECTION_THREAD_LOCAL.remove();
    }
}
