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

package com.mongodb.binding;

import com.mongodb.ReadPreference;
import com.mongodb.connection.*;
import com.mongodb.selector.ReadPreferenceServerSelector;
import com.mongodb.selector.ServerSelector;
import com.mongodb.selector.WritableServerSelector;

import static com.mongodb.assertions.Assertions.notNull;

/**
 * A simple ReadWriteBinding implementation that supplies write connection sources bound to a possibly different primary each time, and a
 * read connection source bound to a possible different server each time.
 *
 * @since 3.0
 */
public class ThreadAffinityBinding extends AbstractReferenceCounted implements ReadWriteBinding {
    private final Cluster cluster;
    private final ReadPreference readPreference;

    private static final ThreadLocal<ConnectionSource> READ_CONNECTION_SOURCE = new ThreadLocal<ConnectionSource>();
    private static final ThreadLocal<ConnectionSource> WRITE_CONNECTION_SOURCE = new ThreadLocal<ConnectionSource>();

    /**
     * Creates an instance.
     * @param cluster        a non-null Cluster which will be used to select a server to bind to
     * @param readPreference a non-null ReadPreference for read operations
     */
    public ThreadAffinityBinding(final Cluster cluster, final ReadPreference readPreference) {
        this.cluster = notNull("cluster", cluster);
        this.readPreference = notNull("readPreference", readPreference);
    }

    @Override
    public ReadWriteBinding retain() {
        return this;
    }

    @Override
    public void release() {

    }

    @Override
    public ReadPreference getReadPreference() {
        return readPreference;
    }

    @Override
    public ConnectionSource getReadConnectionSource() {
        ConnectionSource connectionSource = READ_CONNECTION_SOURCE.get();
        if (connectionSource == null) {
            connectionSource = new ClusterBindingConnectionSource(new ReadPreferenceServerSelector(readPreference));
            READ_CONNECTION_SOURCE.set(connectionSource);
        }

        return connectionSource;
    }

    @Override
    public ConnectionSource getWriteConnectionSource() {
        ConnectionSource connectionSource = WRITE_CONNECTION_SOURCE.get();
        if (connectionSource == null) {
            connectionSource = new ClusterBindingConnectionSource(new WritableServerSelector());
            WRITE_CONNECTION_SOURCE.set(connectionSource);
        }

        return connectionSource;
    }

    public static void destroy() {
        READ_CONNECTION_SOURCE.remove();
        WRITE_CONNECTION_SOURCE.remove();
        ThreadAffinityConnectionFactory.destroy();
    }

    private final class ClusterBindingConnectionSource extends AbstractReferenceCounted implements ConnectionSource {
        private final Server server;
        private Connection connection;

        private ClusterBindingConnectionSource(final ServerSelector serverSelector) {
            this.server = cluster.selectServer(serverSelector);
        }

        @Override
        public ServerDescription getServerDescription() {
            return server.getDescription();
        }

        @Override
        public Connection getConnection() {
            if (connection == null) {
                connection = server.getConnection();
            }

            return connection;
        }

        public ConnectionSource retain() {
            return this;
        }

        @Override
        public void release() {

        }
    }
}
