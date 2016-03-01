/*
 * Copyright (c) 2008-2015 MongoDB, Inc.
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

import com.mongodb.MongoCredential;
import com.mongodb.ServerAddress;
import com.mongodb.event.ConnectionListener;
import com.mongodb.event.ConnectionPoolListener;
import com.mongodb.event.CommandListener;

import java.util.List;

class DefaultClusterableServerFactory implements ClusterableServerFactory {
    private final ClusterId clusterId;
    private final ClusterSettings clusterSettings;
    private final ServerSettings settings;
    private final ConnectionPoolSettings connectionPoolSettings;
    private final StreamFactory streamFactory;
    private final List<MongoCredential> credentialList;
    private final ConnectionPoolListener connectionPoolListener;
    private final ConnectionListener connectionListener;
    private final StreamFactory heartbeatStreamFactory;
    private final CommandListener commandListener;

    public DefaultClusterableServerFactory(final ClusterId clusterId, final ClusterSettings clusterSettings, final ServerSettings settings,
                                           final ConnectionPoolSettings connectionPoolSettings,
                                           final StreamFactory streamFactory,
                                           final StreamFactory heartbeatStreamFactory,
                                           final List<MongoCredential> credentialList,
                                           final ConnectionListener connectionListener,
                                           final ConnectionPoolListener connectionPoolListener, final CommandListener commandListener) {
        this.clusterId = clusterId;
        this.clusterSettings = clusterSettings;
        this.settings = settings;
        this.connectionPoolSettings = connectionPoolSettings;
        this.streamFactory = streamFactory;
        this.credentialList = credentialList;
        this.connectionPoolListener = connectionPoolListener;
        this.connectionListener = connectionListener;
        this.heartbeatStreamFactory = heartbeatStreamFactory;
        this.commandListener = commandListener;
    }

    @Override
    public ClusterableServer create(final ServerAddress serverAddress) {
        ConnectionPool connectionPool = new DefaultConnectionPool(new ServerId(clusterId, serverAddress),
                                                                  new InternalStreamConnectionFactory(streamFactory,
                                                                                                      credentialList,
                                                                                                      connectionListener),
                                                                  connectionPoolSettings, connectionPoolListener);
        ServerMonitorFactory serverMonitorFactory =
            new DefaultServerMonitorFactory(new ServerId(clusterId, serverAddress), settings,
                                            new InternalStreamConnectionFactory(heartbeatStreamFactory,
                                                                                credentialList,
                                                                                connectionListener),
                                            connectionPool);
        return new DefaultServer(serverAddress, clusterSettings.getMode(), connectionPool, new ThreadAffinityConnectionFactory(),
                                 serverMonitorFactory, commandListener);
    }

    @Override
    public ServerSettings getSettings() {
        return settings;
    }
}
