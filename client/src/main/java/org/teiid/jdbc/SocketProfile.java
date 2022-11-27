/*
 * Copyright Red Hat, Inc. and/or its affiliates
 * and other contributors as indicated by the @author tags and
 * the COPYRIGHT.txt file distributed with this work.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.teiid.jdbc;

import java.util.Properties;

import org.teiid.core.TeiidException;
import org.teiid.net.TeiidURL;
import org.teiid.net.socket.OioOjbectChannelFactory;
import org.teiid.net.socket.SocketServerConnection;
import org.teiid.net.socket.SocketServerConnectionFactory;


/**
 * <p> The java.sql.DriverManager class uses this class to connect to Teiid Server.
 * The TeiidDriver class has a static initializer, which
 * is used to instantiate and register itself with java.sql.DriverManager. The
 * DriverManager's <code>getConnection</code> method calls <code>connect</code>
 * method on available registered drivers.
 */

final class SocketProfile implements ConnectionProfile {

    /**
     * This method tries to make a connection to the given URL. This class
     * will return a null if this is not the right driver to connect to the given URL.
     * @param url used to establish a connection.
     * @return Connection object created
     * @throws TeiidSQLException if it is unable to establish a connection to the server.
     */
    public ConnectionImpl connect(String url, Properties info) throws TeiidSQLException {
        int loginTimeoutSeconds = 0;
        SocketServerConnection serverConn;
        try {
            String timeout = info.getProperty(TeiidURL.CONNECTION.LOGIN_TIMEOUT);
            if (timeout != null) {
                loginTimeoutSeconds = Integer.parseInt(timeout);
            }

            if (loginTimeoutSeconds > 0) {
                OioOjbectChannelFactory.TIMEOUTS.set(System.currentTimeMillis() + loginTimeoutSeconds * 1000);
            }
            serverConn = SocketServerConnectionFactory.getInstance().getConnection(info);
        } catch (TeiidException e) {
            throw TeiidSQLException.create(e);
        } finally {
            if (loginTimeoutSeconds > 0) {
                OioOjbectChannelFactory.TIMEOUTS.set(null);
            }
        }

        // construct a MMConnection object.
        ConnectionImpl connection = new ConnectionImpl(serverConn, info, url);
        return connection;
    }

}
