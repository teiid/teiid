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

package org.teiid.net;

import java.util.Properties;


public interface ServerConnectionFactory {

    /**
     * Establish a connection to the server.
     * @param connectionProperties The properties used by the transport to find a connection.  These
     * properties are typically specific to the transport.
     * @return A connection, never null
     * @throws ConnectionException If an error occurs communicating between client and server
     * @throws CommunicationException If an error occurs in connecting, typically due to
     * problems with the connection properties (bad user name, bad password, bad host name, etc)
     */
    ServerConnection getConnection(Properties connectionProperties) throws CommunicationException, ConnectionException;
}
