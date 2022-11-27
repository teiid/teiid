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

package org.teiid.transport;

import java.io.Serializable;

import org.teiid.core.crypto.Cryptor;
import org.teiid.dqp.internal.process.DQPWorkContext;
import org.teiid.net.CommunicationException;
import org.teiid.net.socket.Message;


/**
 * Represents a ClientConnection from the server's point of view.  This interface
 * can be used by a ServerListener implementation to manage the incoming
 * client connections, retrieve information about a particular connection,
 * and send a message to a particular connection in the asynchronous message scenario.
 */
public interface ClientInstance {

    /**
     * Send a message to this particular client using the asynch message key.
     * @param message The message to send
     * @param messageKey The key sent with the asynch query
     */
    void send(Message message, Serializable messageKey);

    /**
     * Shutdown the server's connection to the client.
     * @throws CommunicationException If an error occurs during the shutdown
     */
    void shutdown() throws CommunicationException;

    Cryptor getCryptor();

    DQPWorkContext getWorkContext();
}
