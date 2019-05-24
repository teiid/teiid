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

package org.teiid.client.security;
import java.util.Collection;
import java.util.Properties;

import org.teiid.client.util.ResultsFuture;
import org.teiid.core.ComponentNotFoundException;
import org.teiid.core.TeiidComponentException;
import org.teiid.net.CommunicationException;


/**
 * Generic logon interface.
 */
public interface ILogon {
    static final String KRB5TOKEN = "KRB5TOKEN"; //$NON-NLS-1$
    static final String KRB5_ESTABLISHED = "KRB5_CONTEXT_ESTABLISHED"; //$NON-NLS-1$
    public static final String AUTH_TYPE = "authType"; //$NON-NLS-1$

    @Secure
    LogonResult logon(Properties connectionProperties)
    throws LogonException, TeiidComponentException, CommunicationException;

    @Secure
    LogonResult neogitiateGssLogin(Properties connectionProperties, byte[] serviceToken, boolean createSession) throws LogonException;

   /**
    * Ping the server to see if the client-server connection is alive.
    * @throws InvalidSessionException if the sessionID is invalid
    * @throws ComponentNotFoundException if can't find the Session service.
    */
   ResultsFuture<?> ping()
       throws InvalidSessionException, TeiidComponentException, CommunicationException;

   @Deprecated
   ResultsFuture<?> ping(Collection<String> sessions)
           throws TeiidComponentException, CommunicationException;

   /**
    * Log off the specified session.
    * @throws InvalidSessionException If session has expired or doesn't exist
    * @throws ComponentNotFoundException If couldn't find needed service component
    */
   ResultsFuture<?> logoff() throws InvalidSessionException, TeiidComponentException;

   @Secure
   void assertIdentity(SessionToken sessionId) throws InvalidSessionException, TeiidComponentException, CommunicationException;
}
