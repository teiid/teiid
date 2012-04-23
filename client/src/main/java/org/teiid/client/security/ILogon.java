/*
 * JBoss, Home of Professional Open Source.
 * See the COPYRIGHT.txt file distributed with this work for information
 * regarding copyright ownership.  Some portions may be licensed
 * to Red Hat, Inc. under one or more contributor license agreements.
 * 
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 2.1 of the License, or (at your option) any later version.
 * 
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 * 
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA
 * 02110-1301 USA.
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
