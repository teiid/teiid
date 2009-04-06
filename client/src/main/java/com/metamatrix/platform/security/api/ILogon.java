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

package com.metamatrix.platform.security.api;
import java.util.Properties;

import com.metamatrix.admin.api.exception.security.InvalidSessionException;
import com.metamatrix.api.exception.ComponentNotFoundException;
import com.metamatrix.api.exception.MetaMatrixComponentException;
import com.metamatrix.api.exception.security.LogonException;
import com.metamatrix.dqp.client.ResultsFuture;

/**
 * Generic logon interface.
 */
public interface ILogon {
    LogonResult logon(Properties connectionProperties)
    throws LogonException, MetaMatrixComponentException;
   
   /**
    * Ping the server to see if the client-server connection is alive.
    * @throws InvalidSessionException if the sessionID is invalid
    * @throws ComponentNotFoundException if can't find the Session service.
    */
   ResultsFuture<?> ping()
       throws InvalidSessionException, MetaMatrixComponentException;
   
   
   /**
    * Log off the specified session.
    * @throws InvalidSessionException If session has expired or doesn't exist
    * @throws ComponentNotFoundException If couldn't find needed service component
    */
   ResultsFuture<?> logoff() throws InvalidSessionException, MetaMatrixComponentException;
   
   void assertIdentity(SessionToken sessionId) throws InvalidSessionException, MetaMatrixComponentException;

}
