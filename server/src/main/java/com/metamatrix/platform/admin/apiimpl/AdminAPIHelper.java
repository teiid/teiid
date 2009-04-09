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

package com.metamatrix.platform.admin.apiimpl;

import org.teiid.dqp.internal.process.DQPWorkContext;

import com.metamatrix.admin.api.exception.security.InvalidSessionException;
import com.metamatrix.api.exception.ComponentNotFoundException;
import com.metamatrix.platform.security.api.SessionToken;

/**
 * Static implementation of the AdminHelper.<br>
 * This class is used by all <SubSystem>AdminAPIImpl to do general tasks such as
 * session validation and authorization role checking.
 */
public class AdminAPIHelper {

    /**
     * Get the <code>SessionToken</code> and validate that the session is active
     * for the specified <code>MetaMatrixSessionID</code>.
     * @return The <code>SessionToken</code> for the session in question.
     * @throws InvalidSessionException If session has expired or doesn't exist
     * @throws ComponentNotFoundException If couldn't find needed service component
     */
    public static SessionToken validateSession()
    throws InvalidSessionException, ComponentNotFoundException {
        return DQPWorkContext.getWorkContext().getSessionToken();
    }

}
