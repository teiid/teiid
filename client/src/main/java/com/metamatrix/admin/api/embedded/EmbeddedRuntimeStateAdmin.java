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

package com.metamatrix.admin.api.embedded;

import com.metamatrix.admin.api.core.CoreRuntimeStateAdmin;
import com.metamatrix.admin.api.exception.AdminException;



/**
 * @since 4.3
 */
public interface EmbeddedRuntimeStateAdmin extends CoreRuntimeStateAdmin {

    /**
     * Stop the MM Query.  If millisToWait is >0, then close to incoming queries, wait the time period
     * for work to stop, then stop the MM Query.  Otherwise, stop immediately, aborting all running queries.
     * @param millisToWait Milliseconds to wait (if >0) or <=0 for no wait before stopping
     * @throws AdminException
     * @since 4.3
     */
    void stop(int millisToWait) throws AdminException;

    /**
     * Restart MM Query
     * @throws AdminException if there's a system error.
     * @since 4.3
     */
    void restart() throws AdminException;

    /**
     * Set the log listener to install into MM Query.  This log listener will receive all log messages
     * written by the MM Query at it's current log level and log contexts.
     *
     * @param listener The listener component
     * @throws AdminException if there's a system error.
     * @since 4.3
     */
    void setLogListener(EmbeddedLogger listener) throws AdminException;


}
