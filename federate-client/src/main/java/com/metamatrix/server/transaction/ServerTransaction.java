/*
 * JBoss, Home of Professional Open Source.
 * Copyright (C) 2008 Red Hat, Inc.
 * Copyright (C) 2000-2007 MetaMatrix, Inc.
 * Licensed to Red Hat, Inc. under one or more contributor 
 * license agreements.  See the copyright.txt file in the
 * distribution for a full listing of individual contributors.
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

package com.metamatrix.server.transaction;

import java.util.Date;
import com.metamatrix.platform.security.api.SessionToken;
import com.metamatrix.common.xa.TransactionID;
import com.metamatrix.dqp.internal.datamgr.ConnectorID;


/**
 * ServerTransaction holds state for transaction executing on the server
 * This interface defines the access methods.
 */
public interface ServerTransaction {

    /**
     * Return transactionID.
     * @return TransactionID
     */
    TransactionID getTransactionID();

    /**
     * Return ConnectorID of connector bound to this Transaction.
     * @return ConnectorID
     */
    ConnectorID getConnectorID();

    /**
     * Return Status of Transaction.
     * @return status
     * @see javax.transaction.Status
     */
    int getStatus();

    /**
     * Return sessionToken for session that owns this transaction.
     * @return SessionToken
     */
    SessionToken getSessionToken();

    /**
     * Return RequestID of the request that is currently being executed in this transaction.
     * @return RequestID
     */
    long getRequestID();

    /**
     * Return database that is bound to this transasction.
     * @return String database
     */
    String getDatabase();

    /**
     * Return java.util.Date representing the time this transaction was completed (commited or rolled back)
     * @return java.util.Date
     */
    Date getEndTime();

    /**
     * Return java.util.Date representing the time this transaction began
     * @return java.util.Date
     */
    Date getBeginTime();

    /**
     * Return a String representation of the status.
     * @return String representation of the current status.
     */
    public String getStatusString();

}

