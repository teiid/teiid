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

package com.metamatrix.dqp.transaction;

import javax.transaction.xa.Xid;

import com.metamatrix.common.xa.MMXid;
import com.metamatrix.common.xa.XATransactionException;

/**
 * Defines the remote side of an XAResource
 */
public interface XAServer {

    int prepare(final String threadId,
                MMXid xid) throws XATransactionException;

    void commit(final String threadId,
                       MMXid xid,
                       boolean onePhase) throws XATransactionException;
    
    void rollback(final String threadId,
                         MMXid xid) throws XATransactionException;

    Xid[] recover(int flag) throws XATransactionException;
    
    void forget(final String threadId,
                MMXid xid) throws XATransactionException;
    
    void start(final String threadId,
               MMXid xid,
               int flags,
               int timeout) throws XATransactionException;
    
    void end(final String threadId,
             MMXid xid,
             int flags) throws XATransactionException;
}
