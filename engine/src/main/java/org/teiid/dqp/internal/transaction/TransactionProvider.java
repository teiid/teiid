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

package org.teiid.dqp.internal.transaction;

import java.sql.SQLException;
import java.util.Properties;

import javax.resource.spi.XATerminator;
import javax.transaction.Transaction;
import javax.transaction.TransactionManager;
import javax.transaction.xa.XAException;
import javax.transaction.xa.XAResource;

import com.metamatrix.common.xa.MMXid;
import com.metamatrix.common.xa.XATransactionException;


public interface TransactionProvider {
	
	public interface XAConnectionSource {
		
		XAResource getXAResource() throws SQLException;
		
		void close();
		
	}
    
    void init(Properties props) throws XATransactionException;

    XATerminator getXATerminator();

    TransactionManager getTransactionManager();

    Transaction importTransaction(MMXid xid, int timeout) throws XAException;
    
    String getTransactionID(Transaction tx);
    
    void shutdown();
    
    void registerRecoverySource(String name, XAConnectionSource resource);
    
    void removeRecoverySource(String name);    
}