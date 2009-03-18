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

package org.teiid.dqp.internal.datamgr.impl;

import java.util.Properties;

import org.teiid.dqp.internal.transaction.TransactionProvider;
import org.teiid.dqp.internal.transaction.TransactionServerImpl;

import com.metamatrix.common.application.ApplicationEnvironment;
import com.metamatrix.common.application.exception.ApplicationInitializationException;
import com.metamatrix.common.application.exception.ApplicationLifecycleException;
import com.metamatrix.common.xa.XATransactionException;
import com.metamatrix.core.util.SimpleMock;
import com.metamatrix.dqp.service.TransactionService;
import com.metamatrix.dqp.transaction.TransactionServer;
import com.metamatrix.dqp.transaction.XAServer;

public class FakeTransactionService implements TransactionService {

	private TransactionServerImpl server = new TransactionServerImpl();
	
	public FakeTransactionService() {
		try {
			server.init(SimpleMock.createSimpleMock(TransactionProvider.class));
		} catch (XATransactionException e) {
			throw new RuntimeException(e);
		}
	}
	
	public TransactionServer getTransactionServer() {
		return server;
	}

	public XAServer getXAServer() {
		return server;
	}

	public void initialize(Properties props)
			throws ApplicationInitializationException {
	}

	public void start(ApplicationEnvironment environment)
			throws ApplicationLifecycleException {
	}

	public void stop() throws ApplicationLifecycleException {
	}

}
