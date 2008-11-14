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

package com.metamatrix.dqp.internal.datamgr.impl;

import java.util.Properties;

import com.metamatrix.common.application.ApplicationEnvironment;
import com.metamatrix.common.application.exception.ApplicationInitializationException;
import com.metamatrix.common.application.exception.ApplicationLifecycleException;
import com.metamatrix.common.xa.XATransactionException;
import com.metamatrix.core.util.SimpleMock;
import com.metamatrix.dqp.internal.transaction.TransactionProvider;
import com.metamatrix.dqp.internal.transaction.TransactionServerImpl;
import com.metamatrix.dqp.service.TransactionService;
import com.metamatrix.dqp.transaction.TransactionServer;
import com.metamatrix.dqp.transaction.XAServer;

public class FakeTransactionService implements TransactionService {

	private TransactionServerImpl server = new TransactionServerImpl();
	
	public FakeTransactionService() {
		try {
			server.init(new Properties(), SimpleMock.createSimpleMock(TransactionProvider.class));
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

	public void bind() throws ApplicationLifecycleException {
		
	}

	public void initialize(Properties props)
			throws ApplicationInitializationException {
	}

	public void start(ApplicationEnvironment environment)
			throws ApplicationLifecycleException {
	}

	public void stop() throws ApplicationLifecycleException {
	}

	public void unbind() throws ApplicationLifecycleException {
	}

}
