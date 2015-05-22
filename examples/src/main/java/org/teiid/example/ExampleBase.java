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
package org.teiid.example;

import java.net.InetSocketAddress;
import java.sql.Connection;

import org.teiid.runtime.EmbeddedConfiguration;
import org.teiid.runtime.EmbeddedServer;
import org.teiid.translator.ExecutionFactory;
import org.teiid.translator.TranslatorException;
import org.teiid.transport.SocketConfiguration;
import org.teiid.transport.WireProtocol;

public abstract class ExampleBase {
	
	protected EmbeddedServer server = null;
	protected Connection conn = null;
	
	protected void init(String name, ExecutionFactory<?, ?> factory) throws TranslatorException {
		server = new EmbeddedServer();
		factory.start();
		factory.setSupportsDirectQueryProcedure(true);
		server.addTranslator(name, factory);
	}
	
	protected void start(boolean isRemote) throws Exception{
		
		if(isRemote) {
			SocketConfiguration s = new SocketConfiguration();
			InetSocketAddress addr = new InetSocketAddress("localhost", 31000);//$NON-NLS-1$ 
			s.setBindAddress(addr.getHostName());
			s.setPortNumber(addr.getPort());
			s.setProtocol(WireProtocol.teiid);
			EmbeddedConfiguration config = new EmbeddedConfiguration();
			config.setTransactionManager(EmbeddedHelper.getTransactionManager());
			config.addTransport(s);
			server.start(config);
		} else {
			EmbeddedConfiguration config = new EmbeddedConfiguration();
			config.setTransactionManager(EmbeddedHelper.getTransactionManager());
			server.start(config);
		}
	}
	
	protected void tearDown() throws Exception {
		if(null != conn) {
			conn.close();
			conn = null;
		}
		if(null != server) {
			server.stop();
			server = null;
		}
	}
	
	public abstract void execute() throws Exception;

}
