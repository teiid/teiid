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

package com.metamatrix.common.comm.platform.socket.client;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import com.metamatrix.client.ExceptionUtil;
import com.metamatrix.common.api.MMURL;
import com.metamatrix.common.comm.api.ServerInstanceContext;
import com.metamatrix.common.comm.api.ServerConnection;
import com.metamatrix.common.comm.exception.CommunicationException;
import com.metamatrix.common.comm.exception.ConnectionException;
import com.metamatrix.platform.security.api.LogonResult;

/**
 * Creates a registry that will reconnect itself on failure.
 */
public class ReconnectingServerConnection implements ServerConnection {
	
	private final static int RETRY_COUNT = 3;
	
	final class RegistryProxy implements InvocationHandler {

		public Object invoke(Object proxy, Method method, Object[] args)
				throws Throwable {
			try {
				Throwable t = null;
				for (int i = 0; i < RETRY_COUNT; i++) {
					Object impl = null;
		    		ServerConnection currentRegistry = getServiceRegistry();
		    		synchronized (impls) {
			    		impl = impls.get(proxy.getClass());
			    		if (impl == null) {
			        		impl = currentRegistry.getService(proxy.getClass());
			        		impls.put(proxy.getClass(), impl);
			    		}
		    		}
		
		    		try {
		    			method.invoke(impl, args);
		    		} catch (InvocationTargetException e) {
		    			if (!currentRegistry.isOpen()) {
		    				t = e.getCause();
		    				continue;
		    			}
		    			throw e.getTargetException();
		    		}
				}			
				throw t;
			} catch (Throwable t) {
				throw ExceptionUtil.convertException(method, t);
			}
		}
	}

	private synchronized ServerConnection getServiceRegistry() throws CommunicationException, ConnectionException {
		if (shutdown) {
			throw new IllegalStateException("shutdown"); //TODO
		}
		if (registry == null || !registry.isOpen()) {
			registry = SocketServerConnectionFactory.getInstance().createConnection(mmUrl, p);
			result = registry.getLogonResult();
			synchronized (impls) {
				impls.clear();
			}
		}
		return registry;
	}	

	private ServerConnection registry;
	private final MMURL mmUrl;
	private final Properties p;
	private Map<Class, Object> impls = new HashMap<Class, Object>();

	private volatile boolean shutdown;
	private volatile ServerInstanceContext context;
	private volatile LogonResult result;
	
	public ReconnectingServerConnection(MMURL mmUrl, Properties p) throws CommunicationException, ConnectionException {
		this.mmUrl = mmUrl;
		this.p = p;
		this.getServiceRegistry(); //init
	}

	public ServerInstanceContext getContext() {
		return context;
	}

	public <T> T getService(Class<T> iface) {
		return (T)Proxy.newProxyInstance(Thread.currentThread().getContextClassLoader(), new Class[] {iface}, new RegistryProxy());
	}

	public boolean isOpen() {
		try {
			return !this.shutdown && this.getServiceRegistry().isOpen();
		} catch (CommunicationException e) {
			return false;
		} catch (ConnectionException e) {
			return false;
		}
	}

	public synchronized void shutdown() {
		if (shutdown) {
			return;
		}
		try {
			this.getServiceRegistry().shutdown();
		} catch (CommunicationException e) {
			this.shutdown = true;
		} catch (ConnectionException e) {
			this.shutdown = true;
		}
		this.shutdown = true;
	}

	public LogonResult getLogonResult() {
		return result;
	}

}
