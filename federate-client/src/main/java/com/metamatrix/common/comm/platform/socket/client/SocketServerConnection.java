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

/**
 * 
 */
package com.metamatrix.common.comm.platform.socket.client;

import java.io.IOException;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import com.metamatrix.api.exception.MetaMatrixComponentException;
import com.metamatrix.api.exception.security.InvalidSessionException;
import com.metamatrix.api.exception.security.LogonException;
import com.metamatrix.client.ExceptionUtil;
import com.metamatrix.common.api.HostInfo;
import com.metamatrix.common.api.MMURL;
import com.metamatrix.common.comm.api.ServerConnection;
import com.metamatrix.common.comm.exception.CommunicationException;
import com.metamatrix.common.comm.exception.ConnectionException;
import com.metamatrix.common.comm.exception.SingleInstanceCommunicationException;
import com.metamatrix.common.comm.platform.CommPlatformPlugin;
import com.metamatrix.common.comm.platform.socket.SocketLog;
import com.metamatrix.dqp.client.ClientSideDQP;
import com.metamatrix.platform.security.api.ILogon;
import com.metamatrix.platform.security.api.LogonResult;

/**
 * Represents a client connection that maintains session state and allows for service fail over.
 * Implements a sticky random selection policy.
 */
public class SocketServerConnection implements ServerConnection {
	
	private static final int RETRY_COUNT = 3;

	private Map<HostInfo, SocketServerInstance> existingConnections = new HashMap<HostInfo, SocketServerInstance>();
	private SocketServerInstanceFactory connectionFactory;
    private ServerDiscovery serverDiscovery;
    private SocketLog log;

	private boolean secure;
    private Properties connProps;
	
	private SocketServerInstance serverInstance;
    private volatile LogonResult logonResult;
    private ILogon logon;
    private Timer pingTimer;
    private volatile boolean closed;
    
	public SocketServerConnection(
			SocketServerInstanceFactory connectionFactory, boolean secure,
			ServerDiscovery serverDiscovery, Properties connProps,
			Timer pingTimer, SocketLog log) throws CommunicationException, ConnectionException {
		this.connectionFactory = connectionFactory;
		this.serverDiscovery = serverDiscovery;
		this.connProps = connProps;
		this.secure = secure;
		this.logon = this.getService(ILogon.class);

        authenticate(); 
        
        this.pingTimer = pingTimer;
        if (this.pingTimer != null && logonResult.getPingInterval() > 0) {
        	schedulePing();
        }
	}
	
	/**
	 * Implements a sticky random selection policy
	 * TODO: make this customizable
	 * TODO: put more information on hostinfo as to process response time, last successful connect, etc.
	 */
	public synchronized SocketServerInstance selectServerInstance()
			throws CommunicationException {
		if (closed) {
			throw new CommunicationException(CommPlatformPlugin.Util.getString("SocketServerConnection.closed")); //$NON-NLS-1$ 
		}
		if (this.serverInstance != null) {
			if (this.serverInstance.isOpen()) {
				return this.serverInstance;
			}
			this.serverInstance = null;
		}
		List<HostInfo> hostKeys = new ArrayList<HostInfo>(this.serverDiscovery.getKnownHosts());
		int knownHosts = hostKeys.size();
		for (int i = 0; i < hostKeys.size(); i++) {
			HostInfo hostInfo = hostKeys.remove((int) (Math.random() * hostKeys.size()));

			SocketServerInstance instance = existingConnections.get(hostInfo);
			if (instance != null) {
				if (instance.isOpen()) {
					this.serverInstance = instance;
					return this.serverInstance;
				}
				existingConnections.remove(hostInfo);
				hostKeys.add(hostInfo);
			}
			Exception ex = null;
			try {
				instance = connectionFactory.createServerInstance(hostInfo, secure);
				if (this.logonResult != null) {
					ILogon newLogon = instance.getService(ILogon.class);
					newLogon.assertIdentity(logonResult.getSessionID());
				}
				this.existingConnections.put(hostInfo, instance);
				this.serverDiscovery.connectionSuccessful(hostInfo, instance);
				this.serverInstance = instance;
				return this.serverInstance;
			} catch (IOException e) {
				ex = e;
			} catch (InvalidSessionException e) {
				shutdown();
				throw new CommunicationException(e,CommPlatformPlugin.Util.getString("SocketServerInstance.Connection_Error.Connect_Failed", hostInfo.getHostName(), String.valueOf(hostInfo.getPortNumber()), e.getMessage())); //$NON-NLS-1$
			} catch (SingleInstanceCommunicationException e) { 
				ex = e;
			} catch (MetaMatrixComponentException e) {
				ex = e;
			} 	
			this.serverDiscovery.markInstanceAsBad(hostInfo);
			if (knownHosts == 1) { //just a single host, use the exception
				if (ex instanceof UnknownHostException) {
					throw new SingleInstanceCommunicationException(ex, CommPlatformPlugin.Util.getString("SocketServerInstance.Connection_Error.Unknown_Host", hostInfo.getHostName())); //$NON-NLS-1$
				}
				throw new SingleInstanceCommunicationException(ex,CommPlatformPlugin.Util.getString("SocketServerInstance.Connection_Error.Connect_Failed", hostInfo.getHostName(), String.valueOf(hostInfo.getPortNumber()), ex.getMessage())); //$NON-NLS-1$
			}
			log.logDetail("SocketServerConnection.selectServerInstance", ex, "Unable to connect to host"); //$NON-NLS-1$ //$NON-NLS-2$
		}
		throw new CommunicationException(CommPlatformPlugin.Util.getString("SocketServerInstancePool.No_valid_host_available")); //$NON-NLS-1$
	}
	
	public synchronized void authenticate() throws ConnectionException, CommunicationException {
		this.logonResult = null;
        // Log on to server
        try {
            this.logonResult = logon.logon(connProps);
            if (this.serverDiscovery.setLogonResult(this.logonResult)) {
            	selectNewServerInstance();
            }
            return;
        } catch (LogonException e) {
            // Propagate the original message as it contains the message we want
            // to give to the user
            throw new ConnectionException(e, e.getMessage());
        } catch (MetaMatrixComponentException e) {
            throw new CommunicationException(e, CommPlatformPlugin.Util.getString("PlatformServerConnectionFactory.Unable_to_find_a_component_used_in_logging_on_to_MetaMatrix")); //$NON-NLS-1$
        } 	
	}
	
	private void schedulePing() {
		this.pingTimer.schedule(new TimerTask() {

			@Override
			public void run() {
				try {
					if (isOpen()) {
						logon.ping();
						schedulePing();
					}
				} catch (InvalidSessionException e) {
					shutdown();
				} catch (MetaMatrixComponentException e) {
					shutdown();
				}
				
			}}, logonResult.getPingInterval());
	}
	
	class ServerConnectionInvocationHandler implements InvocationHandler {
		
		private Class<?> targetClass;
		private SocketServerInstance instance;
		private Object target;
		private boolean failOver;
		
		public ServerConnectionInvocationHandler(Class<?> targetClass) {
			this.targetClass = targetClass;
			this.failOver = Boolean.valueOf(connProps.getProperty(MMURL.CONNECTION.AUTO_FAILOVER)).booleanValue()
					|| !ClientSideDQP.class.isAssignableFrom(targetClass);
		}
		
		private synchronized Object getTarget() throws CommunicationException {
			if (this.target == null) {
				this.instance = selectServerInstance();
				this.target = this.instance.getService(targetClass);
			}
			return this.target;
		}
				
		public Object invoke(Object proxy, Method method, Object[] args)
				throws Throwable {
			Throwable exception = null;
            for (int i = 0; i < RETRY_COUNT; i++) {
		        try {
	                return method.invoke(getTarget(), args);
	            } catch (InvocationTargetException t) {
	            	exception = t.getTargetException();
	            } catch (Throwable t) {
	            	exception = t;
	            }
	            if (exception instanceof SingleInstanceCommunicationException
						|| exception.getCause() instanceof SingleInstanceCommunicationException) {
	            	if (!isOpen()) {
	            		break;
	            	}
	            	if (!failOver) {
			        	break;
		        	}
	            	invalidateTarget();
	            } else {
	            	break;
	            }
	            //TODO: look for invalid session exception
			}
	        throw ExceptionUtil.convertException(method, exception);
		}
		
		private synchronized void invalidateTarget() {
			this.target = null;
		}

		synchronized SocketServerInstance getInstance() throws CommunicationException {
			if (instance == null) {
				getTarget();
			}
			return instance;
		}
	    
	}

	public <T> T getService(Class<T> iface) {
		return (T)Proxy.newProxyInstance(Thread.currentThread().getContextClassLoader(), new Class[] {iface}, new ServerConnectionInvocationHandler(iface));
	}

	public synchronized void shutdown() {
		if (this.closed) {
			return;
		}
		try {
			//make a best effort to send the logoff
			Future<?> writeFuture = this.logon.logoff();
			writeFuture.get();
		} catch (InvalidSessionException e) {
			//ignore
		} catch (MetaMatrixComponentException e) {
			//ignore
		} catch (InterruptedException e) {
			//ignore
		} catch (ExecutionException e) {
			//ignore
		}

		for (SocketServerInstance instance : existingConnections.values()) {
			instance.shutdown();
		}
		existingConnections.clear();
		this.closed = true;
		this.serverDiscovery.shutdown();
	}

	public boolean isOpen() {
		if (this.closed) {
			return false;
		}
		try {
			return selectServerInstance().isOpen();
		} catch (CommunicationException e) {
			return false;
		}
	}

	public LogonResult getLogonResult() {
		return logonResult;
	}
	
	synchronized void selectNewServerInstance() {
		this.serverInstance = null;
	}
	
	public static boolean isSameInstance(Object service, Object otherService) throws CommunicationException {
		try {
			ServerConnectionInvocationHandler handler = (ServerConnectionInvocationHandler)Proxy.getInvocationHandler(service);
			ServerConnectionInvocationHandler otherHandler = (ServerConnectionInvocationHandler)Proxy.getInvocationHandler(otherService);
			return handler.getInstance().getHostInfo().equals(otherHandler.getInstance().getHostInfo());
		} catch (IllegalArgumentException e) {
			return false;
		}
	}
	
	public static void selectNewServerInstance(Object service) {
		ServerConnectionInvocationHandler handler = (ServerConnectionInvocationHandler)Proxy.getInvocationHandler(service);
		handler.invalidateTarget();
	}

}