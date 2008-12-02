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
import java.io.Serializable;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import com.metamatrix.api.exception.MetaMatrixComponentException;
import com.metamatrix.api.exception.security.InvalidSessionException;
import com.metamatrix.api.exception.security.LogonException;
import com.metamatrix.client.ExceptionUtil;
import com.metamatrix.common.api.HostInfo;
import com.metamatrix.common.api.MMURL_Properties;
import com.metamatrix.common.comm.api.Message;
import com.metamatrix.common.comm.api.MessageListener;
import com.metamatrix.common.comm.api.ResultsReceiver;
import com.metamatrix.common.comm.api.ServerConnection;
import com.metamatrix.common.comm.exception.CommunicationException;
import com.metamatrix.common.comm.exception.ConnectionException;
import com.metamatrix.common.comm.platform.CommPlatformPlugin;
import com.metamatrix.common.comm.platform.socket.SocketConstants;
import com.metamatrix.common.util.crypto.CryptoException;
import com.metamatrix.dqp.client.ClientSideDQP;
import com.metamatrix.dqp.client.PortableContext;
import com.metamatrix.dqp.client.ResultsFuture;
import com.metamatrix.platform.security.api.ILogon;
import com.metamatrix.platform.security.api.LogonResult;

public class SocketServerConnection implements ServerConnection {

	private Map<HostInfo, SocketServerInstance> existingConnections = new HashMap<HostInfo, SocketServerInstance>();
	private SocketServerInstanceFactory connectionFactory;

	private AtomicInteger MESSAGE_ID = new AtomicInteger();
    private SocketServerInstance serverInstance;
    private ServerDiscovery serverDiscovery;
    private volatile LogonResult logonResult;
    private ILogon logon;
    private Timer pingTimer;
    private Properties connProps;
    private boolean secure;
    
	public SocketServerConnection(
			SocketServerInstanceFactory connectionFactory, boolean secure,
			ServerDiscovery serverDiscovery, Properties connProps,
			Timer pingTimer) throws CommunicationException, ConnectionException {
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
	 */
	public synchronized SocketServerInstance selectServerInstance()
			throws CommunicationException {
		if (this.serverInstance != null) {
			return this.serverInstance;
		}
		List<HostInfo> hostKeys = this.serverDiscovery.getKnownHosts();
		int knownHosts = hostKeys.size();
		for (int i = 0; i < hostKeys.size(); i++) {
			HostInfo hostInfo = hostKeys.remove((int) (Math.random() * hostKeys.size()));

			SocketServerInstance instance = existingConnections.get(hostInfo);
			if (instance != null) {
				if (instance.isOpen()) { // TODO: do ping
					this.serverInstance = instance;
					return this.serverInstance;
				}
				existingConnections.remove(hostInfo);
				hostKeys.add(hostInfo);
			}
			Exception ex = null;
			try {
				this.serverInstance = connectionFactory.createServerInstance(hostInfo, secure);
				if (this.logonResult != null) {
					this.logon.assertIdentity(logonResult.getSessionID());
				}
				this.existingConnections.put(hostInfo, this.serverInstance);
				return this.serverInstance;
			} catch (IOException e) {
				ex = e;
			} catch (InvalidSessionException e) {
				throw new CommunicationException(e,CommPlatformPlugin.Util.getString("SocketServerInstance.Connection_Error.Connect_Failed", hostInfo.getHostName(), String.valueOf(hostInfo.getPortNumber()), e.getMessage())); //$NON-NLS-1$
			} catch (MetaMatrixComponentException e) {
				ex = e;
			}	
			this.serverInstance = null;
			this.serverDiscovery.markInstanceAsBad(hostInfo);
			if (knownHosts == 1) { //just a single host, use the exception
				if (ex instanceof UnknownHostException) {
					throw new CommunicationException(ex, CommPlatformPlugin.Util.getString("SocketServerInstance.Connection_Error.Uknown_Host", hostInfo.getHostName())); //$NON-NLS-1$
				}
				throw new CommunicationException(ex,CommPlatformPlugin.Util.getString("SocketServerInstance.Connection_Error.Connect_Failed", hostInfo.getHostName(), String.valueOf(hostInfo.getPortNumber()), ex.getMessage())); //$NON-NLS-1$
			}
		}
		throw new CommunicationException(CommPlatformPlugin.Util.getString("SocketServerInstancePool.No_valid_host_available")); //$NON-NLS-1$
	}
	
	public synchronized void authenticate() throws ConnectionException, CommunicationException {
		this.logonResult = null;
		CommunicationException ce = null;
		for (int i = 0; i < 3; i++) {
	        // Log on to server
	        try {
	            logonResult = logon.logon(connProps);
	            return;
	        } catch (LogonException e) {
	            // Propagate the original message as it contains the message we want
	            // to give to the user
	            throw new ConnectionException(e, e.getMessage());
	        } catch (MetaMatrixComponentException e) {
	            ce = new CommunicationException(e, CommPlatformPlugin.Util.getString("PlatformServerConnectionFactory.Unable_to_find_a_component_used_in_logging_on_to_MetaMatrix")); //$NON-NLS-1$
	        } 	
	        selectNewServerInstance();
		}
		throw ce;
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
		
		public ServerConnectionInvocationHandler(Class<?> targetClass) {
			this.targetClass = targetClass;
		}

		public Object invoke(Object proxy, Method method, Object[] args)
				throws Throwable {
	        Throwable exception = null;
            try {
                return doOneInvocation(method, args);
            } catch (ExecutionException e) {
            	exception = e.getCause();
            } catch (InvocationTargetException e) {
            	exception = e.getCause();
            } catch (Throwable t) {
            	exception = t;
            }
	        
	        throw ExceptionUtil.convertException(method, exception);
		}
		
	    private Object doOneInvocation(Method method, Object[] args) throws Throwable {
	    	final SocketServerInstance instance = selectServerInstance();
	    	Message message = new Message();
	        message.setContents(new ServiceInvocationStruct(args, method.getName(), targetClass));
	        //ping and close don't need secured. this would be better as an annotation
	        boolean isDQPMethod = ClientSideDQP.class.isAssignableFrom(method.getDeclaringClass());
	        if (!isDQPMethod) {
	        	message.setContents(instance.getCryptor().sealObject(message.getContents()));
	        }
	        boolean isLogonMethod = ILogon.class.isAssignableFrom(method.getDeclaringClass());
	        boolean isAdminMethod = !ClientSideDQP.class.isAssignableFrom(method.getDeclaringClass()) && !isLogonMethod;
	        int retryCount = isAdminMethod?3:1;
	        ResultsFuture results = new ResultsFuture() {
	        	@Override
	        	protected Object convertResult() throws ExecutionException {
	        		try {
						return instance.getCryptor().unsealObject((Serializable)super.convertResult());
					} catch (CryptoException e) {
						throw new ExecutionException(e);
					}
	        	}
	        };
	        final ResultsReceiver receiver = results.getResultsReceiver();
			for (int i = 0; i < retryCount; i++) {
		        try {
			        instance.send(message, new MessageListener() {
		
						public void deliverMessage(Message responseMessage,
								Serializable messageKey) {
							try {
								receiver.receiveResults(convertResponse(responseMessage));
							} catch (Throwable e) {
								receiver.exceptionOccurred(e);
							}
						}
			        	
			        }, new Integer(MESSAGE_ID.getAndIncrement()));
			        break;
		        } catch (CommunicationException e) {
		        	if (!isLogonMethod && Boolean.valueOf(connProps.getProperty(MMURL_Properties.CONNECTION.AUTO_FAILOVER)).booleanValue()) {
			        	selectServerInstance();
		        	}
		        	if (i == retryCount - 1) {
		        		throw e;
		        	} 
		        }
			}
	        if (ResultsFuture.class.isAssignableFrom(method.getReturnType())) {
	        	return results;
	        }
	        return results.get(SocketConstants.getSynchronousTTL(), TimeUnit.MILLISECONDS);
	    }

		private Object convertResponse(Message responseMessage)
				throws Throwable {
			Serializable result = responseMessage.getContents();
	        if (result instanceof Throwable) {
	            throw (Throwable)result;
	        }
	        return result;
		}
	    
	}

	public <T> T getService(Class<T> iface) {
		return (T)Proxy.newProxyInstance(Thread.currentThread().getContextClassLoader(), new Class[] {iface}, new ServerConnectionInvocationHandler(iface));
	}

	public synchronized void shutdown() {
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
	}

	public PortableContext getContext() {
		try {
			return selectServerInstance().getContext();
		} catch (CommunicationException e) {
			throw new IllegalStateException(e);
		}
	}

	public boolean isOpen() {
		try {
			return selectServerInstance().isOpen();
		} catch (CommunicationException e) {
			return false;
		}
	}

	public synchronized LogonResult getLogonResult() {
		return logonResult;
	}
	
	public SocketServerInstance getSocketServerInstance() {
		return this.serverInstance;
	}

	public synchronized void selectNewServerInstance() {
		this.serverInstance = null;
	}

}