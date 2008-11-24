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

import java.io.Serializable;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
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
import com.metamatrix.common.api.MMURL_Properties;
import com.metamatrix.common.comm.api.Message;
import com.metamatrix.common.comm.api.MessageListener;
import com.metamatrix.common.comm.api.ResultsReceiver;
import com.metamatrix.common.comm.api.ServerConnection;
import com.metamatrix.common.comm.api.ServerInstanceContext;
import com.metamatrix.common.comm.exception.CommunicationException;
import com.metamatrix.common.comm.exception.ConnectionException;
import com.metamatrix.common.comm.platform.CommPlatformPlugin;
import com.metamatrix.common.comm.platform.socket.SocketConstants;
import com.metamatrix.dqp.client.ClientSideDQP;
import com.metamatrix.dqp.client.ResultsFuture;
import com.metamatrix.platform.security.api.ILogon;
import com.metamatrix.platform.security.api.LogonResult;
import com.metamatrix.platform.util.ProductInfoConstants;

public class SocketServerConnection implements ServerConnection {

	private AtomicInteger MESSAGE_ID = new AtomicInteger();
    private SocketServerInstance serverConnection;
    private LogonResult logonResult;
    private ILogon logon;
    private Timer pingTimer;
    
	public SocketServerConnection(SocketServerInstance serverConnection, Properties connProps, Timer pingTimer) throws CommunicationException, ConnectionException {
		this.serverConnection = serverConnection;
		this.logon = this.getService(ILogon.class);

        // Log on to server
        try {
            logonResult = logon.logon(connProps);
        } catch (LogonException e) {
            // Propagate the original message as it contains the message we want
            // to give to the user
            throw new ConnectionException(e, e.getMessage());
        } catch (MetaMatrixComponentException e) {
            throw new CommunicationException(e, CommPlatformPlugin.Util.getString("PlatformServerConnectionFactory.Unable_to_find_a_component_used_in_logging_on_to_MetaMatrix")); //$NON-NLS-1$
        } 

        // Update VDB name/version in connection properties
        String vdbName = logonResult.getProductInfo(ProductInfoConstants.VIRTUAL_DB);
        if (vdbName != null) {
            // Some things use one name some another... hard to tell who wants what.
            connProps.setProperty(MMURL_Properties.JDBC.VDB_NAME, vdbName);
            connProps.setProperty(MMURL_Properties.JDBC.VDB_VERSION, logonResult.getProductInfo(ProductInfoConstants.VDB_VERSION));
            connProps.setProperty("vdbName", vdbName); //$NON-NLS-1$
            connProps.setProperty("vdbVersion", logonResult.getProductInfo(ProductInfoConstants.VDB_VERSION)); //$NON-NLS-1$
        }
        
        // Update user name in connection properties to account for fully qualified user names
        String userName = logonResult.getUserName();
        if ( userName != null ) connProps.setProperty( MMURL_Properties.JDBC.USER_NAME, userName );
        
        this.pingTimer = pingTimer;
        if (this.pingTimer != null && logonResult.getPingInterval() > 0) {
        	schedulePing();
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
		
		private Class targetClass;
		
		public ServerConnectionInvocationHandler(Class targetClass) {
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
	    	Message message = new Message();
	        message.setContents(new ServiceInvocationStruct(args, method.getName(), targetClass));
	        message.secure = !ClientSideDQP.class.isAssignableFrom(method.getClass());
	        ResultsFuture results = new ResultsFuture();
	        final ResultsReceiver receiver = results.getResultsReceiver();
	        serverConnection.send(message, new MessageListener() {

				public void deliverMessage(Message responseMessage,
						Serializable messageKey) {
					try {
						receiver.receiveResults(convertResponse(responseMessage));
					} catch (Throwable e) {
						receiver.exceptionOccurred(e);
					}
				}
	        	
	        }, new Integer(MESSAGE_ID.getAndIncrement()));
	        
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

	public void shutdown() {
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
		serverConnection.shutdown();
	}

	public ServerInstanceContext getContext() {
		return this.serverConnection.getContext();
	}

	public boolean isOpen() {
		return this.serverConnection.isOpen();
	}

	public LogonResult getLogonResult() {
		return logonResult;
	}
	
	public SocketServerInstance getSocketServerInstance() {
		return this.serverConnection;
	}

}