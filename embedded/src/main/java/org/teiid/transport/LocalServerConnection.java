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

package org.teiid.transport;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.Properties;

import org.teiid.dqp.internal.process.DQPWorkContext;

import com.metamatrix.api.exception.MetaMatrixComponentException;
import com.metamatrix.api.exception.security.LogonException;
import com.metamatrix.client.ExceptionUtil;
import com.metamatrix.common.comm.ClientServiceRegistry;
import com.metamatrix.common.comm.api.ServerConnection;
import com.metamatrix.common.comm.exception.CommunicationException;
import com.metamatrix.common.comm.exception.ConnectionException;
import com.metamatrix.common.comm.platform.CommPlatformPlugin;
import com.metamatrix.jdbc.JDBCPlugin;
import com.metamatrix.platform.security.api.ILogon;
import com.metamatrix.platform.security.api.LogonResult;

public class LocalServerConnection implements ServerConnection {
	
	private final LogonResult result;
	private boolean shutdown;
	private DQPWorkContext workContext;
	private ClassLoader classLoader;
	ClientServiceRegistry clientServices;
	

	public LocalServerConnection(Properties connectionProperties, ClientServiceRegistry clientServices) throws CommunicationException, ConnectionException{
	
		this.clientServices = clientServices;		
		
		//Initialize the workContext
		workContext = new DQPWorkContext();
		DQPWorkContext.setWorkContext(workContext);
		
		this.result = authenticate(connectionProperties);
		
		this.classLoader = Thread.currentThread().getContextClassLoader();
	}

	public synchronized LogonResult authenticate(Properties connProps) throws ConnectionException, CommunicationException {
        try {
        	LogonResult logonResult = getService(ILogon.class).logon(connProps);
        	return logonResult;
        } catch (LogonException e) {
            // Propagate the original message as it contains the message we want
            // to give to the user
            throw new ConnectionException(e, e.getMessage());
        } catch (MetaMatrixComponentException e) {
        	if (e.getCause() instanceof CommunicationException) {
        		throw (CommunicationException)e.getCause();
        	}
            throw new CommunicationException(e, CommPlatformPlugin.Util.getString("PlatformServerConnectionFactory.Unable_to_find_a_component_used_in_logging_on_to_MetaMatrix")); //$NON-NLS-1$
        } 	
	}	
	
	
	@SuppressWarnings("unchecked")
	public <T> T getService(final Class<T> iface) {

		return (T) Proxy.newProxyInstance(this.getClass().getClassLoader(), new Class[] {iface}, new InvocationHandler() {

			public Object invoke(Object arg0, Method arg1, Object[] arg2)
					throws Throwable {
				if (!isOpen()) {
					throw ExceptionUtil.convertException(arg1, new MetaMatrixComponentException(JDBCPlugin.Util.getString("LocalTransportHandler.session_inactive"))); //$NON-NLS-1$
				}
				ClassLoader current = Thread.currentThread().getContextClassLoader();
				Thread.currentThread().setContextClassLoader(classLoader);
				DQPWorkContext.setWorkContext(workContext);
				try {
					return arg1.invoke(clientServices.getClientService(iface), arg2);
				} catch (InvocationTargetException e) {
					throw e.getTargetException();
				} finally {
					Thread.currentThread().setContextClassLoader(current);
				}
			}
		});
	}

	public boolean isOpen() {
		return !shutdown;
	}

	public void shutdown() {
		if (shutdown) {
			return;
		}
		this.shutdown = true;
	}

	public LogonResult getLogonResult() {
		return result;
	}

	@Override
	public boolean isSameInstance(ServerConnection conn) throws CommunicationException {
		return (conn instanceof LocalServerConnection);
	}
}
