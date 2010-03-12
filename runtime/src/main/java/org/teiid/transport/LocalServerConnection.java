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
import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import javax.naming.InitialContext;
import javax.naming.NamingException;

import org.teiid.dqp.internal.process.DQPWorkContext;

import com.metamatrix.api.exception.MetaMatrixComponentException;
import com.metamatrix.api.exception.security.LogonException;
import com.metamatrix.client.ExceptionUtil;
import com.metamatrix.common.comm.CommonCommPlugin;
import com.metamatrix.common.comm.api.ServerConnection;
import com.metamatrix.common.comm.exception.CommunicationException;
import com.metamatrix.common.comm.exception.ConnectionException;
import com.metamatrix.common.comm.platform.CommPlatformPlugin;
import com.metamatrix.core.MetaMatrixRuntimeException;
import com.metamatrix.platform.security.api.ILogon;
import com.metamatrix.platform.security.api.LogonResult;

public class LocalServerConnection implements ServerConnection {
	private static final String TEIID_RUNTIME = "teiid/engine-deployer"; //$NON-NLS-1$
	
	private final LogonResult result;
	private boolean shutdown;
	private ClientServiceRegistry csr;
    private DQPWorkContext workContext = new DQPWorkContext();

	public LocalServerConnection(Properties connectionProperties) throws CommunicationException, ConnectionException{
		try {
			InitialContext ic = new InitialContext();
			csr = (ClientServiceRegistry)ic.lookup(TEIID_RUNTIME);
		} catch (NamingException e) {
			throw new MetaMatrixRuntimeException(e);
		}
		workContext.setSecurityHelper(csr.getSecurityHelper());
		this.result = authenticate(connectionProperties);
	}
	
	public synchronized LogonResult authenticate(Properties connProps) throws ConnectionException, CommunicationException {
        try {
        	connProps.setProperty(ServerConnection.LOCAL_CONNECTION, Boolean.TRUE.toString()); 
        	LogonResult logonResult = this.getService(ILogon.class).logon(connProps);
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
	
	public <T> T getService(final Class<T> iface) {
		return iface.cast(Proxy.newProxyInstance(this.getClass().getClassLoader(), new Class[] {iface}, new InvocationHandler() {

			public Object invoke(Object arg0, final Method arg1, final Object[] arg2) throws Throwable {
				if (!isOpen()) {
					throw ExceptionUtil.convertException(arg1, new MetaMatrixComponentException(CommonCommPlugin.Util.getString("LocalTransportHandler.Transport_shutdown"))); //$NON-NLS-1$
				}
				try {
					final T service = csr.getClientService(iface);
					return workContext.runInContext(new Callable<Object>() {
						public Object call() throws Exception {
							return arg1.invoke(service, arg2);						
						}
					});
				} catch (InvocationTargetException e) {
					throw e.getTargetException();
				} catch (Throwable e) {
					throw ExceptionUtil.convertException(arg1, e);
				}
			}
		}));
	}

	public boolean isOpen() {
		return !shutdown;
	}

	public void close() {
		shutdown(true);
	}
	
	private void shutdown(boolean logoff) {
		if (shutdown) {
			return;
		}
		
		if (logoff) {
			try {
				//make a best effort to send the logoff
				Future<?> writeFuture = this.getService(ILogon.class).logoff();
				if (writeFuture != null) {
					writeFuture.get(5000, TimeUnit.MILLISECONDS);
				}
			} catch (Exception e) {
				//ignore
			}
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
