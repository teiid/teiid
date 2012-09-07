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

import org.teiid.client.security.ILogon;
import org.teiid.client.security.LogonException;
import org.teiid.client.security.LogonResult;
import org.teiid.client.util.ExceptionUtil;
import org.teiid.core.TeiidComponentException;
import org.teiid.core.TeiidRuntimeException;
import org.teiid.core.util.PropertiesUtils;
import org.teiid.dqp.internal.process.DQPWorkContext;
import org.teiid.jdbc.EmbeddedProfile;
import org.teiid.net.CommunicationException;
import org.teiid.net.ConnectionException;
import org.teiid.net.ServerConnection;
import org.teiid.net.TeiidURL;
import org.teiid.runtime.RuntimePlugin;


public class LocalServerConnection implements ServerConnection {
	private static final String TEIID_RUNTIME = "teiid/engine-deployer"; //$NON-NLS-1$
	
	private LogonResult result;
	private boolean shutdown;
	private ClientServiceRegistry csr;
    private DQPWorkContext workContext = new DQPWorkContext();
    private Properties connectionProperties;
    private boolean passthrough;

	public LocalServerConnection(Properties connectionProperties, boolean useCallingThread) throws CommunicationException, ConnectionException{
		this.connectionProperties = connectionProperties;
		this.csr = getClientServiceRegistry();
		
		String vdbVersion = connectionProperties.getProperty(TeiidURL.JDBC.VDB_VERSION);
		String vdbName = connectionProperties.getProperty(TeiidURL.JDBC.VDB_NAME);
		int firstIndex = vdbName.indexOf('.');
		int lastIndex = vdbName.lastIndexOf('.');
		if (firstIndex != -1 && firstIndex == lastIndex) {
			vdbVersion = vdbName.substring(firstIndex+1);
			vdbName = vdbName.substring(0, firstIndex);
		}
		if (vdbVersion != null) {
			int waitForLoad = PropertiesUtils.getIntProperty(connectionProperties, EmbeddedProfile.WAIT_FOR_LOAD, -1);
			if (waitForLoad != 0) {
				this.csr.waitForFinished(vdbName, Integer.valueOf(vdbVersion), waitForLoad);
			}
		}
		
		workContext.setSecurityHelper(csr.getSecurityHelper());
		workContext.setUseCallingThread(useCallingThread);
		workContext.setSecurityContext(csr.getSecurityHelper().getSecurityContext());
		authenticate();
		passthrough = Boolean.valueOf(connectionProperties.getProperty(TeiidURL.CONNECTION.PASSTHROUGH_AUTHENTICATION, "false")); //$NON-NLS-1$
	}

	protected ClientServiceRegistry getClientServiceRegistry() {
		try {
			InitialContext ic = new InitialContext();
			return (ClientServiceRegistry)ic.lookup(TEIID_RUNTIME);
		} catch (NamingException e) {
			throw new TeiidRuntimeException(e);
		}
	}
	
	public synchronized void authenticate() throws ConnectionException, CommunicationException {
        try {
        	this.result = this.getService(ILogon.class).logon(this.connectionProperties);
        } catch (LogonException e) {
            // Propagate the original message as it contains the message we want
            // to give to the user
            throw new ConnectionException(e, e.getMessage());
        } catch (TeiidComponentException e) {
        	if (e.getCause() instanceof CommunicationException) {
        		throw (CommunicationException)e.getCause();
        	}
            throw new CommunicationException(e);
        } 	
	}	
	
	public <T> T getService(final Class<T> iface) {
		return iface.cast(Proxy.newProxyInstance(this.getClass().getClassLoader(), new Class[] {iface}, new InvocationHandler() {

			public Object invoke(Object arg0, final Method arg1, final Object[] arg2) throws Throwable {
				if (shutdown) {
					throw ExceptionUtil.convertException(arg1, new TeiidComponentException(RuntimePlugin.Util.getString("LocalTransportHandler.Transport_shutdown"))); //$NON-NLS-1$
				}
				try {
					if (passthrough && !arg1.getDeclaringClass().equals(ILogon.class)) {
						// check to make sure the current security context same as logged one
						if (!csr.getSecurityHelper().sameSubject(workContext.getSession().getSecurityDomain(), workContext.getSession().getSecurityContext(), workContext.getSubject())) {
							authenticate();
						}
					}
					
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

	@Override
	public boolean isOpen(long msToTest) {
		if (shutdown) {
			return false;
		}
		return true;
	}

	public void close() {
		shutdown(true);
	}
	
	private void shutdown(boolean logoff) {
		if (shutdown) {
			return;
		}
		
		if (logoff) {
			logoff();
		}
		this.shutdown = true;
	}

	private void logoff() {
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

	public LogonResult getLogonResult() {
		return result;
	}

	@Override
	public boolean isSameInstance(ServerConnection conn) throws CommunicationException {
		return (conn instanceof LocalServerConnection);
	}
	
	@Override
	public void cleanUp() {
		
	}
}
