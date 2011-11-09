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

/**
 * 
 */
package org.teiid.net.socket;

import java.io.IOException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.teiid.client.security.ILogon;
import org.teiid.client.security.InvalidSessionException;
import org.teiid.client.security.LogonException;
import org.teiid.client.security.LogonResult;
import org.teiid.client.util.ExceptionUtil;
import org.teiid.client.util.ResultsFuture;
import org.teiid.core.TeiidComponentException;
import org.teiid.core.TeiidException;
import org.teiid.gss.MakeGSS;
import org.teiid.jdbc.JDBCPlugin;
import org.teiid.net.CommunicationException;
import org.teiid.net.ConnectionException;
import org.teiid.net.HostInfo;
import org.teiid.net.ServerConnection;
import org.teiid.net.TeiidURL;
import org.teiid.net.TeiidURL.CONNECTION.AuthenticationType;


/**
 * Represents a client connection that maintains session state and allows for service fail over.
 * Implements a sticky random selection policy.
 */
public class SocketServerConnection implements ServerConnection {
	
	private static final int FAILOVER_PING_INTERVAL = 1000;
	private SocketServerInstanceFactory connectionFactory;
    private ServerDiscovery serverDiscovery;
    private static Logger log = Logger.getLogger("org.teiid.client.sockets"); //$NON-NLS-1$

	private boolean secure;
    private Properties connProps;
	
	private SocketServerInstance serverInstance;
    private LogonResult logonResult;
    private Map<HostInfo, LogonResult> logonResults = new ConcurrentHashMap<HostInfo, LogonResult>();
    private ILogon logon;
    private boolean closed;
	private boolean failOver;
	private long lastPing = System.currentTimeMillis();
	private int pingFailOverInterval = FAILOVER_PING_INTERVAL;
    
	public SocketServerConnection(
			SocketServerInstanceFactory connectionFactory, boolean secure,
			ServerDiscovery serverDiscovery, Properties connProps) throws CommunicationException, ConnectionException {
		this.connectionFactory = connectionFactory;
		this.serverDiscovery = serverDiscovery;
		this.connProps = connProps;
		this.secure = secure;
		//ILogon that is allowed to failover
		this.logon = this.getService(ILogon.class);
		this.failOver = Boolean.valueOf(connProps.getProperty(TeiidURL.CONNECTION.AUTO_FAILOVER)).booleanValue();
		this.failOver |= Boolean.valueOf(connProps.getProperty(TeiidURL.CONNECTION.ADMIN)).booleanValue();
		selectServerInstance(false);
	}
	
	/**
	 * Implements a sticky random selection policy
	 * TODO: make this customizable
	 * TODO: put more information on hostinfo as to process response time, last successful connect, etc.
	 * @throws ConnectionException 
	 */
	public synchronized SocketServerInstance selectServerInstance(boolean logoff)
			throws CommunicationException, ConnectionException {
		if (closed) {
			throw new CommunicationException(JDBCPlugin.Util.getString("SocketServerConnection.closed")); //$NON-NLS-1$ 
		}
		if (this.serverInstance != null && (!failOver || this.serverInstance.isOpen())) {
			return this.serverInstance;
		}
		List<HostInfo> hostKeys = new ArrayList<HostInfo>(this.serverDiscovery.getKnownHosts(logonResult, null));
		boolean discoverHosts = true;
		closeServerInstance();
		List<HostInfo> hostCopy = new ArrayList<HostInfo>(hostKeys);
		int knownHosts = hostKeys.size();
		while (hostKeys.size() > 0) {
			HostInfo hostInfo = this.serverDiscovery.selectNextInstance(hostKeys);

			Exception ex = null;
			try {
				if (!hostInfo.isResolved()) {
					hostInfo = new HostInfo(hostInfo.getHostName(), new InetSocketAddress(hostInfo.getInetAddress(), hostInfo.getPortNumber()));
				}
				ILogon newLogon = connect(hostInfo);
				if (this.logonResult == null) {
			        try {
			            logon(newLogon, logoff);
						this.serverDiscovery.connectionSuccessful(hostInfo);
			            if (discoverHosts) {
				            List<HostInfo> updatedHosts = this.serverDiscovery.getKnownHosts(logonResult, this.serverInstance);
				            if (updatedHosts.size() > 1 && new HashSet<HostInfo>(updatedHosts).size() > new HashSet<HostInfo>(hostCopy).size()) {
				            	hostKeys = updatedHosts;
				            	closeServerInstance();
				            	discoverHosts = false;
				            	continue;
				            }
			            }
			        } catch (LogonException e) {
			            // Propagate the original message as it contains the message we want
			            // to give to the user
			            throw new ConnectionException(e, e.getMessage());
			        } catch (TeiidComponentException e) {
			        	if (e.getCause() instanceof CommunicationException) {
			        		throw (CommunicationException)e.getCause();
			        	}
			            throw new CommunicationException(e, JDBCPlugin.Util.getString("PlatformServerConnectionFactory.Unable_to_find_a_component_used_in_logging_on_to")); //$NON-NLS-1$
			        } 
				}
				return this.serverInstance;
			} catch (IOException e) {
				ex = e;
			} catch (SingleInstanceCommunicationException e) { 
				ex = e;
			} 	
			this.serverDiscovery.markInstanceAsBad(hostInfo);
			if (knownHosts == 1) { //just a single host, use the exception
				if (ex instanceof UnknownHostException) {
					throw new SingleInstanceCommunicationException(ex, JDBCPlugin.Util.getString("SocketServerInstance.Connection_Error.Unknown_Host", hostInfo.getHostName())); //$NON-NLS-1$
				}
				throw new SingleInstanceCommunicationException(ex,JDBCPlugin.Util.getString("SocketServerInstance.Connection_Error.Connect_Failed", hostInfo.getHostName(), String.valueOf(hostInfo.getPortNumber()), ex.getMessage())); //$NON-NLS-1$
			}
			log.log(Level.FINE, "Unable to connect to host", ex); //$NON-NLS-1$
		}
		throw new CommunicationException(JDBCPlugin.Util.getString("SocketServerInstancePool.No_valid_host_available", hostCopy.toString())); //$NON-NLS-1$
	}

	private void logon(ILogon newLogon, boolean logoff) throws LogonException,
			TeiidComponentException, CommunicationException {

		SocketServerInstance instance = this.serverInstance;
		LogonResult newResult = null;

		AuthenticationType authType  = getAuthenticationType();
		if (AuthenticationType.CLEARTEXT.equals(authType)) {
			newResult = newLogon.logon(connProps);
		}
		else if (AuthenticationType.GSS.equals(authType)) {
			newResult = MakeGSS.authenticate(newLogon, connProps);
		}
		
		if (logoff) {
			if ("7.3".compareTo(this.serverInstance.getServerVersion()) <= 0) { //$NON-NLS-1$
				//just remove the current instance - the server has already logged off the current user
				LogonResult old = this.logonResults.remove(this.serverInstance.getHostInfo());
				this.connectionFactory.disconnected(this.serverInstance, old.getSessionToken());
			}
			logoffAll();
		}
		
		this.logonResult = newResult;
		this.logonResults.put(instance.getHostInfo(), this.logonResult);
		this.connectionFactory.connected(instance, this.logonResult.getSessionToken());
	}
	
	private AuthenticationType getAuthenticationType() {
		String authStr = this.connProps.getProperty(TeiidURL.CONNECTION.AUTHENTICATION_TYPE);
		if (authStr == null) {
			return AuthenticationType.CLEARTEXT;
		}
		return AuthenticationType.valueOf(authStr);
	}

	private ILogon connect(HostInfo hostInfo) throws CommunicationException,
			IOException {
		hostInfo.setSsl(secure);
		this.serverInstance = connectionFactory.getServerInstance(hostInfo);
		this.logonResult = logonResults.get(hostInfo);
		ILogon newLogon = this.serverInstance.getService(ILogon.class);
		if (this.logonResult != null) {
			try {
				newLogon.assertIdentity(logonResult.getSessionToken());
			} catch (TeiidException e) {
				// session is no longer valid
				disconnect();
			}
		}
		return newLogon;
	}
	
	public <T> T getService(Class<T> iface) {
		return iface.cast(Proxy.newProxyInstance(this.getClass().getClassLoader(), new Class[] {iface}, new SocketServerInstanceImpl.RemoteInvocationHandler(iface) {
			@Override
			protected SocketServerInstance getInstance() throws CommunicationException {
				if (failOver && System.currentTimeMillis() - lastPing > pingFailOverInterval) {
					try {
						ResultsFuture<?> future = selectServerInstance(false).getService(ILogon.class).ping();
						future.get();
					} catch (SingleInstanceCommunicationException e) {
						closeServerInstance();
					} catch (CommunicationException e) {
						throw e;
					} catch (InvalidSessionException e) {
						disconnect();
						closeServerInstance();
					} catch (Exception e) {
						closeServerInstance();
					}
				}
				lastPing = System.currentTimeMillis();
				try {
					return selectServerInstance(false);
				} catch (ConnectionException e) {
					throw new CommunicationException(e);
				}
			}
			
			public Object invoke(Object proxy, Method method, Object[] args)
					throws Throwable {
				try {
					return super.invoke(proxy, method, args);
				} catch (Exception e) {
					if (ExceptionUtil.getExceptionOfType(e, InvalidSessionException.class) != null) {
						disconnect();
					}
					throw e;
				}
			}
			
		}));
	}
	
	public synchronized void close() {
		if (this.closed) {
			return;
		}
		
		if (this.serverInstance != null) {
			logoff();
		}
		
		logoffAll();
		
		this.closed = true;
		this.serverDiscovery.shutdown();
	}

	private void logoffAll() {
		for (Map.Entry<HostInfo, LogonResult> logonEntry : logonResults.entrySet()) {
			try {
				connect(logonEntry.getKey());
				logoff();
			} catch (Exception e) {
				
			}
		}
	}

	private void logoff() {
		disconnect();
		try {
			//make a best effort to send the logoff
			Future<?> writeFuture = this.serverInstance.getService(ILogon.class).logoff();
			writeFuture.get(5000, TimeUnit.MILLISECONDS);
		} catch (Exception e) {
			//ignore
		}
		closeServerInstance();
	}

	private void disconnect() {
		this.logonResults.remove(this.serverInstance.getHostInfo());
		if (this.logonResult != null) {
			this.connectionFactory.disconnected(this.serverInstance, this.logonResult.getSessionToken());
			this.logonResult = null;
		}
	}
	
	private synchronized ResultsFuture<?> isOpen() throws CommunicationException, InvalidSessionException, TeiidComponentException {
		if (this.closed) {
			throw new CommunicationException();
		}
		return logon.ping();
	}
	
	public boolean isOpen(long msToTest) {
		try {
			ResultsFuture<?> future = isOpen();
			future.get(msToTest, TimeUnit.MILLISECONDS);
			return true;
		} catch (Throwable th) {
			return false;
		} 
	}

	public LogonResult getLogonResult() {
		return logonResult;
	}
	
	synchronized void closeServerInstance() {
		if (this.serverInstance != null) {
			this.serverInstance.shutdown();
			this.serverInstance = null;
		}
	}
	
	public boolean isSameInstance(ServerConnection otherService) throws CommunicationException {
		if (!(otherService instanceof SocketServerConnection)) {
			return false;
		}
		try {
			return selectServerInstance(false).getHostInfo().equals(((SocketServerConnection)otherService).selectServerInstance(false).getHostInfo());
		} catch (ConnectionException e) {
			throw new CommunicationException(e);
		}
	}
	
	public void cleanUp() {
		closeServerInstance();
	}
	
	public void setFailOver(boolean failOver) {
		this.failOver = failOver;
	}
	
	public void setFailOverPingInterval(int pingFailOverInterval) {
		this.pingFailOverInterval = pingFailOverInterval;
	}
	
	@Override
	public void authenticate() throws ConnectionException,
			CommunicationException {
		if (this.serverInstance == null) {
			selectServerInstance(true); //this will trigger a logon with the new credentials
		} else {
			ILogon logonInstance = this.serverInstance.getService(ILogon.class);
			try {
				this.logon(logonInstance, true);
			} catch (LogonException e) {
				throw new ConnectionException(e);
			} catch (TeiidComponentException e) {
				throw new CommunicationException(e);
			}
		}
	}
}