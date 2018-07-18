/*
 * Copyright Red Hat, Inc. and/or its affiliates
 * and other contributors as indicated by the @author tags and
 * the COPYRIGHT.txt file distributed with this work.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.teiid.net.socket;

import java.io.IOException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
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
import org.teiid.core.util.PropertiesUtils;
import org.teiid.gss.MakeGSS;
import org.teiid.jdbc.JDBCPlugin;
import org.teiid.net.CommunicationException;
import org.teiid.net.ConnectionException;
import org.teiid.net.HostInfo;
import org.teiid.net.ServerConnection;
import org.teiid.net.TeiidURL;


/**
 * Represents a client connection that maintains session state and allows for service fail over.
 * Implements a sticky random selection policy.
 */
public class SocketServerConnection implements ServerConnection {
	
	private static final int FAILOVER_PING_INTERVAL = 1000;
	private SocketServerInstanceFactory connectionFactory;
    private UrlServerDiscovery serverDiscovery;
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
	private String serverVersion;
    
	public SocketServerConnection(
			SocketServerInstanceFactory connectionFactory, boolean secure,
			UrlServerDiscovery serverDiscovery, Properties connProps) throws CommunicationException, ConnectionException {
		this.connectionFactory = connectionFactory;
		this.serverDiscovery = serverDiscovery;
		this.connProps = connProps;
		this.secure = secure;
		//ILogon that is allowed to failover
		this.logon = this.getService(ILogon.class);
		this.failOver = Boolean.valueOf(connProps.getProperty(TeiidURL.CONNECTION.AUTO_FAILOVER)).booleanValue();
		this.serverVersion = selectServerInstance(false).getServerVersion();
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
			 throw new CommunicationException(JDBCPlugin.Event.TEIID20016, JDBCPlugin.Util.gs(JDBCPlugin.Event.TEIID20016));
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
					InetAddress inetAddress = hostInfo.getInetAddress();
					if (!hostInfo.isResolved()) {
						//create a resolved version
						hostInfo = new HostInfo(hostInfo.getHostName(), new InetSocketAddress(inetAddress, hostInfo.getPortNumber()));
					}
				}
				ILogon newLogon = connect(hostInfo);
				if (this.logonResult == null) {
			        try {
			            logon(newLogon, logoff);
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
			             throw new ConnectionException(e);
			        } catch (TeiidComponentException e) {
			        	if (e.getCause() instanceof CommunicationException) {
			        		throw (CommunicationException)e.getCause();
			        	}
			             throw new CommunicationException(JDBCPlugin.Event.TEIID20018, e, JDBCPlugin.Util.gs(JDBCPlugin.Event.TEIID20018));
			        } 
				}
				return this.serverInstance;
			} catch (IOException e) {
				ex = e;
			} catch (SingleInstanceCommunicationException e) { 
				ex = e;
			} 	
			if (knownHosts == 1) { //just a single host, use the exception
				if (ex instanceof UnknownHostException) {
					 throw new SingleInstanceCommunicationException(JDBCPlugin.Event.TEIID20019, ex, JDBCPlugin.Util.gs(JDBCPlugin.Event.TEIID20019, hostInfo.getHostName()));
				}
				 throw new SingleInstanceCommunicationException(JDBCPlugin.Event.TEIID20020, ex,JDBCPlugin.Util.gs(JDBCPlugin.Event.TEIID20020, hostInfo.getHostName(), String.valueOf(hostInfo.getPortNumber()), ex.getMessage()));
			}
			log.log(Level.FINE, "Unable to connect to host", ex); //$NON-NLS-1$
		}
		 throw new CommunicationException(JDBCPlugin.Event.TEIID20021, JDBCPlugin.Util.gs(JDBCPlugin.Event.TEIID20021, hostCopy.toString()));
	}

	private void logon(ILogon newLogon, boolean logoff) throws LogonException, TeiidComponentException, CommunicationException {

		SocketServerInstance instance = this.serverInstance;
		
		updateConnectionProperties(connProps, instance.getLocalAddress(), true);

		LogonResult newResult = null;

		// - if gss
		if (connProps.contains(TeiidURL.CONNECTION.JAAS_NAME)) {
			newResult = MakeGSS.authenticate(newLogon, connProps);
		} else {
			newResult = newLogon.logon(connProps);
		}

		AuthenticationType type = (AuthenticationType) newResult.getProperty(ILogon.AUTH_TYPE);
		
		if (type != null) {
			//server has issued an additional challange
			if (type == AuthenticationType.GSS) {
				newResult = MakeGSS.authenticate(newLogon, connProps);
			} else {
				throw new LogonException(JDBCPlugin.Event.TEIID20034, JDBCPlugin.Util.gs(JDBCPlugin.Event.TEIID20034, type));
			}
		}
		
		if (logoff) {
			LogonResult old = this.logonResults.remove(this.serverInstance.getHostInfo());
			logoffAll();
		}
		
		this.logonResult = newResult;
		this.logonResults.put(instance.getHostInfo(), this.logonResult);
	}
	
	public static void updateConnectionProperties(Properties connectionProperties, InetAddress addr, boolean setMac) {
		if (addr == null) {
			return;
		}
		String address = addr.getHostAddress();
		Object old = connectionProperties.put(TeiidURL.CONNECTION.CLIENT_IP_ADDRESS, address);
		if (old == null || !address.equals(old)) {
		    if (addr.isLoopbackAddress()) {
		        connectionProperties.put(TeiidURL.CONNECTION.CLIENT_HOSTNAME, addr.getCanonicalHostName());
		    } else {
		        connectionProperties.put(TeiidURL.CONNECTION.CLIENT_HOSTNAME, "localhost"); //$NON-NLS-1$
		    }
			if (setMac) {
				try {
					NetworkInterface ni = NetworkInterface.getByInetAddress(addr);
					if (ni != null && ni.getHardwareAddress() != null) {
						StringBuilder sb = new StringBuilder();
						for (byte b : ni.getHardwareAddress()) {
							sb.append(PropertiesUtils.toHex(b >> 4));
							sb.append(PropertiesUtils.toHex(b));
						}
						connectionProperties.put(TeiidURL.CONNECTION.CLIENT_MAC, sb.toString());
					}
		        } catch (SocketException e) {
					connectionProperties.remove(TeiidURL.CONNECTION.CLIENT_MAC);
		        }
			} else {
				connectionProperties.remove(TeiidURL.CONNECTION.CLIENT_MAC);
			}
		}
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
		return iface.cast(Proxy.newProxyInstance(this.getClass().getClassLoader(), new Class[] {iface}, new SocketServerInstanceImpl.RemoteInvocationHandler(iface, PropertiesUtils.getBooleanProperty(connProps, TeiidURL.CONNECTION.ENCRYPT_REQUESTS, false)) {
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
			
			@Override
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
		this.logonResult = null;
	}
	
	private synchronized ResultsFuture<?> isOpen() throws CommunicationException, InvalidSessionException, TeiidComponentException {
		if (this.closed) {
			 throw new CommunicationException(JDBCPlugin.Event.TEIID20023, JDBCPlugin.Util.gs(JDBCPlugin.Event.TEIID20023));
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
	
	@Override
	public boolean supportsContinuous() {
		return false;
	}
	
	@Override
	public boolean isLocal() {
		return false;
	}
	
	@Override
	public String getServerVersion() {
		return this.serverVersion;
	}
}