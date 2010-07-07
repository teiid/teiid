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
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.net.SocketAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.teiid.client.security.ILogon;
import org.teiid.client.security.InvalidSessionException;
import org.teiid.client.security.LogonException;
import org.teiid.client.security.LogonResult;
import org.teiid.client.util.ExceptionUtil;
import org.teiid.client.util.ResultsFuture;
import org.teiid.core.TeiidComponentException;
import org.teiid.net.CommunicationException;
import org.teiid.net.ConnectionException;
import org.teiid.net.HostInfo;
import org.teiid.net.NetPlugin;
import org.teiid.net.ServerConnection;
import org.teiid.net.TeiidURL;


/**
 * Represents a client connection that maintains session state and allows for service fail over.
 * Implements a sticky random selection policy.
 */
public class SocketServerConnection implements ServerConnection {
	
	private static final int RETRY_COUNT = 3;

	private SocketServerInstanceFactory connectionFactory;
    private ServerDiscovery serverDiscovery;
    private static Logger log = Logger.getLogger("org.teiid.client.sockets"); //$NON-NLS-1$

	private boolean secure;
    private Properties connProps;
	
	private SocketServerInstance serverInstance;
    private volatile LogonResult logonResult;
    private ILogon logon;
    private Timer pingTimer;
    private boolean closed;
	private boolean failOver;
    
	public SocketServerConnection(
			SocketServerInstanceFactory connectionFactory, boolean secure,
			ServerDiscovery serverDiscovery, Properties connProps,
			Timer pingTimer) throws CommunicationException, ConnectionException {
		this.connectionFactory = connectionFactory;
		this.serverDiscovery = serverDiscovery;
		this.connProps = connProps;
		this.secure = secure;
		this.logon = this.getService(ILogon.class);
		this.failOver = Boolean.valueOf(connProps.getProperty(TeiidURL.CONNECTION.AUTO_FAILOVER)).booleanValue();
		
        authenticate(); 
        
        this.pingTimer = pingTimer;
        schedulePing();
	}

	private void schedulePing() {
		if (this.pingTimer != null) {
        	this.pingTimer.schedule(new TimerTask() {
        		
        		private ResultsFuture<?> ping;
        		
    			@Override
    			public void run() {
    				if (ping == null) {
    					ping = isOpen();
    				} 
    				if (ping != null) {
    					try {
    						ping.get(1, TimeUnit.SECONDS);
    						ping = null;
							return;
    					} catch (TimeoutException e) {
    						return;
						} catch (Throwable e) {
							handlePingError(e);
						}
    				}
					this.cancel();
    			}
				
        	}, PING_INTERVAL, PING_INTERVAL);
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
			throw new CommunicationException(NetPlugin.Util.getString("SocketServerConnection.closed")); //$NON-NLS-1$ 
		}
		if (this.serverInstance != null && (!failOver || this.serverInstance.isOpen())) {
			return this.serverInstance;
		}
		List<HostInfo> hostKeys = new ArrayList<HostInfo>(this.serverDiscovery.getKnownHosts(logonResult, this.serverInstance));
		closeServerInstance();
		List<HostInfo> hostCopy = new ArrayList<HostInfo>(hostKeys);
		int knownHosts = hostKeys.size();
		while (hostKeys.size() > 0) {
			HostInfo hostInfo = hostKeys.remove((int) (Math.random() * hostKeys.size()));

			Exception ex = null;
			try {
				SocketServerInstance instance = connectionFactory.getServerInstance(hostInfo, secure);
				this.serverInstance = instance;
				if (this.logonResult != null) {
					ILogon newLogon = instance.getService(ILogon.class);
					newLogon.assertIdentity(logonResult.getSessionToken());
				}
				this.serverDiscovery.connectionSuccessful(hostInfo);
				return this.serverInstance;
			} catch (IOException e) {
				ex = e;
			} catch (InvalidSessionException e) {
				shutdown(false);
				throw new CommunicationException(e,NetPlugin.Util.getString("SocketServerInstance.Connection_Error.Connect_Failed", hostInfo.getHostName(), String.valueOf(hostInfo.getPortNumber()), e.getMessage())); //$NON-NLS-1$
			} catch (SingleInstanceCommunicationException e) { 
				ex = e;
			} catch (TeiidComponentException e) {
				ex = e;
			} 	
			this.serverDiscovery.markInstanceAsBad(hostInfo);
			if (knownHosts == 1) { //just a single host, use the exception
				if (ex instanceof UnknownHostException) {
					throw new SingleInstanceCommunicationException(ex, NetPlugin.Util.getString("SocketServerInstance.Connection_Error.Unknown_Host", hostInfo.getHostName())); //$NON-NLS-1$
				}
				throw new SingleInstanceCommunicationException(ex,NetPlugin.Util.getString("SocketServerInstance.Connection_Error.Connect_Failed", hostInfo.getHostName(), String.valueOf(hostInfo.getPortNumber()), ex.getMessage())); //$NON-NLS-1$
			}
			log.log(Level.FINE, "Unable to connect to host", ex); //$NON-NLS-1$
		}
		throw new CommunicationException(NetPlugin.Util.getString("SocketServerInstancePool.No_valid_host_available", hostCopy.toString())); //$NON-NLS-1$
	}
	
	public synchronized void authenticate() throws ConnectionException, CommunicationException {
		this.logonResult = null;
        // Log on to server
        try {
            this.logonResult = logon.logon(connProps);
            List<HostInfo> knownHosts = this.serverDiscovery.getKnownHosts(logonResult, this.serverInstance);
            if (knownHosts.size() > 1 && !new HashSet<HostInfo>(knownHosts).equals(new HashSet<HostInfo>(this.serverDiscovery.getKnownHosts(logonResult, null)))) {
            	//if there are multiple instances, allow for load-balancing
            	closeServerInstance();
            }
            return;
        } catch (LogonException e) {
            // Propagate the original message as it contains the message we want
            // to give to the user
            throw new ConnectionException(e, e.getMessage());
        } catch (TeiidComponentException e) {
        	if (e.getCause() instanceof CommunicationException) {
        		throw (CommunicationException)e.getCause();
        	}
            throw new CommunicationException(e, NetPlugin.Util.getString("PlatformServerConnectionFactory.Unable_to_find_a_component_used_in_logging_on_to")); //$NON-NLS-1$
        } 	
	}
	
	class ServerConnectionInvocationHandler implements InvocationHandler {
		
		private Class<?> targetClass;
		private Object target;
		
		public ServerConnectionInvocationHandler(Class<?> targetClass) {
			this.targetClass = targetClass;
		}
		
		private synchronized Object getTarget() throws CommunicationException {
			if (this.target == null) {
				this.target = selectServerInstance().getService(targetClass);
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
	            if (!failOver || ExceptionUtil.getExceptionOfType(exception, SingleInstanceCommunicationException.class) == null) {
	            	break;
	            }
            	invalidateTarget();
	            //TODO: look for invalid session exception
			}
	        throw ExceptionUtil.convertException(method, exception);
		}
		
		private synchronized void invalidateTarget() {
			this.target = null;
		}
	    
	}

	public <T> T getService(Class<T> iface) {
		return iface.cast(Proxy.newProxyInstance(this.getClass().getClassLoader(), new Class[] {iface}, new ServerConnectionInvocationHandler(iface)));
	}
	public synchronized void close() {
		shutdown(true);
	}
	private synchronized void shutdown(boolean logoff) {
		if (this.closed) {
			return;
		}
		
		if (logoff) {
			try {
				//make a best effort to send the logoff
				Future<?> writeFuture = this.logon.logoff();
				writeFuture.get(5000, TimeUnit.MILLISECONDS);
			} catch (InvalidSessionException e) {
				//ignore
			} catch (InterruptedException e) {
				//ignore
			} catch (ExecutionException e) {
				//ignore
			} catch (TimeoutException e) {
				//ignore
			} catch (TeiidComponentException e) {
				//ignore
			}
		}
		
		closeServerInstance();

		this.closed = true;
		this.serverDiscovery.shutdown();
	}
	
	public synchronized ResultsFuture<?> isOpen() {
		if (this.closed) {
			return null;
		}
		try {
			if (!selectServerInstance().isOpen()) {
				return null;
			}
		} catch (CommunicationException e) {
			return null;
		}
		try {
			return logon.ping();
		} catch (Throwable th) {
			return null;
		} 
	}

	private void handlePingError(Throwable th) {
		if (ExceptionUtil.getExceptionOfType(th, InvalidSessionException.class) != null) {
			shutdown(false);
		} else {
			close();
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
		SocketAddress address = selectServerInstance().getRemoteAddress();
		if (address == null) {
			return false;
		}
		return address.equals(((SocketServerConnection)otherService).selectServerInstance().getRemoteAddress());
	}
	
	public void selectNewServerInstance(Object service) {
		closeServerInstance();
		ServerConnectionInvocationHandler handler = (ServerConnectionInvocationHandler)Proxy.getInvocationHandler(service);
		handler.invalidateTarget();
	}
	
	public void setFailOver(boolean failOver) {
		this.failOver = failOver;
	}
}