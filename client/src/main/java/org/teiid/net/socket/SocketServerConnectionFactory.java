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
import java.io.InputStream;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.net.MalformedURLException;
import java.util.*;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.teiid.client.security.ILogon;
import org.teiid.client.security.InvalidSessionException;
import org.teiid.client.security.SessionToken;
import org.teiid.core.TeiidException;
import org.teiid.core.util.PropertiesUtils;
import org.teiid.core.util.ReflectionHelper;
import org.teiid.jdbc.JDBCPlugin;
import org.teiid.net.CommunicationException;
import org.teiid.net.ConnectionException;
import org.teiid.net.HostInfo;
import org.teiid.net.ServerConnection;
import org.teiid.net.ServerConnectionFactory;
import org.teiid.net.TeiidURL;


/**
 * Responsible for creating socket based connections
 * 
 * The comm approach is object based and layered.  Connections manage failover and identity.  
 * ServerInstances represent the service layer to a particular cluster member.  ObjectChannels
 * abstract the underlying IO.
 * 
 */
public class SocketServerConnectionFactory implements ServerConnectionFactory, SocketServerInstanceFactory {

	private static final String URL = "URL"; //$NON-NLS-1$
	
	private static SocketServerConnectionFactory INSTANCE;
	private static Logger log = Logger.getLogger("org.teiid.client.sockets"); //$NON-NLS-1$

	private final class ShutdownHandler implements InvocationHandler {
		private final CachedInstance key;

		private ShutdownHandler(CachedInstance key) {
			this.key = key;
		}

		@Override
		public Object invoke(Object arg0, Method arg1, Object[] arg2)
				throws Throwable {
			if (arg1.getName().equals("shutdown")) { //$NON-NLS-1$
				CachedInstance purge = null;
				if (!key.actual.isOpen()) {
					return null; //nothing to do
				}
				synchronized (instancePool) {
					instancePool.put(key, key);
					if (instancePool.size() > maxCachedInstances) {
						Iterator<CachedInstance> iter = instancePool.keySet().iterator();
						purge = iter.next();
						iter.remove();
					}
				}
				if (purge != null) {
					purge.actual.shutdown();
				}
				return null;
			}
			try {
				return arg1.invoke(key.actual, arg2);
			} catch (InvocationTargetException e) {
				throw e.getTargetException();
			}
		}
	}

	private static class CachedInstance {
		HostInfo info;
		Integer instance;
		SocketServerInstance actual;
		SocketServerInstance proxy;
		
		public CachedInstance(HostInfo info) {
			this.info = info;
		}

		@Override
		public int hashCode() {
			return info.hashCode();
		}
		
		@Override
		public boolean equals(Object obj) {
			if (this == obj) {
				return true;
			}
			if (!(obj instanceof CachedInstance)) {
				return false;
			}
			CachedInstance other = (CachedInstance) obj;
			if (!info.equals(other.info)) {
				return false;
			}
			if (instance == null || other.instance == null) {
				return true;
			} 
			return instance.equals(other.instance);
		}
	}
	
    private ObjectChannelFactory channelFactory;
	private Timer pingTimer;
	
	private HashMap<HostInfo, Set<SessionToken>> sessions = new HashMap<HostInfo, Set<SessionToken>>();
	
	//instance pooling
	private AtomicInteger instanceCount = new AtomicInteger();
	private Map<CachedInstance, CachedInstance> instancePool = new LinkedHashMap<CachedInstance, CachedInstance>();

	//config properties
	private long synchronousTtl = 240000l;
	private int maxCachedInstances=16;

	private boolean disablePing;

	public static synchronized SocketServerConnectionFactory getInstance() {
		if (INSTANCE == null) {
			INSTANCE = new SocketServerConnectionFactory();
			Properties props = System.getProperties();
			InputStream is = SocketServerConnectionFactory.class.getResourceAsStream("/teiid-client-settings.properties"); //$NON-NLS-1$
			if (is != null) {
				props = new Properties(props);
				try {
					props.load(is);
				} catch (IOException e) {
					
				} finally {
					try {
						is.close();
					} catch (IOException e) {
					}
				}
			}
			INSTANCE.initialize(props);
		}
		return INSTANCE;
	}
	
	public SocketServerConnectionFactory() {
		
	}
	
	public void setDisablePing(boolean disable) {
		this.disablePing = disable;
	}
	
	public void initialize(Properties info) {
		PropertiesUtils.setBeanProperties(this, info, "org.teiid.sockets"); //$NON-NLS-1$
		this.channelFactory = new OioOjbectChannelFactory(info);

		if (disablePing) {
			return;
		}
		this.pingTimer = new Timer("SocketPing", true); //$NON-NLS-1$
		this.pingTimer.schedule(new TimerTask() {
			
			@Override
			public void run() {
				Set<Map.Entry<HostInfo, Set<SessionToken>>> sessionEntries = null;
				synchronized (sessions) {
					sessionEntries = new HashSet<Map.Entry<HostInfo, Set<SessionToken>>>(sessions.entrySet());
				}
				for (Map.Entry<HostInfo, Set<SessionToken>> entry : sessionEntries) {
					SocketServerInstance instance = null;
					HashSet<SessionToken> entries = null;
					synchronized (sessions) {
						entries = new HashSet<SessionToken>(entry.getValue());
					}
					try {
						instance = getServerInstance(entry.getKey());
						ILogon logon = instance.getService(ILogon.class);
						if ("07.01.01".compareTo(instance.getServerVersion()) > 0) { //$NON-NLS-1$
							for (SessionToken session : entries) {
								try {
									logon.assertIdentity(session);
									logon.ping();
									log.log(Level.FINER, "issueing ping for session:", session); //$NON-NLS-1$
								} catch (InvalidSessionException e) {
								}
							}
						} else {
							ArrayList<String> sessionStrings = new ArrayList<String>(entry.getValue().size());
							for (SessionToken session : entries) {
								sessionStrings.add(session.getSessionID());
							}
							logon.ping(sessionStrings);
							log.log(Level.FINER, "issueing ping for sessions:", sessionStrings); //$NON-NLS-1$
						}
					} catch (Exception e) {
						log.log(Level.WARNING, "Error performing keep-alive ping", e); //$NON-NLS-1$
					} finally {
						if (instance != null) {
							instance.shutdown();
						}
					}
				}
			}
		}, ServerConnection.PING_INTERVAL, ServerConnection.PING_INTERVAL);
	}
	
	@Override
	public SocketServerInstance getServerInstance(HostInfo info) throws CommunicationException, IOException {
		CachedInstance key = null;
		boolean useCache = this.maxCachedInstances > 0; 
		if (useCache) {
			CachedInstance instance = null;
			key = new CachedInstance(info);
			synchronized (instancePool) {
				instance = instancePool.remove(key);
			}
			if (instance != null) {
				ILogon logon = instance.actual.getService(ILogon.class);
				boolean valid = false;
				try {
					Future<?> success = logon.ping();
					success.get(this.channelFactory.getSoTimeout(), TimeUnit.MILLISECONDS);
					valid = true;
				} catch (Exception e) {
					log.log(Level.FINE, "Error performing ping, will select another instance", e); //$NON-NLS-1$
				}
				if (valid) {
					return instance.proxy;
				}
				instance.actual.shutdown();
				//technically we only want to remove instances with the same inetaddress 
				while (true) {
					CachedInstance invalid = null;
					synchronized (instancePool) {
						invalid = instancePool.remove(key);
					}
					if (invalid == null) {
						break;
					}
					invalid.actual.shutdown();
				}
			}
		}
		SocketServerInstanceImpl ssii = new SocketServerInstanceImpl(info, getSynchronousTtl(), this.channelFactory.getSoTimeout());
		ssii.connect(this.channelFactory);
		if (useCache) {
			key.actual = ssii;
			key.instance = instanceCount.getAndIncrement();
			//create a proxied socketserverinstance that will pool itself on shutdown
			key.proxy = (SocketServerInstance)Proxy.newProxyInstance(this.getClass().getClassLoader(), new Class[] {SocketServerInstance.class}, new ShutdownHandler(key));
			return key.proxy;
		}
		return ssii;
	}
	
	/**
	 * @param connectionProperties will be updated with additional information before logon
	 */
	public SocketServerConnection getConnection(Properties connectionProperties) throws CommunicationException, ConnectionException {
		
		TeiidURL url;
		try {
			url = new TeiidURL(connectionProperties.getProperty(TeiidURL.CONNECTION.SERVER_URL));
		} catch (MalformedURLException e1) {
			 throw new ConnectionException(JDBCPlugin.Event.TEIID20014, e1, e1.getMessage());
		}
		
		String discoveryStrategyName = connectionProperties.getProperty(TeiidURL.CONNECTION.DISCOVERY_STRATEGY, URL);

		ServerDiscovery discovery;

		if (URL.equalsIgnoreCase(discoveryStrategyName)) {
			discovery = new UrlServerDiscovery();
		} else {
			try {
				discovery = (ServerDiscovery)ReflectionHelper.create(discoveryStrategyName, null, this.getClass().getClassLoader());
			} catch (TeiidException e) {
				 throw new ConnectionException(e);
			}
		}
		
		discovery.init(url, connectionProperties);
		
		return new SocketServerConnection(this, url.isUsingSSL(), discovery, connectionProperties);
	}

	public long getSynchronousTtl() {
		return synchronousTtl;
	}

	public void setSynchronousTtl(long synchronousTTL) {
		this.synchronousTtl = synchronousTTL;
	}

	public int getMaxCachedInstances() {
		return maxCachedInstances;
	}
	
	public void setMaxCachedInstances(int maxCachedInstances) {
		this.maxCachedInstances = maxCachedInstances;
	}
	
	@Override
	public void connected(SocketServerInstance instance, SessionToken session) {
		synchronized (sessions) {
			Set<SessionToken> instanceSessions = sessions.get(instance.getHostInfo());
			if (instanceSessions == null) {
				instanceSessions = new HashSet<SessionToken>();
				sessions.put(instance.getHostInfo(), instanceSessions);
			}
			instanceSessions.add(session);
		}
	}
	
	@Override
	public void disconnected(SocketServerInstance instance, SessionToken session) {
		synchronized (sessions) {
			Set<SessionToken> instanceSessions = sessions.get(instance.getHostInfo());
			if (instanceSessions != null) {
				instanceSessions.remove(session);
				if (instanceSessions.isEmpty()) {
					sessions.remove(instance.getHostInfo());
				}
			}
		}
	}

}
