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

package com.metamatrix.common.comm.platform.socket.client;

import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Timer;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.metamatrix.common.api.HostInfo;
import com.metamatrix.common.api.MMURL;
import com.metamatrix.common.comm.api.ServerConnectionFactory;
import com.metamatrix.common.comm.exception.CommunicationException;
import com.metamatrix.common.comm.exception.ConnectionException;
import com.metamatrix.common.util.NetUtils;
import com.metamatrix.common.util.PropertiesUtils;
import com.metamatrix.core.MetaMatrixCoreException;
import com.metamatrix.core.util.ReflectionHelper;
import com.metamatrix.platform.security.api.ILogon;

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

		//## JDBC4.0-begin ##
		@Override
		//## JDBC4.0-end ##
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
		boolean ssl;
		SocketServerInstance actual;
		SocketServerInstance proxy;
		
		public CachedInstance(HostInfo info, boolean ssl) {
			this.info = info;
			this.ssl = ssl;
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
			if (!info.equals(other.info) || ssl != other.ssl) {
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
	
	//instance pooling
	private AtomicInteger instanceCount = new AtomicInteger();
	private Map<CachedInstance, CachedInstance> instancePool = new LinkedHashMap<CachedInstance, CachedInstance>();

	//config properties
	private long synchronousTtl = 120000l;
	private int maxCachedInstances=16;

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
			INSTANCE.init(props);
		}
		return INSTANCE;
	}
	
	public SocketServerConnectionFactory() {
		
	}
		
	public void init(final Properties props) {
		PropertiesUtils.setBeanProperties(this, props, "org.teiid.sockets"); //$NON-NLS-1$
		this.pingTimer = new Timer("SocketPing", true); //$NON-NLS-1$
		this.channelFactory = new OioOjbectChannelFactory(props);
	}
			
	public SocketServerInstance getServerInstance(HostInfo info, boolean ssl) throws CommunicationException, IOException {
		CachedInstance key = null;
		CachedInstance instance = null;
		boolean useCache = this.maxCachedInstances > 0; 
		if (useCache) {
			key = new CachedInstance(info, ssl);
			synchronized (instancePool) {
				instance = instancePool.remove(key);
			}
			if (instance != null) {
				ILogon logon = instance.actual.getService(ILogon.class);
				boolean valid = false;
				try {
					Future<?> success = logon.ping();
					success.get(this.channelFactory.getSoTimeout(), TimeUnit.MICROSECONDS);
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
		SocketServerInstanceImpl ssii = new SocketServerInstanceImpl(info, ssl, getSynchronousTtl());
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
	public SocketServerConnection createConnection(Properties connectionProperties) throws CommunicationException, ConnectionException {
		
		updateConnectionProperties(connectionProperties);
		
		MMURL url = new MMURL(connectionProperties.getProperty(MMURL.CONNECTION.SERVER_URL));
		
		String discoveryStrategyName = connectionProperties.getProperty(MMURL.CONNECTION.DISCOVERY_STRATEGY, AdminApiServerDiscovery.class.getName());

		ServerDiscovery discovery;

		if (URL.equalsIgnoreCase(discoveryStrategyName)) {
			discovery = new UrlServerDiscovery();
		} else {
			try {
				discovery = (ServerDiscovery)ReflectionHelper.create(discoveryStrategyName, null, this.getClass().getClassLoader());
			} catch (MetaMatrixCoreException e) {
				throw new ConnectionException(e);
			}
		}
		
		discovery.init(url, connectionProperties);
		
		return new SocketServerConnection(this, url.isUsingSSL(), discovery, connectionProperties, pingTimer);
	}

	static void updateConnectionProperties(Properties connectionProperties) {
		try {
			InetAddress addr = NetUtils.getInstance().getInetAddress();
			connectionProperties.put(MMURL.CONNECTION.CLIENT_IP_ADDRESS, addr.getHostAddress());
			connectionProperties.put(MMURL.CONNECTION.CLIENT_HOSTNAME, addr.getCanonicalHostName());
        } catch (UnknownHostException err1) {
        	connectionProperties.put(MMURL.CONNECTION.CLIENT_IP_ADDRESS, "UnknownClientAddress"); //$NON-NLS-1$
        }
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
	public void shutdown(boolean restart) {
		// only applies in the Embedded scenario.
	}

}
