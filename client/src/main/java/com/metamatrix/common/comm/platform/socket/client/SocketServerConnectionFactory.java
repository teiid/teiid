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
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.security.NoSuchAlgorithmException;
import java.util.Properties;
import java.util.Timer;

import javax.net.ssl.SSLEngine;

import com.metamatrix.common.api.HostInfo;
import com.metamatrix.common.api.MMURL;
import com.metamatrix.common.comm.api.ServerConnectionFactory;
import com.metamatrix.common.comm.exception.CommunicationException;
import com.metamatrix.common.comm.exception.ConnectionException;
import com.metamatrix.common.comm.platform.socket.Handshake;
import com.metamatrix.common.comm.platform.socket.PrintStreamSocketLog;
import com.metamatrix.common.comm.platform.socket.SocketLog;
import com.metamatrix.common.comm.platform.socket.SocketUtil;
import com.metamatrix.common.comm.platform.socket.SocketUtil.SSLEngineFactory;
import com.metamatrix.common.util.NetUtils;
import com.metamatrix.common.util.PropertiesUtils;
import com.metamatrix.core.MetaMatrixCoreException;
import com.metamatrix.core.util.ReflectionHelper;

public class SocketServerConnectionFactory implements ServerConnectionFactory, SocketServerInstanceFactory {

	/**Java system property.  The loglevel that SocketLog will use.  Should be a String value {NONE|CRITICAL|ERROR|WARNING|INFO|DETAIL|TRACE}*/
	public static final String SOCKET_LOG_LEVEL   = "metamatrix.sockets.log.level";  //$NON-NLS-1$
	/**Java system property.  Maximum number of threads used to read sockets*/
	public static final String SOCKET_MAX_THREADS   = "metamatrix.sockets.max.threads"; //$NON-NLS-1$
	/**Java system property.  Maximum time to live for a socket reader thread asynchronous calls.  If it times out, it will be removed, and recreated later when needed.*/
	public static final String SOCKET_TTL           = "metamatrix.sockets.ttl"; //$NON-NLS-1$
	/**Java system property.  Maximum time to live for a socket reader thread synchronous calls.  If it times out, it will be removed, and recreated later when needed.*/    
	public static final String SYNCH_SOCKET_TTL           = "metamatrix.synchronous.sockets.ttl"; //$NON-NLS-1$
	/**Java system property.  Input buffer size of the physical sockets.*/
	public static final String SOCKET_INPUT_BUFFER_SIZE           = "metamatrix.sockets.inputBufferSize"; //$NON-NLS-1$
	/**Java system property.  Output buffer size of the physical sockets.*/
	public static final String SOCKET_OUTPUT_BUFFER_SIZE           = "metamatrix.sockets.outputBufferSize"; //$NON-NLS-1$
	/**Java system property.  Value of the conserve-bandwidth flag of the physical sockets.*/
	public static final String SOCKET_CONSERVE_BANDWIDTH           = "metamatrix.sockets.conserveBandwidth"; //$NON-NLS-1$
	public static final String DEFAULT_SOCKET_LOG_LEVEL = "ERROR"; //$NON-NLS-1$
	public static final int DEFAULT_MAX_THREADS = 15;
	public static final long DEFAULT_TTL = 120000L;
	public static final long DEFAULT_SYNCH_TTL = 120000L;
	public static final int DEFAULT_SOCKET_INPUT_BUFFER_SIZE = 102400;
	public static final int DEFAULT_SOCKET_OUTPUT_BUFFER_SIZE = 102400;
	
	private static final String URL = "URL"; //$NON-NLS-1$
	
	private static SocketServerConnectionFactory INSTANCE;
	
    private SocketLog log; 
    private ObjectChannelFactory channelFactory;
	private Timer pingTimer;
	private Properties props;
	private SSLEngineFactory sslEngineFactory;
	
	public static synchronized SocketServerConnectionFactory getInstance() {
		if (INSTANCE == null) {
			INSTANCE = new SocketServerConnectionFactory();
			Properties props = System.getProperties();
			InputStream is = SocketServerConnectionFactory.class.getResourceAsStream("/federate-settings.properties"); //$NON-NLS-1$
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
			INSTANCE.init(props, true);
		}
		return INSTANCE;
	}
	
	public SocketServerConnectionFactory() {
		
	}
	
	public void init(Properties props, boolean usePing) {
		this.props = props;
		this.log = getLog(SocketServerConnectionFactory.class.getSimpleName());
		this.pingTimer = new Timer("SocketPing", true); //$NON-NLS-1$
		this.channelFactory = new NioObjectChannelFactory(
				getConserveBandwidth(), getInputBufferSize(),
				getOutputBufferSize(), Thread.currentThread()
						.getContextClassLoader(), getMaxThreads());
	}
			
	public SocketServerInstance createServerInstance(HostInfo info, boolean ssl) throws CommunicationException, IOException {
		SSLEngine sslEngine = null;
		if (ssl) {
			synchronized (this) {
				if (this.sslEngineFactory == null) {
					try {
						this.sslEngineFactory = SocketUtil.getSSLEngineFactory(this.props);
					} catch (NoSuchAlgorithmException e) {
						throw new CommunicationException(e);
					} catch (IOException e) {
						throw new CommunicationException(e);
					}
				}
				sslEngine = this.sslEngineFactory.getSSLEngine();
			}
		}
		SocketServerInstanceImpl ssii = new SocketServerInstanceImpl(info, sslEngine, this.log, getSynchronousTTL());
		ssii.connect(this.channelFactory, Handshake.HANDSHAKE_TIMEOUT);
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
				discovery = (ServerDiscovery)ReflectionHelper.create(discoveryStrategyName, null, Thread.currentThread().getContextClassLoader());
			} catch (MetaMatrixCoreException e) {
				throw new ConnectionException(e);
			}
		}
		
		discovery.init(url, connectionProperties);
		
		return new SocketServerConnection(this, url.isUsingSSL(), discovery, connectionProperties, pingTimer, this.log);
	}

	/*
	 * Retrieve the asynchronous call Time-To-Live
	 *  
	 * @return number of ms 
	 * @since 4.2
	 */
	public long getTTL() {
	    return PropertiesUtils.getLongProperty(props, SOCKET_TTL, DEFAULT_TTL);
	}

	/*
	 * Retrieve the synchronous call Time-To-Live
	 *  
	 * @return number of ms 
	 * @since 4.2
	 */
	public long getSynchronousTTL() {
	    return PropertiesUtils.getLongProperty(props, SYNCH_SOCKET_TTL, DEFAULT_SYNCH_TTL);
	}

	/* 
	 * Get the max number of threads
	 * 
	 * @return max number
	 * @since 4.2
	 */
	public int getMaxThreads() {
	    return PropertiesUtils.getIntProperty(props, SOCKET_MAX_THREADS, DEFAULT_MAX_THREADS);
	}

	public SocketLog getLog(String contextName) {
	    SocketLog result = new PrintStreamSocketLog(System.out, contextName, getLogLevel());
	    return result;
	}

	/**
	 * Get the logLevel that SocketLog will use. 
	 * @return
	 * @since 4.3
	 */
	public int getLogLevel() {
	    String logLevelString = props.getProperty(SOCKET_LOG_LEVEL, DEFAULT_SOCKET_LOG_LEVEL);
	    return PrintStreamSocketLog.getLogLevelInt(logLevelString);
	}

	public int getInputBufferSize() {
	    return PropertiesUtils.getIntProperty(props, SOCKET_INPUT_BUFFER_SIZE, DEFAULT_SOCKET_INPUT_BUFFER_SIZE);
	}

	public int getOutputBufferSize() {
	    return PropertiesUtils.getIntProperty(props, SOCKET_OUTPUT_BUFFER_SIZE, DEFAULT_SOCKET_OUTPUT_BUFFER_SIZE);
	}

	public boolean getConserveBandwidth() {
	    return PropertiesUtils.getBooleanProperty(props, SOCKET_CONSERVE_BANDWIDTH, false); 
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

}
