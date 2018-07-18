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
import java.net.MalformedURLException;
import java.util.Properties;
import java.util.logging.Logger;

import org.teiid.core.util.PropertiesUtils;
import org.teiid.jdbc.JDBCPlugin;
import org.teiid.net.CommunicationException;
import org.teiid.net.ConnectionException;
import org.teiid.net.HostInfo;
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

	private static final String V_10_2 = "10.02"; //$NON-NLS-1$

	private static SocketServerConnectionFactory INSTANCE;
	private static Logger log = Logger.getLogger("org.teiid.client.sockets"); //$NON-NLS-1$

    private ObjectChannelFactory channelFactory;
	
	//config properties
	private long synchronousTtl = 240000l;

	public static synchronized SocketServerConnectionFactory getInstance() {
		if (INSTANCE == null) {
			INSTANCE = new SocketServerConnectionFactory();
			Properties props = PropertiesUtils.getCombinedProperties();
			InputStream is = SocketServerConnectionFactory.class.getResourceAsStream("/teiid-client-settings.properties"); //$NON-NLS-1$
			if (is != null) {
				Properties newProps = new Properties();
				try {
				    newProps.load(is);
				} catch (IOException e) {
					
				} finally {
					try {
						is.close();
					} catch (IOException e) {
					}
				}
				props.putAll(newProps);
			}
			INSTANCE.initialize(props);
		}
		return INSTANCE;
	}
	
	public SocketServerConnectionFactory() {
		
	}
	
	public void initialize(Properties info) {
		PropertiesUtils.setBeanProperties(this, info, "org.teiid.sockets"); //$NON-NLS-1$
		this.channelFactory = new OioOjbectChannelFactory(info);
	}
	
	@Override
	public SocketServerInstance getServerInstance(HostInfo info) throws CommunicationException, IOException {
		SocketServerInstanceImpl ssii = new SocketServerInstanceImpl(info, getSynchronousTtl(), this.channelFactory.getSoTimeout());
		ssii.connect(this.channelFactory);
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
		
		UrlServerDiscovery discovery = new UrlServerDiscovery();
		
		discovery.init(url, connectionProperties);
		
		return new SocketServerConnection(this, url.isUsingSSL(), discovery, connectionProperties);
	}

	public long getSynchronousTtl() {
		return synchronousTtl;
	}

	public void setSynchronousTtl(long synchronousTTL) {
		this.synchronousTtl = synchronousTTL;
	}

}
