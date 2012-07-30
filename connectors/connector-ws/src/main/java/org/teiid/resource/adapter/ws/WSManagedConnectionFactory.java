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
package org.teiid.resource.adapter.ws;

import java.util.List;

import javax.resource.ResourceException;
import javax.xml.namespace.QName;

import org.apache.cxf.Bus;
import org.apache.cxf.bus.spring.SpringBusFactory;
import org.apache.cxf.configuration.Configurer;
import org.apache.cxf.interceptor.Interceptor;
import org.apache.cxf.jaxws.JaxWsClientFactoryBean;
import org.teiid.core.BundleUtil;
import org.teiid.resource.spi.BasicConnection;
import org.teiid.resource.spi.BasicConnectionFactory;
import org.teiid.resource.spi.BasicManagedConnectionFactory;

public class WSManagedConnectionFactory extends BasicManagedConnectionFactory {

	private static final long serialVersionUID = -2998163922934555003L;
	
	public static final BundleUtil UTIL = BundleUtil.getBundleUtil(WSManagedConnectionFactory.class);

	public enum SecurityType {None,HTTPBasic,WSSecurity}
	
	private String endPoint;
	private String securityType = SecurityType.None.name(); // None, HTTPBasic, WS-Security
	private String configFile; // path to the "jbossws-cxf.xml" file
	private String configName; // config name in the above file
	private String authPassword; // httpbasic - password
	private String authUserName; // httpbasic - username

	private Bus bus;
	private QName portQName;
	private List<? extends Interceptor> outInterceptors;
	private Long requestTimeout = -1L;

	@SuppressWarnings("serial")
	@Override
	public BasicConnectionFactory createConnectionFactory() throws ResourceException {
		String configName = getConfigName();
		if (configName == null) {
			configName = WSConnectionImpl.DEFAULT_LOCAL_NAME; 
		}
		this.portQName = new QName(WSConnectionImpl.DEFAULT_NAMESPACE_URI, configName);
		if (configFile != null) {
			bus = new SpringBusFactory().createBus(configFile);
			JaxWsClientFactoryBean instance = new JaxWsClientFactoryBean();
			Configurer configurer = bus.getExtension(Configurer.class);
	        if (null != configurer) {
	            configurer.configureBean(portQName.toString() + ".jaxws-client.proxyFactory", instance); //$NON-NLS-1$
	        }
	        outInterceptors = instance.getOutInterceptors();
		}
		return new BasicConnectionFactory() {
			@Override
			public BasicConnection getConnection() throws ResourceException {
				return new WSConnectionImpl(WSManagedConnectionFactory.this);
			}
		};
	}
	
	public String getAuthPassword() {
		return this.authPassword;
	}

	public void setAuthPassword(String authPassword) {
		this.authPassword = authPassword;
	}

	public String getAuthUserName() {
		return this.authUserName;
	}

	public void setAuthUserName(String authUserName) {
		this.authUserName = authUserName;
	}

	public String getEndPoint() {
		return this.endPoint;
	}

	public void setEndPoint(String endPoint) {
		this.endPoint = endPoint;
	}
	
	public Long getRequestTimeout() {
		return this.requestTimeout;
	}

	public void setRequestTimeout(Long timeout) {
		this.requestTimeout = timeout;
	}	
	
	public SecurityType getAsSecurityType() {
		return SecurityType.valueOf(this.securityType);
	}

	public String getSecurityType() {
		return this.securityType;
	}

	public void setSecurityType(String securityType) {
		this.securityType = securityType;
	}	

	public String getConfigFile() {
		return configFile;
	}

	public void setConfigFile(String config) {
		this.configFile = config;
	}
	
	public String getConfigName() {
		return configName;
	}

	public void setConfigName(String configName) {
		this.configName = configName;
	}
	
	public Bus getBus() {
		return bus;
	}
	
	public QName getPortQName() {
		return portQName;
	}
	
	public List<? extends Interceptor> getOutInterceptors() {
		return outInterceptors;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((authPassword == null) ? 0 : authPassword.hashCode());
		result = prime * result + ((authUserName == null) ? 0 : authUserName.hashCode());
		result = prime * result + ((configFile == null) ? 0 : configFile.hashCode());
		result = prime * result + ((configName == null) ? 0 : configName.hashCode());
		result = prime * result + ((endPoint == null) ? 0 : endPoint.hashCode());
		result = prime * result + ((securityType == null) ? 0 : securityType.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj) return true;
		if (obj == null) return false;
		if (getClass() != obj.getClass()) return false;
		WSManagedConnectionFactory other = (WSManagedConnectionFactory) obj;
		if (!checkEquals(this.authPassword, other.authPassword)) {
			return false;
		}
		if (!checkEquals(this.authUserName, other.authUserName)) {
			return false;
		}
		if (!checkEquals(this.configFile, other.configFile)) {
			return false;
		}
		if (!checkEquals(this.configName, other.configName)) {
			return false;
		}
		if (!checkEquals(this.endPoint, other.endPoint)) {
			return false;
		}
		if (!checkEquals(this.securityType, other.securityType)) {
			return false;
		}
		return true;
	}
	
}
