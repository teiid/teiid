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
import org.teiid.resource.spi.BasicConnectionFactory;
import org.teiid.resource.spi.BasicManagedConnectionFactory;

public class WSManagedConnectionFactory extends BasicManagedConnectionFactory {

	private static final long serialVersionUID = -2998163922934555003L;
	
	public static final BundleUtil UTIL = BundleUtil.getBundleUtil(WSManagedConnectionFactory.class);

	public enum SecurityType {None,HTTPBasic,WSSecurity}

	//ws properties
	private String endPoint;
	//ws derived state
	private List<? extends Interceptor> outInterceptors;

	//wsdl properties
	private String wsdl;
	private String serviceName = WSManagedConnectionFactory.DEFAULT_LOCAL_NAME;

	//shared properties
	private String securityType = SecurityType.None.name(); // None, HTTPBasic, WS-Security
	private String configFile; // path to the "jbossws-cxf.xml" file
	private String endPointName = WSManagedConnectionFactory.DEFAULT_LOCAL_NAME; 
	private String authPassword; // httpbasic - password
	private String authUserName; // httpbasic - username
	private Long requestTimeout = -1L;
	private String namespaceUri = WSManagedConnectionFactory.DEFAULT_NAMESPACE_URI;
	//shared derived state
	private Bus bus;
	private QName portQName;
	private QName serviceQName;

	static final String DEFAULT_NAMESPACE_URI = "http://teiid.org"; //$NON-NLS-1$ 

	static final String DEFAULT_LOCAL_NAME = "teiid"; //$NON-NLS-1$
	
	@SuppressWarnings("serial")
	@Override
	public BasicConnectionFactory<WSConnectionImpl> createConnectionFactory() throws ResourceException {
		if (endPointName == null) {
			endPointName = WSManagedConnectionFactory.DEFAULT_LOCAL_NAME; 
		}
		if (serviceName == null) {
			serviceName = WSManagedConnectionFactory.DEFAULT_LOCAL_NAME; 
		}
		if (this.namespaceUri == null) {
			this.namespaceUri = WSManagedConnectionFactory.DEFAULT_NAMESPACE_URI;
		}
		this.portQName = new QName(this.namespaceUri, endPointName);
		this.serviceQName = new QName(this.namespaceUri, this.serviceName);
		
		if (configFile != null) {
			bus = new SpringBusFactory().createBus(configFile);
			JaxWsClientFactoryBean instance = new JaxWsClientFactoryBean();
			if (this.wsdl == null) {
				Configurer configurer = bus.getExtension(Configurer.class);
		        if (null != configurer) {
		            configurer.configureBean(portQName.toString() + ".jaxws-client.proxyFactory", instance); //$NON-NLS-1$
		        }
		        outInterceptors = instance.getOutInterceptors();
			}
		}
		return new BasicConnectionFactory<WSConnectionImpl>() {
			@Override
			public WSConnectionImpl getConnection() throws ResourceException {
				return new WSConnectionImpl(WSManagedConnectionFactory.this);
			}
		};
	}
	
	public QName getServiceQName() {
		return serviceQName;
	}
	
	public String getNamespaceUri() {
		return namespaceUri;
	}
	
	public void setNamespaceUri(String namespaceUri) {
		this.namespaceUri = namespaceUri;
	}
	
	public String getServiceName() {
		return serviceName;
	}
	
	public void setServiceName(String serviceName) {
		this.serviceName = serviceName;
	}
	
	public String getEndPointName() {
		return this.endPointName;
	}
	
	public void setEndPointName(String portName) {
		this.endPointName = portName;
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
		return endPointName;
	}

	public void setConfigName(String configName) {
		this.endPointName = configName;
	}
	
	public Bus getBus() {
		return bus;
	}
	
	public QName getPortQName() {
		return portQName;
	}
	
	public String getWsdl() {
		return wsdl;
	}
	
	public void setWsdl(String wsdl) {
		this.wsdl = wsdl;
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
		result = prime * result + ((endPointName == null) ? 0 : endPointName.hashCode());
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
		if (!checkEquals(this.endPointName, other.endPointName)) {
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
