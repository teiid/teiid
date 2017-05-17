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
package org.teiid.resource.adapter.ws;

import java.io.File;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.List;

import javax.resource.ResourceException;
import javax.resource.spi.InvalidPropertyException;
import javax.xml.namespace.QName;

import org.apache.cxf.Bus;
import org.apache.cxf.bus.spring.SpringBusFactory;
import org.apache.cxf.configuration.Configurer;
import org.apache.cxf.interceptor.Interceptor;
import org.apache.cxf.jaxws.JaxWsClientFactoryBean;
import org.teiid.core.BundleUtil;
import org.teiid.core.util.EquivalenceUtil;
import org.teiid.resource.spi.BasicConnectionFactory;
import org.teiid.resource.spi.BasicManagedConnectionFactory;

public class WSManagedConnectionFactory extends BasicManagedConnectionFactory {

	private static final long serialVersionUID = -2998163922934555003L;

	public static final BundleUtil UTIL = BundleUtil.getBundleUtil(WSManagedConnectionFactory.class);

	public enum SecurityType {None,HTTPBasic,Digest,WSSecurity,Kerberos,OAuth}

	//ws properties
	private String endPoint;
	//ws derived state
	private List<? extends Interceptor> outInterceptors;

	//wsdl properties
	private String wsdl;
	private String serviceName = WSManagedConnectionFactory.DEFAULT_LOCAL_NAME;
	private URL wsdlUrl;
	//shared properties
	private String securityType = SecurityType.None.name(); // None, HTTPBasic, WS-Security
	private String configFile; // path to the "jbossws-cxf.xml" file
	private String endPointName = WSManagedConnectionFactory.DEFAULT_LOCAL_NAME;
	private String authPassword; // httpbasic - password
	private String authUserName; // httpbasic - username
	private Long requestTimeout;
	private Long connectTimeout;
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
		if (this.endPointName == null) {
			this.endPointName = WSManagedConnectionFactory.DEFAULT_LOCAL_NAME;
		}
		if (this.serviceName == null) {
			this.serviceName = WSManagedConnectionFactory.DEFAULT_LOCAL_NAME;
		}
		if (this.namespaceUri == null) {
			this.namespaceUri = WSManagedConnectionFactory.DEFAULT_NAMESPACE_URI;
		}
		this.portQName = new QName(this.namespaceUri, this.endPointName);
		this.serviceQName = new QName(this.namespaceUri, this.serviceName);
		if (this.wsdl != null) {
			try {
				this.wsdlUrl = new URL(this.wsdl);
			} catch (MalformedURLException e) {
				File f = new File(this.wsdl);
				try {
					this.wsdlUrl = f.toURI().toURL();
				} catch (MalformedURLException e1) {
					throw new InvalidPropertyException(e1);
				}
			}
		}
		if (this.configFile != null) {
			this.bus = new SpringBusFactory().createBus(this.configFile);
			JaxWsClientFactoryBean instance = new JaxWsClientFactoryBean();
			if (this.wsdl == null) {
				Configurer configurer = this.bus.getExtension(Configurer.class);
		        if (null != configurer) {
		            configurer.configureBean(this.portQName.toString() + ".jaxws-client.proxyFactory", instance); //$NON-NLS-1$
		        }
		        this.outInterceptors = instance.getOutInterceptors();
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
		return this.serviceQName;
	}

	public String getNamespaceUri() {
		return this.namespaceUri;
	}

	public void setNamespaceUri(String namespaceUri) {
		this.namespaceUri = namespaceUri;
	}

	public String getServiceName() {
		return this.serviceName;
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
		return this.configFile;
	}

	public void setConfigFile(String config) {
		this.configFile = config;
	}

	public String getConfigName() {
		return this.endPointName;
	}

	public void setConfigName(String configName) {
		this.endPointName = configName;
	}

	public Bus getBus() {
		return this.bus;
	}

	public QName getPortQName() {
		return this.portQName;
	}

	public String getWsdl() {
		return this.wsdl;
	}

	public URL getWsdlUrl() {
		return this.wsdlUrl;
	}

	public void setWsdl(String wsdl) {
		this.wsdl = wsdl;
	}

	public List<? extends Interceptor> getOutInterceptors() {
		return this.outInterceptors;
	}
	
	public Long getConnectTimeout() {
		return connectTimeout;
	}
	
	public void setConnectTimeout(Long connectTimeout) {
		this.connectTimeout = connectTimeout;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((this.authPassword == null) ? 0 : this.authPassword.hashCode());
		result = prime * result + ((this.authUserName == null) ? 0 : this.authUserName.hashCode());
		result = prime * result + ((this.configFile == null) ? 0 : this.configFile.hashCode());
		result = prime * result + ((this.endPointName == null) ? 0 : this.endPointName.hashCode());
		result = prime * result + ((this.endPoint == null) ? 0 : this.endPoint.hashCode());
		result = prime * result + ((this.securityType == null) ? 0 : this.securityType.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj) {
			return true;
		}
		if (obj == null) {
			return false;
		}
		if (getClass() != obj.getClass()) {
			return false;
		}
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
		return EquivalenceUtil.areEqual(this.requestTimeout, other.requestTimeout)
				&& EquivalenceUtil.areEqual(this.connectTimeout, other.connectTimeout);
	}
}
