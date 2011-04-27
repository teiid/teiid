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
package org.teiid.resource.adapter.salesforce;

import java.net.MalformedURLException;
import java.net.URL;

import javax.resource.ResourceException;
import javax.xml.namespace.QName;

import org.apache.cxf.Bus;
import org.apache.cxf.bus.spring.SpringBusFactory;
import org.apache.cxf.configuration.Configurer;
import org.apache.cxf.jaxws.JaxWsClientFactoryBean;
import org.teiid.core.TeiidRuntimeException;
import org.teiid.resource.spi.BasicConnectionFactory;
import org.teiid.resource.spi.BasicManagedConnectionFactory;

import com.sforce.soap.partner.SforceService;


public class SalesForceManagedConnectionFactory extends BasicManagedConnectionFactory {
	private static final long serialVersionUID = 5298591275313314698L;
	
	private String username;
	private String password;
	private URL URL; //sf url
	private String configFile; // path to the "jbossws-cxf.xml" file

	//cxf bus
	private Bus bus;
	
	public String getUsername() {
		return username;
	}
	public void setUsername(String username) {
		if (username.trim().length() == 0) {
			throw new TeiidRuntimeException("Name can not be null"); //$NON-NLS-1$
		}
		this.username = username;
	}

	public String getPassword() {
		return this.password;
	}
	public void setPassword(String password) {
		this.password = password;
	}
	public URL getURL() {
		return this.URL;
	}
	
	public void setURL(String uRL) {
		try {
			this.URL = new URL(uRL);
		} catch (MalformedURLException e) {
			throw new TeiidRuntimeException("URL Supplied is not valid URL"+ e.getMessage());//$NON-NLS-1$
		}
	}
	
	public String getConfigFile() {
		return configFile;
	}

	public void setConfigFile(String config) {
		this.configFile = config;
	}
	
	@Override
	public BasicConnectionFactory createConnectionFactory() throws ResourceException {
		QName portQName = SforceService.SERVICE;
		if (this.configFile != null) {
			this.bus = new SpringBusFactory().createBus(this.configFile);
			JaxWsClientFactoryBean instance = new JaxWsClientFactoryBean();
			Configurer configurer = this.bus.getExtension(Configurer.class);
	        if (null != configurer) {
	            configurer.configureBean(portQName.toString() + ".jaxws-client.proxyFactory", instance); //$NON-NLS-1$
	        }
		}
		
		return new BasicConnectionFactory() {
			private static final long serialVersionUID = 5028356110047329135L;

			@Override
			public SalesforceConnectionImpl getConnection() throws ResourceException {
				return new SalesforceConnectionImpl(getUsername(), getPassword(), getURL(), SalesForceManagedConnectionFactory.this);
			}
		};
	}
	
	public Bus getBus() {
		return bus;
	}
}
