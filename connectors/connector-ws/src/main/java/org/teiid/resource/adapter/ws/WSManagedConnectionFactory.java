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

import javax.resource.ResourceException;
import javax.resource.cci.ConnectionSpec;

import org.teiid.resource.spi.BasicConnection;
import org.teiid.resource.spi.BasicConnectionFactory;
import org.teiid.resource.spi.BasicManagedConnectionFactory;

public class WSManagedConnectionFactory extends BasicManagedConnectionFactory {

	private static final long serialVersionUID = -2998163922934555003L;

	enum InvocationType {HTTP_GET, HTTP_POST, SOAP};
	enum SecurityType {None,HTTPBasic,WSSecurity}
	enum ParameterType{None,Name_Value,XMLRequest,XMLInQueryString};
	
	private String invocationType;
	private String endPoint;
	
	private String securityType; // None, HTTPBasic, WS-Security
	private String wsSecurityConfigURL; // path to the "jboss-wsse-client.xml" file
	private String wsSecurityConfigName; // ws-security config name in the above file
	private String authPassword; // httpbasic - password
	private String authUserName; // httpbasic - username
	private String parameterMethod;
	private String xMLParamName;
	

	@Override
	public Object createConnectionFactory() throws ResourceException {
		return new BasicConnectionFactory() {
			@Override
			public BasicConnection getConnection() throws ResourceException {
				return new WSConnection(WSManagedConnectionFactory.this);
			}
		};
	}
	
	
	public InvocationType getInvocationType() {
		return InvocationType.valueOf(invocationType);
	}


	public void setInvocationType(String invocationType) {
		this.invocationType = invocationType;
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
	
	public SecurityType getSecurityType() {
		return SecurityType.valueOf(this.securityType);
	}

	public void setSecurityType(String securityType) {
		this.securityType = securityType;
	}	

	public String getWsSecurityConfigURL() {
		return wsSecurityConfigURL;
	}

	public void setWsSecurityConfigURL(String wsSecurityConfigURL) {
		this.wsSecurityConfigURL = wsSecurityConfigURL;
	}
	
	public String getWsSecurityConfigName() {
		return wsSecurityConfigName;
	}

	public void setWsSecurityConfigName(String wsSecurityConfigName) {
		this.wsSecurityConfigName = wsSecurityConfigName;
	}
	
	public ParameterType getParameterMethod() {
		return ParameterType.valueOf(parameterMethod);
	}

	public void setParameterMethod(String parameterMethod) {
		this.parameterMethod = parameterMethod;
	}
	
	public String getXMLParamName() {
		return xMLParamName;
	}

	public void setXMLParamName(String xMLParamName) {
		this.xMLParamName = xMLParamName;
	}
}
