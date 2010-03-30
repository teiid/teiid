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
package org.teiid.connector.xml.soap;

import com.metamatrix.connector.xml.base.XMLBaseManagedConnectionFactory;

public class SoapManagedConnectionFactory extends XMLBaseManagedConnectionFactory implements SecurityManagedConnectionFactory {
	private static final long serialVersionUID = 6175706287620555719L;

	private String authPassword;

	public String getAuthPassword() {
		return this.authPassword;
	}

	public void setAuthPassword(String authPassword) {
		this.authPassword = authPassword;
	}

	private String sAMLPropertyFile;

	public String getSAMLPropertyFile() {
		return this.sAMLPropertyFile;
	}

	public void setSAMLPropertyFile(String sAMLPropertyFile) {
		this.sAMLPropertyFile = sAMLPropertyFile;
	}

	private String wsdl;

	public String getWsdl() {
		return this.wsdl;
	}

	public void setWsdl(String wsdl) {
		this.wsdl = wsdl;
	}

	private String authUserName;

	public String getAuthUserName() {
		return this.authUserName;
	}

	public void setAuthUserName(String authUserName) {
		this.authUserName = authUserName;
	}

	private String wSSecurityType;

	public String getWSSecurityType() {
		return this.wSSecurityType;
	}

	public void setWSSecurityType(String wSSecurityType) {
		this.wSSecurityType = wSSecurityType;
	}

	private String encryptUserName;

	public String getEncryptUserName() {
		return this.encryptUserName;
	}

	public void setEncryptUserName(String encryptUserName) {
		this.encryptUserName = encryptUserName;
	}

	private String endPoint;

	public String getEndPoint() {
		return this.endPoint;
	}

	public void setEndPoint(String endPoint) {
		this.endPoint = endPoint;
	}

	private String securityType;

	public String getSecurityType() {
		return this.securityType;
	}

	public void setSecurityType(String securityType) {
		this.securityType = securityType;
	}

	private String cryptoPropertyFile;

	public String getCryptoPropertyFile() {
		return this.cryptoPropertyFile;
	}

	public void setCryptoPropertyFile(String cryptoPropertyFile) {
		this.cryptoPropertyFile = cryptoPropertyFile;
	}

	private String encryptPropertyFile;

	public String getEncryptPropertyFile() {
		return this.encryptPropertyFile;
	}

	public void setEncryptPropertyFile(String encryptPropertyFile) {
		this.encryptPropertyFile = encryptPropertyFile;
	}

	private String trustType;

	public String getTrustType() {
		return this.trustType;
	}

	public void setTrustType(String trustType) {
		this.trustType = trustType;
	}
	
	private String portName;

	public String getPortName() {
		return portName;
	}

	public void setPortName(String portName) {
		this.portName = portName;
	} 

	private String serviceName;
	
	public String getServiceName() {
		return serviceName;
	}

	public void setServiceName(String serviceName) {
		this.serviceName = serviceName;
	}

	private int queryTimeout;

	public int getQueryTimeout() {
		return queryTimeout;
	}

	public void setQueryTimeout(Integer queryTimeout) {
		this.queryTimeout = queryTimeout;
	}
	
	// SOAP - Relational Stuff
	
//	private String xMLParmName;
//
//	public String getXMLParmName() {
//		return this.xMLParmName;
//	}
//
//	public void setXMLParmName(String xMLParmName) {
//		this.xMLParmName = xMLParmName;
//	}
//
//	private boolean exceptionOnSOAPFault;
//
//	public boolean getExceptionOnSOAPFault() {
//		return this.exceptionOnSOAPFault;
//	}
//
//	public void setExceptionOnSOAPFault(Boolean exceptionOnSOAPFault) {
//		this.exceptionOnSOAPFault = exceptionOnSOAPFault;
//	}
//
//	private String sOAPAction;
//
//	public String getSOAPAction() {
//		return this.sOAPAction;
//	}
//
//	public void setSOAPAction(String sOAPAction) {
//		this.sOAPAction = sOAPAction;
//	}
//
//	private String accessMethod;
//
//	public String getAccessMethod() {
//		return this.accessMethod;
//	}
//
//	public void setAccessMethod(String accessMethod) {
//		this.accessMethod = accessMethod;
//	}
//
//	private String proxyUri;
//
//	public String getProxyUri() {
//		return this.proxyUri;
//	}
//
//	public void setProxyUri(String proxyUri) {
//		this.proxyUri = proxyUri;
//	}
//
//	private String encodingStyle;
//
//	public String getEncodingStyle() {
//		return this.encodingStyle;
//	}
//
//	public void setEncodingStyle(String encodingStyle) {
//		this.encodingStyle = encodingStyle;
//	}
//
	private String uri;

	public String getUri() {
		return this.uri;
	}

	public void setUri(String uri) {
		this.uri = uri;
	}

	private String parameterMethod;

	public String getParameterMethod() {
		return this.parameterMethod;
	}

	public void setParameterMethod(String parameterMethod) {
		this.parameterMethod = parameterMethod;
	}
//
//	private String hostnameVerifier;
//
//	public String getHostnameVerifier() {
//		return this.hostnameVerifier;
//	}
//
//	public void setHostnameVerifier(String hostnameVerifier) {
//		this.hostnameVerifier = hostnameVerifier;
//	}
	
	
}
