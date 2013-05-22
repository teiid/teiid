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

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

import javax.xml.namespace.QName;

import org.apache.cxf.ws.security.trust.STSClient;

public class WSSecurityCredential implements Serializable {
	private static final long serialVersionUID = 7725328159246858176L;
	private boolean useSts;
	private String stsWsdlLocation;
	private QName stsService;
	private QName stsPort;

	public enum SecurityHandler {WSPOLICY, WSS4J};

	private HashMap<String, Object> requestProps = new HashMap<String, Object>();
	private HashMap<String, Object> responseProps = new HashMap<String, Object>();
	private HashMap<String, Object> stsProps = new HashMap<String, Object>();
	private SecurityHandler securityHandler = SecurityHandler.WSS4J;

	public SecurityHandler getSecurityHandler() {
		return this.securityHandler;
	}

	public void setSecurityHandler(SecurityHandler securityHandler) {
		this.securityHandler = securityHandler;
	}

	public HashMap<String, Object> getRequestPropterties() {
		return this.requestProps;
	}

	public HashMap<String, Object> getResponsePropterties() {
		return this.responseProps;
	}

	public HashMap<String, Object> getStsPropterties() {
		return this.stsProps;
	}

	/**
	 * This is configuration for WS-Trust STSClient.
	 *
	 * @param stsWsdlLocation
	 * @param stsService
	 * @param stsPort
	 */
	public void setSTSClient(String stsWsdlLocation, QName stsService,
			QName stsPort) {
		this.stsWsdlLocation = stsWsdlLocation;
		this.stsService = stsService;
		this.stsPort = stsPort;
		this.useSts = true;
	}

	public STSClient buildStsClient(org.apache.cxf.Bus bus) {
		STSClient stsClient = new STSClient(bus);
		stsClient.setWsdlLocation(this.stsWsdlLocation);
		stsClient.setServiceQName(this.stsService);
		stsClient.setEndpointQName(this.stsPort);
		Map<String, Object> props = stsClient.getProperties();
		props.putAll(this.stsProps);
		return stsClient;
	}

	public boolean useSts() {
		return this.useSts;
	}
}
