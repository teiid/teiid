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


package org.teiid.resource.adapter.google.auth;

/**
 * 
 * We use OAuth2Devices. This response contains verificationUrl that user has to type into a browser.
 * After filling in the userCode he can allow Google Teiid Connector to access his spreadsheets.
 * 
 * https://developers.google.com/accounts/docs/OAuth2ForDevices
 * 
 * @author fnguyen
 *
 */
public class AuthUrlResponse { 
	private String deviceCode=null;
	private String userCode =null;
	private String verificationUrl =null;
	private Integer expiresIn =null;
	private Integer interval =null;
	
	public String getDeviceCode() {
		return deviceCode;
	}
	public String getUserCode() {
		return userCode;
	}
	public String getVerificationUrl() {
		return verificationUrl;
	}
	public Integer getExpiresIn() {
		return expiresIn;
	}
	public Integer getInterval() {
		return interval;
	}
	public AuthUrlResponse(Object deviceCode, Object userCode,
			Object verificationUrl, Object expiresIn, Object interval) {
		super();
		this.deviceCode = (String)deviceCode;
		this.userCode = (String)userCode;
		this.verificationUrl = (String)verificationUrl;
		Number n = (Number)expiresIn;
		if (n != null) {
			this.expiresIn = n.intValue();
		}
		n = (Number)interval;
		if (n != null) {
			this.interval = n.intValue();
		}
	}
	@Override
	@SuppressWarnings("nls")
	public String toString() {
		return "AuthUrlResponse [deviceCode=" + deviceCode + ", userCode="
				+ userCode + ", verificationUrl=" + verificationUrl
				+ ", expiresIn=" + expiresIn + ", interval=" + interval
				+ "]";
	}
}