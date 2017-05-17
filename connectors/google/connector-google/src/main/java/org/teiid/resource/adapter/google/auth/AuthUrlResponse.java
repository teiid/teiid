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