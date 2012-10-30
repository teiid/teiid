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
		this.expiresIn = (Integer)expiresIn;
		this.interval = (Integer)interval;
	}
	@Override
	public String toString() {
		return "AuthUrlResponse [deviceCode=" + deviceCode + ", userCode="
				+ userCode + ", verificationUrl=" + verificationUrl
				+ ", expiresIn=" + expiresIn + ", interval=" + interval
				+ "]";
	}
}