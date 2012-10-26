package org.teiid.resource.adapter.google.auth;

import java.util.Date;

public class OAuth2Tokens {
	private String accessToken = null;
	private String refreshToken = null;
	private Integer expiresIn = null;
	private Date created = null;
	
	public String getAccessToken() {
		return accessToken;
	}
	public String getRefreshToken() {
		return refreshToken;
	}
	public Integer getExpiresIn() {
		return expiresIn;
	}
	
	
	public Date getCreated() {
		return created;
	}
	public OAuth2Tokens(Object accessToken, Object refreshToken,
			Object expiresIn) {
		super();
		this.accessToken = (String)accessToken;
		this.refreshToken =(String) refreshToken;
		this.expiresIn = (Integer)expiresIn;
		created = new Date();
	}

	@Override
	public String toString() {
		return "AccessTokens [accessToken=" + accessToken
				+ ", refreshToken=" + refreshToken + ", expiresIn="
				+ expiresIn + "]";
	}
}