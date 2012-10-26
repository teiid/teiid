package org.teiid.resource.adapter.google.auth;


public class OAuth2HeaderFactory implements AuthHeaderFactory {
	private OAuth2Tokens tokens = null;
	private OAuth2Authenticator authenticator = new OAuth2Authenticator();
	
	public OAuth2HeaderFactory(String refreshToken){
		this.tokens = new OAuth2Tokens("", refreshToken, 1000);
	}
	
	public void login() {
		tokens = authenticator.refreshToken(tokens);
	}
	
	@Override
	public String getAuthHeader() {
		return "Bearer "+  tokens.getAccessToken();
		
	}

}
