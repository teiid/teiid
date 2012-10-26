package org.teiid.resource.adapter.google.auth;

import java.io.IOException;
import java.util.Collections;

//import com.google.api.client.auth.oauth2.BearerToken;
//import com.google.api.client.auth.oauth2.Credential;
//import com.google.api.client.auth.oauth2.TokenResponse;
//import com.google.api.client.googleapis.auth.oauth2.GoogleAuthorizationCodeFlow;
//import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
//import com.google.api.client.http.BasicAuthentication;
//import com.google.api.client.http.GenericUrl;
//import com.google.api.client.http.javanet.NetHttpTransport;
//import com.google.api.client.json.jackson.JacksonFactory;

public class OAuth2AuthenticatorGoogle {
	private final String CLIENT_ID = "217138521084.apps.googleusercontent.com";
	private final String CLIENT_SECRET = "gXQ6-lOkEjE1lVcz7giB4Poy";
	private final String SCOPE_SPREADSHEET = "https://spreadsheets.google.com/feeds/";
	private final String SCOPE_DRIVE = "https://docs.google.com/feeds";
	private final String AUTHORIZATION_SERVER_URL = "https://accounts.google.com/o/oauth2/device/code";
	private final String GRANT_TYPE = "http://oauth.net/grant_type/device/1.0";
	private final String TOKEN_URL = "https://accounts.google.com/o/oauth2/token";

	public void flow() throws IOException{
//	    GoogleCredential gc =  new GoogleCredential.Builder()
//	    		.setTransport(new NetHttpTransport())
//	            .setJsonFactory(new JacksonFactory())
//	            .setClientSecrets(CLIENT_ID,CLIENT_SECRET)
//	            .build()
//	            .setFromTokenResponse(new TokenResponse().setRefreshToken("1/j7FBwqkdGxCwSFAT1VxZAev7HC2qOamO9UXX_Xxz2nQ"));
//	    
//		gc.refreshToken();
//		System.out.println(gc.getAccessToken());
	}
}
