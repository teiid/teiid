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

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.http.HttpResponse;
import org.apache.http.NameValuePair;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.message.BasicNameValuePair;
import org.teiid.resource.adapter.google.common.SpreadsheetAuthException;
import org.teiid.resource.adapter.google.dataprotocol.GoogleDataProtocolAPI;
import org.teiid.resource.adapter.google.dataprotocol.GoogleJSONParser;


/**
 * This authenticator is used to work with OAuth2 for devices[1]. This class can be used for
 * two purposes: 
 *  - Allowing user to give permission to Teiid Connector thus creating a refresh and access tokens
 *  - Allowing refreshing the access token using the refresh token.
 *  
 *  
 * <code>
		OAuth2Authenticator oauth = new OAuth2Authenticator();
		AuthUrlResponse authResponse = oauth.getAuthUrl();	
 * </code>
 * The authResponse contains link which user has to type into the browser. Then he has to enter
 * userCode.
 * 
 * After he has done that we can use the deviceCode to retrieve access and refresh tokens: 
 * <code> 
 * System.out.println(oauth.getAccessGoogleTokens(authResponse.getDeviceCode()));
 * </code>
 * 
 * [1] https://developers.google.com/accounts/docs/OAuth2ForDevices
 * @author fnguyen
 *
 */
public class OAuth2Authenticator {
	private final String CLIENT_ID = "217138521084.apps.googleusercontent.com"; //$NON-NLS-1$
	private final String CLIENT_SECRET = "gXQ6-lOkEjE1lVcz7giB4Poy"; //$NON-NLS-1$
	private final String SCOPE_SPREADSHEET = "https://spreadsheets.google.com/feeds/"; //$NON-NLS-1$
//	private final String SCOPE_DRIVE = "https://docs.google.com/feeds";
	private final String SCOPE_DRIVE= " https://www.googleapis.com/auth/drive"; //$NON-NLS-1$
	 
	private final String AUTHORIZATION_SERVER_URL = "https://accounts.google.com/o/oauth2/device/code"; //$NON-NLS-1$
	private final String GRANT_TYPE = "http://oauth.net/grant_type/device/1.0"; //$NON-NLS-1$
	private final String TOKEN_URL = "https://accounts.google.com/o/oauth2/token"; //$NON-NLS-1$


				
	/**
	 * AuthUrl is url that user has to type into his browser.
	 * @return
	 */
	public AuthUrlResponse getAuthUrl() {
		List<NameValuePair> nvps = new ArrayList<NameValuePair>();
		nvps.add(new BasicNameValuePair("client_id", CLIENT_ID)); //$NON-NLS-1$
		nvps.add(new BasicNameValuePair("scope", SCOPE_SPREADSHEET+ SCOPE_DRIVE)); //$NON-NLS-1$
		Map<?, ?> json = jsonResponseHttpPost(AUTHORIZATION_SERVER_URL, nvps);
		
		return new AuthUrlResponse(json.get("device_code"), //$NON-NLS-1$
				json.get("user_code"), json.get("verification_url"), //$NON-NLS-1$ //$NON-NLS-2$
				json.get("expires_in"), json.get("interval")); //$NON-NLS-1$ //$NON-NLS-2$
	}

	/**
	 * Use this method to retrieve access tokens when you think user has already authorized the
	 * request.
	 * 
	 * @param deviceCode
	 * @return
	 */
	public OAuth2Tokens getAccessGoogleTokens(String deviceCode) {
		
		List<NameValuePair> nvps = new ArrayList<NameValuePair>();
		nvps.add(new BasicNameValuePair("client_id", CLIENT_ID)); //$NON-NLS-1$
		nvps.add(new BasicNameValuePair("client_secret", CLIENT_SECRET)); //$NON-NLS-1$
		nvps.add(new BasicNameValuePair("code", deviceCode)); //$NON-NLS-1$
		nvps.add(new BasicNameValuePair("grant_type", GRANT_TYPE)); //$NON-NLS-1$
		Map<?, ?> json = jsonResponseHttpPost(TOKEN_URL, nvps);
		return new OAuth2Tokens(json.get("access_token"), //$NON-NLS-1$
				json.get("refresh_token"), json.get("expires_in")); //$NON-NLS-1$ //$NON-NLS-2$
	}

	/**
	 * 
	 * @param new immutable instance of OAuth2Tokens
	 * @return
	 */
//	curl --data-urlencode client_id=217138521084.apps.googleusercontent.com \
//	--data-urlencode client_secret=gXQ6-lOkEjE1lVcz7giB4Poy \
//	--data-urlencode refresh_token=1/A6ifXgNxCYVGTkPUTnD6Y35v_GyfmuRAsKKL4eww8xs \
//	--data-urlencode grant_type=refresh_token \
//	https://accounts.google.com/o/oauth2/token
	public OAuth2Tokens refreshToken(OAuth2Tokens at) {
		
		List<NameValuePair> nvps = new ArrayList<NameValuePair>();
		nvps.add(new BasicNameValuePair("client_id", CLIENT_ID)); //$NON-NLS-1$
		nvps.add(new BasicNameValuePair("client_secret", CLIENT_SECRET)); //$NON-NLS-1$
		nvps.add(new BasicNameValuePair("refresh_token", at.getRefreshToken())); //$NON-NLS-1$
		nvps.add(new BasicNameValuePair("grant_type", "refresh_token")); //$NON-NLS-1$ //$NON-NLS-2$
		Map<?, ?> json = jsonResponseHttpPost(TOKEN_URL, nvps);
		return new OAuth2Tokens(json.get("access_token"), //$NON-NLS-1$
				at.getRefreshToken(), json.get("expires_in")); //$NON-NLS-1$
	}

	private Map<?, ?> jsonResponseHttpPost(String url, List<NameValuePair> data) {
		DefaultHttpClient httpclient = new DefaultHttpClient();
		HttpPost httpPost = new HttpPost(url);

		HttpResponse response = null;
		try {
			httpPost.setEntity(new UrlEncodedFormEntity(data));
			response = httpclient.execute(httpPost);
		} catch (Exception ex) {
			throw new SpreadsheetAuthException(
					"Error when attempting Http Request to OAuth2", ex);
		}

		if (response.getStatusLine().getStatusCode() != 200) {
			String msg = null;
			msg = response.getStatusLine().getStatusCode() + ": "
					+ response.getStatusLine().getReasonPhrase();
			throw new SpreadsheetAuthException(
					"Error when attempting OAuth2 process: " + msg);
		} 

		InputStream is = null;
		try {
			//Parse JSON response
			is = response.getEntity().getContent();
			GoogleJSONParser parser = new GoogleJSONParser();
			Map<?,?> jsonResponse = (Map<?, ?>) parser.parseObject(new InputStreamReader(is, Charset.forName(GoogleDataProtocolAPI.ENCODING)), false);
			
			//Report errors
			if (jsonResponse.get("error") != null){ //$NON-NLS-1$
				throw new SpreadsheetAuthException("OAuth2 service says: "+ jsonResponse.get("error")); //$NON-NLS-2$
			}
			return jsonResponse;

		} catch (IOException e) {
			throw new SpreadsheetAuthException(
					"Error reading Client Login response", e);
		} finally {
			if (is != null)
				try {
					is.close();
				} catch (IOException e) {
				}
		}
	}
}
