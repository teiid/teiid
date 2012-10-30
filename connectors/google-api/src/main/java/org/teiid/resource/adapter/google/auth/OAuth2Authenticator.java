package org.teiid.resource.adapter.google.auth;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.http.HttpResponse;
import org.apache.http.NameValuePair;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.message.BasicNameValuePair;
import org.codehaus.jackson.map.ObjectMapper;
import org.teiid.resource.adapter.google.common.SpreadsheetAuthException;


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
	private final String CLIENT_ID = "217138521084.apps.googleusercontent.com";
	private final String CLIENT_SECRET = "gXQ6-lOkEjE1lVcz7giB4Poy";
	private final String SCOPE_SPREADSHEET = "https://spreadsheets.google.com/feeds/";
//	private final String SCOPE_DRIVE = "https://docs.google.com/feeds";
	private final String SCOPE_DRIVE= " https://www.googleapis.com/auth/drive";
	 
	private final String AUTHORIZATION_SERVER_URL = "https://accounts.google.com/o/oauth2/device/code";
	private final String GRANT_TYPE = "http://oauth.net/grant_type/device/1.0";
	private final String TOKEN_URL = "https://accounts.google.com/o/oauth2/token";


				
	/**
	 * AuthUrl is url that user has to type into his browser.
	 * @return
	 */
	public AuthUrlResponse getAuthUrl() {
		List<NameValuePair> nvps = new ArrayList<NameValuePair>();
		nvps.add(new BasicNameValuePair("client_id", CLIENT_ID));
		nvps.add(new BasicNameValuePair("scope", SCOPE_SPREADSHEET+ SCOPE_DRIVE));
		Map<?, ?> json = jsonResponseHttpPost(AUTHORIZATION_SERVER_URL, nvps);
		
		return new AuthUrlResponse(json.get("device_code"),
				json.get("user_code"), json.get("verification_url"),
				json.get("expires_in"), json.get("interval"));
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
		nvps.add(new BasicNameValuePair("client_id", CLIENT_ID));
		nvps.add(new BasicNameValuePair("client_secret", CLIENT_SECRET));
		nvps.add(new BasicNameValuePair("code", deviceCode));
		nvps.add(new BasicNameValuePair("grant_type", GRANT_TYPE));
		Map<?, ?> json = jsonResponseHttpPost(TOKEN_URL, nvps);
		return new OAuth2Tokens(json.get("access_token"),
				json.get("refresh_token"), json.get("expires_in"));
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
		nvps.add(new BasicNameValuePair("client_id", CLIENT_ID));
		nvps.add(new BasicNameValuePair("client_secret", CLIENT_SECRET));
		nvps.add(new BasicNameValuePair("refresh_token", at.getRefreshToken()));
		nvps.add(new BasicNameValuePair("grant_type", "refresh_token"));
		Map<?, ?> json = jsonResponseHttpPost(TOKEN_URL, nvps);
		return new OAuth2Tokens(json.get("access_token"),
				at.getRefreshToken(), json.get("expires_in"));
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
		} else {
			StringBuilder sb = new StringBuilder();

			BufferedReader br = null;
			try {
				//Read whole response as a String
				br = new BufferedReader(new InputStreamReader(response
						.getEntity().getContent()));
				String line = null;
				while ((line = br.readLine()) != null)
					sb.append(line);

				//Parse JSON response
				ObjectMapper jacksonMapper = new ObjectMapper();
				Map<?,?> jsonResponse = jacksonMapper.readValue(sb.toString(), Map.class);
				
				//Report errors
				if (jsonResponse.get("error") != null){
					throw new SpreadsheetAuthException("OAuth2 service says: "+ jsonResponse.get("error"));
				}
				return jsonResponse;

			} catch (IOException e) {
				throw new SpreadsheetAuthException(
						"Error reading Client Login response", e);
			} finally {
				if (br != null)
					try {
						br.close();
					} catch (IOException e) {
						e.printStackTrace();
					}
			}
		}
	}
}
