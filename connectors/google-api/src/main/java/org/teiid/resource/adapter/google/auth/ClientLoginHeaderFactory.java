package org.teiid.resource.adapter.google.auth;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

import org.apache.http.HttpResponse;
import org.apache.http.NameValuePair;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.message.BasicNameValuePair;
import org.teiid.resource.adapter.google.common.SpreadsheetAuthException;


/**
 * Header factory for ClientLogin method. See [1] for details about Client Login auth method.
 * 
 * [1] https://developers.google.com/gdata/articles/using_cURL#authenticating-clientlogin
 * 
 * @author fnguyen
 *
 */
public class ClientLoginHeaderFactory implements AuthHeaderFactory {
	private String authkey = null;
	private String username = null;
	private String password =null;
	
	public ClientLoginHeaderFactory(String username, String password) {
		this.username=username;
		this.password = password;
	}

	public void login(){
		DefaultHttpClient httpclient = new DefaultHttpClient();
		HttpPost httpPost = new HttpPost(
				"https://www.google.com/accounts/ClientLogin");
		List<NameValuePair> nvps = new ArrayList<NameValuePair>();
		nvps.add(new BasicNameValuePair("Email", username));
		nvps.add(new BasicNameValuePair("Passwd", password));
		nvps.add(new BasicNameValuePair("accountType", "GOOGLE"));
		nvps.add(new BasicNameValuePair("source", "ClientLoginHttpFactory"));
		nvps.add(new BasicNameValuePair("service", "wise"));
		HttpResponse response = null;
		try {
			httpPost.setEntity(new UrlEncodedFormEntity(nvps));
			response = httpclient.execute(httpPost);
		} catch (Exception ex) {
			throw new SpreadsheetAuthException("Error when attempting Client Login", ex);
		}
		if (response.getStatusLine().getStatusCode() != 200) {
			String msg = null;
			msg = response.getStatusLine().getStatusCode() + ": "
					+ response.getStatusLine().getReasonPhrase();
			throw new SpreadsheetAuthException("Error when attempting Client Login: "
					+ msg);
		} else {
			BufferedReader br = null;
			try {
				br = new BufferedReader(new InputStreamReader(
						response.getEntity().getContent()));
				br.readLine();
				br.readLine();
				// Third line is the auth.
				// TODO Little hackish. Some idea how to solve this? 
				authkey = br.readLine();
				if (authkey == null) {
					throw new SpreadsheetAuthException("Authkey read from server is null");
				} else {
					authkey = authkey.substring(authkey.indexOf('=')+1);
				}
				
			} catch (IOException e) {
				throw new SpreadsheetAuthException("Error reading Client Login response", e);
			} finally{
				if (br!= null)
					try {
						br.close();
					} catch (IOException e) {
						e.printStackTrace();
					}
			}
		}
	}
	@Override
	public String getAuthHeader() {
		return "GoogleLogin Auth="+authkey;
	}
}
