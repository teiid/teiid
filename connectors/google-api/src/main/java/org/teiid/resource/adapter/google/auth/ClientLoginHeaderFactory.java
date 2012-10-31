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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
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
				"https://www.google.com/accounts/ClientLogin"); //$NON-NLS-1$
		List<NameValuePair> nvps = new ArrayList<NameValuePair>(); 
		nvps.add(new BasicNameValuePair("Email", username)); //$NON-NLS-1$
		nvps.add(new BasicNameValuePair("Passwd", password)); //$NON-NLS-1$
		nvps.add(new BasicNameValuePair("accountType", "GOOGLE")); //$NON-NLS-1$ //$NON-NLS-2$
		nvps.add(new BasicNameValuePair("source", "ClientLoginHttpFactory")); //$NON-NLS-1$ //$NON-NLS-2$
		nvps.add(new BasicNameValuePair("service", "wise")); //$NON-NLS-1$ //$NON-NLS-2$
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
		}
		BufferedReader br = null;
		try {
			br = new BufferedReader(new InputStreamReader(
					response.getEntity().getContent(), Charset.forName("UTF-8"))); //$NON-NLS-1$
			br.readLine();
			br.readLine();
			// Third line is the auth.
			// TODO Little hackish. Some idea how to solve this? 
			authkey = br.readLine();
			if (authkey == null) {
				throw new SpreadsheetAuthException("Authkey read from server is null");
			} 
			authkey = authkey.substring(authkey.indexOf('=')+1);
		} catch (IOException e) {
			throw new SpreadsheetAuthException("Error reading Client Login response", e);
		} finally{
			if (br!= null)
				try {
					br.close();
				} catch (IOException e) {
				}
		}
	}
	@Override
	public String getAuthHeader() {
		return "GoogleLogin Auth="+authkey; //$NON-NLS-1$
	}
}
