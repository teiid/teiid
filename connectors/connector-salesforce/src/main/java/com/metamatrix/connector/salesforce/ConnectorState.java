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
package com.metamatrix.connector.salesforce;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.Properties;

import org.teiid.connector.api.ConnectorException;
import org.teiid.connector.api.ConnectorLogger;


public class ConnectorState {
	
	public static final String USERNAME = "username";
	public static final String PASSWORD = "password";
	public static final String URL = "URL";
	
	String username;
	String password;
	URL url;
	
	
	public ConnectorState(Properties props, ConnectorLogger logger) throws ConnectorException {
		if (logger == null) {
            throw new ConnectorException("Internal Exception: logger is null");
        }
		
		String username = props.getProperty(USERNAME);
        if (username != null) {
            setUsername(username);
        }
		
		String password = props.getProperty(PASSWORD);
        if (password != null) {
            setPassword(password);
        }
        
        String url = props.getProperty(URL);
		if(null != url && url.length()!= 0) {
			URL salesforceURL;
			try {
				salesforceURL = new URL(url);
			} catch (MalformedURLException e) {
				throw new ConnectorException(e, e.getMessage());
			}
			setUrl(salesforceURL);
		}
	}

	private void setUrl(URL salesforceURL) {
		url = salesforceURL;
	}

	private void setUsername(String username) {
		this.username = username;		
	}
	
	private void setPassword(String password) {
		this.password = password;		
	}

	public URL getURL() {
		return url;
	}
	
	public String getUsername() {
		return username;
	}

	public String getPassword() {
		return password;
	}

}
