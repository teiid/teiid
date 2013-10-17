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
package org.teiid.resource.adapter.google;

import javax.resource.ResourceException;

import org.teiid.core.BundleUtil;
import org.teiid.resource.spi.BasicConnectionFactory;
import org.teiid.resource.spi.BasicManagedConnectionFactory;

public class SpreadsheetManagedConnectionFactory extends BasicManagedConnectionFactory {
	
	private static final long serialVersionUID = -1832915223199053471L;
	public static final String CLIENT_LOGIN = "ClientLogin"; //$NON-NLS-1$
	public static final String OAUTH2_LOGIN = "OAuth2"; //$NON-NLS-1$
	private Integer batchSize = 4096;
	public static final BundleUtil UTIL = BundleUtil.getBundleUtil(SpreadsheetManagedConnectionFactory.class);
	public static final String SPREADSHEET_NAME = "SpreadsheetName"; //$NON-NLS-1$

	private String spreadsheetName;
	
	//Can be either ClientLogin or OAuth2
	private String authMethod;
	
	//In case of ClientLogin user has to supply his Google account username and password
	private String username;
	private String password;
	
	//In case of OAuth2 authentiation user has to supply refreshToken
	private String refreshToken;
	

	@Override
	@SuppressWarnings("serial")
	public BasicConnectionFactory<SpreadsheetConnectionImpl> createConnectionFactory() throws ResourceException {
		return new BasicConnectionFactory<SpreadsheetConnectionImpl>() {
			@Override
			public SpreadsheetConnectionImpl getConnection() throws ResourceException {
				return new SpreadsheetConnectionImpl(SpreadsheetManagedConnectionFactory.this);
			}
		};
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + (int) (batchSize ^ (batchSize >>> 32));
		result = prime * result
				+ ((password == null) ? 0 : password.hashCode());
		result = prime * result
				+ ((username == null) ? 0 : username.hashCode());
		return result;
	}




	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		SpreadsheetManagedConnectionFactory other = (SpreadsheetManagedConnectionFactory) obj;
		if (batchSize != other.batchSize)
			return false;
		if (password == null) {
			if (other.password != null)
				return false;
		} else if (!password.equals(other.password))
			return false;
		if (username == null) {
			if (other.username != null)
				return false;
		} else if (!username.equals(other.username))
			return false;
		return true;
	}

	public String getUsername() {
		return username;
	}

	public void setUsername(String username) {
		this.username = username;
	}


	public String getPassword() {
		return password;
	}

	public void setPassword(String googlePassword) {
		this.password = googlePassword;
	}

	public Integer getBatchSize() {
		return batchSize;
	}

	public void setBatchSize(Integer batchSize) {
		this.batchSize = batchSize;
	}

	public String getAuthMethod() {
		return authMethod;
	}

	public void setAuthMethod(String authMethod) {
		this.authMethod = authMethod;
	}

	public String getSpreadsheetName() {
		return spreadsheetName;
	}

	public void setSpreadsheetName(String spreadsheetName) {
		this.spreadsheetName = spreadsheetName;
	}

	public String getRefreshToken() {
		return refreshToken;
	}

	public void setRefreshToken(String refreshToken) {
		this.refreshToken = refreshToken;
	}
	
}
