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

import java.util.concurrent.atomic.AtomicReference;

import javax.resource.ResourceException;
import javax.resource.spi.InvalidPropertyException;

import org.teiid.core.BundleUtil;
import org.teiid.resource.adapter.google.metadata.SpreadsheetInfo;
import org.teiid.resource.spi.BasicConnectionFactory;
import org.teiid.resource.spi.BasicManagedConnectionFactory;

public class SpreadsheetManagedConnectionFactory extends BasicManagedConnectionFactory {
	
	private static final long serialVersionUID = -1832915223199053471L;
	public static final String OAUTH2_LOGIN = "OAuth2"; //$NON-NLS-1$
	private Integer batchSize = 4096;
	public static final BundleUtil UTIL = BundleUtil.getBundleUtil(SpreadsheetManagedConnectionFactory.class);
	public static final String SPREADSHEET_NAME = "SpreadsheetName"; //$NON-NLS-1$

	private String spreadsheetName;
	
	private String authMethod = SpreadsheetManagedConnectionFactory.OAUTH2_LOGIN;
	private String refreshToken;
	
	private String clientId;
	private String clientSecret;
	
	private Boolean key = false;
	
	@Override
	@SuppressWarnings("serial")
	public BasicConnectionFactory<SpreadsheetConnectionImpl> createConnectionFactory() throws ResourceException {
	    checkConfig();
	    
		return new BasicConnectionFactory<SpreadsheetConnectionImpl>() {
		    
		    //share the spreadsheet info among all connections
		    private AtomicReference<SpreadsheetInfo> spreadsheetInfo = new AtomicReference<SpreadsheetInfo>();
		    
			@Override
			public SpreadsheetConnectionImpl getConnection() throws ResourceException {
				return new SpreadsheetConnectionImpl(SpreadsheetManagedConnectionFactory.this, spreadsheetInfo);
			}
		};
	}
	
   private void checkConfig() throws ResourceException {

        //SpreadsheetName should be set
        if (getSpreadsheetName()==null ||  getSpreadsheetName().trim().equals("")){ //$NON-NLS-1$
            throw new InvalidPropertyException(SpreadsheetManagedConnectionFactory.UTIL.
                    getString("provide_spreadsheetname",SpreadsheetManagedConnectionFactory.SPREADSHEET_NAME));      //$NON-NLS-1$
        }
        
        //Auth method must be OAUTH2
        if (getAuthMethod()!=null && !getAuthMethod().equals(SpreadsheetManagedConnectionFactory.OAUTH2_LOGIN)){
            throw new InvalidPropertyException(SpreadsheetManagedConnectionFactory.UTIL.
                    getString("provide_auth", //$NON-NLS-1$
                            SpreadsheetManagedConnectionFactory.OAUTH2_LOGIN));     
        }
        
        //OAuth login requires refreshToken
        //if (config.getAuthMethod().equals(SpreadsheetManagedConnectionFactory.OAUTH2_LOGIN)){
            if (getRefreshToken() == null || getRefreshToken().trim().equals("")){ //$NON-NLS-1$
                throw new InvalidPropertyException(SpreadsheetManagedConnectionFactory.UTIL.getString("oauth_requires_pass"));   //$NON-NLS-1$
            }
        //}
    }


	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + (int) (batchSize ^ (batchSize >>> 32));
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
		return true;
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

	public Boolean getKey() {
        return key;
    }
	
	public void setKey(Boolean key) {
	    if (key == null) {
	        key = false;
	    }
        this.key = key;
    }

    public String getClientId() {
        return clientId;
    }
    
    public void setClientId(String clientId) {
        this.clientId = clientId;
    }
    
    public String getClientSecret() {
        return clientSecret;
    }
    
    public void setClientSecret(String clientSecret) {
        this.clientSecret = clientSecret;
    }
	
}
