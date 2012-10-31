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

import org.teiid.logging.LogConstants;
import org.teiid.logging.LogManager;
import org.teiid.resource.adapter.google.auth.AuthHeaderFactory;
import org.teiid.resource.adapter.google.auth.ClientLoginHeaderFactory;
import org.teiid.resource.adapter.google.auth.OAuth2HeaderFactory;
import org.teiid.resource.adapter.google.common.SpreadsheetAuthException;
import org.teiid.resource.adapter.google.dataprotocol.GoogleDataProtocolAPI;
import org.teiid.resource.adapter.google.gdata.GDataClientLoginAPI;
import org.teiid.resource.adapter.google.gdata.SpreadsheetMetadataExtractor;
import org.teiid.resource.adapter.google.metadata.SpreadsheetInfo;
import org.teiid.resource.adapter.google.result.RowsResult;
import org.teiid.resource.spi.BasicConnection;



/** 
 * Represents a connection to an Google spreadsheet data source. 
 */
public class SpreadsheetConnectionImpl extends BasicConnection implements GoogleSpreadsheetConnection  {  
	private SpreadsheetInfo info = null;
	private SpreadsheetManagedConnectionFactory config;
	private GDataClientLoginAPI gdata = null;
	private GoogleDataProtocolAPI dataProtocol = null;
	
	public SpreadsheetConnectionImpl(SpreadsheetManagedConnectionFactory config) {
		this.config = config;
		checkConfig(config);
		
		AuthHeaderFactory authHeaderFactory = null;
		if (SpreadsheetManagedConnectionFactory.CLIENT_LOGIN.equals(config.getAuthMethod())){
			authHeaderFactory = new ClientLoginHeaderFactory(config.getUsername(),config.getPassword() );
		} else {
			authHeaderFactory = new OAuth2HeaderFactory(config.getRefreshToken().trim());
		}
		gdata=new GDataClientLoginAPI();
		dataProtocol = new GoogleDataProtocolAPI();
		authHeaderFactory.login();
		dataProtocol.setHeaderFactory(authHeaderFactory);
		gdata.setHeaderFactory(authHeaderFactory);
		dataProtocol.setSpreadSheetBrowser(gdata);
		
		LogManager.logInfo(LogConstants.CTX_CONNECTOR,SpreadsheetManagedConnectionFactory.UTIL.
				getString("init") ); //$NON-NLS-1$
	}
	
	private void checkConfig(SpreadsheetManagedConnectionFactory config2) {
		//SpreadsheetName should be set
		if (config.getSpreadsheetName()==null ||  config.getSpreadsheetName().trim().equals("")){ //$NON-NLS-1$
			throw new SpreadsheetAuthException(SpreadsheetManagedConnectionFactory.UTIL.
					getString("provide_spreadsheetname",SpreadsheetManagedConnectionFactory.SPREADSHEET_NAME));		 //$NON-NLS-1$
		}
		//Auth method must be either CLIENT_LOGIN or OAUTH2
		if (config.getAuthMethod()==null ||  (!config.getAuthMethod().equals(SpreadsheetManagedConnectionFactory.CLIENT_LOGIN)
		 && !config.getAuthMethod().equals(SpreadsheetManagedConnectionFactory.OAUTH2_LOGIN))){
			throw new SpreadsheetAuthException(SpreadsheetManagedConnectionFactory.UTIL.
					getString("provide_auth", SpreadsheetManagedConnectionFactory.CLIENT_LOGIN,  //$NON-NLS-1$
							SpreadsheetManagedConnectionFactory.OAUTH2_LOGIN));		
		}
		
		//Client login requires to suppoly username and password
		if (config.getAuthMethod().equals(SpreadsheetManagedConnectionFactory.CLIENT_LOGIN)){
			if (config.getUsername() == null || config.getPassword() == null ||
					config.getUsername().trim().equals("") || //$NON-NLS-1$
					config.getPassword().trim().equals("") //$NON-NLS-1$
					){
				throw new SpreadsheetAuthException(SpreadsheetManagedConnectionFactory.UTIL.
						getString("client_login_requires_pass"));	 //$NON-NLS-1$
			}
		}
		
		//OAuth login requires refreshToken
		if (config.getAuthMethod().equals(SpreadsheetManagedConnectionFactory.OAUTH2_LOGIN)){
			if (config.getRefreshToken() == null ||
					config.getRefreshToken().trim().equals("")  //$NON-NLS-1$
					){
				throw new SpreadsheetAuthException(SpreadsheetManagedConnectionFactory.UTIL.
						getString("oauth_requires_pass"));	 //$NON-NLS-1$
			}
		}
		
	}

	/** 
	 * Closes Google spreadsheet context, effectively closing the connection to Google spreadsheet.
	 * (non-Javadoc)
	 */
	@Override
    public void close() {
		LogManager.logInfo(LogConstants.CTX_CONNECTOR, 
				SpreadsheetManagedConnectionFactory.UTIL.
				getString("closing")); //$NON-NLS-1$
	}
	
	public boolean isAlive() {
		LogManager.logTrace(LogConstants.CTX_CONNECTOR, SpreadsheetManagedConnectionFactory.UTIL.
				getString("alive")); //$NON-NLS-1$
		return true;
	}

	@Override
	public RowsResult executeQuery(
			String worksheetName, String query, 
			 Integer offset, Integer limit) {
		
		return dataProtocol.executeQuery(config.getSpreadsheetName(), worksheetName, query, (int)config.getBatchSize(), 
				offset, limit);
	}	
	
	@Override
	public SpreadsheetInfo getSpreadsheetInfo() {
		if (info == null) {
			SpreadsheetMetadataExtractor metadataExtractor = new SpreadsheetMetadataExtractor();
			metadataExtractor.setGdataAPI(gdata);
			metadataExtractor.setVisualizationAPI(dataProtocol);
			info = metadataExtractor.extractMetadata(config.getSpreadsheetName());
		}
		return info;
	}


}
