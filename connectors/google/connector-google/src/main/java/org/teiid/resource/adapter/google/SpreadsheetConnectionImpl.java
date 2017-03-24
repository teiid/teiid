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

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import org.teiid.logging.LogConstants;
import org.teiid.logging.LogManager;
import org.teiid.resource.adapter.google.auth.AuthHeaderFactory;
import org.teiid.resource.adapter.google.auth.OAuth2HeaderFactory;
import org.teiid.resource.adapter.google.dataprotocol.GoogleDataProtocolAPI;
import org.teiid.resource.adapter.google.gdata.GDataClientLoginAPI;
import org.teiid.resource.adapter.google.gdata.SpreadsheetMetadataExtractor;
import org.teiid.resource.spi.BasicConnection;
import org.teiid.translator.google.api.GoogleSpreadsheetConnection;
import org.teiid.translator.google.api.UpdateSet;
import org.teiid.translator.google.api.metadata.SpreadsheetInfo;
import org.teiid.translator.google.api.result.RowsResult;
import org.teiid.translator.google.api.result.UpdateResult;



/** 
 * Represents a connection to an Google spreadsheet data source. 
 */
public class SpreadsheetConnectionImpl extends BasicConnection implements GoogleSpreadsheetConnection  {  
	private SpreadsheetManagedConnectionFactory config;
	private GDataClientLoginAPI gdata = null;
	private GoogleDataProtocolAPI dataProtocol = null;
    private AtomicReference<SpreadsheetInfo> spreadsheetInfo;
	
	public SpreadsheetConnectionImpl(SpreadsheetManagedConnectionFactory config, AtomicReference<SpreadsheetInfo> spreadsheetInfo) {
		this.config = config;
		this.spreadsheetInfo = spreadsheetInfo;
		AuthHeaderFactory authHeaderFactory = new OAuth2HeaderFactory(config.getRefreshToken().trim());
		gdata=new GDataClientLoginAPI();
		dataProtocol = new GoogleDataProtocolAPI();
		authHeaderFactory.login();
		dataProtocol.setHeaderFactory(authHeaderFactory);
		gdata.setHeaderFactory(authHeaderFactory);
		
		LogManager.logDetail(LogConstants.CTX_CONNECTOR,SpreadsheetManagedConnectionFactory.UTIL.getString("init") ); //$NON-NLS-1$
	}
	
	/** 
	 * Closes Google spreadsheet context, effectively closing the connection to Google spreadsheet.
	 * (non-Javadoc)
	 */
	@Override
    public void close() {
		LogManager.logDetail(LogConstants.CTX_CONNECTOR, 
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
			 Integer offset, Integer limit, int batchSize) {
		
		return dataProtocol.executeQuery(getSpreadsheetInfo(), worksheetName, query, Math.min(batchSize, config.getBatchSize()), 
				offset, limit);
	}	
	
	@Override
	public SpreadsheetInfo getSpreadsheetInfo() {
	    SpreadsheetInfo info = spreadsheetInfo.get();
	    if (info == null) {
	        synchronized (spreadsheetInfo) {
	            info = spreadsheetInfo.get();
	            if (info == null) {
    	            SpreadsheetMetadataExtractor metadataExtractor = new SpreadsheetMetadataExtractor();
    	            metadataExtractor.setGdataAPI(gdata);
    	            metadataExtractor.setVisualizationAPI(dataProtocol);
    	            info = metadataExtractor.extractMetadata(config.getSpreadsheetName());
    	            spreadsheetInfo.set(info);
	            }                
            }
		}
		return info;
	}

	@Override
	public UpdateResult executeListFeedUpdate(String worksheetID, String criteria, List<UpdateSet> set) {
		return gdata.listFeedUpdate(getSpreadsheetInfo().getSpreadsheetKey(), worksheetID, criteria, set);
	}

	@Override
	public UpdateResult deleteRows(String worksheetID, String criteria) {
		return gdata.listFeedDelete(getSpreadsheetInfo().getSpreadsheetKey(), worksheetID, criteria);
	}
	@Override
	public UpdateResult executeRowInsert(String worksheetID, Map<String,String> pair){
		return gdata.listFeedInsert(getSpreadsheetInfo().getSpreadsheetKey(), worksheetID, pair);
	}

}
