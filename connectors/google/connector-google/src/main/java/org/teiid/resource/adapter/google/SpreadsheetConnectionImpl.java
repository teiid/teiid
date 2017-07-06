/*
 * Copyright Red Hat, Inc. and/or its affiliates
 * and other contributors as indicated by the @author tags and
 * the COPYRIGHT.txt file distributed with this work.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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
    	            info = metadataExtractor.extractMetadata(config.getSpreadsheetName(), config.getKey());
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
