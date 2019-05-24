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
import org.teiid.resource.adapter.google.dataprotocol.GoogleDataProtocolAPI;
import org.teiid.resource.adapter.google.gdata.GDataClientLoginAPI;
import org.teiid.resource.adapter.google.v4.OAuth2HeaderFactory;
import org.teiid.resource.adapter.google.v4.SheetsAPI;
import org.teiid.resource.adapter.google.v4.SpreadsheetMetadataExtractor;
import org.teiid.resource.spi.BasicConnection;
import org.teiid.translator.google.api.GoogleSpreadsheetConnection;
import org.teiid.translator.google.api.SpreadsheetOperationException;
import org.teiid.translator.google.api.UpdateSet;
import org.teiid.translator.google.api.metadata.SpreadsheetInfo;
import org.teiid.translator.google.api.result.RowsResult;
import org.teiid.translator.google.api.result.UpdateResult;



/**
 * Represents a connection to an Google spreadsheet data source.
 *
 * Uses a mixture of Sheets v4 api, visualization, and Sheets v3
 */
public class SpreadsheetConnectionImpl4 extends BasicConnection implements GoogleSpreadsheetConnection  {
    private SpreadsheetManagedConnectionFactory config;
    private SheetsAPI sheetsAPI = null; //v4 specific
    private GoogleDataProtocolAPI googleDataProtocolAPI; //visualization api
    private GDataClientLoginAPI gdata;
    private AtomicReference<SpreadsheetInfo> spreadsheetInfo;
    private AtomicReference<SpreadsheetInfo> v2spreadsheetInfo;

    public SpreadsheetConnectionImpl4(SpreadsheetManagedConnectionFactory config, AtomicReference<SpreadsheetInfo> spreadsheetInfo, AtomicReference<SpreadsheetInfo> v2SpreadsheetInfo) {
        this.config = config;
        this.spreadsheetInfo = spreadsheetInfo;
        this.v2spreadsheetInfo = v2SpreadsheetInfo;

        String refreshToken = config.getRefreshToken().trim();
        OAuth2HeaderFactory authHeaderFactory = new OAuth2HeaderFactory(refreshToken,
                config.getClientId(),
                config.getClientSecret());
        authHeaderFactory.refreshToken();
        sheetsAPI=new SheetsAPI(authHeaderFactory);
        googleDataProtocolAPI = new GoogleDataProtocolAPI();
        googleDataProtocolAPI.setHeaderFactory(authHeaderFactory);

        //v2 for update/delete
        gdata = new GDataClientLoginAPI();
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
            String worksheetTitle, String query,
             Integer offset, Integer limit, int batchSize) {

        return googleDataProtocolAPI.executeQuery(getSpreadsheetInfo(), worksheetTitle, query, Math.min(batchSize, config.getBatchSize()),
                offset, limit);
    }

    @Override
    public SpreadsheetInfo getSpreadsheetInfo() {
        SpreadsheetInfo info = spreadsheetInfo.get();
        if (info == null) {
            synchronized (spreadsheetInfo) {
                info = spreadsheetInfo.get();
                if (info == null) {
                    SpreadsheetMetadataExtractor metadataExtractor = new SpreadsheetMetadataExtractor(sheetsAPI, googleDataProtocolAPI);
                    info = metadataExtractor.extractMetadata(config.getSpreadsheetId());
                    spreadsheetInfo.set(info);
                }
            }
        }
        return info;
    }

    public SpreadsheetInfo getV2SpreadsheetInfo() {
        SpreadsheetInfo info = v2spreadsheetInfo.get();
        if (info == null) {
            synchronized (v2spreadsheetInfo) {
                info = v2spreadsheetInfo.get();
                if (info == null) {
                    org.teiid.resource.adapter.google.gdata.SpreadsheetMetadataExtractor metadataExtractor = new org.teiid.resource.adapter.google.gdata.SpreadsheetMetadataExtractor();
                    metadataExtractor.setVisualizationAPI(googleDataProtocolAPI);
                    metadataExtractor.setGdataAPI(gdata);
                    info = metadataExtractor.extractMetadata(config.getSpreadsheetId(), true);
                    v2spreadsheetInfo.set(info);
                }
            }
        }
        return info;
    }

    @Override
    public UpdateResult updateRows(String worksheetTitle, String criteria, List<UpdateSet> set) {
        SpreadsheetInfo info = getV2SpreadsheetInfo();
        org.teiid.translator.google.api.metadata.Worksheet sheet = info.getWorksheetByName(worksheetTitle);
        if (sheet == null) {
            throw new SpreadsheetOperationException(SpreadsheetManagedConnectionFactory.UTIL.getString("not_visible")); //$NON-NLS-1$
        }
        return gdata.listFeedUpdate(info.getSpreadsheetKey(), sheet.getId(), criteria, set, sheet.getColumnsAsList());
    }

    @Override
    public UpdateResult deleteRows(String worksheetTitle, String criteria) {
        SpreadsheetInfo info = getV2SpreadsheetInfo();
        org.teiid.translator.google.api.metadata.Worksheet sheet = info.getWorksheetByName(worksheetTitle);
        if (sheet == null) {
            throw new SpreadsheetOperationException(SpreadsheetManagedConnectionFactory.UTIL.getString("not_visible")); //$NON-NLS-1$
        }
        return gdata.listFeedDelete(info.getSpreadsheetKey(), sheet.getId(), criteria);
    }
    @Override
    public UpdateResult executeRowInsert(String worksheetTitle, Map<String,Object> pairs){
        return sheetsAPI.insert(getSpreadsheetInfo().getSpreadsheetKey(), pairs, getSpreadsheetInfo().getWorksheetByName(worksheetTitle));
    }

}
