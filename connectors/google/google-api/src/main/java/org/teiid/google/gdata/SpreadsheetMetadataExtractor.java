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

package org.teiid.google.gdata;

import java.io.IOException;
import java.util.List;

import org.teiid.google.SpreadsheetConfiguration;
import org.teiid.google.dataprotocol.GoogleDataProtocolAPI;
import org.teiid.translator.google.api.SpreadsheetOperationException;
import org.teiid.translator.google.api.metadata.Column;
import org.teiid.translator.google.api.metadata.SpreadsheetInfo;
import org.teiid.translator.google.api.metadata.Worksheet;

import com.google.gdata.data.spreadsheet.SpreadsheetEntry;
import com.google.gdata.data.spreadsheet.WorksheetEntry;
import com.google.gdata.util.ServiceException;

/**
 * Creates metadata by using GData API.
 *
 * We retrieve worksheet names and possibly headers
 * @author fnguyen
 *
 */
public class SpreadsheetMetadataExtractor {
    private GDataClientLoginAPI gdataAPI = null;
    private GoogleDataProtocolAPI visualizationAPI= null;

    public GoogleDataProtocolAPI getVisualizationAPI() {
        return visualizationAPI;
    }

    public void setVisualizationAPI(GoogleDataProtocolAPI visualizationAPI) {
        this.visualizationAPI = visualizationAPI;
    }

    public GDataClientLoginAPI getGdataAPI() {
        return gdataAPI;
    }

    public void setGdataAPI(GDataClientLoginAPI gdataAPI) {
        this.gdataAPI = gdataAPI;
    }


    public void extractMetadata(SpreadsheetInfo metadata, String prefix, String spreadsheetName, boolean isKey){
        SpreadsheetEntry sentry = gdataAPI.getSpreadsheetEntry(spreadsheetName, isKey);
        try {
            for (WorksheetEntry wentry : sentry.getWorksheets()) {
                String title = wentry.getTitle().getPlainText();
                Worksheet worksheet = metadata.createWorksheet(prefix + title);
                worksheet.setSpreadsheetId(sentry.getKey());
                worksheet.setId(wentry.getId().substring(wentry.getId().lastIndexOf('/')+1));
                List<Column> cols = visualizationAPI.getMetadata(sentry.getKey(), title);
                if(!cols.isEmpty()){
                    if(cols.get(0).getLabel()!=null){
                        worksheet.setHeaderEnabled(true);
                    }
                }
                for(Column c: cols){
                    worksheet.addColumn(c.getLabel()!=null ? c.getLabel(): c.getAlphaName(), c);
                }
            }
        } catch (IOException ex) {
            throw new SpreadsheetOperationException(
                    SpreadsheetConfiguration.UTIL.gs("metadata_error"), ex); //$NON-NLS-1$
        } catch (ServiceException ex) {
            throw new SpreadsheetOperationException(
                    SpreadsheetConfiguration.UTIL.gs("metadata_error"), ex); //$NON-NLS-1$
        }
    }

}

