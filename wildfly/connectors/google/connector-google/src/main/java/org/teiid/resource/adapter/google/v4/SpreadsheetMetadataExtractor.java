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

package org.teiid.resource.adapter.google.v4;

import java.io.IOException;
import java.util.List;

import org.teiid.resource.adapter.google.SpreadsheetManagedConnectionFactory;
import org.teiid.resource.adapter.google.dataprotocol.GoogleDataProtocolAPI;
import org.teiid.translator.google.api.SpreadsheetOperationException;
import org.teiid.translator.google.api.metadata.Column;
import org.teiid.translator.google.api.metadata.SpreadsheetInfo;
import org.teiid.translator.google.api.metadata.Worksheet;

import com.google.api.services.sheets.v4.model.Sheet;
import com.google.api.services.sheets.v4.model.Spreadsheet;

/**
 * Creates metadata by using SheetsAPI.
 *
 * We retrieve worksheet names and possibly headers
 *
 */
public class SpreadsheetMetadataExtractor {
    private SheetsAPI sheetsAPI = null;
    private GoogleDataProtocolAPI dataProtocolAPI;

    public SpreadsheetMetadataExtractor(SheetsAPI api, GoogleDataProtocolAPI dataProtocolAPI) {
        this.sheetsAPI = api;
        this.dataProtocolAPI = dataProtocolAPI;
    }

    public SpreadsheetInfo extractMetadata(String spreadsheetId){
        try {
            Spreadsheet spreadsheet = sheetsAPI.getSpreadsheet(spreadsheetId);
            SpreadsheetInfo metadata = new SpreadsheetInfo(spreadsheet.getProperties().getTitle());
            metadata.setSpreadsheetKey(spreadsheet.getSpreadsheetId());
            for (Sheet sheet : spreadsheet.getSheets()) {
                String title = sheet.getProperties().getTitle();
                Worksheet worksheet = metadata.createWorksheet(title);
                worksheet.setId(sheet.getProperties().getSheetId().toString());
                List<Column> cols = dataProtocolAPI.getMetadata(spreadsheet.getSpreadsheetId(), title);
                if(!cols.isEmpty()){
                    if(cols.get(0).getLabel()!=null){
                        worksheet.setHeaderEnabled(true);
                    }
                }
                for(Column c: cols){
                    worksheet.addColumn(c.getLabel()!=null ? c.getLabel(): c.getAlphaName(), c);
                }
            }
            return metadata;
        } catch (IOException ex) {
            throw new SpreadsheetOperationException(
                    SpreadsheetManagedConnectionFactory.UTIL.gs("metadata_error"), ex); //$NON-NLS-1$
        }
    }

}

