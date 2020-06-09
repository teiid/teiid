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

package org.teiid.google.v4;

import java.io.IOException;
import java.util.List;

import org.teiid.google.SpreadsheetConfiguration;
import org.teiid.google.dataprotocol.GoogleDataProtocolAPI;
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

    public void extractMetadata(SpreadsheetInfo metadata, String prefix, String spreadsheetId){
        try {
            Spreadsheet spreadsheet = sheetsAPI.getSpreadsheet(spreadsheetId);
            for (Sheet sheet : spreadsheet.getSheets()) {
                String title = sheet.getProperties().getTitle();
                //add a sheet with the spreadsheet prefix
                Worksheet worksheet = metadata.createWorksheet(prefix + title, title);
                //for some reason a sheet can have a 0 id, which is invalid
                //so use the index as the id, but change to 1 based
                Integer sheetId = sheet.getProperties().getIndex() + 1;
                worksheet.setId(sheetId.toString());
                worksheet.setSpreadsheetId(spreadsheet.getSpreadsheetId());
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
        } catch (IOException ex) {
            throw new SpreadsheetOperationException(
                    SpreadsheetConfiguration.UTIL.gs("metadata_error"), ex); //$NON-NLS-1$
        }
    }

}

