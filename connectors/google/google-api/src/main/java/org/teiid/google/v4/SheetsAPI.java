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

import static org.teiid.google.v4.ClientConstants.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.teiid.translator.google.api.SpreadsheetOperationException;
import org.teiid.translator.google.api.metadata.Worksheet;
import org.teiid.translator.google.api.result.UpdateResult;

import com.google.api.services.sheets.v4.Sheets;
import com.google.api.services.sheets.v4.model.Spreadsheet;
import com.google.api.services.sheets.v4.model.ValueRange;


/**
 * High level api for accessing sheets
 */
public class SheetsAPI {

    private Sheets service;

    public SheetsAPI(OAuth2HeaderFactory headerFactory) {
        this.service = new Sheets.Builder(HTTP_TRANSPORT, JSON_FACTORY, headerFactory.getCredential())
                .setApplicationName("GdataSpreadsheetBrowser") //$NON-NLS-1$
                .build();
    }

    public Spreadsheet getSpreadsheet(String spreadsheetId) throws IOException {
        return this.service.spreadsheets().get(spreadsheetId).execute();
    }

    /**
     * Insert row into spreadsheet
     * @param spreadsheetId spreadsheet identifier
     * @param pairs name - value pair that should be inserted into spreadsheet
     * @param worksheet
     * @return 1 if the row is successfully inserted
     */
    public UpdateResult insert(String spreadsheetId, Map<String, Object> pairs, Worksheet worksheet) {
        ValueRange content = new ValueRange();

        List<Object> row = new ArrayList<>();
        for (String label : worksheet.getColumns().keySet()) {
            Object value = pairs.get(label);
            if (value != null) {
                if (value instanceof String) {
                    value = "'" + value; //$NON-NLS-1$
                } else if(!(value instanceof Boolean || value instanceof Double)) {
                    value = value.toString();
                } //else directly supported
            }
            row.add(value);
        }

        content.setValues(Arrays.asList(row));

        try {
            service.spreadsheets().values()
            .append(spreadsheetId, worksheet.getTitle(), content)
            .setValueInputOption("USER_ENTERED") //$NON-NLS-1$ -- TODO: this could be configurable
            .execute();
        } catch (IOException e) {
            throw new SpreadsheetOperationException("Error inserting spreadsheet row", e);
        }

        return new UpdateResult(1, 1);
    }

}
