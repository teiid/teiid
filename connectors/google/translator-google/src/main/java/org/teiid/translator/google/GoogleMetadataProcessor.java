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

package org.teiid.translator.google;


import org.teiid.logging.LogConstants;
import org.teiid.logging.LogManager;
import org.teiid.metadata.MetadataFactory;
import org.teiid.metadata.Table;
import org.teiid.translator.MetadataProcessor;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.TranslatorProperty;
import org.teiid.translator.TranslatorProperty.PropertyType;
import org.teiid.translator.TypeFacility;
import org.teiid.translator.google.api.GoogleSpreadsheetConnection;
import org.teiid.translator.google.api.metadata.Column;
import org.teiid.translator.google.api.metadata.SpreadsheetInfo;
import org.teiid.translator.google.api.metadata.Worksheet;
import org.teiid.util.FullyQualifiedName;

public class GoogleMetadataProcessor implements MetadataProcessor<GoogleSpreadsheetConnection>{

    private boolean allTypesUpdatable = true;

    /**
     * Creates metadata from all spreadsheets in the user account. Table name
     * consists of Spreadsheet name and worksheet name. Columns of the table are
     * columns of the worksheet.
     */
    public void process(MetadataFactory mf, GoogleSpreadsheetConnection conn) throws TranslatorException {
        SpreadsheetInfo ssMetadata = conn.getSpreadsheetInfo();
        for (Worksheet worksheet : ssMetadata.getWorksheets()) {
            addTable(mf, worksheet);
        }
    }

    /**
     * Adds new table to metadata.
     *
     * @param worksheet    Name of the worksheet
     */
    private void addTable(MetadataFactory mf, Worksheet worksheet) {
        if (worksheet.getColumnCount() == 0){
            return;
        }
        Table table = mf.addTable(worksheet.getName());
        table.setProperty(FQN, new FullyQualifiedName("worksheet", worksheet.getName()).toString()); //$NON-NLS-1$
        table.setNameInSource(worksheet.getName());
        if (worksheet.isHeaderEnabled()) {
            table.setSupportsUpdate(true);
        }
        addColumnsToTable(mf, table, worksheet);
    }

    /**
     * Adds column to table
     *
     * @param table      Teiid table
     * @param worksheet
     */
    private void addColumnsToTable(MetadataFactory mf, Table table, Worksheet worksheet) {
        boolean updatable = true;
        for(Column column : worksheet.getColumnsAsList()){
            String type = null;
            switch(column.getDataType()){
            case DATE:
                updatable = false;
                type = TypeFacility.RUNTIME_NAMES.DATE;
                break;
            case BOOLEAN:
                type = TypeFacility.RUNTIME_NAMES.BOOLEAN;
                break;
            case DATETIME:
                updatable = false;
                type = TypeFacility.RUNTIME_NAMES.TIMESTAMP;
                break;
            case NUMBER:
                updatable = false;
                type = TypeFacility.RUNTIME_NAMES.DOUBLE;
                break;
            case TIMEOFDAY:
                updatable = false;
                type = TypeFacility.RUNTIME_NAMES.TIME;
                break;
            default:
                type = TypeFacility.RUNTIME_NAMES.STRING;
            }
            String name = column.getAlphaName();
            if (worksheet.isHeaderEnabled()) {
                name = column.getLabel();
                if (name == null) {
                    LogManager.logDetail(LogConstants.CTX_CONNECTOR, SpreadsheetExecutionFactory.UTIL.getString("missing_label", column.getAlphaName())); //$NON-NLS-1$
                    continue;
                }
            }
            org.teiid.metadata.Column c = mf.addColumn(name, type, table);
            if (table.supportsUpdate()) {
                c.setUpdatable(updatable || allTypesUpdatable);
            }
            c.setNameInSource(worksheet.isHeaderEnabled()?column.getLabel():column.getAlphaName());
            c.setNativeType(column.getDataType().name());
        }
    }

    @TranslatorProperty (display="All Types Updatable", category=PropertyType.IMPORT, description="Allow all types to be updatable even those that may have formatting or locale inconsistencies.")
    public boolean isAllTypesUpdatable() {
        return allTypesUpdatable;
    }

    public void setAllTypesUpdatable(boolean allTypesUpdatable) {
        this.allTypesUpdatable = allTypesUpdatable;
    }

}
