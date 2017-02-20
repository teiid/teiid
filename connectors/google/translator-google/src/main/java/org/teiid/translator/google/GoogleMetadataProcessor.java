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

package org.teiid.translator.google;


import org.teiid.logging.LogConstants;
import org.teiid.logging.LogManager;
import org.teiid.metadata.MetadataFactory;
import org.teiid.metadata.Table;
import org.teiid.translator.MetadataProcessor;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.TypeFacility;
import org.teiid.translator.google.api.GoogleSpreadsheetConnection;
import org.teiid.translator.google.api.metadata.Column;
import org.teiid.translator.google.api.metadata.SpreadsheetInfo;
import org.teiid.translator.google.api.metadata.Worksheet;

public class GoogleMetadataProcessor implements MetadataProcessor<GoogleSpreadsheetConnection>{
	
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
	 * @param spreadsheet  Name of the spreadsheet
	 * @param worksheet    Name of the worksheet
	 * @throws TranslatorException
	 */
	private void addTable(MetadataFactory mf, Worksheet worksheet) {
		if (worksheet.getColumnCount() == 0){
			return;
		}
		Table table = mf.addTable(worksheet.getName());
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
	 * @throws TranslatorException
	 */
	private void addColumnsToTable(MetadataFactory mf, Table table, Worksheet worksheet) {
		for(Column column : worksheet.getColumnsAsList()){
			String type = null;
			switch(column.getDataType()){
			case DATE:
				type = TypeFacility.RUNTIME_NAMES.DATE;
				break;
			case BOOLEAN:
				type = TypeFacility.RUNTIME_NAMES.BOOLEAN;
				break;
			case DATETIME:
				type = TypeFacility.RUNTIME_NAMES.TIMESTAMP;
				break;
			case NUMBER:
				type = TypeFacility.RUNTIME_NAMES.DOUBLE;
				break;
			case TIMEOFDAY:
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
			    c.setUpdatable(true);
			}
			c.setNameInSource(worksheet.isHeaderEnabled()?column.getLabel():column.getAlphaName());
			c.setNativeType(column.getDataType().name());
		}    
	}
	
}
