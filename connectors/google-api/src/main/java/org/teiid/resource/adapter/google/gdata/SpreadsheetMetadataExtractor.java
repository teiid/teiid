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

package org.teiid.resource.adapter.google.gdata;

import java.io.IOException;
import java.util.List;

import org.teiid.resource.adapter.google.common.SpreadsheetOperationException;
import org.teiid.resource.adapter.google.dataprotocol.GoogleDataProtocolAPI;
import org.teiid.resource.adapter.google.metadata.Column;
import org.teiid.resource.adapter.google.metadata.SpreadsheetInfo;
import org.teiid.resource.adapter.google.metadata.Worksheet;

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
	
	
	public SpreadsheetInfo extractMetadata(String spreadsheetName){
		SpreadsheetEntry sentry = gdataAPI
				.getSpreadsheetEntryByTitle(spreadsheetName);
		SpreadsheetInfo metadata = new SpreadsheetInfo(spreadsheetName);
		try {
			for (WorksheetEntry wentry : sentry.getWorksheets()) {
				String title = wentry.getTitle().getPlainText();
				Worksheet worksheet = metadata.createWorksheet(title);
				List<Column> cols = visualizationAPI.getMetadata(spreadsheetName, title);
				if (cols.isEmpty()) {
					worksheet.setColumnCount(0);
				} else {
					worksheet.setColumns(cols);
				}
			}
		} catch (IOException ex) {
			throw new SpreadsheetOperationException(
					"Error getting metadata about Spreadsheets worksheet", ex);
		} catch (ServiceException ex) {
			throw new SpreadsheetOperationException(
					"Error getting metadata about Spreadsheets worksheet", ex);
		}
		return metadata;
	}

}

