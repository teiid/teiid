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

package org.teiid.translator.goole.api;

import java.util.List;
import java.util.Map;

import javax.resource.cci.Connection;

import org.teiid.translator.goole.api.metadata.SpreadsheetInfo;
import org.teiid.translator.goole.api.result.RowsResult;
import org.teiid.translator.goole.api.result.UpdateResult;


/**
 * Connection to GoogleSpreadsheet API. 
 * 
 */
public interface GoogleSpreadsheetConnection extends Connection {
	public RowsResult executeQuery(String worksheetName, String query, Integer offset, Integer limit, int batchSize);
	public UpdateResult executeListFeedUpdate(String worksheetID, String criteria, List<UpdateSet> set);
	public UpdateResult deleteRows(String worksheetID, String criteria);
	public UpdateResult executeRowInsert(String worksheetID, Map<String,String> pair);
	/**
	 * Returns information about existing Spreadsheets and worksheets.
	 * @return
	 */
	public SpreadsheetInfo getSpreadsheetInfo();
}
