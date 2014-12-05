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

package org.teiid.resource.adapter.google;

import java.util.List;
import java.util.Map;

import javax.resource.cci.Connection;

import org.teiid.resource.adapter.google.common.UpdateResult;
import org.teiid.resource.adapter.google.common.UpdateSet;
import org.teiid.resource.adapter.google.metadata.SpreadsheetInfo;
import org.teiid.resource.adapter.google.result.RowsResult;


/**
 * Connection to GoogleSpreadsheet API. 
 * 
 */
public interface GoogleSpreadsheetConnection extends Connection {
	public RowsResult executeQuery(String worksheetName, String query, Integer offset, Integer limit, int batchSize);
	public UpdateResult executeListFeedUpdate(String worksheetID, String criteria, List<UpdateSet> set);
	public UpdateResult deleteRows(String worksheetID, String criteria);
	public UpdateResult executeRowInsert(String worksheetID, Map<String,String> pair);
	public SpreadsheetInfo getSpreadsheetInfo();
}
