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

import java.util.List;

import org.teiid.resource.adapter.google.auth.AuthHeaderFactory;
import org.teiid.resource.adapter.google.common.GDataAPI;
import org.teiid.resource.adapter.google.common.SpreadsheetOperationException;

import com.google.gdata.client.spreadsheet.FeedURLFactory;
import com.google.gdata.client.spreadsheet.SpreadsheetQuery;
import com.google.gdata.client.spreadsheet.SpreadsheetService;
import com.google.gdata.data.spreadsheet.SpreadsheetEntry;
import com.google.gdata.data.spreadsheet.SpreadsheetFeed;

/**
 * Spreadsheet browser implemented by gdata-java-client: http://code.google.com/p/gdata-java-client/
 * 
 * This browser authenticates using Client Login. 
 * 
 * @author fnguyen
 *
 */
public class GDataClientLoginAPI implements GDataAPI {
	private static final int RETRY_DELAY = 3000;
	private SpreadsheetService service;
	private FeedURLFactory factory;
	private AuthHeaderFactory headerFactory = null;
	
			
	public void setHeaderFactory(AuthHeaderFactory headerFactory) {
		this.headerFactory = headerFactory;
		service.setHeader("Authorization" , headerFactory.getAuthHeader()); //$NON-NLS-1$
	}

	public GDataClientLoginAPI() {
		service = new SpreadsheetService("GdataSpreadsheetBrowser"); //$NON-NLS-1$
		this.factory = FeedURLFactory.getDefault();
	}

	public SpreadsheetEntry getSpreadsheetEntryByTitle(String sheetTitle) {
		SpreadsheetQuery squery = new SpreadsheetQuery(factory.getSpreadsheetsFeedUrl());
		squery.setTitleExact(true);
		squery.setTitleQuery(sheetTitle);
		SpreadsheetFeed feed = getSpreadsheetFeedQuery(squery);
		List<SpreadsheetEntry> entry = feed.getEntries();
		if (entry.size() == 0)
			throw new SpreadsheetOperationException("Couldn't find spreadsheet:"+ sheetTitle);
		
		return entry.get(0);
	}
	

	public String getSpreadsheetKeyByTitle(String sheetName) {
		return getSpreadsheetEntryByTitle(sheetName).getKey();
	}
	
	private SpreadsheetFeed getSpreadsheetFeedQuery(SpreadsheetQuery squery) {
		try {
			return service.getFeed(squery,
				SpreadsheetFeed.class);
		} catch (Exception ex) {
			try {
				Thread.sleep(RETRY_DELAY);
			} catch (InterruptedException e) {
			}
			//Try to relogi
			headerFactory.login();
			service.setHeader("Authorization" , headerFactory.getAuthHeader()); //$NON-NLS-1$
			try {
			return service.getFeed(squery,
					SpreadsheetFeed.class);
			} catch (Exception ex2){
				throw new SpreadsheetOperationException("Error getting spreadsheet feed. Possibly bad authentication or connection problems. "+ ex2);
			}
		}
	}

}
