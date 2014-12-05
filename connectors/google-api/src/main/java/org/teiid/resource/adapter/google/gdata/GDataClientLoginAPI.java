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
import java.net.MalformedURLException;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.teiid.logging.LogManager;
import org.teiid.resource.adapter.google.auth.AuthHeaderFactory;
import org.teiid.resource.adapter.google.common.GDataAPI;
import org.teiid.resource.adapter.google.common.SpreadsheetOperationException;
import org.teiid.resource.adapter.google.common.UpdateResult;
import org.teiid.resource.adapter.google.common.UpdateSet;

import com.google.gdata.client.spreadsheet.FeedURLFactory;
import com.google.gdata.client.spreadsheet.SpreadsheetQuery;
import com.google.gdata.client.spreadsheet.SpreadsheetService;
import com.google.gdata.data.BaseFeed;
import com.google.gdata.data.spreadsheet.ListEntry;
import com.google.gdata.data.spreadsheet.ListFeed;
import com.google.gdata.data.spreadsheet.SpreadsheetEntry;
import com.google.gdata.data.spreadsheet.SpreadsheetFeed;
import com.google.gdata.util.ServiceException;

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
		SpreadsheetFeed feed = (SpreadsheetFeed) getSpreadsheetFeedQuery(squery, SpreadsheetFeed.class);
		List<SpreadsheetEntry> entry = feed.getEntries();
		if (entry.size() == 0)
			throw new SpreadsheetOperationException("Couldn't find spreadsheet:" + sheetTitle);

		return entry.get(0);
	}
	

	public String getSpreadsheetKeyByTitle(String sheetName) {
		return getSpreadsheetEntryByTitle(sheetName).getKey();
	}

	private BaseFeed<?, ?> getSpreadsheetFeedQuery(SpreadsheetQuery squery, Class<? extends BaseFeed<?, ?>> feedClass) {
		try { 
			return service.getFeed(squery, feedClass);
		} catch (Exception ex) {
			try {
				Thread.sleep(RETRY_DELAY);
			} catch (InterruptedException e) {
			}
			// Try to relogin
			reauthenticate();
			try {
				return service.getFeed(squery, SpreadsheetFeed.class);
			} catch (Exception ex2) {
				throw new SpreadsheetOperationException("Error getting spreadsheet feed. Possibly bad authentication or connection problems. " + ex2);
			}
		}
	}

	private void reauthenticate() {
		headerFactory.login();
		service.setHeader("Authorization", headerFactory.getAuthHeader()); //$NON-NLS-1$
	}
/**
 * Updates spreadsheet using the listfeed. 
 * 
 * @param spreadsheetKey  key that identifies spreadsheet
 * @param worksheetID  id that identifies worksheet
 * @param criteria  update criteria
 * @param updateSet  fields that should be updated
 * @return number of updated rows
 */
	public UpdateResult listFeedUpdate(String spreadsheetKey, String worksheetID, String criteria, List<UpdateSet> updateSet) {
		SpreadsheetQuery query = null;
		try {
			query = new SpreadsheetQuery(factory.getListFeedUrl(spreadsheetKey, worksheetID, "private", "full")); //$NON-NLS-1$ //$NON-NLS-2$
			query.setStringCustomParameter("sq", criteria); //$NON-NLS-1$

		} catch (MalformedURLException e) {
			throw new SpreadsheetOperationException("Error getting spreadsheet URL: " + e);
		}
		ListFeed listfeed = (ListFeed) getSpreadsheetFeedQuery(query, ListFeed.class);
		int counter=0; 
		for (ListEntry row : listfeed.getEntries()) {
			for (UpdateSet set : updateSet) {
				row.getCustomElements().setValueLocal(set.getColumnID(), set.getValue());
			}
			try {
				row.update();
			} catch (IOException e) {
				LogManager.logWarning(this.getClass().getName(), e,"Error occured when updating spreadsheet row");
				continue;
			} catch (ServiceException e2) {
				LogManager.logWarning(this.getClass().getName(), e2,"Error occured when updating spreadsheet row");
				continue;
			}
			counter++;
		}
		return new UpdateResult(listfeed.getEntries().size(), counter);
	}
	/**
	 * Deletes spreadsheet rows using the listfeed. 
	 * 
	 * @param spreadsheetKey  key that identifies spreadsheet
	 * @param worksheetID  id that identifies worksheet
	 * @param criteria  delete criteria
	 * @return number of deleted rows
	 */
	public UpdateResult listFeedDelete(String spreadsheetKey, String worksheetID, String criteria) {
		SpreadsheetQuery query = null;
		try {
			query = new SpreadsheetQuery(factory.getListFeedUrl(spreadsheetKey, worksheetID, "private", "full")); //$NON-NLS-1$ //$NON-NLS-2$
			query.setStringCustomParameter("sq", criteria); //$NON-NLS-1$
			

		} catch (MalformedURLException e) {
			throw new SpreadsheetOperationException("Error getting spreadsheet URL: " + e);
		}
		ListFeed listfeed = (ListFeed) getSpreadsheetFeedQuery(query, ListFeed.class);
        int counter=0;
		for(int i=listfeed.getEntries().size()-1;i>-1;i--) {
			ListEntry row=listfeed.getEntries().get(i);
			try {
				row.delete();
			} catch (IOException e) {
				LogManager.logWarning(this.getClass().getName(), e,"Error occured when deleting spreadsheet row");
				continue;
			} catch (ServiceException e2) {
				LogManager.logWarning(this.getClass().getName(), e2,"Error occured when deleting spreadsheet row");
				continue;
			}
			counter++;
		}
		return new UpdateResult(listfeed.getEntries().size(), counter);
	}
/**
 * Insert row into spreadsheet using listfeed
 * @param spreadsheetKey  key that identifies spreadsheet
 * @param worksheetID  key that identifies worksheet
 * @param pair name - value pair that should be inserted into spreadsheet
 * @return 1 if the row is successfully inserted
 */
	public UpdateResult listFeedInsert(String spreadsheetKey, String worksheetID, Map<String, String> pair) {
		SpreadsheetQuery query = null;
		try {
			query = new SpreadsheetQuery(factory.getListFeedUrl(spreadsheetKey, worksheetID, "private", "full")); //$NON-NLS-1$ //$NON-NLS-2$

		} catch (MalformedURLException e) {
			throw new SpreadsheetOperationException("Error getting spreadsheet URL: " + e);
		}

		ListEntry row = new ListEntry();
		for (Entry<String, String> entry : pair.entrySet()) {
			row.getCustomElements().setValueLocal(entry.getKey(), entry.getValue());
		}

		try {
			service.insert(query.getFeedUrl(), row);
		} catch (Exception e) {
			throw new SpreadsheetOperationException("Error inserting spreadsheet row: " + e);
		}

		return new UpdateResult(1, 1);
	}

}
