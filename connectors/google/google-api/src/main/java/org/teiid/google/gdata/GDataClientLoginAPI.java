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

package org.teiid.google.gdata;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.teiid.core.types.DataTypeManager;
import org.teiid.google.auth.AuthHeaderFactory;
import org.teiid.logging.LogManager;
import org.teiid.translator.google.api.SpreadsheetOperationException;
import org.teiid.translator.google.api.UpdateSet;
import org.teiid.translator.google.api.metadata.Column;
import org.teiid.translator.google.api.metadata.SpreadsheetColumnType;
import org.teiid.translator.google.api.result.UpdateResult;

import com.google.gdata.client.spreadsheet.FeedURLFactory;
import com.google.gdata.client.spreadsheet.SpreadsheetQuery;
import com.google.gdata.client.spreadsheet.SpreadsheetService;
import com.google.gdata.data.BaseFeed;
import com.google.gdata.data.IEntry;
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
public class GDataClientLoginAPI {
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

    public SpreadsheetEntry getSpreadsheetEntry(String sheetName, boolean key) {
        if (key) {
            try {
                return getSpreadsheetEntry(new URL(factory.getSpreadsheetsFeedUrl(), "full/"+sheetName), SpreadsheetEntry.class);
            } catch (MalformedURLException e) {
                throw new SpreadsheetOperationException(e);
            }
        }
        SpreadsheetQuery squery = new SpreadsheetQuery(factory.getSpreadsheetsFeedUrl());
        squery.setTitleExact(true);
        squery.setTitleQuery(sheetName);
        SpreadsheetFeed feed = (SpreadsheetFeed) getSpreadsheetFeedQuery(squery, SpreadsheetFeed.class);
        List<SpreadsheetEntry> entry = feed.getEntries();
        if (entry.size() == 0)
            throw new SpreadsheetOperationException("Couldn't find spreadsheet:" + sheetName);

        if (entry.size() > 1) {
            throw new SpreadsheetOperationException("Multiple worksheets with the given title:" + sheetName + ".  Consider using a sheet key instead.");
        }

        return entry.get(0);
    }


    protected BaseFeed<?, ?> getSpreadsheetFeedQuery(SpreadsheetQuery squery, Class<? extends BaseFeed<?, ?>> feedClass) {
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

    private <E extends IEntry> E getSpreadsheetEntry(URL entryUrl, Class<E> entryClass) {
        try {
            return service.getEntry(entryUrl, entryClass);
        } catch (Exception ex) {
            try {
                Thread.sleep(RETRY_DELAY);
            } catch (InterruptedException e) {
            }
            // Try to relogin
            reauthenticate();
            try {
                return service.getEntry(entryUrl, entryClass);
            } catch (Exception ex2) {
                throw new SpreadsheetOperationException("Error getting spreadsheet feed. Possibly bad authentication or connection problems. " + ex2);
            }
        }
    }

    private void reauthenticate() {
        headerFactory.refreshToken();
        service.setHeader("Authorization", headerFactory.getAuthHeader()); //$NON-NLS-1$
    }
/**
 * Updates spreadsheet using the listfeed.
 *
 * @param spreadsheetKey  key that identifies spreadsheet
 * @param worksheetID  id that identifies worksheet
 * @param criteria  update criteria
 * @param updateSet  fields that should be updated
 * @param allColumns
 * @return number of updated rows
 */
    public UpdateResult listFeedUpdate(String spreadsheetKey, String worksheetID, String criteria, List<UpdateSet> updateSet, List<Column> allColumns) {
        SpreadsheetQuery query = null;
        try {
            query = new SpreadsheetQuery(factory.getListFeedUrl(spreadsheetKey, worksheetID, "private", "full")); //$NON-NLS-1$ //$NON-NLS-2$
            if (criteria != null) {
                query.setStringCustomParameter("sq", criteria); //$NON-NLS-1$
            }

        } catch (MalformedURLException e) {
            throw new SpreadsheetOperationException("Error getting spreadsheet URL: " + e);
        }
        ListFeed listfeed = (ListFeed) getSpreadsheetFeedQuery(query, ListFeed.class);
        int counter=0;

        //TEIID-4870 existing string values can get corrupted unless we re-set the entry
        List<Column> stringColumns = new ArrayList<Column>();
        for (Column c : allColumns) {
            if (c.getLabel() != null && c.getDataType() == SpreadsheetColumnType.STRING) {
                stringColumns.add(c);
                //could skip if in the update set
            }
        }
        for (ListEntry row : listfeed.getEntries()) {
            for (int i = 0; i < stringColumns.size(); i++) {
                Column c = stringColumns.get(i);
                String value = row.getCustomElements().getValue(c.getLabel());
                if (value != null && !value.isEmpty()) {
                    row.getCustomElements().setValueLocal(c.getLabel(), "'" + value); //$NON-NLS-1$
                }
            }
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
            if (criteria != null) {
                query.setStringCustomParameter("sq", criteria); //$NON-NLS-1$
            }
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
    public UpdateResult listFeedInsert(String spreadsheetKey, String worksheetID, Map<String, Object> pair) {
        SpreadsheetQuery query = null;
        try {
            query = new SpreadsheetQuery(factory.getListFeedUrl(spreadsheetKey, worksheetID, "private", "full")); //$NON-NLS-1$ //$NON-NLS-2$

        } catch (MalformedURLException e) {
            throw new SpreadsheetOperationException("Error getting spreadsheet URL: " + e);
        }

        ListEntry row = new ListEntry();
        for (Entry<String, Object> entry : pair.entrySet()) {
            Object value = entry.getValue();
            Class<?> type = value.getClass();
            String valString = null;
            if (type.equals(DataTypeManager.DefaultDataClasses.STRING)) {
                valString = "'"+value; //$NON-NLS-1$
            } else {
                valString = value.toString();
            }
            row.getCustomElements().setValueLocal(entry.getKey(), valString);
        }

        try {
            service.insert(query.getFeedUrl(), row);
        } catch (Exception e) {
            throw new SpreadsheetOperationException("Error inserting spreadsheet row: " + e);
        }

        return new UpdateResult(1, 1);
    }

}
