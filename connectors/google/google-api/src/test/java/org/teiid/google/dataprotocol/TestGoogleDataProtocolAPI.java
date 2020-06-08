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

package org.teiid.google.dataprotocol;

import static org.junit.Assert.*;

import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Arrays;

import org.junit.Test;
import org.teiid.google.dataprotocol.GoogleDataProtocolAPI;
import org.teiid.google.gdata.GDataClientLoginAPI;
import org.teiid.translator.google.api.UpdateSet;
import org.teiid.translator.google.api.metadata.Column;
import org.teiid.translator.google.api.metadata.SpreadsheetColumnType;

import com.google.gdata.data.spreadsheet.ListEntry;
import com.google.gdata.data.spreadsheet.ListFeed;

@SuppressWarnings("nls")
public class TestGoogleDataProtocolAPI {

    @Test public void testValueConversion() {
        Date date = (Date)GoogleDataProtocolAPI.convertValue(null, "Date(2001,0,1)", SpreadsheetColumnType.DATE);
        assertEquals("2001-01-01", date.toString());
        Timestamp ts = (Timestamp)GoogleDataProtocolAPI.convertValue(null, "Date(2001,0,1,1,2,3)", SpreadsheetColumnType.DATETIME);
        assertEquals("2001-01-01 01:02:03.0", ts.toString());
        Time t = (Time)GoogleDataProtocolAPI.convertValue(null, Arrays.asList(1.0, 2.0, 3.0, 4.0), SpreadsheetColumnType.TIMEOFDAY);
        assertEquals("01:02:03", t.toString());
    }

    @Test public void testColumnsWithoutLabel() {
        GDataClientLoginAPI api = new GDataClientLoginAPI() {
            protected com.google.gdata.data.BaseFeed<?,?> getSpreadsheetFeedQuery(com.google.gdata.client.spreadsheet.SpreadsheetQuery squery, java.lang.Class<? extends com.google.gdata.data.BaseFeed<?,?>> feedClass) {
                ListFeed lf = new ListFeed();
                lf.setEntries(Arrays.asList(new ListEntry()));
                return lf;
            };
        };
        Column c1 = new Column();
        c1.setLabel("valid");
        c1.setDataType(SpreadsheetColumnType.STRING);
        c1.setAlphaName("A");

        Column c2 = new Column();
        c2.setDataType(SpreadsheetColumnType.STRING);
        c2.setAlphaName("B");

        //should succeed without an NPE
        api.listFeedUpdate("x", "y", "", Arrays.asList(new UpdateSet("valid", "value")), Arrays.asList(c1, c2));
    }


}
