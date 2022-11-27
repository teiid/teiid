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

import static org.junit.Assert.*;

import java.util.Properties;

import org.junit.Test;
import org.mockito.Mockito;
import org.teiid.metadata.MetadataFactory;
import org.teiid.metadata.Table;
import org.teiid.query.metadata.SystemMetadata;
import org.teiid.translator.google.api.GoogleSpreadsheetConnection;
import org.teiid.translator.google.api.metadata.Column;
import org.teiid.translator.google.api.metadata.SpreadsheetColumnType;
import org.teiid.translator.google.api.metadata.SpreadsheetInfo;
import org.teiid.translator.google.api.metadata.Util;
import org.teiid.translator.google.api.metadata.Worksheet;

@SuppressWarnings("nls")
public class TestMetadataProcessor {

    @Test public void testRemoveColumns() throws Exception {
        GoogleSpreadsheetConnection conn = Mockito.mock(GoogleSpreadsheetConnection.class);

        SpreadsheetInfo people=  new SpreadsheetInfo();
        Worksheet worksheet = people.createWorksheet("PeopleList");
        worksheet.setHeaderEnabled(true);
        for (int i = 1; i <= 3; i++) {
            Column newCol = new Column();
            newCol.setAlphaName(Util.convertColumnIDtoString(i));
            newCol.setLabel("c" + i);
            if (i == 1) {
                newCol.setDataType(SpreadsheetColumnType.DATETIME);
            }
            worksheet.addColumn(newCol.getAlphaName(), newCol);
        }
        Column newCol = new Column();
        newCol.setAlphaName("empty");
        worksheet.addColumn(null, newCol);

        Mockito.stub(conn.getSpreadsheetInfo()).toReturn(people);

        MetadataFactory factory = new MetadataFactory("", 1, "", SystemMetadata.getInstance().getRuntimeTypeMap(), new Properties(), "");
        GoogleMetadataProcessor processor = new GoogleMetadataProcessor();
        processor.process(factory, conn);
        Table t = factory.getSchema().getTables().get("PeopleList");
        assertTrue(t.supportsUpdate());
        assertEquals(3, t.getColumns().size());
        assertTrue(t.getColumns().get(0).isUpdatable());

        processor.setAllTypesUpdatable(false);
        factory = new MetadataFactory("", 1, "", SystemMetadata.getInstance().getRuntimeTypeMap(), new Properties(), "");
        processor.process(factory, conn);
        t = factory.getSchema().getTables().get("PeopleList");
        assertFalse(t.getColumns().get(0).isUpdatable());
    }

}
