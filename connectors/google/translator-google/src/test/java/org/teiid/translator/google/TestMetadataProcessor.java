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

import static org.junit.Assert.*;

import java.util.Properties;

import org.junit.Test;
import org.mockito.Mockito;
import org.teiid.metadata.MetadataFactory;
import org.teiid.metadata.Table;
import org.teiid.query.metadata.SystemMetadata;
import org.teiid.translator.goole.api.GoogleSpreadsheetConnection;
import org.teiid.translator.goole.api.metadata.Column;
import org.teiid.translator.goole.api.metadata.SpreadsheetInfo;
import org.teiid.translator.goole.api.metadata.Util;
import org.teiid.translator.goole.api.metadata.Worksheet;

@SuppressWarnings("nls")
public class TestMetadataProcessor {

    @Test public void testRemoveColumns() throws Exception {
        GoogleSpreadsheetConnection conn = Mockito.mock(GoogleSpreadsheetConnection.class);
        
        SpreadsheetInfo people=  new SpreadsheetInfo("People");
        Worksheet worksheet = people.createWorksheet("PeopleList");
        worksheet.setHeaderEnabled(true);
        for (int i = 1; i <= 3; i++) {
            Column newCol = new Column();
            newCol.setAlphaName(Util.convertColumnIDtoString(i));
            newCol.setLabel("c" + i);
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
        assertEquals(3, t.getColumns().size());
    }
    
}
