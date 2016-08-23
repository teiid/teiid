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
package org.teiid.translator.salesforce.execution;

import static org.junit.Assert.*;

import java.sql.Date;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;
import java.util.TimeZone;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.teiid.core.types.DataTypeManager;
import org.teiid.core.util.TimestampWithTimezone;
import org.teiid.language.ColumnReference;
import org.teiid.language.Expression;
import org.teiid.language.ExpressionValueSource;
import org.teiid.language.Insert;
import org.teiid.language.Literal;
import org.teiid.language.NamedTable;
import org.teiid.metadata.Column;
import org.teiid.metadata.RuntimeMetadata;
import org.teiid.metadata.Table;
import org.teiid.query.unittest.TimestampUtil;
import org.teiid.translator.DataNotAvailableException;
import org.teiid.translator.ExecutionContext;
import org.teiid.translator.salesforce.SalesForceExecutionFactory;
import org.teiid.translator.salesforce.SalesforceConnection;

@SuppressWarnings("nls")
public class TestSingleInsert {

    @BeforeClass
    public static void oneTimeSetup() {
        TimestampWithTimezone.resetCalendar(TimeZone.getTimeZone("GMT-1"));
    }
    
    @AfterClass
    public static void oneTimeTeardown() {
        TimestampWithTimezone.resetCalendar(null);
    }

    @Test
    public void testDateTypes() throws Exception {
        NamedTable table = new NamedTable("temp", null, Mockito.mock(Table.class));
        
        ArrayList<ColumnReference> elements = new ArrayList<ColumnReference>();
        elements.add(new ColumnReference(table, "one", Mockito.mock(Column.class), Integer.class));
        elements.add(new ColumnReference(table, "two", Mockito.mock(Column.class), Date.class));
        elements.add(new ColumnReference(table, "three", Mockito.mock(Column.class), Timestamp.class));

        List<Expression> values = new ArrayList<Expression>();
        values.add(new Literal(1, DataTypeManager.DefaultDataClasses.INTEGER));
        values.add(new Literal(TimestampUtil.createDate(100, 01, 1), DataTypeManager.DefaultDataClasses.DATE));
        values.add(new Literal(TimestampUtil.createTimestamp(100, 01, 1, 0, 4, 0, 0), DataTypeManager.DefaultDataClasses.TIMESTAMP));
        
        ExpressionValueSource valueSource = new ExpressionValueSource(values);
        
        Insert insert = new Insert(table, elements, valueSource);
        
        SalesforceConnection connection = Mockito.mock(SalesforceConnection.class);
        
        Mockito.stub(connection.create(Mockito.any(DataPayload.class))).toAnswer(new Answer<Integer>() {
            @Override
            public Integer answer(InvocationOnMock invocation) throws Throwable {
                DataPayload payload = (DataPayload) invocation.getArguments()[0];
                List<DataPayload.Field> fields = payload.getMessageElements();
                assertEquals(3, fields.size());
                assertEquals(1, fields.get(0).value);
                assertEquals(TimestampUtil.createDate(100, 01, 1), fields.get(1).value);
                Calendar cal = (Calendar) fields.get(2).value;
                assertEquals(TimeZone.getTimeZone("GMT-1"), cal.getTimeZone());
                return 1;
            }
        });
        
        SalesForceExecutionFactory config = new SalesForceExecutionFactory();
        config.setMaxBulkInsertBatchSize(1);
        
        InsertExecutionImpl updateExecution = new InsertExecutionImpl(config, insert, connection, Mockito.mock(RuntimeMetadata.class), Mockito.mock(ExecutionContext.class));
        while(true) {
            try {
                updateExecution.execute();
                org.junit.Assert.assertArrayEquals(new int[] {1}, updateExecution.getUpdateCounts());
                break;
            } catch(DataNotAvailableException e) {
                continue;
            }
        }
    }
}
