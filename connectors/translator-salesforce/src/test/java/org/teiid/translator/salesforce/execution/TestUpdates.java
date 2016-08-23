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

import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.teiid.cdk.api.TranslationUtility;
import org.teiid.language.Delete;
import org.teiid.metadata.RuntimeMetadata;
import org.teiid.translator.DataNotAvailableException;
import org.teiid.translator.ExecutionContext;
import org.teiid.translator.salesforce.SalesForceExecutionFactory;
import org.teiid.translator.salesforce.SalesforceConnection;
import org.teiid.translator.salesforce.execution.visitors.TestVisitors;

import com.sforce.soap.partner.QueryResult;
import com.sforce.soap.partner.sobject.SObject;

@SuppressWarnings("nls")
public class TestUpdates {
    
    private static TranslationUtility translationUtility = new TranslationUtility(TestVisitors.exampleSalesforce());

    @Test
    public void testDateTypes() throws Exception {
        Delete delete = (Delete) translationUtility.parseCommand("delete from contacts");
        
        SalesforceConnection connection = Mockito.mock(SalesforceConnection.class);
        
        SalesForceExecutionFactory config = new SalesForceExecutionFactory();
        
        DeleteExecutionImpl updateExecution = new DeleteExecutionImpl(config, delete, connection, Mockito.mock(RuntimeMetadata.class), Mockito.mock(ExecutionContext.class));
        
        ArgumentCaptor<String> queryArgument = ArgumentCaptor.forClass(String.class);
        QueryResult qr = new QueryResult();
        SObject so = new SObject();
        so.setType("Contact");
        so.addField("Id", "x");
        qr.setRecords(new SObject[] {so});
        qr.setSize(1);
        qr.setDone(false);
        
        Mockito.stub(connection.query(queryArgument.capture(), Mockito.anyInt(), Mockito.anyBoolean())).toReturn(qr);
        Mockito.stub(connection.delete(new String[] {"x"})).toReturn(1);
        
        while(true) {
            try {
                updateExecution.execute();
                org.junit.Assert.assertArrayEquals(new int[] {1}, updateExecution.getUpdateCounts());
                break;
            } catch(DataNotAvailableException e) {
                continue;
            }
        }
        
        Mockito.verify(connection, Mockito.times(1)).query(queryArgument.capture(), Mockito.anyInt(), Mockito.anyBoolean());
        
        String query = queryArgument.getValue();
        assertEquals("SELECT Id FROM Contact ", query);

    }
}
