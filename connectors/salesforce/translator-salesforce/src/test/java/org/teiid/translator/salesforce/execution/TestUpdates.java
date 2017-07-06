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
    public void testIds() throws Exception {
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
        qr.setDone(true);
        
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
