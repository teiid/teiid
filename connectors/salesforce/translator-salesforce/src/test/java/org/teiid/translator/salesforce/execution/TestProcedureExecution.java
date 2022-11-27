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

import java.util.Arrays;
import java.util.Calendar;

import org.junit.Test;
import org.mockito.Mockito;
import org.teiid.cdk.api.TranslationUtility;
import org.teiid.language.Call;
import org.teiid.metadata.RuntimeMetadata;
import org.teiid.translator.ExecutionContext;
import org.teiid.translator.salesforce.SalesforceConnection;
import org.teiid.translator.salesforce.execution.visitors.TestVisitors;

@SuppressWarnings("nls")
public class TestProcedureExecution {

    private static TranslationUtility translationUtility = new TranslationUtility(TestVisitors.exampleSalesforce());

    @Test public void testProcedureName() throws Exception {
        Call command = (Call)translationUtility.parseCommand("exec getupdated('foo', {d '1970-01-01'}, {d '1990-01-01'})"); //$NON-NLS-1$
        SalesforceConnection sfc = Mockito.mock(SalesforceConnection.class);
        UpdatedResult ur = new UpdatedResult();
        ur.setIDs(Arrays.asList("1", "2"));
        Mockito.stub(sfc.getUpdated(Mockito.eq("foo"), (Calendar)Mockito.anyObject(), (Calendar)Mockito.anyObject())).toReturn(ur);
        ProcedureExecutionParentImpl pepi = new ProcedureExecutionParentImpl(command, sfc, Mockito.mock(RuntimeMetadata.class), Mockito.mock(ExecutionContext.class));
        pepi.execute();
        assertNotNull(pepi.next());
        assertNotNull(pepi.next());
        assertNull(pepi.next());
        pepi.close();
    }

}
