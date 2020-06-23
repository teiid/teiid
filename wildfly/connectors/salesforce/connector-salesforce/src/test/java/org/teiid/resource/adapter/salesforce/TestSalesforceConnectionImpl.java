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

package org.teiid.resource.adapter.salesforce;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.Calendar;

import org.junit.Test;
import org.mockito.Mockito;
import org.teiid.salesforce.BaseSalesforceConnection;
import org.teiid.translator.salesforce.execution.DataPayload;
import org.teiid.translator.salesforce.execution.DeletedResult;

import com.sforce.soap.partner.DeletedRecord;
import com.sforce.soap.partner.GetDeletedResult;
import com.sforce.soap.partner.sobject.SObject;

@SuppressWarnings("nls")
public class TestSalesforceConnectionImpl {

    @Test public void testGetDeleted() throws Exception {
        TeiidPartnerConnection pc = Mockito.mock(TeiidPartnerConnection.class);
        GetDeletedResult gdr = new GetDeletedResult();
        Calendar c = Calendar.getInstance();
        gdr.setEarliestDateAvailable(c);
        gdr.setLatestDateCovered(c);
        DeletedRecord dr = new DeletedRecord();
        dr.setDeletedDate(c);
        dr.setId("id");
        gdr.setDeletedRecords(new DeletedRecord[] {dr});
        Mockito.stub(pc.getDeleted("x", null, null)).toReturn(gdr);
        SalesforceConnectionImpl sfci = new SalesforceConnectionImpl(pc);
        DeletedResult result = sfci.getDeleted("x", null, null);
        assertEquals(1, result.getResultRecords().size());
    }

    @Test public void testUpdateValues() throws Exception {
        DataPayload payload = new DataPayload();
        payload.addField("hello", "world");
        payload.addField("null", null);
        SObject obj = BaseSalesforceConnection.toUpdateSObject(new ArrayList<>(), payload);
        assertArrayEquals(new String [] {"null"}, obj.getFieldsToNull());
    }

}
