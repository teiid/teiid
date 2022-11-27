package org.teiid.translator.salesforce;

import static org.junit.Assert.*;

import org.junit.Test;
import org.teiid.translator.salesforce34.SalesForce34ExecutionFactory;
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

import com.sforce.soap.partner.QueryResult;
import com.sforce.soap.partner.sobject.SObject;

public class TestSalesForce34ExecutionFactory {

    @Test public void testCreateResults() {
        SalesForce34ExecutionFactory ef = new SalesForce34ExecutionFactory();
        QueryResult qr = ef.buildQueryResult(new SObject[1]);
        assertTrue(qr.isDone());
        assertEquals(1, qr.getSize());
    }

}
