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

package org.teiid.translator.couchbase;

import static org.junit.Assert.*;

import java.math.BigDecimal;
import java.math.BigInteger;

import org.junit.Test;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.TypeFacility;

import com.couchbase.client.java.document.json.JsonArray;

public class TestCouchbase {

    @Test public void testConvert() throws TranslatorException {
        CouchbaseExecutionFactory cef = new CouchbaseExecutionFactory();
        cef.start();
        assertFalse(cef.supportsConvert(TypeFacility.RUNTIME_CODES.STRING, TypeFacility.RUNTIME_CODES.DATE));
        assertTrue(cef.supportsConvert(TypeFacility.RUNTIME_CODES.STRING, TypeFacility.RUNTIME_CODES.DOUBLE));
    }

    @Test(expected=IllegalArgumentException.class) public void testSetValue() {
        CouchbaseExecutionFactory cef = new CouchbaseExecutionFactory();
        JsonArray array = JsonArray.create();
        cef.setValue(array, Float.class, 1.0f);
        assertEquals(1.0f, array.get(0));
    }

    @Test public void testRetrieve() throws TranslatorException {
        CouchbaseExecutionFactory cef = new CouchbaseExecutionFactory();
        assertEquals(BigInteger.valueOf(1), cef.retrieveValue(BigInteger.class, new BigDecimal(1)));
        assertEquals(BigInteger.valueOf(1), cef.retrieveValue(BigInteger.class, 1));
        assertEquals(BigDecimal.valueOf(2), cef.retrieveValue(BigDecimal.class, BigInteger.valueOf(2)));
    }

    @Test public void testUseDouble() throws TranslatorException {
        CouchbaseExecutionFactory cef = new CouchbaseExecutionFactory();
        cef.setUseDouble(false);
        cef.start();
        assertTrue(cef.supportsConvert(TypeFacility.RUNTIME_CODES.STRING, TypeFacility.RUNTIME_CODES.LONG));
        cef.setUseDouble(true);
        assertFalse(cef.supportsConvert(TypeFacility.RUNTIME_CODES.STRING, TypeFacility.RUNTIME_CODES.LONG));
    }
}
