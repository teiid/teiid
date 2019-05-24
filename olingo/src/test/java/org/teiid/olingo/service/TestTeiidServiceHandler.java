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
package org.teiid.olingo.service;

import org.junit.Test;
import org.teiid.olingo.service.TeiidServiceHandler;
import static org.junit.Assert.*;
public class TestTeiidServiceHandler {

    @Test
    public void testSkipToken() throws Exception {
        TeiidServiceHandler handler = new TeiidServiceHandler("foo");

        assertEquals("$skiptoken=xxx",
                handler.buildNextToken("$skiptoken=sdhhdd--23", "xxx"));
        assertEquals("$filter=e1 eq '1'&$skiptoken=xxx",
                handler.buildNextToken("$filter=e1 eq '1'&$skiptoken=sdhhdd--23", "xxx"));
        assertEquals("$filter=e1 eq '1'&$top=23&$skiptoken=xxx",
                handler.buildNextToken("$filter=e1 eq '1'&$skiptoken=sdhhdd--23&$top=23", "xxx"));

    }
}
