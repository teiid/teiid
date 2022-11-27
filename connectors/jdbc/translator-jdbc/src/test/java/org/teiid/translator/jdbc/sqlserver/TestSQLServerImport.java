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

package org.teiid.translator.jdbc.sqlserver;

import static org.junit.Assert.*;

import org.junit.Test;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.jdbc.sqlserver.SQLServerExecutionFactory.SQLServerMetadataProcessor;

@SuppressWarnings("nls")
public class TestSQLServerImport {

    @Test public void testProcedureName() throws TranslatorException {
        SQLServerExecutionFactory ssef = new SQLServerExecutionFactory();
        ssef.start();

        SQLServerMetadataProcessor mp = (SQLServerMetadataProcessor) ssef.getMetadataProcessor();
        assertEquals("xyz", mp.modifyProcedureNameInSource("xyz;1"));
        assertEquals("xyz;1a", mp.modifyProcedureNameInSource("xyz;1a"));
    }

}
