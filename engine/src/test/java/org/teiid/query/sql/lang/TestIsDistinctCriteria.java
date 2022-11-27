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

package org.teiid.query.sql.lang;

import static org.junit.Assert.*;

import org.junit.Test;
import org.teiid.api.exception.query.QueryParserException;
import org.teiid.core.util.UnitTestUtil;
import org.teiid.query.parser.QueryParser;
import org.teiid.query.sql.proc.TriggerAction;

@SuppressWarnings("nls")
public class TestIsDistinctCriteria {

    @Test public void testParseClone() throws QueryParserException {
        TriggerAction ta = (TriggerAction)QueryParser.getQueryParser().parseProcedure("for each row begin atomic if (\"new\" is not distinct from \"old\") raise sqlexception ''; end", true);
        assertEquals("FOR EACH ROW\nBEGIN ATOMIC\nIF(\"new\" IS NOT DISTINCT FROM \"old\")\nBEGIN\nRAISE SQLEXCEPTION '';\nEND\nEND", ta.toString());
        QueryParser.getQueryParser().parseProcedure(ta.toString(), true);
        TriggerAction clone = ta.clone();
        assertEquals(ta.toString(), clone.toString());
        UnitTestUtil.helpTestEquivalence(0, ta, ta.clone());
    }

}
