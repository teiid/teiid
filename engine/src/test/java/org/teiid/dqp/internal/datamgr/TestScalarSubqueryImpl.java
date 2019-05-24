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

package org.teiid.dqp.internal.datamgr;

import junit.framework.TestCase;

import org.teiid.language.ScalarSubquery;
import org.teiid.language.Select;
import org.teiid.query.sql.lang.Query;
import org.teiid.query.sql.symbol.Expression;


/**
 */
public class TestScalarSubqueryImpl extends TestCase {

    /**
     * Constructor for TestScalarSubqueryImpl.
     * @param name
     */
    public TestScalarSubqueryImpl(String name) {
        super(name);
    }

    public static org.teiid.query.sql.symbol.ScalarSubquery helpExample() {
        Query query = TestQueryImpl.helpExample(true);
        org.teiid.query.sql.symbol.ScalarSubquery ss = new org.teiid.query.sql.symbol.ScalarSubquery(query);
        ss.setType(((Expression)query.getProjectedSymbols().get(0)).getType());
        return ss;
    }

    public static ScalarSubquery example() throws Exception {
        return (ScalarSubquery)TstLanguageBridgeFactory.factory.translate(helpExample());
    }

    public void testGetQuery() throws Exception {
        assertNotNull(example().getSubquery());    }

    public void testGetType() throws Exception {
        Select query = TstLanguageBridgeFactory.factory.translate(TestQueryImpl.helpExample(true));
        Class<?> firstSymbolType = query.getDerivedColumns().get(0).getExpression().getType();
        assertEquals("Got incorrect type", firstSymbolType, example().getType()); //$NON-NLS-1$
    }

}
