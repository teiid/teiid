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


import org.teiid.language.DerivedTable;
import org.teiid.query.sql.lang.SubqueryFromClause;


import junit.framework.TestCase;

public class TestInlineViewImpl extends TestCase {

    public TestInlineViewImpl(String name) {
        super(name);
    }

    public static SubqueryFromClause helpExample() {
        return new SubqueryFromClause("xyz", TestQueryImpl.helpExample(true)); //$NON-NLS-1$
    }

    public static DerivedTable example() throws Exception {
        return (DerivedTable)TstLanguageBridgeFactory.factory.translate(helpExample());
    }

    public void testGetName() throws Exception {
        assertEquals("xyz", example().getCorrelationName()); //$NON-NLS-1$
    }

    public void testGetQuery() throws Exception {
        assertEquals("SELECT DISTINCT g1.e1, g1.e2, g1.e3, g1.e4 FROM g1, g2 AS myAlias, g3, g4 WHERE 100 >= 200 AND 500 < 600 GROUP BY g1.e1, g1.e2, g1.e3, g1.e4 HAVING 100 >= 200 AND 500 < 600 ORDER BY g1.e1, g1.e2 DESC, g1.e3, g1.e4 DESC", example().getQuery().toString()); //$NON-NLS-1$
    }

}
