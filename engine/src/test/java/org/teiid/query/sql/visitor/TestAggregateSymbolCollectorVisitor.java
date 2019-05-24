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

package org.teiid.query.sql.visitor;

import java.util.ArrayList;
import java.util.List;

import junit.framework.TestCase;

import org.teiid.api.exception.query.QueryParserException;
import org.teiid.query.parser.QueryParser;
import org.teiid.query.sql.lang.Command;
import org.teiid.query.sql.symbol.Expression;



public class TestAggregateSymbolCollectorVisitor extends TestCase {

    public TestAggregateSymbolCollectorVisitor(String name) {
        super(name);
    }

    public void helpTestCollectAggregates(String sql, String[] aggs, String[] elems) throws QueryParserException {
        // Parse command
        Command command = QueryParser.getQueryParser().parseCommand(sql);

        // Find aggregates
        List<Expression> foundAggs = new ArrayList<Expression>();
        List<Expression> foundElements = new ArrayList<Expression>();
        AggregateSymbolCollectorVisitor.getAggregates(command, foundAggs, foundElements, null, null, null);

        // Compare
        assertEquals("Incorrect number of aggregates: " + foundAggs, aggs.length, foundAggs.size()); //$NON-NLS-1$
        for(int i=0; i<aggs.length; i++) {
            assertEquals("Incorrect agg match at " + i, aggs[i], foundAggs.get(i).toString()); //$NON-NLS-1$
        }

        assertEquals("Incorrect number of elements: " + foundElements, elems.length, foundElements.size()); //$NON-NLS-1$
        for(int i=0; i<elems.length; i++) {
            assertEquals("Incorrect agg match at " + i, elems[i], foundElements.get(i).toString()); //$NON-NLS-1$
        }

    }

    public void testCollectAggs1() throws QueryParserException {
        helpTestCollectAggregates("SELECT COUNT(*) FROM pm1.g1", new String[] { "COUNT(*)"}, new String[] { }); //$NON-NLS-1$ //$NON-NLS-2$
    }

    public void testCollectAggs2() throws QueryParserException {
        helpTestCollectAggregates("SELECT * FROM pm1.g1 HAVING MAX(e2) > 0",  //$NON-NLS-1$
            new String[] { "MAX(e2)"},  //$NON-NLS-1$
            new String[] { });
    }

    public void testCollectAggs3() throws QueryParserException {
        helpTestCollectAggregates(
            "SELECT COUNT(e1), MAX(DISTINCT e1) FROM pm1.g1 GROUP BY e1 HAVING MAX(e2) > 0 AND NOT MIN(e2) < 100",  //$NON-NLS-1$
            new String[] { "COUNT(e1)", "MAX(DISTINCT e1)", "MAX(e2)", "MIN(e2)"},  //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
            new String[] { "e1"}); //$NON-NLS-1$
    }

    public void testCollectAggs4() throws QueryParserException {
        helpTestCollectAggregates(
            "SELECT e1 FROM pm1.g1 GROUP BY e1 HAVING MAX(e2) > 0 AND NOT MIN(e2) < 100 AND e3 < 200",  //$NON-NLS-1$
            new String[] { "MAX(e2)", "MIN(e2)"},  //$NON-NLS-1$ //$NON-NLS-2$
            new String[] { "e1", "e1", "e3"}); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
    }


}
