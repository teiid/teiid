/*
 * JBoss, Home of Professional Open Source.
 * See the COPYRIGHT.txt file distributed with this work for information
 * regarding copyright ownership.  Some portions may be licensed
 * to Red Hat, Inc. under one or more contributor license agreements.
 * 
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 2.1 of the License, or (at your option) any later version.
 * 
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 * 
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA
 * 02110-1301 USA.
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
