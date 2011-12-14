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
