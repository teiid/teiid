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

package org.teiid.dqp.internal.datamgr.language;

import org.teiid.connector.language.IQuery;
import org.teiid.connector.language.ISelectSymbol;
import org.teiid.dqp.internal.datamgr.language.ScalarSubqueryImpl;

import junit.framework.TestCase;

import com.metamatrix.query.sql.lang.Query;
import com.metamatrix.query.sql.symbol.ScalarSubquery;
import com.metamatrix.query.sql.symbol.SingleElementSymbol;

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

    public static ScalarSubquery helpExample() {
        Query query = TestQueryImpl.helpExample();
        ScalarSubquery ss = new ScalarSubquery(query);
        ss.setType(((SingleElementSymbol)query.getProjectedSymbols().get(0)).getType());
        return ss;
    }
    
    public static ScalarSubqueryImpl example() throws Exception {
        return (ScalarSubqueryImpl)TstLanguageBridgeFactory.factory.translate(helpExample());
    }

    public void testGetQuery() throws Exception {
        assertNotNull(example().getQuery());    }
    
    public void testGetType() throws Exception {
        IQuery query = TstLanguageBridgeFactory.factory.translate(TestQueryImpl.helpExample());
        Class firstSymbolType = ((ISelectSymbol) query.getSelect().getSelectSymbols().get(0)).getExpression().getType();
                
        assertEquals("Got incorrect type", firstSymbolType, example().getType()); //$NON-NLS-1$
    }
    
}
