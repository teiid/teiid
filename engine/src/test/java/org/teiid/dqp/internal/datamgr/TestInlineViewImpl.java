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
