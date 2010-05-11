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

package org.teiid.query.optimizer.xml;

import java.util.Collection;

import org.teiid.api.exception.query.QueryParserException;
import org.teiid.query.optimizer.xml.ContextReplacerVisitor;
import org.teiid.query.parser.QueryParser;
import org.teiid.query.sql.lang.Criteria;
import org.teiid.query.sql.symbol.GroupSymbol;

import junit.framework.TestCase;


/**
 */
public class TestContextReplacerVisitor extends TestCase {

	public TestContextReplacerVisitor(String name) {
	    super(name);
	}	
	
	public GroupSymbol exampleGroupSymbol(int number) { 
		return new GroupSymbol("group." + number); //$NON-NLS-1$
	}
	
	public void helpTestReplacer(String critString, String expectedCrit, int numContexts) throws QueryParserException {
        Criteria crit = QueryParser.getQueryParser().parseCriteria(critString);
        
		Collection contexts = ContextReplacerVisitor.replaceContextFunctions(crit);
		assertTrue("Actual crit " +  crit + " didn't meet expected crit " + expectedCrit, expectedCrit.equalsIgnoreCase(crit.toString())); //$NON-NLS-1$ //$NON-NLS-2$
        assertTrue("Should've gotten " + numContexts + " but got " + contexts.size(), contexts.size() == numContexts); //$NON-NLS-1$ //$NON-NLS-2$
	}
		
	public void testSimplePredicate() throws Exception {
        helpTestReplacer("context(a, x)='3'", "x = '3'", 1); //$NON-NLS-1$ //$NON-NLS-2$
	}

    /**
     * per 7332 - case-sensitivity bug
     */
    public void testSimplePredicate2() throws Exception {
        helpTestReplacer("Context(a, x)='3'", "x = '3'", 1); //$NON-NLS-1$ //$NON-NLS-2$
    }

    public void testConjunct() throws Exception {
        helpTestReplacer("context(a, x)='3' OR context(b, y)='j'", "(x = '3') or (y = 'j')", 2); //$NON-NLS-1$ //$NON-NLS-2$
    }

}
