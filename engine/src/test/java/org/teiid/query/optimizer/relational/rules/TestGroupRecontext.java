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

package org.teiid.query.optimizer.relational.rules;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.teiid.query.optimizer.relational.rules.FrameUtil;
import org.teiid.query.optimizer.relational.rules.RulePlaceAccess;
import org.teiid.query.sql.lang.Query;
import org.teiid.query.sql.lang.SubqueryCompareCriteria;
import org.teiid.query.sql.symbol.ElementSymbol;
import org.teiid.query.sql.symbol.GroupSymbol;

import junit.framework.TestCase;


public class TestGroupRecontext extends TestCase {

	// ################################## FRAMEWORK ################################
	
	public TestGroupRecontext(String name) { 
		super(name);
	}	

	// ################################## TEST HELPERS ################################
	
	public void helpTestRecontextGroup(String oldGroupName, String oldGroupDefinition, String[] knownGroupNames, String expectedName) {
	    GroupSymbol oldSymbol = null;	    
	    if(oldGroupDefinition == null) { 
	    	oldSymbol = new GroupSymbol(oldGroupName);    
	    } else {
	        oldSymbol = new GroupSymbol(oldGroupName, oldGroupDefinition);
	    }	    
	    oldSymbol.setMetadataID(oldSymbol.getName());		// Dummy metadata ID
	    	    	    
	    Set<String> known = new HashSet<String>();
	    for (int i = 0; i < knownGroupNames.length; i++) {
            known.add(knownGroupNames[i].toUpperCase());
        }
	    
        GroupSymbol newSymbol = RulePlaceAccess.recontextSymbol(oldSymbol, known);
		
		assertEquals("New recontexted group name is not as expected: ", expectedName, newSymbol.getName());		 //$NON-NLS-1$
	}
	
	// ################################## ACTUAL TESTS ################################

	public void testRecontextGroup1() {
		helpTestRecontextGroup("abc", "m.g", new String[] {}, "abc__1"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
	}
	
	public void testRecontextGroup2() {
		helpTestRecontextGroup("m.g", null, new String[] {}, "g__1"); //$NON-NLS-1$ //$NON-NLS-2$
	}

	public void testRecontextGroup3() {
		helpTestRecontextGroup("abc__1", "m.g", new String[] {}, "abc__2"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
	}
	
	public void testRecontextGroup4() {
		helpTestRecontextGroup("abc__x", "m.g", new String[] {}, "abc__x__1"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
	}

	public void testRecontextGroup5() {
		helpTestRecontextGroup("abc__x", "m.g", new String[] {}, "abc__x__1"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
	}

	public void testRecontextGroup6() {
		helpTestRecontextGroup("abc__", "m.g", new String[] {}, "abc____1"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
	}

	public void testRecontextGroup7() {
		helpTestRecontextGroup("abc____1", "m.g", new String[] {}, "abc____2"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
	}

	public void testRecontextGroup8() {
		helpTestRecontextGroup("abc", "m.g", new String[] {"abc__1"}, "abc__2"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
	}
	
	public void testRecontextGroup9() {
		helpTestRecontextGroup("abc__1", "m.g", new String[] {"abc__2", "abc__3"}, "abc__4"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$ //$NON-NLS-5$
	}

	public void testRecontextGroup10() {
		helpTestRecontextGroup("m.c.g", null, new String[] {"g__1" }, "g__2"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
	}

    public void testRecontextGroup11() {
        helpTestRecontextGroup("m.c.g", null, new String[] {"G__1" }, "g__2"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
    }
	
    public void testConvertSubqueryCompareCriteria() throws Exception{
        ElementSymbol e1 = new ElementSymbol("e1"); //$NON-NLS-1$
        ElementSymbol x1 = new ElementSymbol("x1"); //$NON-NLS-1$
        Map<ElementSymbol, ElementSymbol> symbolMap = new HashMap<ElementSymbol, ElementSymbol>();
        symbolMap.put(e1, x1);
        
        Query query = new Query();
        SubqueryCompareCriteria crit = new SubqueryCompareCriteria(e1, query, SubqueryCompareCriteria.EQ, SubqueryCompareCriteria.ALL);
        SubqueryCompareCriteria expected = new SubqueryCompareCriteria(x1, query, SubqueryCompareCriteria.EQ, SubqueryCompareCriteria.ALL);

        FrameUtil.convertCriteria(crit, symbolMap, null, true);  
        
        assertEquals(crit, expected);
    }
	
}
