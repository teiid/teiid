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

package org.teiid.connector.language;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.teiid.connector.language.ICriteria;
import org.teiid.connector.language.IQuery;
import org.teiid.connector.language.LanguageUtil;
import org.teiid.dqp.internal.datamgr.language.LanguageFactoryImpl;

import junit.framework.TestCase;

import com.metamatrix.cdk.api.TranslationUtility;
import com.metamatrix.cdk.unittest.FakeTranslationFactory;

/**
 */
public class TestLanguageUtil extends TestCase {

    /**
     * Constructor for TestLanguageUtil.
     * @param name
     */
    public TestLanguageUtil(String name) {
        super(name);
    }

    private ICriteria convertCriteria(String criteriaStr) {
        // Create ICriteria from criteriaStr
        TranslationUtility util = FakeTranslationFactory.getInstance().getBQTTranslationUtility();
        String sql = "SELECT IntKey FROM BQT1.SmallA WHERE " + criteriaStr; //$NON-NLS-1$
        IQuery query = (IQuery) util.parseCommand(sql);
        ICriteria criteria = query.getWhere();
        return criteria;
    }
    
    public void helpTestSeparateByAnd(String criteriaStr, String[] expected) throws Exception {
        ICriteria criteria = convertCriteria(criteriaStr);

        // Execute        
        List crits = LanguageUtil.separateCriteriaByAnd(criteria);
        
        // Build expected and actual sets
        Set expectedSet = new HashSet();
        for(int i=0; i<expected.length; i++) {
            expectedSet.add(expected[i]);
        }
        Set actualSet = new HashSet();
        for(int i=0; i<crits.size(); i++) {
            actualSet.add(crits.get(i).toString());
        }
        
        // Compare
        assertEquals("Did not get expected criteria pieces", expectedSet, actualSet); //$NON-NLS-1$
    }

    public void testSeparateCrit_predicate() throws Exception {
        helpTestSeparateByAnd("intkey = 1", new String[] { "SmallA.IntKey = 1" }); //$NON-NLS-1$ //$NON-NLS-2$
    }

    public void testSeparateCrit_ORisConjunct() throws Exception {
        helpTestSeparateByAnd("intkey = 1 OR intkey = 2", new String[] { "(SmallA.IntKey = 1) OR (SmallA.IntKey = 2)" }); //$NON-NLS-1$ //$NON-NLS-2$
    }

    public void testSeparateCrit_nestedAND() throws Exception {
        helpTestSeparateByAnd("((intkey = 1 AND intkey = 2) AND (intkey = 3) AND (intkey = 4))",  //$NON-NLS-1$
            new String[] { "SmallA.IntKey = 1", //$NON-NLS-1$
                "SmallA.IntKey = 2",  //$NON-NLS-1$
                "SmallA.IntKey = 3", //$NON-NLS-1$
                "SmallA.IntKey = 4" }); //$NON-NLS-1$ 
    }

    public void testSeparateCrit_NOT() throws Exception {
        helpTestSeparateByAnd("((NOT (intkey = 1 AND intkey = 2)) AND (intkey = 3) AND (intkey = 4))",  //$NON-NLS-1$
            new String[] { "NOT ((SmallA.IntKey = 1) AND (SmallA.IntKey = 2))", //$NON-NLS-1$
                "SmallA.IntKey = 3", //$NON-NLS-1$
                "SmallA.IntKey = 4" }); //$NON-NLS-1$        
    }

    public void helpTestCombineCriteria(String primaryStr, String additionalStr, String expected) throws Exception {
        ICriteria primaryCrit = (primaryStr == null ? null : convertCriteria(primaryStr));
        ICriteria additionalCrit = (additionalStr == null ? null : convertCriteria(additionalStr));

        // Execute        
        ICriteria crit = LanguageUtil.combineCriteria(primaryCrit, additionalCrit, LanguageFactoryImpl.INSTANCE);
        
        // Compare
        String critStr = (crit == null ? null : crit.toString());
        assertEquals("Did not get expected criteria", expected, critStr); //$NON-NLS-1$
    }
    
    public void testCombineCrit_bothNull() throws Exception {
        helpTestCombineCriteria(null, null, null);
    }

    public void testCombineCrit_primaryNull() throws Exception {
        helpTestCombineCriteria(null, "intkey = 1", "SmallA.IntKey = 1");  //$NON-NLS-1$//$NON-NLS-2$
    }

    public void testCombineCrit_additionalNull() throws Exception {
        helpTestCombineCriteria("intkey = 1", null, "SmallA.IntKey = 1"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    public void testCombineCrit_bothPredicates() throws Exception {
        helpTestCombineCriteria("intkey = 1", "intkey = 2", "(SmallA.IntKey = 1) AND (SmallA.IntKey = 2)"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
    }

    public void testCombineCrit_primaryPredicate() throws Exception {
        helpTestCombineCriteria("intkey = 1", "intkey = 2 AND intkey = 3", "(SmallA.IntKey = 1) AND ((SmallA.IntKey = 2) AND (SmallA.IntKey = 3))"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
    }

    public void testCombineCrit_additionalPredicate() throws Exception {
        helpTestCombineCriteria("intkey = 1 AND intkey = 2", "intkey = 3", "(SmallA.IntKey = 1) AND (SmallA.IntKey = 2) AND (SmallA.IntKey = 3)"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
    }
    
}
