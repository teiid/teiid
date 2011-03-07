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

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import junit.framework.TestCase;

import org.teiid.cdk.api.TranslationUtility;
import org.teiid.cdk.unittest.FakeTranslationFactory;
import org.teiid.language.Condition;
import org.teiid.language.LanguageFactory;
import org.teiid.language.LanguageUtil;
import org.teiid.language.Select;



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

    private Condition convertCriteria(String criteriaStr) {
        // Create ICriteria from criteriaStr
        TranslationUtility util = FakeTranslationFactory.getInstance().getBQTTranslationUtility();
        String sql = "SELECT IntKey FROM BQT1.SmallA WHERE " + criteriaStr; //$NON-NLS-1$
        Select query = (Select) util.parseCommand(sql);
        Condition criteria = query.getWhere();
        return criteria;
    }
    
    public void helpTestSeparateByAnd(String criteriaStr, String[] expected) throws Exception {
        Condition criteria = convertCriteria(criteriaStr);

        // Execute        
        List<Condition> crits = LanguageUtil.separateCriteriaByAnd(criteria);
        
        // Build expected and actual sets
        Set<String> expectedSet = new HashSet<String>(Arrays.asList(expected));
        Set<String> actualSet = new HashSet<String>();
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
        helpTestSeparateByAnd("intkey = 1 OR intnum = 2", new String[] { "SmallA.IntKey = 1 OR SmallA.IntNum = 2" }); //$NON-NLS-1$ //$NON-NLS-2$
    }

    public void testSeparateCrit_nestedAND() throws Exception {
        helpTestSeparateByAnd("((intkey = 1 AND intnum = 2) AND (stringnum = '3') AND (stringkey = '4'))",  //$NON-NLS-1$
            new String[] { "SmallA.IntKey = 1", //$NON-NLS-1$
                "SmallA.IntNum = 2",  //$NON-NLS-1$
                "SmallA.StringNum = '3'", //$NON-NLS-1$
                "SmallA.StringKey = '4'" }); //$NON-NLS-1$ 
    }

    public void testSeparateCrit_NOT() throws Exception {
        helpTestSeparateByAnd("(NOT (intkey = 1 AND intnum = 2) AND (stringnum = '3') AND (stringkey = '4'))",  //$NON-NLS-1$
            new String[] { "SmallA.IntKey <> 1 OR SmallA.IntNum <> 2", //$NON-NLS-1$
                "SmallA.StringNum = '3'", //$NON-NLS-1$
                "SmallA.StringKey = '4'" }); //$NON-NLS-1$        
    }

    public void helpTestCombineCriteria(String primaryStr, String additionalStr, String expected) throws Exception {
        Condition primaryCrit = (primaryStr == null ? null : convertCriteria(primaryStr));
        Condition additionalCrit = (additionalStr == null ? null : convertCriteria(additionalStr));

        // Execute        
        Condition crit = LanguageUtil.combineCriteria(primaryCrit, additionalCrit, LanguageFactory.INSTANCE);
        
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
        helpTestCombineCriteria("intkey = 1", "intkey = 2", "SmallA.IntKey = 1 AND SmallA.IntKey = 2"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
    }

    public void testCombineCrit_primaryPredicate() throws Exception {
        helpTestCombineCriteria("intkey = 1", "intnum = 2 AND stringkey = '3'", "SmallA.IntKey = 1 AND SmallA.IntNum = 2 AND SmallA.StringKey = '3'"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
    }

    public void testCombineCrit_additionalPredicate() throws Exception {
        helpTestCombineCriteria("intkey = 1 AND intnum = 2", "stringkey = '3'", "SmallA.IntKey = 1 AND SmallA.IntNum = 2 AND SmallA.StringKey = '3'"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
    }
    
}
