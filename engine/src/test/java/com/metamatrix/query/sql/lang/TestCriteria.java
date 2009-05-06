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

package com.metamatrix.query.sql.lang;

import java.util.Arrays;
import java.util.Collection;

import junit.framework.*;

import com.metamatrix.api.exception.query.QueryParserException;
import com.metamatrix.query.parser.QueryParser;
import com.metamatrix.query.sql.symbol.Constant;
import com.metamatrix.query.sql.symbol.ElementSymbol;

/**
 */
public class TestCriteria extends TestCase {

    /**
     * Constructor for TestCriteria.
     * @param arg0
     */
    public TestCriteria(String arg0) {
        super(arg0);
    }

	public CompareCriteria exampleCompareCrit(int num) {
		return new CompareCriteria(
			new ElementSymbol("" + num),  //$NON-NLS-1$
			CompareCriteria.EQ, 
			new Constant("" + num)); //$NON-NLS-1$
	}

	public void helpTestSeparateCriteria(Criteria originalCrit, Criteria[] partsArray) {
		Collection expectedParts = Arrays.asList(partsArray);
		Collection actualParts = Criteria.separateCriteriaByAnd(originalCrit);
		
		assertEquals("Didn't get the same parts ", expectedParts, actualParts); //$NON-NLS-1$
	}

    public void testSeparateCriteriaByAnd1() {
    	CompareCriteria crit1 = exampleCompareCrit(1);    	
    	helpTestSeparateCriteria(crit1, new Criteria[] { crit1 });
    }

    public void testSeparateCriteriaByAnd2() {
    	CompareCriteria crit1 = exampleCompareCrit(1);    	
    	CompareCriteria crit2 = exampleCompareCrit(2);  
    	CompoundCriteria compCrit = new CompoundCriteria();
    	compCrit.setOperator(CompoundCriteria.AND);
    	compCrit.addCriteria(crit1);
    	compCrit.addCriteria(crit2);
    	helpTestSeparateCriteria(compCrit, new Criteria[] { crit1, crit2 });
    }

    public void testSeparateCriteriaByAnd3() {
    	CompareCriteria crit1 = exampleCompareCrit(1);    	
    	CompareCriteria crit2 = exampleCompareCrit(2);  
    	CompareCriteria crit3 = exampleCompareCrit(3);  
    	CompoundCriteria compCrit1 = new CompoundCriteria();
    	compCrit1.setOperator(CompoundCriteria.AND);
    	compCrit1.addCriteria(crit2);
    	compCrit1.addCriteria(crit3);
    	CompoundCriteria compCrit2 = new CompoundCriteria();
    	compCrit2.setOperator(CompoundCriteria.AND);
    	compCrit2.addCriteria(crit1);
    	compCrit2.addCriteria(compCrit1);
    	helpTestSeparateCriteria(compCrit2, new Criteria[] { crit1, crit2, crit3 });
    }

    public void testSeparateCriteriaByAnd4() {
    	CompareCriteria crit1 = exampleCompareCrit(1);    	
    	CompareCriteria crit2 = exampleCompareCrit(2);  
    	CompareCriteria crit3 = exampleCompareCrit(3);  
    	CompoundCriteria compCrit1 = new CompoundCriteria();
    	compCrit1.setOperator(CompoundCriteria.OR);
    	compCrit1.addCriteria(crit2);
    	compCrit1.addCriteria(crit3);
    	CompoundCriteria compCrit2 = new CompoundCriteria();
    	compCrit2.setOperator(CompoundCriteria.AND);
    	compCrit2.addCriteria(crit1);
    	compCrit2.addCriteria(compCrit1);
    	helpTestSeparateCriteria(compCrit2, new Criteria[] { crit1, compCrit1 });
    }

    public void testSeparateCriteriaByAnd5() {
    	CompareCriteria crit1 = exampleCompareCrit(1);    	
    	CompareCriteria crit2 = exampleCompareCrit(2);  
    	CompareCriteria crit3 = exampleCompareCrit(3);  
    	CompoundCriteria compCrit1 = new CompoundCriteria();
    	compCrit1.setOperator(CompoundCriteria.AND);
    	compCrit1.addCriteria(crit2);
    	compCrit1.addCriteria(crit3);
    	NotCriteria notCrit = new NotCriteria(compCrit1);
    	CompoundCriteria compCrit2 = new CompoundCriteria();
    	compCrit2.setOperator(CompoundCriteria.AND);
    	compCrit2.addCriteria(crit1);
    	compCrit2.addCriteria(notCrit);
    	helpTestSeparateCriteria(compCrit2, new Criteria[] { crit1, notCrit });
    }

	public void helpTestCombineCriteria(Criteria crit1, Criteria crit2, Criteria expected) {
		Criteria actual = Criteria.combineCriteria(crit1, crit2);
		assertEquals("Didn't combine the criteria correctly ", expected, actual); //$NON-NLS-1$
	}

    public void testCombineCriteria1() {
    	helpTestCombineCriteria(null, null, null);
    }

    public void testCombineCriteria2() {
    	helpTestCombineCriteria(exampleCompareCrit(1), null, exampleCompareCrit(1));
    }
    
    public void testCombineCriteria3() {
    	helpTestCombineCriteria(null, exampleCompareCrit(1), exampleCompareCrit(1));
    }
    
    public void testCombineCriteria4() {
    	CompareCriteria crit1 = exampleCompareCrit(1);    	
    	CompareCriteria crit2 = exampleCompareCrit(2);  
    	CompoundCriteria compCrit = new CompoundCriteria();
    	compCrit.setOperator(CompoundCriteria.AND);
    	compCrit.addCriteria(crit1);
    	compCrit.addCriteria(crit2);
    	helpTestCombineCriteria(crit1, crit2, compCrit);
    }

    public void testCombineCriteria5() {
    	CompareCriteria crit1 = exampleCompareCrit(1);    	
    	CompareCriteria crit2 = exampleCompareCrit(2);  
    	CompareCriteria crit3 = exampleCompareCrit(3);  
    	CompoundCriteria compCrit = new CompoundCriteria();
    	compCrit.setOperator(CompoundCriteria.AND);
    	compCrit.addCriteria(crit1);
    	compCrit.addCriteria(crit2);
    	
    	CompoundCriteria compCrit2 = new CompoundCriteria();
    	compCrit2.setOperator(CompoundCriteria.AND);
    	compCrit2.addCriteria(crit1);
    	compCrit2.addCriteria(crit2);
    	compCrit2.addCriteria(crit3);

    	helpTestCombineCriteria(compCrit, crit3, compCrit2);
    }

    public void testCombineCriteria6() {
    	CompareCriteria crit1 = exampleCompareCrit(1);    	
    	CompareCriteria crit2 = exampleCompareCrit(2);  
    	CompareCriteria crit3 = exampleCompareCrit(3);  
    	CompoundCriteria compCrit = new CompoundCriteria();
    	compCrit.setOperator(CompoundCriteria.AND);
    	compCrit.addCriteria(crit1);
    	compCrit.addCriteria(crit2);
    	
    	CompoundCriteria compCrit2 = new CompoundCriteria();
    	compCrit2.setOperator(CompoundCriteria.AND);
    	compCrit2.addCriteria(crit3);
    	compCrit2.addCriteria(crit1);
    	compCrit2.addCriteria(crit2);
    	helpTestCombineCriteria(crit3, compCrit, compCrit2);
    }
    
    private void helpTestNormalize(String critString,
                             String resultString, String cnfString) throws QueryParserException {
        QueryParser parser = new QueryParser();
        Criteria crit = parser.parseCriteria(critString);
        
        assertEquals(resultString, Criteria.normalize(crit, true).toString());
        assertEquals(cnfString, Criteria.normalize(crit, false).toString());
    }

    public void testNF() throws Exception {
        String critString = "((4 = '4') AND (5 = '5')) OR ((1 = '1') AND ((2 = '2') OR (3 = '3')))"; //$NON-NLS-1$
        String resultString = "((4 = '4') AND (5 = '5')) OR ((1 = '1') AND (2 = '2')) OR ((1 = '1') AND (3 = '3'))"; //$NON-NLS-1$
        String cnf = "((4 = '4') OR (1 = '1')) AND ((5 = '5') OR (1 = '1')) AND ((4 = '4') OR ((2 = '2') OR (3 = '3'))) AND ((5 = '5') OR ((2 = '2') OR (3 = '3')))"; //$NON-NLS-1$
        helpTestNormalize(critString, resultString, cnf); 
    }
    
    public void testNF1() throws Exception {
        String critString = "4 = '4'"; //$NON-NLS-1$
        String resultString = "4 = '4'"; //$NON-NLS-1$ 
        helpTestNormalize(critString, resultString, resultString); 
    }

    public void testNF2() throws Exception {
        String critString = "((4 = '4') OR (1 = '1') OR (2 = '2')) AND ((3 = '3') OR (5 = '5'))"; //$NON-NLS-1$
        String resultString = "((4 = '4') AND (3 = '3')) OR ((1 = '1') AND (3 = '3')) OR ((2 = '2') AND (3 = '3')) OR ((4 = '4') AND (5 = '5')) OR ((1 = '1') AND (5 = '5')) OR ((2 = '2') AND (5 = '5'))"; //$NON-NLS-1$
        String cnf = "((4 = '4') OR (1 = '1') OR (2 = '2')) AND ((3 = '3') OR (5 = '5'))"; //$NON-NLS-1$
        helpTestNormalize(critString, resultString, cnf); 
    }
    
    public void testNF3() throws Exception {
        String critString = "NOT (((1 = '1') OR (2 = '2')) AND ((3 = '3') OR (5 = '5')))"; //$NON-NLS-1$
        String resultString = "((NOT (1 = '1')) AND (NOT (2 = '2'))) OR ((NOT (3 = '3')) AND (NOT (5 = '5')))"; //$NON-NLS-1$
        String cnf = "((NOT (1 = '1')) OR (NOT (3 = '3'))) AND ((NOT (2 = '2')) OR (NOT (3 = '3'))) AND ((NOT (1 = '1')) OR (NOT (5 = '5'))) AND ((NOT (2 = '2')) OR (NOT (5 = '5')))"; //$NON-NLS-1$
        helpTestNormalize(critString, resultString, cnf); 
    }
    
    public void testNF4() throws Exception {
        String critString = "(1 = '1') OR (2 = '2')"; //$NON-NLS-1$
        String resultString = "(1 = '1') OR (2 = '2')"; //$NON-NLS-1$
        String cnf = "(1 = '1') OR (2 = '2')"; //$NON-NLS-1$
        helpTestNormalize(critString, resultString, cnf); 
    }

}
