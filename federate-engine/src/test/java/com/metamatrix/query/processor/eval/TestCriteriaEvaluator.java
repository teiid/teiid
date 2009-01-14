/*
 * JBoss, Home of Professional Open Source.
 * Copyright (C) 2008 Red Hat, Inc.
 * Copyright (C) 2000-2007 MetaMatrix, Inc.
 * Licensed to Red Hat, Inc. under one or more contributor 
 * license agreements.  See the copyright.txt file in the
 * distribution for a full listing of individual contributors.
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

package com.metamatrix.query.processor.eval;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import junit.framework.TestCase;

import com.metamatrix.api.exception.MetaMatrixComponentException;
import com.metamatrix.api.exception.query.CriteriaEvaluationException;
import com.metamatrix.common.buffer.BlockedException;
import com.metamatrix.query.eval.CriteriaEvaluator;
import com.metamatrix.query.sql.lang.CollectionValueIterator;
import com.metamatrix.query.sql.lang.Criteria;
import com.metamatrix.query.sql.lang.ExistsCriteria;
import com.metamatrix.query.sql.lang.IsNullCriteria;
import com.metamatrix.query.sql.lang.MatchCriteria;
import com.metamatrix.query.sql.lang.Query;
import com.metamatrix.query.sql.lang.SetCriteria;
import com.metamatrix.query.sql.lang.SubqueryCompareCriteria;
import com.metamatrix.query.sql.symbol.Constant;
import com.metamatrix.query.sql.symbol.ElementSymbol;

public class TestCriteriaEvaluator extends TestCase {

	// ################################## FRAMEWORK ################################
	
	public TestCriteriaEvaluator(String name) { 
		super(name);
	}	
	
	// ################################## TEST HELPERS ################################
	
    private void helpTestMatch(String value, String pattern, char escape, boolean negated, boolean expectedMatch) throws CriteriaEvaluationException, BlockedException, MetaMatrixComponentException {
        MatchCriteria crit = new MatchCriteria(new Constant(value), new Constant(pattern), escape);
        crit.setNegated(negated);
        boolean actualMatch = CriteriaEvaluator.evaluate(crit);
        // Compare actual and expected match
        assertEquals("Match criteria test failed for value=[" + value + "], pattern=[" + pattern + "], hasEscape=" + (escape != MatchCriteria.NULL_ESCAPE_CHAR) + ": ", expectedMatch, actualMatch); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
    }
    
    private void helpTestMatch(String value, String pattern, char escape, boolean expectedMatch) throws CriteriaEvaluationException, BlockedException, MetaMatrixComponentException {
        helpTestMatch(value, pattern, escape, false, expectedMatch);
    }
	
    private void helpTestIsNull(String value, boolean negated, boolean expectedMatch) throws CriteriaEvaluationException, BlockedException, MetaMatrixComponentException {
        IsNullCriteria criteria = new IsNullCriteria(new Constant(value));
        criteria.setNegated(negated);
        
        boolean result = CriteriaEvaluator.evaluate(criteria);
        assertEquals("Result did not match expected value", expectedMatch, result); //$NON-NLS-1$
    }
    
    private void helpTestSetCriteria(int value, boolean negated, boolean expectedMatch) throws CriteriaEvaluationException, BlockedException, MetaMatrixComponentException {
        helpTestSetCriteria(new Integer(value), negated, expectedMatch);
    }
    
    private void helpTestSetCriteria(Integer value, boolean negated, boolean expectedMatch) throws CriteriaEvaluationException, BlockedException, MetaMatrixComponentException {
        Collection constants = new ArrayList(2);
        constants.add(new Constant(new Integer(1000)));
        constants.add(new Constant(new Integer(5000)));
        SetCriteria crit = new SetCriteria(new Constant(value), constants);
        crit.setNegated(negated);
        boolean result = CriteriaEvaluator.evaluate(crit);
        assertEquals("Result did not match expected value", expectedMatch, result); //$NON-NLS-1$
    }
        
    private void helpTestCompareSubqueryCriteria(Criteria crit, boolean expectedResult) throws CriteriaEvaluationException, BlockedException, MetaMatrixComponentException{
        
        Map elementMap = new HashMap();
        ElementSymbol e1 = new ElementSymbol("e1"); //$NON-NLS-1$
        elementMap.put(e1, new Integer(0));
        
        List tuple = Arrays.asList(new String[]{"a"}); //$NON-NLS-1$
        
        assertEquals(expectedResult, new CriteriaEvaluator(elementMap, null, null).evaluate(crit, tuple));
    }

    private SubqueryCompareCriteria helpGetCompareSubqueryCriteria(int operator, int predicateQuantifier){
        ElementSymbol e1 = new ElementSymbol("e1"); //$NON-NLS-1$
        SubqueryCompareCriteria crit = new SubqueryCompareCriteria(e1, new Query(), operator, predicateQuantifier);
        return crit;        
    }

	// ################################## ACTUAL TESTS ################################
	
    public void testIsNull1() throws Exception {
        helpTestIsNull(null, false, true);
    }
    
    public void testIsNull2() throws Exception {
        helpTestIsNull(null, true, false);
    }
    
    public void testIsNull3() throws Exception {
        helpTestIsNull("x", false, false); //$NON-NLS-1$
    }
    
    public void testIsNull4() throws Exception {
        helpTestIsNull("x", true, true); //$NON-NLS-1$
    }
    
	public void testMatch1() throws Exception {
		helpTestMatch("", "", MatchCriteria.NULL_ESCAPE_CHAR, true);		 //$NON-NLS-1$ //$NON-NLS-2$
	}

	public void testMatch2() throws Exception {
		helpTestMatch("x", "", MatchCriteria.NULL_ESCAPE_CHAR, false);		 //$NON-NLS-1$ //$NON-NLS-2$
	}

	public void testMatch3() throws Exception {
		helpTestMatch("", "%", MatchCriteria.NULL_ESCAPE_CHAR, true);		 //$NON-NLS-1$ //$NON-NLS-2$
	}

	public void testMatch4() throws Exception {
		helpTestMatch("x", "%", MatchCriteria.NULL_ESCAPE_CHAR, true);		 //$NON-NLS-1$ //$NON-NLS-2$
	}

	public void testMatch5() throws Exception {
		helpTestMatch("xx", "%", MatchCriteria.NULL_ESCAPE_CHAR, true);		 //$NON-NLS-1$ //$NON-NLS-2$
	}

	public void testMatch6() throws Exception {
		helpTestMatch("xx", "%", MatchCriteria.NULL_ESCAPE_CHAR, true);		 //$NON-NLS-1$ //$NON-NLS-2$
	}

	public void testMatch7() throws Exception {
		helpTestMatch("a", "a%", MatchCriteria.NULL_ESCAPE_CHAR, true);		 //$NON-NLS-1$ //$NON-NLS-2$
	}

	public void testMatch8() throws Exception {
		helpTestMatch("ab", "a%", MatchCriteria.NULL_ESCAPE_CHAR, true);		 //$NON-NLS-1$ //$NON-NLS-2$
	}

	public void testMatch9() throws Exception {
		helpTestMatch("a.", "a%", MatchCriteria.NULL_ESCAPE_CHAR, true);		 //$NON-NLS-1$ //$NON-NLS-2$
	}

	public void testMatch10() throws Exception {
		helpTestMatch("a.", "a%", MatchCriteria.NULL_ESCAPE_CHAR, true);		 //$NON-NLS-1$ //$NON-NLS-2$
	}
	
	public void testMatch11() throws Exception {
		helpTestMatch("ax.", "a%", MatchCriteria.NULL_ESCAPE_CHAR, true);		 //$NON-NLS-1$ //$NON-NLS-2$
	}
	
	public void testMatch12() throws Exception {
		helpTestMatch("a..", "a%", MatchCriteria.NULL_ESCAPE_CHAR, true);		 //$NON-NLS-1$ //$NON-NLS-2$
	}

	public void testMatch13() throws Exception {
//		helpTestMatch("x.y", "%.", MatchCriteria.NULL_ESCAPE_CHAR, false);		
		helpTestMatch("a.b", "a%.", MatchCriteria.NULL_ESCAPE_CHAR, false);		 //$NON-NLS-1$ //$NON-NLS-2$
	}

	public void testMatch14() throws Exception {
		helpTestMatch("aaa", "%aaa", MatchCriteria.NULL_ESCAPE_CHAR, true);		 //$NON-NLS-1$ //$NON-NLS-2$
	}

	public void testMatch15() throws Exception {
		helpTestMatch("baaa", "%aaa", MatchCriteria.NULL_ESCAPE_CHAR, true);		 //$NON-NLS-1$ //$NON-NLS-2$
	}

	public void testMatch16() throws Exception {
		helpTestMatch("aaaa", "%aaa", MatchCriteria.NULL_ESCAPE_CHAR, true);		 //$NON-NLS-1$ //$NON-NLS-2$
	}
	
	public void testMatch17() throws Exception {
		helpTestMatch("aaxaa", "%aaa", MatchCriteria.NULL_ESCAPE_CHAR, false);		 //$NON-NLS-1$ //$NON-NLS-2$
	}
	
	public void testMatch18() throws Exception {
		helpTestMatch("", "a%b%", MatchCriteria.NULL_ESCAPE_CHAR, false);		 //$NON-NLS-1$ //$NON-NLS-2$
	}

	public void testMatch19() throws Exception {
		helpTestMatch("a", "a%b%", MatchCriteria.NULL_ESCAPE_CHAR, false);		 //$NON-NLS-1$ //$NON-NLS-2$
	}
	
	public void testMatch20() throws Exception {
		helpTestMatch("ab", "a%b%", MatchCriteria.NULL_ESCAPE_CHAR, true);		 //$NON-NLS-1$ //$NON-NLS-2$
	}
	
	public void testMatch21() throws Exception {
		helpTestMatch("axb", "a%b%", MatchCriteria.NULL_ESCAPE_CHAR, true);		 //$NON-NLS-1$ //$NON-NLS-2$
	}
	
	public void testMatch22() throws Exception {
		helpTestMatch("abx", "a%b%", MatchCriteria.NULL_ESCAPE_CHAR, true);		 //$NON-NLS-1$ //$NON-NLS-2$
	}
	
	public void testMatch23() throws Exception {
		helpTestMatch("", "X%", 'X', false);		 //$NON-NLS-1$ //$NON-NLS-2$
	}

	public void testMatch24() throws Exception {
		helpTestMatch("x", "X%", 'X', false);		 //$NON-NLS-1$ //$NON-NLS-2$
	}

	public void testMatch25() throws Exception {
		helpTestMatch("xx", "X%", 'X', false);		 //$NON-NLS-1$ //$NON-NLS-2$
	}

	public void testMatch26() throws Exception {
		helpTestMatch("a%", "aX%", 'X', true);		 //$NON-NLS-1$ //$NON-NLS-2$
	}

	public void testMatch27() throws Exception {
		helpTestMatch("aX%", "aX%", 'X', false);		 //$NON-NLS-1$ //$NON-NLS-2$
	}

	public void testMatch28() throws Exception {
		helpTestMatch("a%bb", "aX%b%", 'X', true);		 //$NON-NLS-1$ //$NON-NLS-2$
	}

	public void testMatch29() throws Exception {
		helpTestMatch("aX%bb", "aX%b%", 'X', false);		 //$NON-NLS-1$ //$NON-NLS-2$
	}
	
	public void testMatch30() throws Exception {
		helpTestMatch("", "_", MatchCriteria.NULL_ESCAPE_CHAR, false); //$NON-NLS-1$ //$NON-NLS-2$
	}

	public void testMatch31() throws Exception {
		helpTestMatch("X", "_", MatchCriteria.NULL_ESCAPE_CHAR, true); //$NON-NLS-1$ //$NON-NLS-2$
	}

	public void testMatch32() throws Exception {
		helpTestMatch("XX", "_", MatchCriteria.NULL_ESCAPE_CHAR, false); //$NON-NLS-1$ //$NON-NLS-2$
	}

	public void testMatch33() throws Exception {
		helpTestMatch("", "__", MatchCriteria.NULL_ESCAPE_CHAR, false); //$NON-NLS-1$ //$NON-NLS-2$
	}

	public void testMatch34() throws Exception {
		helpTestMatch("X", "__", MatchCriteria.NULL_ESCAPE_CHAR, false); //$NON-NLS-1$ //$NON-NLS-2$
	}

	public void testMatch35() throws Exception {
		helpTestMatch("XX", "__", MatchCriteria.NULL_ESCAPE_CHAR, true); //$NON-NLS-1$ //$NON-NLS-2$
	}

	public void testMatch36() throws Exception {
		helpTestMatch("XX", "_%_", MatchCriteria.NULL_ESCAPE_CHAR, true); //$NON-NLS-1$ //$NON-NLS-2$
	}

	public void testMatch37() throws Exception {
		helpTestMatch("XaaY", "_%_", MatchCriteria.NULL_ESCAPE_CHAR, true); //$NON-NLS-1$ //$NON-NLS-2$
	}

	public void testMatch38() throws Exception {
		helpTestMatch("a.b.c", "a.b.c", MatchCriteria.NULL_ESCAPE_CHAR, true); //$NON-NLS-1$ //$NON-NLS-2$
	}

	public void testMatch39() throws Exception {
		helpTestMatch("a.b.c", "a%.c", MatchCriteria.NULL_ESCAPE_CHAR, true); //$NON-NLS-1$ //$NON-NLS-2$
	}

	public void testMatch40() throws Exception {
		helpTestMatch("a.b.", "a.b.", MatchCriteria.NULL_ESCAPE_CHAR, true); //$NON-NLS-1$ //$NON-NLS-2$
	}
    
    public void testMatch41() throws Exception {
        helpTestMatch("asjdfajsdf (&). asdfasdf\nkjhkjh", "%&%", MatchCriteria.NULL_ESCAPE_CHAR, true);     //$NON-NLS-1$ //$NON-NLS-2$
    }
    
    public void testMatch42() throws Exception {
        helpTestMatch("x", "", MatchCriteria.NULL_ESCAPE_CHAR, true, true); //$NON-NLS-1$ //$NON-NLS-2$
    }
    
    public void testMatch43() throws Exception {
        helpTestMatch("a.b.", "a.b.", MatchCriteria.NULL_ESCAPE_CHAR, true, false); //$NON-NLS-1$ //$NON-NLS-2$
    }
    
    public void testMatch44() throws Exception {
        helpTestMatch(null, "a.b.", MatchCriteria.NULL_ESCAPE_CHAR, false); //$NON-NLS-1$ 
    }
    
    public void testMatch45() throws Exception {
        helpTestMatch("a.b.", null, MatchCriteria.NULL_ESCAPE_CHAR, false); //$NON-NLS-1$ 
    }
    
    public void testMatch46() throws Exception {
        helpTestMatch("ab\r\n", "ab%", MatchCriteria.NULL_ESCAPE_CHAR, true); //$NON-NLS-1$ //$NON-NLS-2$
    }
    
    public void testMatch47() throws Exception {
        helpTestMatch("", "", 'a', true); //$NON-NLS-1$ //$NON-NLS-2$
    }
    
    //should succeed - should be able to escape the escape char
    public void testMatch48() throws Exception {
        helpTestMatch("abc", "aa%", 'a', true); //$NON-NLS-1$ //$NON-NLS-2$
    }
    
    //should fail - invalid match sequence
    public void testMatch49() throws Exception {
        try {
            helpTestMatch("abc", "a", 'a', true); //$NON-NLS-1$ //$NON-NLS-2$
        } catch (CriteriaEvaluationException cee) {
            assertEquals("Invalid escape sequence \"a\" with escape character \"a\"", cee.getMessage()); //$NON-NLS-1$
        }
    }
    
    //should fail - can't escape a non match char
    public void testMatch50() throws Exception {
        try {
            helpTestMatch("abc", "ab", 'a', true); //$NON-NLS-1$ //$NON-NLS-2$
        } catch (CriteriaEvaluationException cee) {
            assertEquals("Invalid escape sequence \"ab\" with escape character \"a\"", cee.getMessage()); //$NON-NLS-1$
        }
    }
    
    //should be able to use a regex reserved char as the escape char
    public void testMatch51() throws Exception {
        helpTestMatch("$", "$$", '$', true); //$NON-NLS-1$ //$NON-NLS-2$
    }
    
    public void testMatch52() throws Exception {
        helpTestMatch("abc\nde", "a%e", MatchCriteria.NULL_ESCAPE_CHAR, true); //$NON-NLS-1$ //$NON-NLS-2$
    }
    
    public void testMatch53() throws Exception {
        helpTestMatch("\\", "\\%", MatchCriteria.NULL_ESCAPE_CHAR, true); //$NON-NLS-1$ //$NON-NLS-2$
    }
    
    public void testSetCriteria1() throws Exception {
        helpTestSetCriteria(1000, false, true);
    }
    
    public void testSetCriteria2() throws Exception {
        helpTestSetCriteria(1, false, false);
    }
    
    public void testSetCriteria3() throws Exception {
        helpTestSetCriteria(1000, true, false);
    }
    
    public void testSetCriteria4() throws Exception {
        helpTestSetCriteria(1, true, true);
    }
    
    public void testSetCriteria5() throws Exception {
        helpTestSetCriteria(null, true, false);
    }
    
    public void testSetCriteria6() throws Exception {
        helpTestSetCriteria(null, false, false);
    }
    
    public void testExistsCriteria() throws Exception {
        ExistsCriteria crit = new ExistsCriteria(new Query());
        ArrayList values = new ArrayList();
        values.add("a"); //$NON-NLS-1$
        values.add("b"); //$NON-NLS-1$
        values.add("c"); //$NON-NLS-1$
        CollectionValueIterator valueIter = new CollectionValueIterator(values);
        crit.setValueIterator(valueIter);
        
        assertTrue( CriteriaEvaluator.evaluate(crit) );
    }

    public void testExistsCriteria2() throws Exception {
        ExistsCriteria crit = new ExistsCriteria(new Query());
        CollectionValueIterator valueIter = new CollectionValueIterator(Collections.EMPTY_LIST);
        crit.setValueIterator(valueIter);
        
        assertFalse( CriteriaEvaluator.evaluate(crit) );
    }

    /**
     * If rows are returned but they contain null, the result should
     * still be true.
     */
    public void testExistsCriteria3() throws Exception {
        ExistsCriteria crit = new ExistsCriteria(new Query());
        ArrayList values = new ArrayList();
        values.add(null);
        values.add(null);
        values.add(null);
        CollectionValueIterator valueIter = new CollectionValueIterator(values);
        crit.setValueIterator(valueIter);
        
        assertTrue( CriteriaEvaluator.evaluate(crit) );
    }

    /**
     * Special case: if ALL is specified and the subquery returns no rows,
     * the result is true.
     */
    public void testCompareSubqueryCriteriaNoRows() throws Exception {
        SubqueryCompareCriteria crit = helpGetCompareSubqueryCriteria(SubqueryCompareCriteria.EQ, SubqueryCompareCriteria.ALL);
        CollectionValueIterator valueIter = new CollectionValueIterator(Collections.EMPTY_LIST);
        crit.setValueIterator(valueIter);
        helpTestCompareSubqueryCriteria(crit, true); 
    }

    /**
     * Special case: if ANY/SOME is specified and the subquery returns no rows,
     * the result is false.
     */
    public void testCompareSubqueryCriteriaNoRows2() throws Exception {
        SubqueryCompareCriteria crit = helpGetCompareSubqueryCriteria(SubqueryCompareCriteria.EQ, SubqueryCompareCriteria.SOME);
        CollectionValueIterator valueIter = new CollectionValueIterator(Collections.EMPTY_LIST);
        crit.setValueIterator(valueIter);
        helpTestCompareSubqueryCriteria(crit, false); 
    }

    /**
     * Special case: if no predicate quantifier is specified and the subquery returns no rows,
     * the result is false.
     */
    public void testCompareSubqueryCriteriaNoRows3() throws Exception {
        SubqueryCompareCriteria crit = helpGetCompareSubqueryCriteria(SubqueryCompareCriteria.EQ, SubqueryCompareCriteria.NO_QUANTIFIER);
        CollectionValueIterator valueIter = new CollectionValueIterator(Collections.EMPTY_LIST);
        crit.setValueIterator(valueIter);
        helpTestCompareSubqueryCriteria(crit, false); 
    }

    public void testCompareSubqueryCriteria2() throws Exception {
        SubqueryCompareCriteria crit = helpGetCompareSubqueryCriteria(SubqueryCompareCriteria.EQ, SubqueryCompareCriteria.ALL);
        ArrayList values = new ArrayList();
        values.add("a"); //$NON-NLS-1$
        values.add("b"); //$NON-NLS-1$
        values.add("c"); //$NON-NLS-1$
        CollectionValueIterator valueIter = new CollectionValueIterator(values);
        crit.setValueIterator(valueIter);
        helpTestCompareSubqueryCriteria(crit, false); 
    }

    public void testCompareSubqueryCriteria3() throws Exception {
        SubqueryCompareCriteria crit = helpGetCompareSubqueryCriteria(SubqueryCompareCriteria.EQ, SubqueryCompareCriteria.SOME);
        ArrayList values = new ArrayList();
        values.add("a"); //$NON-NLS-1$
        values.add("b"); //$NON-NLS-1$
        values.add("c"); //$NON-NLS-1$
        CollectionValueIterator valueIter = new CollectionValueIterator(values);
        crit.setValueIterator(valueIter);
        helpTestCompareSubqueryCriteria(crit, true); 
    }
    
    public void testCompareSubqueryCriteria4() throws Exception {
        SubqueryCompareCriteria crit = helpGetCompareSubqueryCriteria(SubqueryCompareCriteria.EQ, SubqueryCompareCriteria.SOME);
        ArrayList values = new ArrayList();
        values.add("b"); //$NON-NLS-1$
        values.add("c"); //$NON-NLS-1$
        CollectionValueIterator valueIter = new CollectionValueIterator(values);
        crit.setValueIterator(valueIter);
        helpTestCompareSubqueryCriteria(crit, false); 
    }

    public void testCompareSubqueryCriteria5() throws Exception {
        SubqueryCompareCriteria crit = helpGetCompareSubqueryCriteria(SubqueryCompareCriteria.EQ, SubqueryCompareCriteria.SOME);
        ArrayList values = new ArrayList();
        values.add("a"); //$NON-NLS-1$
        values.add("b"); //$NON-NLS-1$
        values.add("c"); //$NON-NLS-1$
        CollectionValueIterator valueIter = new CollectionValueIterator(values);
        crit.setValueIterator(valueIter);
        helpTestCompareSubqueryCriteria(crit, true); 
    }

    public void testCompareSubqueryCriteria6() throws Exception {
        SubqueryCompareCriteria crit = helpGetCompareSubqueryCriteria(SubqueryCompareCriteria.EQ, SubqueryCompareCriteria.NO_QUANTIFIER);
        ArrayList values = new ArrayList();
        values.add("a"); //$NON-NLS-1$
        CollectionValueIterator valueIter = new CollectionValueIterator(values);
        crit.setValueIterator(valueIter);
        helpTestCompareSubqueryCriteria(crit, true); 
    }

    public void testCompareSubqueryCriteria7() throws Exception {
        SubqueryCompareCriteria crit = helpGetCompareSubqueryCriteria(SubqueryCompareCriteria.EQ, SubqueryCompareCriteria.NO_QUANTIFIER);
        ArrayList values = new ArrayList();
        values.add("b"); //$NON-NLS-1$
        CollectionValueIterator valueIter = new CollectionValueIterator(values);
        crit.setValueIterator(valueIter);
        helpTestCompareSubqueryCriteria(crit, false); 
    }


    /**
     * Should fail because the subquery needs to be scalar since it doesn't
     * have a predicate quantifier, but there is more than one value in the
     * ValueIterator
     */
    public void testCompareSubqueryCriteriaFails1() throws Exception {
        SubqueryCompareCriteria crit = helpGetCompareSubqueryCriteria(SubqueryCompareCriteria.EQ, SubqueryCompareCriteria.NO_QUANTIFIER);
        ArrayList values = new ArrayList();
        values.add("a"); //$NON-NLS-1$
        values.add("b"); //$NON-NLS-1$
        values.add("c"); //$NON-NLS-1$
        CollectionValueIterator valueIter = new CollectionValueIterator(values);
        crit.setValueIterator(valueIter);
        try {
        	helpTestCompareSubqueryCriteria(crit, false);
        } catch (CriteriaEvaluationException e) {
        	assertEquals("The subquery of this compare criteria has to be scalar, but returned more than one value: e1 = (<undefined>)", e.getMessage()); //$NON-NLS-1$
        }
    }

    public void testCompareSubqueryCriteriaNulls2() throws Exception {
        SubqueryCompareCriteria crit = helpGetCompareSubqueryCriteria(SubqueryCompareCriteria.EQ, SubqueryCompareCriteria.NO_QUANTIFIER);
        ArrayList values = new ArrayList();
        values.add(null);
        CollectionValueIterator valueIter = new CollectionValueIterator(values);
        crit.setValueIterator(valueIter);
        helpTestCompareSubqueryCriteria(crit, false); 
    }

    public void testCompareSubqueryCriteriaNulls3() throws Exception {
        SubqueryCompareCriteria crit = helpGetCompareSubqueryCriteria(SubqueryCompareCriteria.EQ, SubqueryCompareCriteria.ALL);
        ArrayList values = new ArrayList();
        values.add(null);
        values.add(null);
        CollectionValueIterator valueIter = new CollectionValueIterator(values);
        crit.setValueIterator(valueIter);
        helpTestCompareSubqueryCriteria(crit, false); 
    }

    public void testCompareSubqueryCriteriaNulls4() throws Exception {
        SubqueryCompareCriteria crit = helpGetCompareSubqueryCriteria(SubqueryCompareCriteria.EQ, SubqueryCompareCriteria.SOME);
        ArrayList values = new ArrayList();
        values.add(null);
        values.add(null);
        CollectionValueIterator valueIter = new CollectionValueIterator(values);
        crit.setValueIterator(valueIter);
        helpTestCompareSubqueryCriteria(crit, false); 
    }

    public void testCompareSubqueryCriteriaNulls5() throws Exception {
        SubqueryCompareCriteria crit = helpGetCompareSubqueryCriteria(SubqueryCompareCriteria.EQ, SubqueryCompareCriteria.SOME);
        ArrayList values = new ArrayList();
        values.add(null);
        values.add("a"); //$NON-NLS-1$
        values.add(null);
        CollectionValueIterator valueIter = new CollectionValueIterator(values);
        crit.setValueIterator(valueIter);
        helpTestCompareSubqueryCriteria(crit, true); 
    }

    public void testCompareSubqueryCriteriaNulls6() throws Exception {
        SubqueryCompareCriteria crit = helpGetCompareSubqueryCriteria(SubqueryCompareCriteria.EQ, SubqueryCompareCriteria.SOME);
        ArrayList values = new ArrayList();
        values.add("a"); //$NON-NLS-1$
        values.add(null);
        values.add("a"); //$NON-NLS-1$
        CollectionValueIterator valueIter = new CollectionValueIterator(values);
        crit.setValueIterator(valueIter);
        helpTestCompareSubqueryCriteria(crit, true); 
    }

    /**
     * null is unknown
     */
    public void testCompareSubqueryCriteriaNulls7() throws Exception{
        SubqueryCompareCriteria crit = helpGetCompareSubqueryCriteria(SubqueryCompareCriteria.LT, SubqueryCompareCriteria.ALL);
        ArrayList values = new ArrayList();
        values.add(null);
        values.add(null);
        CollectionValueIterator valueIter = new CollectionValueIterator(values);
        crit.setValueIterator(valueIter);
        helpTestCompareSubqueryCriteria(crit, false); 
    }

    /**
     * null is unknown
     */
    public void testCompareSubqueryCriteriaNulls8() throws Exception {
        SubqueryCompareCriteria crit = helpGetCompareSubqueryCriteria(SubqueryCompareCriteria.GT, SubqueryCompareCriteria.ALL);
        ArrayList values = new ArrayList();
        values.add(null);
        values.add(null);
        CollectionValueIterator valueIter = new CollectionValueIterator(values);
        crit.setValueIterator(valueIter);
        helpTestCompareSubqueryCriteria(crit, false); 
    }
}
