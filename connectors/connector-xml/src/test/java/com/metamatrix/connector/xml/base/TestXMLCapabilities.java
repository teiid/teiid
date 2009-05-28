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

package com.metamatrix.connector.xml.base;

import junit.framework.TestCase;


/**
 *
 */
public class TestXMLCapabilities extends TestCase {

	
	private XMLCapabilities m_caps = null;
    /**
     * Constructor for XMLCapabilitiesTest.
     * @param arg0
     */
    public TestXMLCapabilities(String arg0) {
        super(arg0);
    }

    @Override
	public void setUp() {
    	m_caps = new XMLCapabilities();
    	
    }
    
    public void testGetMaxInCriteriaSize() {
        assertEquals(Integer.MAX_VALUE, m_caps.getMaxInCriteriaSize());
    }
    
    public void testSupportsCompareCriteriaEquals() {
    	assertTrue(m_caps.supportsCompareCriteriaEquals());
    }

    public void testSupportsInCriteria() {
    	assertTrue(m_caps.supportsInCriteria());
    }

    public void testXMLCapabilities() {
        XMLCapabilities caps = new XMLCapabilities();
        assertNotNull(caps);
    }

    /*
     * Class under test for List getSupportedFunctions()
     */
    public void testGetSupportedFunctions() {
       assertNotNull(m_caps.getSupportedFunctions());
    }

    public void testSupportsSelectDistinct() {
        assertFalse(m_caps.supportsSelectDistinct());
    }

    public void testSupportsAliasedGroup() {
        assertFalse(m_caps.supportsAliasedGroup());
    }

    public void testSupportsJoins() {
       assertFalse(m_caps.supportsInnerJoins());
    }

    public void testSupportsSelfJoins() {
        assertFalse(m_caps.supportsSelfJoins());
    }

    public void testSupportsOuterJoins() {
        assertFalse(m_caps.supportsOuterJoins());
    }

    public void testSupportsFullOuterJoins() {
        assertFalse(m_caps.supportsFullOuterJoins());
    }

    public void testSupportsBetweenCriteria() {
       assertFalse(m_caps.supportsBetweenCriteria());
    }

    public void testSupportsLikeCriteria() {
    	assertFalse(m_caps.supportsLikeCriteria());
    }

    public void testSupportsLikeCriteriaEscapeCharacter() {
        assertFalse(m_caps.supportsLikeCriteriaEscapeCharacter());
    }

    public void testSupportsInCriteriaSubquery() {
        assertFalse(m_caps.supportsInCriteriaSubquery());
    }

    public void testSupportsIsNullCriteria() {
        assertFalse(m_caps.supportsIsNullCriteria());
    }

    public void testSupportsOrCriteria() {
    	assertFalse(m_caps.supportsOrCriteria());
    }

    public void testSupportsNotCriteria() {
        assertFalse(m_caps.supportsNotCriteria());
    }

    public void testSupportsExistsCriteria() {
        assertFalse(m_caps.supportsExistsCriteria());
    }

    public void testSupportsQuantifiedCompareCriteriaSome() {
    	assertFalse(m_caps.supportsQuantifiedCompareCriteriaSome());
    }

    public void testSupportsQuantifiedCompareCriteriaAll() {
    	assertFalse(m_caps.supportsQuantifiedCompareCriteriaAll());
    }

    public void testSupportsOrderBy() {
    	assertFalse(m_caps.supportsOrderBy());
    }

    public void testSupportsAggregatesSum() {
        assertFalse(m_caps.supportsAggregatesSum());
    }

    public void testSupportsAggregatesAvg() {
        assertFalse(m_caps.supportsAggregatesAvg());
    }

    public void testSupportsAggregatesMin() {
        assertFalse(m_caps.supportsAggregatesMin());
    }

    public void testSupportsAggregatesMax() {
        assertFalse(m_caps.supportsAggregatesMax());
    }

    public void testSupportsAggregatesCount() {
        assertFalse(m_caps.supportsAggregatesCount());
    }

    public void testSupportsAggregatesCountStar() {
        assertFalse(m_caps.supportsAggregatesCountStar());
    }

    public void testSupportsAggregatesDistinct() {
        assertFalse(m_caps.supportsAggregatesDistinct());
    }

    public void testSupportsScalarSubqueries() {
        assertFalse(m_caps.supportsScalarSubqueries());
    }

    public void testSupportsCorrelatedSubqueries() {
        assertFalse(m_caps.supportsCorrelatedSubqueries());
    }

    public void testSupportsCaseExpressions() {
        assertFalse(m_caps.supportsCaseExpressions());
    }

    public void testSupportsSearchedCaseExpressions() {
        assertFalse(m_caps.supportsSearchedCaseExpressions());
    }

    public void testSupportsInlineViews() {
        assertFalse(m_caps.supportsInlineViews());
    }

    public void testSupportsUnions() {
        assertFalse(m_caps.supportsUnions());
    }

}
