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

package org.teiid.query.optimizer.relational;

import junit.framework.TestCase;

import org.teiid.query.optimizer.relational.rules.RuleConstants;
import org.teiid.query.optimizer.relational.rules.RulePushSelectCriteria;


/**
 */
public class TestRuleStack extends TestCase {

    /**
     * Constructor for TestRuleStack.
     * @param arg0
     */
    public TestRuleStack(String arg0) {
        super(arg0);
    }

    
    public void testInitialization() {
        RuleStack stack = new RuleStack();
        assertEquals("Initial stack is not empty", true, stack.isEmpty()); //$NON-NLS-1$
        assertEquals("Initial size is not 0", 0, stack.size()); //$NON-NLS-1$
        assertNull("Top is not null", stack.pop()); //$NON-NLS-1$
    }
    
    public void helpTestPop(RuleStack stack, OptimizerRule expectedPop, int expectedSize) {
        OptimizerRule out = stack.pop();
        int outSize = stack.size();
        
        assertSame("Did not get same object", expectedPop, out); //$NON-NLS-1$
        assertEquals("Stack changed size", expectedSize, outSize);                     //$NON-NLS-1$
    }
    
    public void testPopOneRule() {
        RuleStack stack = new RuleStack();
        int expectedSize = stack.size();
        
        OptimizerRule rule = new RulePushSelectCriteria();
        stack.push(rule);
        
        helpTestPop(stack, rule, expectedSize);
    }

    public void testPopNothing() {
        RuleStack stack = new RuleStack();
        helpTestPop(stack, null, 0);
    }
    
    public void testRemove() {
        // Set up
        RuleStack stack = new RuleStack();
        stack.push(RuleConstants.ACCESS_PATTERN_VALIDATION);
        stack.push(RuleConstants.COLLAPSE_SOURCE);
        stack.push(RuleConstants.ACCESS_PATTERN_VALIDATION);
        
        // Remove all instances of ASSIGN_OUTPUT_ELEMENTS
        stack.remove(RuleConstants.ACCESS_PATTERN_VALIDATION);
        
        // Verify size and pop'ed values
        assertEquals(1, stack.size());
        assertEquals(RuleConstants.COLLAPSE_SOURCE, stack.pop());
        assertEquals(null, stack.pop());
    }
    
    public void testContains() {
        // Set up
        RuleStack stack = new RuleStack();
        stack.push(RuleConstants.ACCESS_PATTERN_VALIDATION);
        stack.push(RuleConstants.COLLAPSE_SOURCE);
        
        assertEquals(true, stack.contains(RuleConstants.ACCESS_PATTERN_VALIDATION));
        assertEquals(false, stack.contains(RuleConstants.PLACE_ACCESS));
    }

}
