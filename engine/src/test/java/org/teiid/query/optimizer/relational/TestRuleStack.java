/*
 * Copyright Red Hat, Inc. and/or its affiliates
 * and other contributors as indicated by the @author tags and
 * the COPYRIGHT.txt file distributed with this work.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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
