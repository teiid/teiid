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

package org.teiid.dqp.internal.datamgr;

import org.teiid.language.Like;
import org.teiid.language.Literal;
import org.teiid.query.sql.lang.MatchCriteria;
import org.teiid.query.sql.symbol.Constant;
import org.teiid.query.sql.symbol.ElementSymbol;


import junit.framework.TestCase;

public class TestLikeCriteriaImpl extends TestCase {

    /**
     * Constructor for TestLikeCriteriaImpl.
     * @param name
     */
    public TestLikeCriteriaImpl(String name) {
        super(name);
    }

    public static MatchCriteria helpExample(String right, char escape, boolean negated) {
        ElementSymbol e1 = TestElementImpl.helpExample("vm1.g1", "e1"); //$NON-NLS-1$ //$NON-NLS-2$
        MatchCriteria match = new MatchCriteria(e1, new Constant(right), escape);
        match.setNegated(negated);
        return match;
    }

    public static Like example(String right, char escape, boolean negated) throws Exception {
        return TstLanguageBridgeFactory.factory.translate(helpExample(right, escape, negated));
    }

    public void testGetLeftExpression() throws Exception {
        assertNotNull(example("abc", '.', false).getLeftExpression()); //$NON-NLS-1$
    }

    public void testGetRightExpression() throws Exception {
        Like like = example("abc", '.', false); //$NON-NLS-1$
        assertNotNull(like.getRightExpression());
        assertTrue(like.getRightExpression() instanceof Literal);
        assertEquals("abc", ((Literal)like.getRightExpression()).getValue()); //$NON-NLS-1$
    }

    public void testGetEscapeCharacter() throws Exception {
        assertEquals(new Character('.'), example("abc", '.', false).getEscapeCharacter()); //$NON-NLS-1$
    }

    public void testIsNegated() throws Exception {
        assertTrue(example("abc", '.', true).isNegated()); //$NON-NLS-1$
        assertFalse(example("abc", '.', false).isNegated()); //$NON-NLS-1$
    }

}
