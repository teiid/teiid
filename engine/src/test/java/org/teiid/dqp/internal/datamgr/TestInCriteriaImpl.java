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

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.List;

import org.junit.Test;
import org.teiid.language.AndOr;
import org.teiid.language.AndOr.Operator;
import org.teiid.language.Expression;
import org.teiid.language.In;
import org.teiid.language.Literal;
import org.teiid.query.sql.lang.SetCriteria;
import org.teiid.query.unittest.RealMetadataFactory;

public class TestInCriteriaImpl {

    public static SetCriteria helpExample(boolean negated) {
        ArrayList<org.teiid.query.sql.symbol.Expression> values = new ArrayList<org.teiid.query.sql.symbol.Expression>();
        values.add(TestLiteralImpl.helpExample(100));
        values.add(TestLiteralImpl.helpExample(200));
        values.add(TestLiteralImpl.helpExample(300));
        values.add(TestLiteralImpl.helpExample(400));
        SetCriteria crit = new SetCriteria(TestLiteralImpl.helpExample(300), values);
        crit.setNegated(negated);
        return crit;
    }

    public static In example(boolean negated) throws Exception {
        return (In)TstLanguageBridgeFactory.factory.translate(helpExample(negated));
    }

    @Test public void testGetLeftExpression() throws Exception {
        In inCriteria = example(false);
        assertNotNull(inCriteria.getLeftExpression());
        assertTrue(inCriteria.getLeftExpression() instanceof Literal);
        assertEquals(new Integer(300), ((Literal)inCriteria.getLeftExpression()).getValue());
    }

    @Test public void testExpansion() throws Exception {
        SetCriteria inCriteria = helpExample(false);
        LanguageBridgeFactory lbf = new LanguageBridgeFactory(RealMetadataFactory.example1Cached());
        lbf.setConvertIn(true);
        AndOr or = (AndOr) lbf.translate(inCriteria);
        assertEquals(Operator.OR, or.getOperator());
        inCriteria.setNegated(true);
        AndOr and = (AndOr) lbf.translate(inCriteria);
        assertEquals(Operator.AND, and.getOperator());
    }

    @Test public void testExpansion1() throws Exception {
        SetCriteria inCriteria = helpExample(false);
        LanguageBridgeFactory lbf = new LanguageBridgeFactory(RealMetadataFactory.example1Cached());
        lbf.setMaxInPredicateSize(2);
        AndOr or = (AndOr) lbf.translate(inCriteria);
        assertEquals(Operator.OR, or.getOperator());
        assertEquals(2, ((In)or.getRightCondition()).getRightExpressions().size());
        inCriteria.setNegated(true);
        AndOr and = (AndOr) lbf.translate(inCriteria);
        assertEquals(Operator.AND, and.getOperator());
        assertEquals("300 NOT IN (100, 200) AND 300 NOT IN (300, 400)", and.toString());
    }

    @Test public void testGetRightExpressions() throws Exception {
        List<Expression> values = example(false).getRightExpressions();
        assertNotNull(values);
        assertEquals(4, values.size());
    }

    @Test public void testIsNegated() throws Exception {
        assertTrue(example(true).isNegated());
        assertFalse(example(false).isNegated());
    }

}
