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

package org.teiid.query.processor.relational;

import static org.junit.Assert.*;

import java.util.Arrays;
import java.util.List;

import org.junit.Test;
import org.teiid.query.sql.lang.CompareCriteria;
import org.teiid.query.sql.lang.Criteria;
import org.teiid.query.sql.lang.SetCriteria;
import org.teiid.query.sql.symbol.Constant;
import org.teiid.query.sql.symbol.ElementSymbol;
import org.teiid.query.sql.symbol.Reference;
import org.teiid.query.util.CommandContext;


public class TestDependentCriteriaProcessor {

    @Test public void testNegatedSetCriteria() throws Exception {
        DependentAccessNode dan = new DependentAccessNode(0);
        SetCriteria sc = new SetCriteria(new ElementSymbol("e1"), Arrays.asList(new Constant(1), new Constant(2))); //$NON-NLS-1$
        sc.setAllConstants(true);
        sc.negate();
        DependentCriteriaProcessor dcp = new DependentCriteriaProcessor(1, -1, dan, sc);
        Criteria result = dcp.prepareCriteria();
        assertEquals(sc, result);
        assertFalse(dcp.hasNextCommand());
    }

    @Test public void testSetCriteria() throws Exception {
        DependentAccessNode dan = new DependentAccessNode(0);
        SetCriteria sc = new SetCriteria(new ElementSymbol("e1"), Arrays.asList(new Constant(1), new Constant(2))); //$NON-NLS-1$
        sc.setAllConstants(true);
        DependentCriteriaProcessor dcp = new DependentCriteriaProcessor(1, -1, dan, sc);
        Criteria result = dcp.prepareCriteria();
        assertEquals(new CompareCriteria(new ElementSymbol("e1"), CompareCriteria.EQ, new Constant(1)), result); //$NON-NLS-1$
        assertTrue(dcp.hasNextCommand());
    }

    @Test public void testEvaluatedSetCriteria() throws Exception {
        DependentAccessNode dan = new DependentAccessNode(0);
        CommandContext cc = new CommandContext();
        dan.setContext(cc);
        List<Reference> references = Arrays.asList(new Reference(1), new Reference(2));
        for (Reference reference : references) {
            cc.getVariableContext().setGlobalValue(reference.getContextSymbol(), 1);
        }
        SetCriteria sc = new SetCriteria(new ElementSymbol("e1"), references); //$NON-NLS-1$
        sc.setAllConstants(true);
        DependentCriteriaProcessor dcp = new DependentCriteriaProcessor(1, -1, dan, sc);
        Criteria result = dcp.prepareCriteria();
        assertEquals(new CompareCriteria(new ElementSymbol("e1"), CompareCriteria.EQ, new Constant(1)), result); //$NON-NLS-1$
        assertFalse(dcp.hasNextCommand());
    }

}
