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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Test;
import org.teiid.api.exception.query.ExpressionEvaluationException;
import org.teiid.common.buffer.BlockedException;
import org.teiid.common.buffer.BufferManager;
import org.teiid.common.buffer.BufferManagerFactory;
import org.teiid.common.buffer.TupleBatch;
import org.teiid.core.TeiidComponentException;
import org.teiid.core.TeiidProcessingException;
import org.teiid.core.types.DataTypeManager;
import org.teiid.query.eval.Evaluator;
import org.teiid.query.function.FunctionDescriptor;
import org.teiid.query.processor.BatchIterator;
import org.teiid.query.processor.FakeDataManager;
import org.teiid.query.processor.ProcessorDataManager;
import org.teiid.query.processor.QueryProcessor;
import org.teiid.query.sql.lang.CompareCriteria;
import org.teiid.query.sql.lang.Criteria;
import org.teiid.query.sql.symbol.Constant;
import org.teiid.query.sql.symbol.ElementSymbol;
import org.teiid.query.sql.symbol.Expression;
import org.teiid.query.sql.symbol.Function;
import org.teiid.query.unittest.RealMetadataFactory;
import org.teiid.query.util.CommandContext;

@SuppressWarnings("unchecked")
public class TestSelectNode {

    public void helpTestSelect(List elements, Criteria criteria, List[] data, List childElements, ProcessorDataManager dataMgr, List[] expected) throws TeiidComponentException, TeiidProcessingException {
        helpTestSelect(elements, criteria, childElements, dataMgr, expected, new FakeRelationalNode(2, data));
    }

    public void helpTestSelect(List elements, Criteria criteria, List childElements, ProcessorDataManager dataMgr, List[] expected, RelationalNode child) throws TeiidComponentException, TeiidProcessingException {
        SelectNode selectNode = new SelectNode(1);
        helpTestSelect(elements, criteria, childElements, dataMgr, expected, child, selectNode);
    }

    private void helpTestSelect(List elements, Criteria criteria, List childElements,
            ProcessorDataManager dataMgr, List[] expected,
            RelationalNode child,
            SelectNode selectNode) throws TeiidComponentException,
            TeiidProcessingException {
        BufferManager mgr = BufferManagerFactory.getStandaloneBufferManager();
        CommandContext context = new CommandContext("pid", "test", null, null, 1);               //$NON-NLS-1$ //$NON-NLS-2$

        child.setElements(childElements);
        child.initialize(context, mgr, dataMgr);
        selectNode.setCriteria(criteria);
        selectNode.setElements(elements);
        selectNode.addChild(child);
        selectNode.initialize(context, mgr, dataMgr);

        selectNode.open();

        BatchIterator iterator = new BatchIterator(selectNode);

        for (int i = 0; i < expected.length; i++) {
            while (true) {
                try {
                    assertEquals("Rows don't match at " + i, expected[i], iterator.nextTuple()); //$NON-NLS-1$
                    break;
                } catch (BlockedException e) {
                    continue;
                }
            }
        }
        assertFalse(iterator.hasNext());
    }

    /**
     * Ensures that a final empty batch is reindexed so that the batch iterator works correctly
     */
    @Test public void testEmptyBatchIndexing() throws TeiidComponentException, TeiidProcessingException {
        ElementSymbol es1 = new ElementSymbol("e1"); //$NON-NLS-1$
        es1.setType(DataTypeManager.DefaultDataClasses.INTEGER);

        List elements = new ArrayList();
        elements.add(es1);

        CompareCriteria crit = new CompareCriteria(new Constant(0), CompareCriteria.EQ, new Constant(new Integer(1)));

        List childElements = new ArrayList();
        childElements.add(es1);

        RelationalNode child = new RelationalNode(0) {
            int i = 0;

            @Override
            public Object clone() {
                return null;
            }

            @Override
            protected TupleBatch nextBatchDirect() throws BlockedException,
                    TeiidComponentException, TeiidProcessingException {
                if (i++ == 0) {
                    return new TupleBatch(1, new List[] {Arrays.asList(1), Arrays.asList(1)});
                }
                TupleBatch batch = new TupleBatch(3, new List[0] );
                batch.setTerminationFlag(true);
                return batch;
            }

        };

        helpTestSelect(elements, crit, childElements, null, new List[0], child);
    }

    @Test public void testTimeslicing() throws TeiidComponentException, TeiidProcessingException {
        ElementSymbol es1 = new ElementSymbol("e1"); //$NON-NLS-1$
        es1.setType(DataTypeManager.DefaultDataClasses.INTEGER);

        List elements = new ArrayList();
        elements.add(es1);

        CompareCriteria crit = new CompareCriteria(es1, CompareCriteria.EQ, new Constant(new Integer(1)));

        List[] data = new List[] {
            Arrays.asList(1),
            Arrays.asList(1),
            Arrays.asList(1)
        };

        List childElements = new ArrayList();
        childElements.add(es1);

        helpTestSelect(elements, crit, childElements, null, data, new FakeRelationalNode(2, data), new SelectNode(3) {
            int i = 0;

            @Override
            protected Evaluator getEvaluator(Map elementMap) {
                return new Evaluator(elementMap, getDataManager(), getContext()) {
                    @Override
                    public Boolean evaluateTVL(Criteria criteria, List<?> tuple)
                            throws ExpressionEvaluationException,
                            BlockedException, TeiidComponentException {
                        if (i++ == 1) {
                            throw new QueryProcessor.ExpiredTimeSliceException();
                        }
                        return super.evaluateTVL(criteria, tuple);
                    }
                };
            }

        });
    }

    @Test public void testNoRows() throws TeiidComponentException, TeiidProcessingException {
        ElementSymbol es1 = new ElementSymbol("e1"); //$NON-NLS-1$
        es1.setType(DataTypeManager.DefaultDataClasses.INTEGER);

        ElementSymbol es2 = new ElementSymbol("e2"); //$NON-NLS-1$
        es2.setType(DataTypeManager.DefaultDataClasses.STRING);

        List elements = new ArrayList();
        elements.add(es1);

        List[] data = new List[0];

        CompareCriteria crit = new CompareCriteria(es1, CompareCriteria.EQ, new Constant(new Integer(1)));

        List childElements = new ArrayList();
        childElements.add(es1);
        childElements.add(es2);

        helpTestSelect(elements, crit, data, childElements, null, data);

    }

    @Test public void testSimpleSelect() throws TeiidComponentException, TeiidProcessingException {
        ElementSymbol es1 = new ElementSymbol("e1"); //$NON-NLS-1$
        es1.setType(DataTypeManager.DefaultDataClasses.INTEGER);

        ElementSymbol es2 = new ElementSymbol("e2"); //$NON-NLS-1$
        es2.setType(DataTypeManager.DefaultDataClasses.STRING);

        List elements = new ArrayList();
        elements.add(es1);

        CompareCriteria crit = new CompareCriteria(es1, CompareCriteria.EQ, new Constant(new Integer(1)));

        List[] data = new List[20];
        for(int i=0; i<20; i++) {
            data[i] = new ArrayList();
            data[i].add(new Integer((i*51) % 11));

            String str = "" + (i*3); //$NON-NLS-1$
            str = str.substring(0,1);
            data[i].add(str);
        }

        List childElements = new ArrayList();
        childElements.add(es1);
        childElements.add(es2);

        List[] expected = new List[] {
            Arrays.asList(new Object[] { new Integer(1) }),
            Arrays.asList(new Object[] { new Integer(1) })
        };

        helpTestSelect(elements, crit, data, childElements, null, expected);

    }

    @Test public void testSelectWithLookup() throws TeiidComponentException, TeiidProcessingException {
        ElementSymbol es1 = new ElementSymbol("e1"); //$NON-NLS-1$
        es1.setType(DataTypeManager.DefaultDataClasses.INTEGER);

        ElementSymbol es2 = new ElementSymbol("e2"); //$NON-NLS-1$
        es2.setType(DataTypeManager.DefaultDataClasses.STRING);

        List elements = new ArrayList();
        elements.add(es1);

        Function func = new Function("lookup", new Expression[] { new Constant("pm1.g1"), new Constant("e2"), new Constant("e1"), es1 }); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
        FunctionDescriptor desc = RealMetadataFactory.SFM.getSystemFunctionLibrary().findFunction("lookup", new Class[] { String.class, String.class, String.class, Integer.class } ); //$NON-NLS-1$
        func.setFunctionDescriptor(desc);
        func.setType(DataTypeManager.DefaultDataClasses.INTEGER);
        CompareCriteria crit = new CompareCriteria(func, CompareCriteria.EQ, new Constant(new Integer(1)));

        List[] data = new List[20];
        for(int i=0; i<20; i++) {
            data[i] = new ArrayList();
            data[i].add(new Integer((i*51) % 11));

            String str = "" + (i*3); //$NON-NLS-1$
            str = str.substring(0,1);
            data[i].add(str);
        }

        List childElements = new ArrayList();
        childElements.add(es1);
        childElements.add(es2);

        List[] expected = new List[] {
            Arrays.asList(new Object[] { new Integer(0) }),
            Arrays.asList(new Object[] { new Integer(0) })
        };

        FakeDataManager dataMgr = new FakeDataManager();
        dataMgr.setThrowBlocked(true);
        Map valueMap = new HashMap();
        valueMap.put(new Integer(0), new Integer(1));
        valueMap.put(new Integer(1), new Integer(2));
        dataMgr.defineCodeTable("pm1.g1", "e1", "e2", valueMap); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$

        helpTestSelect(elements, crit, data, childElements, dataMgr, expected);

    }
}
