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

import org.junit.Test;
import org.teiid.common.buffer.BufferManager;
import org.teiid.common.buffer.BufferManagerFactory;
import org.teiid.query.parser.QueryParser;
import org.teiid.query.processor.FakeDataManager;
import org.teiid.query.processor.TestProcessor;
import org.teiid.query.resolver.TestResolver;
import org.teiid.query.sql.lang.Command;
import org.teiid.query.sql.lang.CompoundCriteria;
import org.teiid.query.sql.lang.IsNullCriteria;
import org.teiid.query.sql.lang.Query;
import org.teiid.query.sql.lang.SetClauseList;
import org.teiid.query.sql.lang.Update;
import org.teiid.query.sql.symbol.Constant;
import org.teiid.query.sql.symbol.ElementSymbol;
import org.teiid.query.sql.symbol.GroupSymbol;
import org.teiid.query.unittest.RealMetadataFactory;
import org.teiid.query.util.CommandContext;



/**
 * @since 4.2
 */
public class TestAccessNode {

    private void helpTestOpen(Command command, String expectedCommand, boolean shouldRegisterRequest) throws Exception {
        // Setup
        AccessNode node = new AccessNode(1);
        node.setCommand(command);
        CommandContext context = new CommandContext();
        BufferManager bm = BufferManagerFactory.getStandaloneBufferManager();
        FakeDataManager dataManager = new FakeDataManager();
        TestProcessor.sampleData1(dataManager);
        node.setElements(command.getProjectedSymbols());
        node.initialize(context, bm, dataManager);
        node.setShouldEvaluateExpressions(true);
        // Call open()
        node.open();
        if (shouldRegisterRequest) {
            assertEquals(Arrays.asList(expectedCommand), dataManager.getQueries());
        } else {
            assertEquals(0, dataManager.getQueries().size());
        }
    }

    @Test public void testOpen_Defect16059() throws Exception {
        Query query = (Query)TestResolver.helpResolve("SELECT e1, e2 FROM pm1.g1 WHERE e2 = 5 AND ? IS NULL", RealMetadataFactory.example1Cached()); //$NON-NLS-1$
        IsNullCriteria nullCrit = (IsNullCriteria)((CompoundCriteria)query.getCriteria()).getCriteria(1);
        nullCrit.setExpression(new Constant(null));

        helpTestOpen(query, "SELECT e1, e2 FROM pm1.g1 WHERE e2 = 5", true); //$NON-NLS-1$
    }

    @Test public void testOpen_Defect16059_2() throws Exception {
        Query query = (Query)TestResolver.helpResolve("SELECT e1, e2 FROM pm1.g1 WHERE e2 = 5 AND ? IS NOT NULL", RealMetadataFactory.example1Cached()); //$NON-NLS-1$
        IsNullCriteria nullCrit = (IsNullCriteria)((CompoundCriteria)query.getCriteria()).getCriteria(1);
        nullCrit.setExpression(new Constant(null));

        helpTestOpen(query, null, false);
    }

    @Test public void testExecCount()throws Exception{
        // Setup
        AccessNode node = new AccessNode(1);
        Query query = (Query)TestResolver.helpResolve("SELECT e1, e2 FROM pm1.g1 WHERE e2 = 5", RealMetadataFactory.example1Cached()); //$NON-NLS-1$
        node.setCommand(query);
        CommandContext context = new CommandContext();
        BufferManager bm = BufferManagerFactory.getStandaloneBufferManager();
        FakeDataManager dataManager = new FakeDataManager();
        TestProcessor.sampleData1(dataManager);
        node.setElements(query.getProjectedSymbols());
        node.initialize(context, bm, dataManager);
        // Call open()
        node.open();
        assertEquals(Arrays.asList("SELECT e1, e2 FROM pm1.g1 WHERE e2 = 5"), dataManager.getQueries()); //$NON-NLS-1$
    }

    @Test public void testShouldExecuteUpdate() throws Exception {
        Update update = new Update();

        update.setGroup(new GroupSymbol("test")); //$NON-NLS-1$

        update.addChange(new ElementSymbol("e1"), new Constant("1")); //$NON-NLS-1$ //$NON-NLS-2$

        assertTrue(RelationalNodeUtil.shouldExecute(update, false));

        update.setChangeList(new SetClauseList());

        assertFalse(RelationalNodeUtil.shouldExecute(update, false));
    }

    @Test public void testShouldExecuteLimitZero() throws Exception {
        Query query = (Query)QueryParser.getQueryParser().parseCommand("SELECT e1, e2 FROM pm1.g1 LIMIT 0"); //$NON-NLS-1$
        assertFalse(RelationalNodeUtil.shouldExecute(query, false));
    }

    @Test public void testShouldExecuteAgg() throws Exception {
        Query query = (Query)QueryParser.getQueryParser().parseCommand("SELECT count(*) FROM pm1.g1 where false"); //$NON-NLS-1$
        assertTrue(RelationalNodeUtil.shouldExecute(query, false));
    }

    @Test public void testUninitailizedClose() throws Exception {
        new AccessNode().close();
    }
}
