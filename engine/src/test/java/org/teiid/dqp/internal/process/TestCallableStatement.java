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

package org.teiid.dqp.internal.process;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.junit.Test;
import org.teiid.api.exception.query.QueryResolverException;
import org.teiid.cache.DefaultCacheFactory;
import org.teiid.core.TeiidComponentException;
import org.teiid.core.TeiidProcessingException;
import org.teiid.query.optimizer.capabilities.DefaultCapabilitiesFinder;
import org.teiid.query.processor.HardcodedDataManager;
import org.teiid.query.processor.TestProcessor;
import org.teiid.query.unittest.RealMetadataFactory;

@SuppressWarnings("nls")
public class TestCallableStatement {

    @Test public void testMissingInput() throws Exception {
        String sql = "{? = call pm4.spTest9()}"; //$NON-NLS-1$

        try {
            TestPreparedStatement.helpTestProcessing(sql, Collections.EMPTY_LIST, null, new HardcodedDataManager(), RealMetadataFactory.exampleBQTCached(), true, RealMetadataFactory.exampleBQTVDB());
            fail();
        } catch (QueryResolverException e) {
            assertEquals("TEIID30089 Required parameter 'pm4.spTest9.inkey' has no value was set or is an invalid parameter.", e.getMessage()); //$NON-NLS-1$
        }
    }

    @Test public void testProcedurePlanCaching() throws Exception {
        String sql = "{? = call BQT_V.v_spTest9(?)}"; //$NON-NLS-1$

        List values = new ArrayList();
        values.add(1);

        List[] expected = new List[1];
        expected[0] = Arrays.asList(1);

        HardcodedDataManager dataManager = new HardcodedDataManager();
        dataManager.addData("ret = EXEC pm4.spTest9(1)", expected);

        TestPreparedStatement.helpTestProcessing(sql, values, expected, dataManager, RealMetadataFactory.exampleBQTCached(), true, RealMetadataFactory.exampleBQTVDB());
    }

    @Test public void testReturnParameter() throws Exception {
        String sql = "{? = call pm4.spTest9(inkey=>?)}"; //$NON-NLS-1$

        List values = new ArrayList();
        values.add(1);

        List[] expected = new List[1];
        expected[0] = Arrays.asList(1);

        HardcodedDataManager dataManager = new HardcodedDataManager();
        dataManager.addData("? = EXEC pm4.spTest9(1)", expected);

        helpProcess(sql, values, expected, dataManager);
    }

    /**
     * help process a physical callable statement
     */
    private void helpProcess(String sql, List values, List[] expected,
            HardcodedDataManager dataManager) throws TeiidComponentException,
            TeiidProcessingException, Exception {
        SessionAwareCache<PreparedPlan> planCache = new SessionAwareCache<PreparedPlan>("preparedplan", DefaultCacheFactory.INSTANCE, SessionAwareCache.Type.PREPAREDPLAN, 0); //$NON-NLS-1$
        PreparedStatementRequest plan = TestPreparedStatement.helpGetProcessorPlan(sql, values, new DefaultCapabilitiesFinder(), RealMetadataFactory.exampleBQTCached(), planCache, 1, true, false, RealMetadataFactory.exampleBQTVDB());
        TestProcessor.doProcess(plan.processPlan, dataManager, expected, plan.context);

        TestPreparedStatement.helpGetProcessorPlan(sql, values, new DefaultCapabilitiesFinder(), RealMetadataFactory.exampleBQTCached(), planCache, 1, true, false, RealMetadataFactory.exampleBQTVDB());
        assertEquals(0, planCache.getCacheHitCount());
    }

    @Test public void testNoReturnParameter() throws Exception {
        String sql = "{call pm4.spTest9(?)}"; //$NON-NLS-1$

        List values = new ArrayList();
        values.add(1);

        List[] expected = new List[1];
        expected[0] = Arrays.asList(1);

        HardcodedDataManager dataManager = new HardcodedDataManager();
        dataManager.addData("EXEC pm4.spTest9(1)", expected);

        helpProcess(sql, values, expected, dataManager);
    }

    @Test public void testOutParameter() throws Exception {
        String sql = "{call pm2.spTest8(?, ?)}"; //$NON-NLS-1$

        List values = new ArrayList();
        values.add(2);

        List[] expected = new List[1];
        expected[0] = Arrays.asList(null, null, 1);

        HardcodedDataManager dataManager = new HardcodedDataManager();
        dataManager.addData("EXEC pm2.spTest8(2)", expected);

        helpProcess(sql, values, expected, dataManager);
    }

    @Test(expected=QueryResolverException.class) public void testInvalidReturn() throws Exception {
        String sql = "{? = call pm2.spTest8(?, ?)}"; //$NON-NLS-1$

        List values = Arrays.asList(2);

        List[] expected = new List[0];

        HardcodedDataManager dataManager = new HardcodedDataManager();
        TestPreparedStatement.helpTestProcessing(sql, values, expected, dataManager, RealMetadataFactory.exampleBQTCached(), true, RealMetadataFactory.exampleBQTVDB());
    }

    @Test public void testInputExpression() throws Exception {
        String sql = "{call pm2.spTest8(1, ?)}"; //$NON-NLS-1$

        List[] expected = new List[1];
        expected[0] = Arrays.asList(null, null, 0);

        HardcodedDataManager dataManager = new HardcodedDataManager();
        dataManager.addData("EXEC pm2.spTest8(1)", expected);

        helpProcess(sql, null, expected, dataManager);
    }

}
