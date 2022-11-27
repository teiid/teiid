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

package org.teiid.query.processor;

import static org.junit.Assert.*;
import static org.teiid.query.processor.TestProcessor.*;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import org.junit.Test;
import org.teiid.UserDefinedAggregate;
import org.teiid.api.exception.query.FunctionExecutionException;
import org.teiid.api.exception.query.QueryMetadataException;
import org.teiid.common.buffer.BufferManagerFactory;
import org.teiid.common.buffer.impl.BufferManagerImpl;
import org.teiid.core.TeiidComponentException;
import org.teiid.core.types.ArrayImpl;
import org.teiid.core.types.BinaryType;
import org.teiid.core.types.BlobType;
import org.teiid.core.types.ClobImpl;
import org.teiid.core.types.ClobType;
import org.teiid.core.types.DataTypeManager;
import org.teiid.core.util.ObjectConverterUtil;
import org.teiid.core.util.UnitTestUtil;
import org.teiid.metadata.AggregateAttributes;
import org.teiid.metadata.FunctionMethod;
import org.teiid.metadata.FunctionParameter;
import org.teiid.metadata.MetadataStore;
import org.teiid.metadata.Schema;
import org.teiid.query.metadata.QueryMetadataInterface;
import org.teiid.query.metadata.TransformationMetadata;
import org.teiid.query.optimizer.TestAggregatePushdown;
import org.teiid.query.optimizer.TestOptimizer;
import org.teiid.query.optimizer.TestOptimizer.ComparisonMode;
import org.teiid.query.optimizer.capabilities.BasicSourceCapabilities;
import org.teiid.query.optimizer.capabilities.CapabilitiesFinder;
import org.teiid.query.optimizer.capabilities.DefaultCapabilitiesFinder;
import org.teiid.query.optimizer.capabilities.FakeCapabilitiesFinder;
import org.teiid.query.optimizer.capabilities.SourceCapabilities.Capability;
import org.teiid.query.resolver.TestResolver;
import org.teiid.query.sql.lang.Command;
import org.teiid.query.unittest.RealMetadataFactory;
import org.teiid.query.util.CommandContext;
import org.teiid.query.util.Options;
import org.teiid.query.validator.TestValidator;
import org.teiid.translator.SourceSystemFunctions;

@SuppressWarnings({"nls", "unchecked", "rawtypes"})
public class TestAggregateProcessing {

    static void sampleDataBQT3(FakeDataManager dataMgr) throws Exception {
        QueryMetadataInterface metadata = RealMetadataFactory.exampleBQTCached();

        // Group bqt1.smalla

        List[] tuples = new List[20];
        for (int i = 0; i < tuples.length; i++) {
            tuples[i] = new ArrayList(17);
            tuples[i].add(new Integer(i));
            tuples[i].add("" + i); //$NON-NLS-1$
            tuples[i].add(new Integer(i + 1));
            for (int j = 0; j < 14; j++) {
                tuples[i].add(null);
            }
        }

        dataMgr.registerTuples(metadata, "bqt1.smalla", tuples); //$NON-NLS-1$

        tuples = new List[20];
        for (int i = 0; i < tuples.length; i++) {
            tuples[i] = new ArrayList(17);
            tuples[i].add(new Integer(i));
            for (int j = 0; j < 16; j++) {
                tuples[i].add(null);
            }
        }

        dataMgr.registerTuples(metadata, "bqt2.mediumb", tuples); //$NON-NLS-1$
    }

    private void sampleDataBQT_defect9842(FakeDataManager dataMgr) throws Exception {
        QueryMetadataInterface metadata = RealMetadataFactory.exampleBQTCached();

        List[] tuples = new List[5];
        for (int i = 0; i < tuples.length; i++) {
            int k = i + 10;
            tuples[i] = new ArrayList(17);
            if (i < 2) {
                tuples[i].add(new Integer(1)); // need duplicate values
            } else {
                tuples[i].add(new Integer(2)); // need duplicate values
            }
            tuples[i].add("" + k); //$NON-NLS-1$
            tuples[i].add(new Integer(k + 1));
            tuples[i].add("" + (k + 1)); //$NON-NLS-1$
            tuples[i].add(new Float(0.5));
            for (int j = 0; j < 8; j++) {
                tuples[i].add(null);
            }
            tuples[i].add(new Short((short) k));
            tuples[i].add(null);
            tuples[i].add(new BigDecimal("" + k)); //$NON-NLS-1$
            tuples[i].add(null);
        }

        dataMgr.registerTuples(metadata, "bqt1.smalla", tuples); //$NON-NLS-1$
    }

    @Test public void testAggregateOnBQT() throws Exception {
        // Create query
        String sql = "SELECT IntKey, SUM(IntNum) FROM BQT1.SmallA GROUP BY IntKey, IntNum HAVING IntNum > 10 ORDER BY IntKey"; //$NON-NLS-1$

        // Create expected results
        List[] expected = new List[] {
                Arrays.asList(new Object[] { new Integer(10), new Long(11) }),
                Arrays.asList(new Object[] { new Integer(11), new Long(12) }),
                Arrays.asList(new Object[] { new Integer(12), new Long(13) }),
                Arrays.asList(new Object[] { new Integer(13), new Long(14) }),
                Arrays.asList(new Object[] { new Integer(14), new Long(15) }),
                Arrays.asList(new Object[] { new Integer(15), new Long(16) }),
                Arrays.asList(new Object[] { new Integer(16), new Long(17) }),
                Arrays.asList(new Object[] { new Integer(17), new Long(18) }),
                Arrays.asList(new Object[] { new Integer(18), new Long(19) }),
                Arrays.asList(new Object[] { new Integer(19), new Long(20) }) };

        // Construct data manager with data
        FakeDataManager dataManager = new FakeDataManager();
        sampleDataBQT3(dataManager);

        // Plan query
        ProcessorPlan plan = helpGetPlan(sql, RealMetadataFactory
                .exampleBQTCached());

        // Run query
        helpProcess(plan, dataManager, expected);
    }

    @Test public void testAggregateSubquery() throws Exception {
        // Create query
        String sql = "SELECT IntKey, SUM((select IntNum from bqt1.smallb where intkey = smalla.intkey)) FROM BQT1.SmallA GROUP BY IntKey, IntNum HAVING IntNum > 10 ORDER BY IntKey"; //$NON-NLS-1$

        // Create expected results
        List[] expected = new List[] {
                Arrays.asList(new Object[] { 1, 2L }),
                Arrays.asList(new Object[] { 2, 3L }) };

        // Construct data manager with data
        HardcodedDataManager dataManager = new HardcodedDataManager();
        dataManager.addData("SELECT g_0.IntKey, g_0.IntNum FROM BQT1.SmallA AS g_0 WHERE g_0.IntNum > 10", new List<?>[] {Arrays.asList(1, 2), Arrays.asList(2, 3)});
        dataManager.addData("SELECT g_0.IntNum FROM BQT1.SmallB AS g_0 WHERE g_0.IntKey = 1", new List<?>[] {Arrays.asList(2)});
        dataManager.addData("SELECT g_0.IntNum FROM BQT1.SmallB AS g_0 WHERE g_0.IntKey = 2", new List<?>[] {Arrays.asList(3)});

        // Plan query
        ProcessorPlan plan = helpGetPlan(sql, RealMetadataFactory.exampleBQTCached(), TestOptimizer.getGenericFinder());

        // Run query
        helpProcess(plan, dataManager, expected);
    }

    @Test public void testAggregateOnBQT2() throws Exception {
        // Create query
        String sql = "SELECT IntNum, IsNotNull FROM (SELECT IntNum, LongNum, COUNT(IntNum) AS IsNotNull FROM BQT1.SmallA GROUP BY IntNum, LongNum HAVING LongNum IS NULL ) AS x ORDER BY IntNum, IsNotNull"; //$NON-NLS-1$

        // Create expected results
        List[] expected = new List[] {
                Arrays.asList(new Object[] { new Integer(1), new Integer(1) }),
                Arrays.asList(new Object[] { new Integer(2), new Integer(1) }),
                Arrays.asList(new Object[] { new Integer(3), new Integer(1) }),
                Arrays.asList(new Object[] { new Integer(4), new Integer(1) }),
                Arrays.asList(new Object[] { new Integer(5), new Integer(1) }),
                Arrays.asList(new Object[] { new Integer(6), new Integer(1) }),
                Arrays.asList(new Object[] { new Integer(7), new Integer(1) }),
                Arrays.asList(new Object[] { new Integer(8), new Integer(1) }),
                Arrays.asList(new Object[] { new Integer(9), new Integer(1) }),
                Arrays.asList(new Object[] { new Integer(10), new Integer(1) }),
                Arrays.asList(new Object[] { new Integer(11), new Integer(1) }),
                Arrays.asList(new Object[] { new Integer(12), new Integer(1) }),
                Arrays.asList(new Object[] { new Integer(13), new Integer(1) }),
                Arrays.asList(new Object[] { new Integer(14), new Integer(1) }),
                Arrays.asList(new Object[] { new Integer(15), new Integer(1) }),
                Arrays.asList(new Object[] { new Integer(16), new Integer(1) }),
                Arrays.asList(new Object[] { new Integer(17), new Integer(1) }),
                Arrays.asList(new Object[] { new Integer(18), new Integer(1) }),
                Arrays.asList(new Object[] { new Integer(19), new Integer(1) }),
                Arrays.asList(new Object[] { new Integer(20), new Integer(1) }) };

        // Construct data manager with data
        FakeDataManager dataManager = new FakeDataManager();
        sampleDataBQT3(dataManager);

        // Plan query
        ProcessorPlan plan = helpGetPlan(sql, RealMetadataFactory
                .exampleBQTCached());

        // Run query
        helpProcess(plan, dataManager, expected);
    }

    @Test public void testAggregateOnBQT_defect9842() throws Exception {
        // Create query
        String sql = "SELECT IntKey, SUM((BigDecimalValue)*(ShortValue)-(BigDecimalValue)*(ShortValue)*(FloatNum)) " + //$NON-NLS-1$
                "AS MySum FROM BQT1.SmallA GROUP BY IntKey ORDER BY IntKey"; //$NON-NLS-1$

        // Create expected results
        List[] expected = new List[] {
                Arrays.asList(new Object[] { new Integer(1),
                        new BigDecimal("110.5") }), //$NON-NLS-1$
                Arrays.asList(new Object[] { new Integer(2),
                        new BigDecimal("254.5") }) //$NON-NLS-1$
        };

        // Construct data manager with data
        FakeDataManager dataManager = new FakeDataManager();
        sampleDataBQT_defect9842(dataManager);

        // Plan query
        ProcessorPlan plan = helpGetPlan(sql, RealMetadataFactory
                .exampleBQTCached());

        // Run query
        helpProcess(plan, dataManager, expected);
    }

    @Test public void testCase186260() {
        /*
         * This case revealed that an expression like "COUNT( DISTINCT e1 )", where the type of e1 is
         * anything but integer, was not handled properly.  We tried to use "integer" (the type of the
         * COUNT expression) to work with the e1 tuples.
         */
        // Create query
        String sql = "SELECT COUNT(DISTINCT pm1.g2.e1), COUNT(DISTINCT pm1.g3.e1) FROM pm1.g2, pm1.g3"; //$NON-NLS-1$

        // Create expected results
        List[] expected = new List[] {
            Arrays.asList(new Object[] { new Integer(3), new Integer(3) }),
        };

        // Construct data manager with data
        FakeDataManager dataManager = new FakeDataManager();
        sampleData1(dataManager);

        // Plan query
        ProcessorPlan plan = helpGetPlan(sql, RealMetadataFactory.example1Cached());

        // Run query
        helpProcess(plan, dataManager, expected);
    }

    @Test public void testAggregatePushdown() {
        Command command = helpParse("select e1, count(e2), max(e2) from (select e1, e2, e3 from pm1.g1 union all select e1, e2, e3 from pm1.g2 union all select e1, e2, e3 from pm2.g1) z group by e1"); //$NON-NLS-1$

        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        capFinder.addCapabilities("pm1", TestAggregatePushdown.getAggregateCapabilities()); //$NON-NLS-1$
        capFinder.addCapabilities("pm2", TestOptimizer.getTypicalCapabilities()); //$NON-NLS-1$
        HardcodedDataManager dataManager = new HardcodedDataManager();

        dataManager.addData("SELECT g_0.e1, COUNT(g_0.e2), MAX(g_0.e2) FROM pm1.g1 AS g_0 GROUP BY g_0.e1", //$NON-NLS-1$
                new List[] {
                    Arrays.asList("a", Integer.valueOf(2), Integer.valueOf(1)), //$NON-NLS-1$
                });
        dataManager.addData("SELECT g_0.e1, COUNT(g_0.e2), MAX(g_0.e2) FROM pm1.g2 AS g_0 GROUP BY g_0.e1", //$NON-NLS-1$
                new List[] {
                    Arrays.asList("a", Integer.valueOf(3), Integer.valueOf(2)), //$NON-NLS-1$
                });
        dataManager.addData("SELECT g_0.e1, g_0.e2 FROM pm2.g1 AS g_0", //$NON-NLS-1$
                new List[] {
                    Arrays.asList("a", Integer.valueOf(3)), //$NON-NLS-1$
                    Arrays.asList("xyz", Integer.valueOf(4)), //$NON-NLS-1$
                    Arrays.asList(null, Integer.valueOf(5)),
                });

        ProcessorPlan plan = helpGetPlan(command, RealMetadataFactory.example1Cached(), capFinder);

        List[] expected = new List[] {
                Arrays.asList(null, Integer.valueOf(1), Integer.valueOf(5)),
                Arrays.asList("a", Integer.valueOf(6), Integer.valueOf(3)), //$NON-NLS-1$
                Arrays.asList("xyz", Integer.valueOf(1), Integer.valueOf(4)) //$NON-NLS-1$
            };

        helpProcess(plan, dataManager, expected);
    }

    @Test public void testUnionAggregatePushdown() {
        Command command = helpParse("select count(*), max(e3) from (select e1, e2, e3 from pm1.g1 union all (select convert(e2, string) as a, e2, e3 from pm2.g2 order by a limit 10)) x group by e1, e2"); //$NON-NLS-1$

        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        capFinder.addCapabilities("pm1", TestAggregatePushdown.getAggregateCapabilities()); //$NON-NLS-1$
        BasicSourceCapabilities bac = TestAggregatePushdown.getAggregateCapabilities();
        bac.setFunctionSupport("convert", true); //$NON-NLS-1$
        capFinder.addCapabilities("pm2", bac); //$NON-NLS-1$
        HardcodedDataManager dataManager = new HardcodedDataManager();

        dataManager.addData("SELECT g_0.e1, g_0.e2, COUNT(*), MAX(g_0.e3) FROM pm1.g1 AS g_0 GROUP BY g_0.e1, g_0.e2", //$NON-NLS-1$
                new List[] {
                    Arrays.asList("2", Integer.valueOf(2), Integer.valueOf(2), null), //$NON-NLS-1$
                    Arrays.asList("1", Integer.valueOf(1), Integer.valueOf(3), Boolean.TRUE), //$NON-NLS-1$
                });
        dataManager.addData("SELECT v_0.c_0, v_0.c_1, COUNT(*), MAX(v_0.c_2) FROM (SELECT convert(g_0.e2, string) AS c_0, g_0.e2 AS c_1, g_0.e3 AS c_2 FROM pm2.g2 AS g_0 ORDER BY c_0 LIMIT 10) AS v_0 GROUP BY v_0.c_0, v_0.c_1", //$NON-NLS-1$
                new List[] {
                    Arrays.asList("1", Integer.valueOf(1), Integer.valueOf(4), Boolean.FALSE), //$NON-NLS-1$
                });

        ProcessorPlan plan = helpGetPlan(command, RealMetadataFactory.example1Cached(), capFinder);

        List[] expected = new List[] {
                Arrays.asList(Integer.valueOf(7), Boolean.TRUE),
                Arrays.asList(Integer.valueOf(2), null),
            };

        helpProcess(plan, dataManager, expected);
    }

    @Test public void testPushDownOverUnionMixed1() throws Exception {
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = TestAggregatePushdown.getAggregateCapabilities();
        caps.setFunctionSupport(SourceSystemFunctions.POWER, true);
        caps.setFunctionSupport(SourceSystemFunctions.CONVERT, true);
        capFinder.addCapabilities("pm2", caps); //$NON-NLS-1$
        capFinder.addCapabilities("pm1", TestOptimizer.getTypicalCapabilities()); //$NON-NLS-1$

        ProcessorPlan plan = helpGetPlan(helpParse("select max(e2), count(*), stddev_pop(e2), var_samp(e2) from (select e1, e2 from pm1.g1 union all select e1, e2 from pm2.g2) z"), RealMetadataFactory.example1Cached(), capFinder); //$NON-NLS-1$

        HardcodedDataManager dataManager = new HardcodedDataManager();
        dataManager.addData("SELECT g_0.e2 FROM pm1.g1 AS g_0", new List[] {Arrays.asList(1), Arrays.asList(2)});
        dataManager.addData("SELECT MAX(g_0.e2), COUNT(*), COUNT(g_0.e2), SUM(power(g_0.e2, 2)), SUM(g_0.e2) FROM pm2.g2 AS g_0", new List[] {Arrays.asList(5, 6, 4, BigInteger.valueOf(50L), 10L)});

        List[] expected = new List[] {
            Arrays.asList(5, 8, 2.1147629234082532, 5.366666666666666),
        };

        helpProcess(plan, dataManager, expected);
    }

    @Test public void testPushDownOverUnionMixed2() throws Exception {
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = TestAggregatePushdown.getAggregateCapabilities();
        caps.setFunctionSupport(SourceSystemFunctions.POWER, true);
        caps.setFunctionSupport(SourceSystemFunctions.CONVERT, true);
        capFinder.addCapabilities("pm2", caps); //$NON-NLS-1$
        capFinder.addCapabilities("pm1", TestOptimizer.getTypicalCapabilities()); //$NON-NLS-1$

        ProcessorPlan plan = helpGetPlan(helpParse("select max(e2), count_big(*), stddev_pop(e2), var_samp(e2) from (select e1, e2 from pm1.g1 union all select e1, e2 from pm2.g2) z"), RealMetadataFactory.example1Cached(), capFinder); //$NON-NLS-1$

        HardcodedDataManager dataManager = new HardcodedDataManager();
        dataManager.addData("SELECT g_0.e2 FROM pm1.g1 AS g_0", new List[] {Arrays.asList(1), Arrays.asList(2)});
        dataManager.addData("SELECT MAX(g_0.e2), COUNT_BIG(*), COUNT(g_0.e2), SUM(power(g_0.e2, 2)), SUM(g_0.e2) FROM pm2.g2 AS g_0", new List[] {Arrays.asList(5, 6L, 4, BigInteger.valueOf(50L), 10L)});

        List[] expected = new List[] {
            Arrays.asList(5, 8L, 2.1147629234082532, 5.366666666666666),
        };

        helpProcess(plan, dataManager, expected);
    }

    @Test public void testBooleanAgg() {
        String sql = "select every(e3), any(e3) from pm1.g1"; //$NON-NLS-1$

        List[] expected = new List[] {
                Arrays.asList(Boolean.FALSE, Boolean.TRUE),
        };

        FakeDataManager dataManager = new FakeDataManager();
        sampleData1(dataManager);

        ProcessorPlan plan = helpGetPlan(helpParse(sql), RealMetadataFactory.example1Cached());

        helpProcess(plan, dataManager, expected);
    }

    @Test public void testStatsFunctions() {
        String sql = "select stddev_pop(e2), var_samp(e2) from pm1.g1"; //$NON-NLS-1$

        List[] expected = new List[] {
                Arrays.asList(1.0671873729054748, 1.3666666666666667),
        };

        FakeDataManager dataManager = new FakeDataManager();
        sampleData1(dataManager);

        ProcessorPlan plan = helpGetPlan(helpParse(sql), RealMetadataFactory.example1Cached());

        helpProcess(plan, dataManager, expected);
    }

    @Test public void testStatsFunctions1() {
        String sql = "select stddev_samp(e2), var_pop(e2) from (select 2 e2) x"; //$NON-NLS-1$

        List[] expected = new List[] {
                Arrays.asList(null, 0.0),
        };

        FakeDataManager dataManager = new FakeDataManager();
        sampleData1(dataManager);

        ProcessorPlan plan = helpGetPlan(helpParse(sql), RealMetadataFactory.example1Cached());

        helpProcess(plan, dataManager, expected);
    }

    @Test public void testJira1621() throws Exception {
        // Create query
        String sql = "SELECT sum(t2.e4) as s, max(t1.e1 || t2.e1) FROM pm1.g1 as t1, pm1.g2 as t2, pm1.g3 as t3 WHERE t1.e1 = coalesce(t2.e1, 'b') AND t2.e2 = t3.e2 GROUP BY t2.e2, t2.e3, t3.e2 ORDER BY s"; //$NON-NLS-1$

        // Create expected results
        List[] expected = new List[] {
                Arrays.asList(null, "cc"),
                Arrays.asList(0.0, "bb"),
                Arrays.asList(2.0, null),
                Arrays.asList(21.0, "aa"),
                Arrays.asList(24.0, "aa")
        };

        // Construct data manager with data
        FakeDataManager dataManager = new FakeDataManager();
        sampleData1(dataManager);

        // Plan query
        ProcessorPlan plan = helpGetPlan(sql, RealMetadataFactory.example1Cached());

        // Run query
        helpProcess(plan, dataManager, expected);
    }

    @Test public void testMultiJoinCriteria() throws Exception {
        String sql = "SELECT count(t2.e4) as s FROM pm1.g1 as t1, pm1.g2 as t2, pm1.g3 as t3 WHERE t1.e1 = t2.e1 and t2.e2 = t3.e2 and t1.e3 || t2.e3 = t3.e3"; //$NON-NLS-1$

        List[] expected = new List[] {
                Arrays.asList(0)
        };

        FakeDataManager dataManager = new FakeDataManager();
        sampleData1(dataManager);

        ProcessorPlan plan = helpGetPlan(sql, RealMetadataFactory.example1Cached());

        helpProcess(plan, dataManager, expected);
    }

    @Test public void testMultiJoinCriteria1() throws Exception {
        String sql = "SELECT max(t3.e4), max(t2.e4) as s FROM pm1.g1 as t1, pm1.g2 as t2, pm1.g3 as t3, pm1.g4 as t4, pm2.g1 as t5 "
                + "WHERE t1.e1 = t2.e1 and (t2.e2 = t3.e2 and t1.e3 || t2.e3 = t3.e3) and t3.e3 = t4.e3 and t4.e4 = t5.e4"; //$NON-NLS-1$

        List[] expected = new List[] {
                Arrays.asList(null, null)
        };

        HardcodedDataManager dataManager = new HardcodedDataManager(false);

        ProcessorPlan plan = helpGetPlan(sql, RealMetadataFactory.example1Cached());

        helpProcess(plan, dataManager, expected);
    }

    @Test public void testMultiJoinCriteria1a() throws Exception {
        String sql = "SELECT max(t3.e4), max(t2.e4) as s FROM pm1.g1 as t1, pm1.g2 as t2, pm1.g3 as t3, pm1.g4 as t4, pm2.g1 as t5 "
                + "WHERE t1.e1 = t2.e1 and (t2.e2 = t3.e2 and t1.e3 || t2.e3 = t3.e3) and t3.e3 = t4.e3 and t4.e4 = t5.e4"; //$NON-NLS-1$

        List[] expected = new List[] {
                Arrays.asList(null, null)
        };

        HardcodedDataManager dataManager = new HardcodedDataManager(true);

        BasicSourceCapabilities caps = TestAggregatePushdown.getAggregateCapabilities();
        //caps.setCapabilitySupport(Capability.QUERY_GROUP_BY, false);
        caps.setCapabilitySupport(Capability.QUERY_FROM_JOIN_INNER, false);
        caps.setCapabilitySupport(Capability.QUERY_FROM_JOIN_OUTER, false);
        caps.setFunctionSupport("convert", true);

        ProcessorPlan plan = helpGetPlan(sql, RealMetadataFactory.example1Cached(), new DefaultCapabilitiesFinder(caps));

        dataManager.addData("SELECT DISTINCT g_0.e3 AS c_0, g_0.e4 AS c_1 FROM pm1.g4 AS g_0 ORDER BY c_0", Arrays.asList(false, 1.0));
        dataManager.addData("SELECT DISTINCT g_0.e4 AS c_0 FROM pm2.g1 AS g_0 ORDER BY c_0", Arrays.asList(1.0));
        dataManager.addData("SELECT DISTINCT g_0.e1 AS c_0, g_0.e3 AS c_1 FROM pm1.g1 AS g_0 ORDER BY c_0", Arrays.asList("a", false));
        dataManager.addData("SELECT DISTINCT g_0.e1 AS c_0, g_0.e2 AS c_1, g_0.e3 AS c_2, g_0.e4 AS c_3 FROM pm1.g2 AS g_0 ORDER BY c_0", Arrays.asList("a", 1, false, 1.0));
        dataManager.addData("SELECT v_0.c_0, v_0.c_1, v_0.c_2, MAX(v_0.c_3) AS c_3 FROM (SELECT g_0.e2 AS c_0, convert(g_0.e3, string) AS c_1, g_0.e3 AS c_2, g_0.e4 AS c_3 FROM pm1.g3 AS g_0) AS v_0 GROUP BY v_0.c_0, v_0.c_1, v_0.c_2 ORDER BY c_0, c_1");
        //dataManager.addData("SELECT g_0.e2, convert(g_0.e3, string), g_0.e3, g_0.e4 FROM pm1.g3 AS g_0 WHERE g_0.e3 = FALSE", Arrays.asList(1, "false", false, 1.0));

        CommandContext cc = createCommandContext();
        cc.setMetadata(RealMetadataFactory.example1Cached());
        helpProcess(plan, cc, dataManager, expected);
    }

    @Test public void testMultiJoinCriteria2() throws Exception {
        String sql = "SELECT max(t3.e4), max(t2.e4) as s FROM pm1.g1 as t1, pm1.g2 as t2, pm1.g3 as t3, pm1.g4 as t4 "
                + "WHERE t1.e1 = t2.e1 and (t2.e2 = t3.e2 and t1.e3 || t2.e3 = t3.e3) and t3.e3 = t4.e3"; //$NON-NLS-1$

        List[] expected = new List[] {
                Arrays.asList(null, null)
        };

        HardcodedDataManager dataManager = new HardcodedDataManager(false);

        ProcessorPlan plan = helpGetPlan(sql, RealMetadataFactory.example1Cached());

        helpProcess(plan, dataManager, expected);
    }

    @Test public void testMultiJoinGroupBy() throws Exception {
        String sql = "SELECT count(t2.e4) as s, t1.e3 || t2.e3 FROM pm1.g1 as t1, pm1.g2 as t2, pm1.g3 as t3 WHERE t1.e1 = t2.e1 and t2.e2 = t3.e2 GROUP BY t1.e3 || t2.e3"; //$NON-NLS-1$

        List[] expected = new List[] {
                Arrays.asList(9, "falsefalse"),
                Arrays.asList(2, "falsetrue"),
                Arrays.asList(4, "truefalse"),
                Arrays.asList(1, "truetrue"),
        };

        FakeDataManager dataManager = new FakeDataManager();
        sampleData1(dataManager);

        ProcessorPlan plan = helpGetPlan(sql, RealMetadataFactory.example1Cached());

        helpProcess(plan, dataManager, expected);
    }

    @Test public void testArrayAggOrderByPersistence() throws Exception {
        // Create query
        String sql = "SELECT array_agg(e2 order by e1) from pm1.g1 group by e3"; //$NON-NLS-1$

        // Create expected results
        List[] expected = new List[] {
                Arrays.asList(new ArrayImpl(new Integer[] {1, 0, 0, 2})),
                Arrays.asList(new ArrayImpl(new Integer[] {3, 1})),
        };

        // Construct data manager with data
        FakeDataManager dataManager = new FakeDataManager();
        sampleData1(dataManager);

        // Plan query
        ProcessorPlan plan = helpGetPlan(sql, RealMetadataFactory.example1Cached());
        CommandContext cc = TestProcessor.createCommandContext();
        BufferManagerImpl impl = BufferManagerFactory.getTestBufferManager(0, 2);
        cc.setBufferManager(impl);
        // Run query
        helpProcess(plan, cc, dataManager, expected);
    }

    @Test public void testDupGroupCombination() throws Exception {
        String sql = "select count(e2), e1 from (select distinct e1, e2, e3 from pm1.g1) x group by e1"; //$NON-NLS-1$

        List[] expected = new List[] {
                Arrays.asList(2, "a"),
        };

        HardcodedDataManager dataManager = new HardcodedDataManager();
        dataManager.addData("SELECT pm1.g1.e1, pm1.g1.e2, pm1.g1.e3 FROM pm1.g1", new List[] {
                Arrays.asList("a", 0, Boolean.TRUE),
                Arrays.asList("a", 0, Boolean.FALSE),
        });

        ProcessorPlan plan = helpGetPlan(sql, RealMetadataFactory.example1Cached());
        helpProcess(plan, dataManager, expected);
    }

    @Test public void testAggFilter() throws Exception {
        // Create query
        String sql = "SELECT e2, count(*) filter (where e3) from pm1.g1 group by e2 order by e2"; //$NON-NLS-1$

        // Create expected results
        List[] expected = new List[] {
                Arrays.asList(0, 0),
                Arrays.asList(1, 1),
                Arrays.asList(2, 0),
                Arrays.asList(3, 1),
        };

        // Construct data manager with data
        FakeDataManager dataManager = new FakeDataManager();
        sampleData1(dataManager);

        ProcessorPlan plan = helpGetPlan(sql, RealMetadataFactory.example1Cached());
        helpProcess(plan, dataManager, expected);
    }

    @Test public void testAggSumStateNPE() throws Exception {
        // need a group/sort query where the group by is not pushed down
        String sql = "SELECT B.INTKEY, SUM(cast(B.BIGDECIMALVALUE as BIGINTEGER)) FROM bqt1.SMALLA AS B group by INTKEY ORDER BY INTKEY;"; //$NON-NLS-1$

        List[] expected = new List[] {
                Arrays.asList(0, BigInteger.valueOf(0)),
                Arrays.asList(1, null),
        };

        HardcodedDataManager dataManager = new HardcodedDataManager();
        List<?>[] data = new List[2];
        data[0] = Arrays.asList(0, BigDecimal.valueOf(0));
        data[1] = Arrays.asList(1, null); //need the last value to be null

        dataManager.addData("SELECT g_0.IntKey, g_0.BigDecimalValue FROM BQT1.SmallA AS g_0", data);

        ProcessorPlan plan = helpGetPlan(sql, RealMetadataFactory.exampleBQTCached(), TestAggregatePushdown.getAggregatesFinder());
        helpProcess(plan, dataManager, expected);
    }

    @Test public void testGroupSortMultipleAggregates() throws Exception {
        String sql = "select e1, min(e2), max(e3) from pm1.g1 group by e1";

        List[] expected = new List[] {
                Arrays.asList(null, 1, false),
                Arrays.asList("a", 0, true),
                Arrays.asList("b", 2, false),
                Arrays.asList("c", 1, true),
        };

        // Construct data manager with data
        FakeDataManager dataManager = new FakeDataManager();
        sampleData1(dataManager);

        ProcessorPlan plan = helpGetPlan(sql, RealMetadataFactory.example1Cached());
        helpProcess(plan, dataManager, expected);
    }

    public static class SumAll implements UserDefinedAggregate<Integer> {

        private boolean isNull = true;
        private int result;

        public void addInput(Integer... vals) {
            isNull = false;
            for (int i : vals) {
                result += i;
            }
        }

        @Override
        public Integer getResult(org.teiid.CommandContext commandContext) {
            if (isNull) {
                return null;
            }
            return result;
        }

        @Override
        public void reset() {
            isNull = true;
            result = 0;
        }

    }

    public static class CustomSum implements UserDefinedAggregate<Integer> {

        private boolean isNull = true;
        private int result;

        public void addInput(Integer... vals) {
            isNull = false;
            for (Integer i : vals) {
                if (i == null) {
                    result += 0;
                    continue;
                }
                result += i;
            }
        }

        @Override
        public Integer getResult(org.teiid.CommandContext commandContext) {
            if (isNull) {
                return null;
            }
            return result;
        }

        @Override
        public void reset() {
            isNull = true;
            result = 0;
        }

    }


    public static class LongSumAll implements UserDefinedAggregate<Long> {

        private boolean isNull = true;
        private long result;

        public void addInput(Integer... vals) {
            isNull = false;
            for (int i : vals) {
                result += i;
            }
        }

        @Override
        public Long getResult(org.teiid.CommandContext commandContext) {
            if (isNull) {
                return null;
            }
            return result;
        }

        @Override
        public void reset() {
            isNull = true;
            result = 0;
        }

    }

    @Test public void testUserDefined() throws Exception {
        MetadataStore ms = RealMetadataFactory.example1Store();
        Schema s = ms.getSchemas().get("PM1");
        AggregateAttributes aa = addAgg(s, "myagg", SumAll.class, DataTypeManager.DefaultDataTypes.INTEGER).getAggregateAttributes();
        addAgg(s, "myagg2", LongSumAll.class, DataTypeManager.DefaultDataTypes.LONG);
        TransformationMetadata metadata = RealMetadataFactory.createTransformationMetadata(ms, "test");

        Command c = TestResolver.helpResolve("select myagg2(distinct e2) from pm1.g1", metadata);
        assertEquals(DataTypeManager.DefaultDataClasses.LONG, c.getProjectedSymbols().get(0).getType());

        //must be in agg form
        TestValidator.helpValidate("SELECT myagg(e2) from pm1.g1", new String[] {}, metadata);

        //run validations over default AggregateAttributes
        TestValidator.helpValidate("SELECT myagg(distinct e2) from pm1.g1", new String[] {"myagg(DISTINCT e2)"}, metadata);
        TestValidator.helpValidate("SELECT myagg(e2 order by e1) from pm1.g1", new String[] {"myagg(ALL e2 ORDER BY e1)"}, metadata);
        TestValidator.helpValidate("SELECT myagg(ALL e2, e2) over () from pm1.g1", new String[] {}, metadata);

        aa.setAllowsDistinct(true);
        aa.setAllowsOrderBy(true);

        TestValidator.helpValidate("SELECT myagg(distinct e2) from pm1.g1", new String[] {}, metadata);
        TestValidator.helpValidate("SELECT myagg(e2 order by e1) from pm1.g1", new String[] {}, metadata);

        aa.setAnalytic(true);

        TestValidator.helpValidate("SELECT myagg(distinct e2) from pm1.g1", new String[] {"myagg(DISTINCT e2)"}, metadata);
        TestValidator.helpValidate("SELECT myagg(e2, e2) over () from pm1.g1", new String[] {}, metadata);

        aa.setAnalytic(false);

        // Create expected results
        List[] expected = new List[] {
                Arrays.asList(6, 6),
                Arrays.asList(8, 8),
        };

        // Construct data manager with data
        FakeDataManager dataManager = new FakeDataManager();
        sampleData1(dataManager);

        ProcessorPlan plan = helpGetPlan("select myagg(all e2, e2 order by e1), myagg(e2, e2) from pm1.g1 group by e3", metadata);
        helpProcess(plan, dataManager, expected);
    }

    private FunctionMethod addAgg(Schema s, String name, Class<?> clazz, String returns) {
        FunctionMethod fm = new FunctionMethod();
        fm.setName(name);
        fm.setInvocationClass(clazz.getName());
        fm.setInvocationMethod("addInput");
        FunctionParameter fp = new FunctionParameter();
        fp.setType(DataTypeManager.DefaultDataTypes.INTEGER);
        fp.setName("arg");
        fp.setVarArg(true);
        fm.getInputParameters().add(fp);
        FunctionParameter fpout = new FunctionParameter();
        fpout.setType(returns);
        fpout.setName("outp");
        fm.setOutputParameter(fpout);

        AggregateAttributes aa = new AggregateAttributes();
        fm.setAggregateAttributes(aa);
        s.getFunctions().put(fm.getName(), fm);
        return fm;
    }

    @Test public void testNullDependentAggParitioned() throws Exception {
        MetadataStore ms = RealMetadataFactory.example1Store();
        Schema s = ms.getSchemas().get("PM1");
        FunctionMethod fm = addAgg(s, "myagg", SumAll.class, DataTypeManager.DefaultDataTypes.INTEGER);
        fm.setNullOnNull(false);
        fm.getAggregateAttributes().setDecomposable(true);
        TransformationMetadata metadata = RealMetadataFactory.createTransformationMetadata(ms, "test");

        final String userSql = "SELECT myagg(e2), source_name FROM (select e2, 'a' as source_name from pm1.g1 union all select e2, 'b' from pm2.g1) x group by source_name"; //$NON-NLS-1$
        BasicSourceCapabilities caps = TestAggregatePushdown.getAggregateCapabilities();
        caps.setFunctionSupport("myagg", true);
        ProcessorPlan plan = TestOptimizer.helpPlan(userSql, metadata, new String[] {"SELECT myagg(ALL v_0.c_1), v_0.c_0 FROM (SELECT 'a' AS c_0, g_0.e2 AS c_1 FROM pm1.g1 AS g_0) AS v_0 GROUP BY v_0.c_0",
                "SELECT myagg(ALL v_0.c_1), v_0.c_0 FROM (SELECT 'b' AS c_0, g_0.e2 AS c_1 FROM pm2.g1 AS g_0) AS v_0 GROUP BY v_0.c_0"}, new DefaultCapabilitiesFinder(caps), ComparisonMode.EXACT_COMMAND_STRING);

        TestOptimizer.checkNodeTypes(plan, new int[] {
                2,      // Access
                0,      // DependentAccess
                0,      // DependentSelect
                0,      // DependentProject
                0,      // DupRemove
                0,      // Grouping
                0,      // NestedLoopJoinStrategy
                0,      // MergeJoinStrategy
                0,      // Null
                0,      // PlanExecution
                1,      // Project
                0,      // Select
                0,      // Sort
                1       // UnionAll
            });
    }

    @Test public void testNullDependentAgg() throws Exception {
        MetadataStore ms = RealMetadataFactory.example1Store();
        Schema s = ms.getSchemas().get("PM1");
        FunctionMethod fm = addAgg(s, "myagg", SumAll.class, DataTypeManager.DefaultDataTypes.INTEGER);
        fm.setNullOnNull(false);
        fm.getAggregateAttributes().setDecomposable(true);
        TransformationMetadata metadata = RealMetadataFactory.createTransformationMetadata(ms, "test");

        final String userSql = "SELECT myagg(e2) FROM (select e2, e1 as source_name from pm1.g1 union all select e2, e1 from pm2.g1) x"; //$NON-NLS-1$
        BasicSourceCapabilities caps = TestAggregatePushdown.getAggregateCapabilities();
        caps.setFunctionSupport("myagg", true);
        ProcessorPlan plan = TestOptimizer.helpPlan(userSql, metadata, new String[] {"SELECT myagg(ALL g_0.e2) FROM pm1.g1 AS g_0 HAVING COUNT(*) > 0",
                "SELECT myagg(ALL g_0.e2) FROM pm2.g1 AS g_0 HAVING COUNT(*) > 0"}, new DefaultCapabilitiesFinder(caps), ComparisonMode.EXACT_COMMAND_STRING);

        TestOptimizer.checkNodeTypes(plan, new int[] {
                2,      // Access
                0,      // DependentAccess
                0,      // DependentSelect
                0,      // DependentProject
                0,      // DupRemove
                1,      // Grouping
                0,      // NestedLoopJoinStrategy
                0,      // MergeJoinStrategy
                0,      // Null
                0,      // PlanExecution
                1,      // Project
                0,      // Select
                0,      // Sort
                1       // UnionAll
            });
    }

    @Test public void testNullDependentAgg1() throws Exception {
        MetadataStore ms = RealMetadataFactory.example1Store();
        Schema s = ms.getSchemas().get("PM1");
        FunctionMethod fm = addAgg(s, "myagg", CustomSum.class, DataTypeManager.DefaultDataTypes.STRING);
        fm.setNullOnNull(false);
        fm.getAggregateAttributes().setDecomposable(true);
        TransformationMetadata metadata = RealMetadataFactory.createTransformationMetadata(ms, "test");

        final String userSql = "SELECT myagg(e2) FROM (select e2, e1 as source_name from pm1.g1 union all select e2, e1 from pm2.g1) x"; //$NON-NLS-1$
        BasicSourceCapabilities caps = TestAggregatePushdown.getAggregateCapabilities();
        caps.setFunctionSupport("myagg", true);
        caps.setCapabilitySupport(Capability.CRITERIA_COMPARE_ORDERED, false);
        ProcessorPlan plan = TestOptimizer.helpPlan(userSql, metadata, new String[] {"SELECT COUNT(*), myagg(ALL g_0.e2) FROM pm2.g1 AS g_0", "SELECT COUNT(*), myagg(ALL g_0.e2) FROM pm1.g1 AS g_0"}, new DefaultCapabilitiesFinder(caps), ComparisonMode.EXACT_COMMAND_STRING);

        TestOptimizer.checkNodeTypes(plan, new int[] {
                2,      // Access
                0,      // DependentAccess
                0,      // DependentSelect
                0,      // DependentProject
                0,      // DupRemove
                1,      // Grouping
                0,      // NestedLoopJoinStrategy
                0,      // MergeJoinStrategy
                0,      // Null
                0,      // PlanExecution
                3,      // Project
                2,      // Select
                0,      // Sort
                1       // UnionAll
            });

        HardcodedDataManager dataManager = new HardcodedDataManager();

        dataManager.addData("SELECT COUNT(*), myagg(ALL g_0.e2) FROM pm1.g1 AS g_0", new List<?>[] {Arrays.asList(0, null)});
        dataManager.addData("SELECT COUNT(*), myagg(ALL g_0.e2) FROM pm2.g1 AS g_0", new List<?>[] {Arrays.asList(0, null)});

        //if we don't filter the nulls, then we'd get 0
        helpProcess(plan, dataManager, new List[] {Collections.singletonList(null)});
    }

    @Test public void testMultiCount() throws Exception {
        // Create query
        String sql = "SELECT count(pm1.g1.e2), count(pm2.g2.e2) from pm1.g1, pm2.g2 where pm1.g1.e1 = pm2.g2.e1"; //$NON-NLS-1$

        // Create expected results
        List[] expected = new List[] {
                Arrays.asList(3, 3),
        };

        // Construct data manager with data
        HardcodedDataManager dataManager = new HardcodedDataManager();

        dataManager.addData("SELECT g_0.e1 AS c_0, g_0.e2 AS c_1 FROM pm1.g1 AS g_0 ORDER BY c_0", new List<?>[] {Arrays.asList("a", 1), Arrays.asList("b", 1), Arrays.asList("b", 2)});
        dataManager.addData("SELECT g_0.e1 AS c_0, g_0.e2 AS c_1 FROM pm2.g2 AS g_0 ORDER BY c_0", new List<?>[] {Arrays.asList("a", 6), Arrays.asList("b", 5)});

        ProcessorPlan plan = helpGetPlan(sql, RealMetadataFactory.example1Cached(), TestAggregatePushdown.getAggregatesFinder());
        helpProcess(plan, dataManager, expected);
    }

    @Test public void testUnaliasedAggInDeleteCompensation() throws Exception {
        String sql = "delete from pm3.g1 where e1 = (SELECT MAX(e1) FROM pm3.g1 as z where e2 = pm3.g1.e2)"; //$NON-NLS-1$

        List[] expected = new List[] {
                Arrays.asList(1),
        };

        HardcodedDataManager dataManager = new HardcodedDataManager();

        dataManager.addData("SELECT g_0.e1 AS c_0, g_0.e2 AS c_1 FROM pm3.g1 AS g_0 ORDER BY c_1, c_0", new List<?>[] {Arrays.asList("a", 1)});
        dataManager.addData("SELECT MAX(g_0.e1) AS c_0, g_0.e2 AS c_1 FROM pm3.g1 AS g_0 GROUP BY g_0.e2 ORDER BY c_1, c_0", new List<?>[] {Arrays.asList("a", 1)});
        dataManager.addData("DELETE FROM pm3.g1 WHERE pm3.g1.e1 = 'a'", new List<?>[] {Arrays.asList(1)});
        ProcessorPlan plan = helpGetPlan(sql, RealMetadataFactory.example4(), TestAggregatePushdown.getAggregatesFinder());
        helpProcess(plan, dataManager, expected);
    }

    @Test public void testEmptyCountOverJoin() {
        Command command = helpParse("select count(pm1.g1.e2) from pm1.g1, pm1.g2 where pm1.g1.e1 = pm1.g2.e1"); //$NON-NLS-1$

        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities bsc = TestAggregatePushdown.getAggregateCapabilities();
        bsc.setCapabilitySupport(Capability.QUERY_FROM_JOIN_INNER, false);
        bsc.setCapabilitySupport(Capability.QUERY_FROM_JOIN_OUTER, false);
        capFinder.addCapabilities("pm1", bsc); //$NON-NLS-1$
        HardcodedDataManager dataManager = new HardcodedDataManager();

        dataManager.addData("SELECT g_0.e1 AS c_0, COUNT(g_0.e2) AS c_1 FROM pm1.g1 AS g_0 GROUP BY g_0.e1 ORDER BY c_0", //$NON-NLS-1$
                new List[] {
                     //$NON-NLS-1$
                });
        dataManager.addData("SELECT g_0.e1 AS c_0 FROM pm1.g2 AS g_0 ORDER BY c_0", //$NON-NLS-1$
                new List[] { //$NON-NLS-1$
                });

        ProcessorPlan plan = helpGetPlan(command, RealMetadataFactory.example1Cached(), capFinder);

        List[] expected = new List[] {
                Arrays.asList(0) //$NON-NLS-1$
            };

        helpProcess(plan, dataManager, expected);
    }

    @Test public void testCountOfGroupingColumn() {
        Command command = helpParse("select e1, count(e1) from pm1.g1, (select 1 from pm1.g2 limit 2) x group by e1"); //$NON-NLS-1$

        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities bsc = TestOptimizer.getTypicalCapabilities();
        capFinder.addCapabilities("pm1", bsc); //$NON-NLS-1$
        HardcodedDataManager dataManager = new HardcodedDataManager();

        dataManager.addData("SELECT g_0.e1 FROM pm1.g1 AS g_0", //$NON-NLS-1$
                new List[] {
                Arrays.asList("a"),
                Arrays.asList("a"),
                Arrays.asList("b"),
                     //$NON-NLS-1$
                });
        dataManager.addData("SELECT 1 FROM pm1.g2 AS g_0", //$NON-NLS-1$
                new List[] { //$NON-NLS-1$
                Arrays.asList(1),
                Arrays.asList(1),
                });

        ProcessorPlan plan = helpGetPlan(command, RealMetadataFactory.example1Cached(), capFinder);

        List[] expected = new List[] {
                Arrays.asList("a", 4), //$NON-NLS-1$
                Arrays.asList("b", 2) //$NON-NLS-1$
            };

        helpProcess(plan, dataManager, expected);
    }

    @Test public void testUnaliasedViewAgg() throws Exception {
        String sql = "SELECT MIN(x.count) FROM agg x"; //$NON-NLS-1$

        TransformationMetadata metadata = RealMetadataFactory.fromDDL("create foreign table smalla (intkey integer); create view agg (count integer) as select count(*) from smalla", "x", "y");

        TestOptimizer.helpPlan(sql, metadata, new String[] {"SELECT MIN(v_0.c_0) FROM (SELECT COUNT(*) AS c_0 FROM y.smalla AS g_0) AS v_0"}, TestAggregatePushdown.getAggregatesFinder(), ComparisonMode.EXACT_COMMAND_STRING);
    }

    @Test public void testStringAgg() throws Exception {
        // Create query
        String sql = "SELECT string_agg(e1, ',') from pm1.g1 group by e3"; //$NON-NLS-1$

        // Create expected results
        List[] expected = new List[] {
                Arrays.asList("a,b,a"),
                Arrays.asList("a,c"),
        };

        FakeDataManager dataManager = new FakeDataManager();
        sampleData1(dataManager);

        ProcessorPlan plan = helpGetPlan(sql, RealMetadataFactory.example1Cached(), TestAggregatePushdown.getAggregatesFinder());
        helpProcess(plan, dataManager, expected);
    }

    @Test public void testStringAggNoRows() throws Exception {
        // Create query
        String sql = "SELECT string_agg(e1, ',') from pm1.g1 where e2 > 10"; //$NON-NLS-1$

        // Create expected results
        List[] expected = new List[] {
                Collections.singletonList(null),
        };

        FakeDataManager dataManager = new FakeDataManager();
        sampleData1(dataManager);

        ProcessorPlan plan = helpGetPlan(sql, RealMetadataFactory.example1Cached(), TestAggregatePushdown.getAggregatesFinder());
        helpProcess(plan, dataManager, expected);
    }

    @Test public void testStringAggBinary() throws Exception {
        // Create query
        String sql = "SELECT cast(string_agg(to_bytes(e1, 'UTF-8'), X'AB') as varbinary) from pm1.g1 group by e3"; //$NON-NLS-1$

        // Create expected results
        List[] expected = new List[] {
                Arrays.asList(new BinaryType(new byte[] {(byte)0x61, (byte)0xAB, (byte)0x62, (byte)0xAB, (byte)0x61})),
                Arrays.asList(new BinaryType(new byte[] {(byte)0x61, (byte)0xAB, (byte)0x63})),
        };

        FakeDataManager dataManager = new FakeDataManager();
        sampleData1(dataManager);

        ProcessorPlan plan = helpGetPlan(sql, RealMetadataFactory.example1Cached(), TestAggregatePushdown.getAggregatesFinder());
        helpProcess(plan, dataManager, expected);
    }

    @Test public void testCorrelatedGroupingColumn() throws Exception {
        // Create query
        String sql = "SELECT A.e2, A.e1 FROM pm1.g1 AS A GROUP BY A.e2, A.e1 HAVING A.e1 = (SELECT MAX(B.e1) FROM pm1.g1 AS B WHERE A.e2 = B.e2)"; //$NON-NLS-1$

        // Create expected results
        List[] expected = new List[] {
                Arrays.asList(new Object[] { 0, "a" }),
                Arrays.asList(new Object[] { 1, "c" }),
                Arrays.asList(new Object[] { 2, "b" }),
                Arrays.asList(new Object[] { 3, "a" })};

        // Construct data manager with data
        FakeDataManager dataManager = new FakeDataManager();
        FakeDataStore.sampleData1(dataManager, RealMetadataFactory.example1Cached());

        // Plan query
        ProcessorPlan plan = helpGetPlan(sql, RealMetadataFactory.example1Cached());

        // Run query
        helpProcess(plan, dataManager, expected);
    }

    @Test public void testRollup() throws Exception {
        String sql = "select e1, sum(e2) from pm1.g1 group by rollup(e1)"; //$NON-NLS-1$

        List[] expected = new List[] {
                Arrays.asList("a", Long.valueOf(3)),
                Arrays.asList("b", Long.valueOf(1)),
                Arrays.asList(null, Long.valueOf(4))
                };

        ProcessorPlan plan = helpGetPlan(sql, RealMetadataFactory.example1Cached());

        HardcodedDataManager hdm = new HardcodedDataManager();
        hdm.addData("SELECT pm1.g1.e1, pm1.g1.e2 FROM pm1.g1", Arrays.asList("a", 1), Arrays.asList("a", 2), Arrays.asList("b", 1));
        helpProcess(plan, hdm, expected);

        expected = new List[] {
                Arrays.asList("a", Long.valueOf(4)),
                Arrays.asList(null, Long.valueOf(4))
                };

        plan.close();
        plan.reset();

        hdm = new HardcodedDataManager();
        hdm.addData("SELECT pm1.g1.e1, pm1.g1.e2 FROM pm1.g1", Arrays.asList("a", 1), Arrays.asList("a", 3));
        helpProcess(plan, hdm, expected);
    }

    @Test public void testRollupHaving() throws Exception {
        String sql = "select e1, sum(e2) from pm1.g1 group by rollup(e1) having e1 is not null"; //$NON-NLS-1$

        List[] expected = new List[] {
                Arrays.asList("a", Long.valueOf(3)),
                Arrays.asList("b", Long.valueOf(1))
                };

        ProcessorPlan plan = helpGetPlan(sql, RealMetadataFactory.example1Cached());

        HardcodedDataManager hdm = new HardcodedDataManager();
        hdm.addData("SELECT pm1.g1.e1, pm1.g1.e2 FROM pm1.g1", Arrays.asList("a", 1), Arrays.asList("a", 2), Arrays.asList("b", 1));
        helpProcess(plan, hdm, expected);
    }

    @Test public void testRollup2() throws Exception {
        String sql = "select e1, e2, sum(e4) from pm1.g1 group by rollup(e1, e2)"; //$NON-NLS-1$

        List[] expected = new List[] {
                Arrays.asList("a", 1, 1.0),
                Arrays.asList("a", 3, 2.0),
                Arrays.asList("a", null, 3.0),
                Arrays.asList("b", 2, 3.0),
                Arrays.asList("b", 4, 4.0),
                Arrays.asList("b", null, 7.0),
                Arrays.asList(null, null, 10.0),
                };

        ProcessorPlan plan = helpGetPlan(sql, RealMetadataFactory.example1Cached());

        HardcodedDataManager hdm = new HardcodedDataManager();
        hdm.addData("SELECT pm1.g1.e1, pm1.g1.e2, pm1.g1.e4 FROM pm1.g1", Arrays.asList("a", 1, 1.0), Arrays.asList("a", 3, 2.0), Arrays.asList("b", 2, 3.0), Arrays.asList("b", 4, 4.0));
        helpProcess(plan, hdm, expected);

        plan.close();
        plan.reset();

        //an empty rollup should produce no rows
        hdm = new HardcodedDataManager();
        hdm.addData("SELECT pm1.g1.e1, pm1.g1.e2, pm1.g1.e4 FROM pm1.g1");
        helpProcess(plan, hdm, new List<?>[0]);
    }

    @Test public void testRollup3() throws Exception {
        String sql = "select e1, e2, e3, sum(e4) from pm1.g1 group by rollup(e1, e2, e3)"; //$NON-NLS-1$

        List[] expected = new List[] {
                Arrays.asList("a", 1, true, 3.0),
                Arrays.asList("a", 1, null, 3.0),
                Arrays.asList("a", null, null, 3.0),
                Arrays.asList("b", 2, false, 7.0),
                Arrays.asList("b", 2, null, 7.0),
                Arrays.asList("b", null, null, 7.0),
                Arrays.asList(null, null, null, 10.0),
                };

        ProcessorPlan plan = helpGetPlan(sql, RealMetadataFactory.example1Cached());

        HardcodedDataManager hdm = new HardcodedDataManager();
        hdm.addData("SELECT pm1.g1.e1, pm1.g1.e2, pm1.g1.e3, pm1.g1.e4 FROM pm1.g1", Arrays.asList("a", 1, Boolean.TRUE, 1.0), Arrays.asList("a", 1, Boolean.TRUE, 2.0), Arrays.asList("b", 2, Boolean.FALSE, 3.0), Arrays.asList("b", 2, Boolean.FALSE, 4.0));
        helpProcess(plan, hdm, expected);
    }

    @Test public void testAggregateGroupByFunctionDependent() throws Exception {
        String sql = "select count(x.e2), nvl(x.e1, '') from pm1.g1 x makedep, pm2.g2 where x.e3 = pm2.g2.e3 group by nvl(x.e1, '')"; //$NON-NLS-1$

        List[] expected = new List[] {
                Arrays.asList(1, "a"),
        };

        HardcodedDataManager dataManager = new HardcodedDataManager(RealMetadataFactory.example1Cached());
        dataManager.addData("SELECT g_0.e3 AS c_0 FROM g2 AS g_0 ORDER BY c_0", new List[] {
                Arrays.asList(Boolean.FALSE),
        });
        dataManager.addData("SELECT v_0.c_0, v_0.c_1, COUNT(v_0.c_2) FROM (SELECT g_0.e3 AS c_0, ifnull(g_0.e1, '') AS c_1, g_0.e2 AS c_2 FROM g1 AS g_0) AS v_0 WHERE v_0.c_0 = FALSE GROUP BY v_0.c_0, v_0.c_1", new List[] {
                Arrays.asList(Boolean.FALSE, "a", 1)
        });
        BasicSourceCapabilities capabilities = TestAggregatePushdown.getAggregateCapabilities();
        capabilities.setFunctionSupport("ifnull", true);
        CommandContext cc = createCommandContext();
        ProcessorPlan plan = helpGetPlan(helpParse(sql), RealMetadataFactory.example1Cached(), new DefaultCapabilitiesFinder(capabilities), cc);
        helpProcess(plan, cc, dataManager, expected);
    }

    @Test public void testSelectAllWithGrouping() {
        Command command = helpParse("select * from (select pm1.g1.e1 x, pm2.g2.e1 y from pm1.g1, pm2.g2) z group by x, y"); //$NON-NLS-1$

        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        capFinder.addCapabilities("pm1", TestAggregatePushdown.getAggregateCapabilities()); //$NON-NLS-1$
        capFinder.addCapabilities("pm2", TestOptimizer.getTypicalCapabilities()); //$NON-NLS-1$
        HardcodedDataManager dataManager = new HardcodedDataManager();

        dataManager.addData("SELECT g_0.e1 FROM pm1.g1 AS g_0 GROUP BY g_0.e1", //$NON-NLS-1$
                new List[] {
                    Arrays.asList("a"), //$NON-NLS-1$
                });
        dataManager.addData("SELECT g_0.e1 FROM pm2.g2 AS g_0", //$NON-NLS-1$
                new List[] {
                    Arrays.asList("b"), //$NON-NLS-1$
                });

        ProcessorPlan plan = helpGetPlan(command, RealMetadataFactory.example1Cached(), capFinder);

        List[] expected = new List[] {
                Arrays.asList("a", "b"),
            };

        helpProcess(plan, dataManager, expected);
    }

    //TODO: the rewriter may need to correct this case, but at least the grouping node can
    //now handle it
    @Test public void testDuplicateGroupBy() {
        Command command = helpParse("select e2 from pm1.g1 group by e2, e2"); //$NON-NLS-1$

        CapabilitiesFinder capFinder = TestOptimizer.getGenericFinder();
        HardcodedDataManager dataManager = new HardcodedDataManager();

        dataManager.addData("SELECT g_0.e2 FROM pm1.g1 AS g_0", //$NON-NLS-1$
                new List[] {
                    Arrays.asList(1), //$NON-NLS-1$
                    Arrays.asList(2), //$NON-NLS-1$
                    Arrays.asList(2), //$NON-NLS-1$
                });

        ProcessorPlan plan = helpGetPlan(command, RealMetadataFactory.example1Cached(), capFinder);

        List[] expected = new List[] {
                Arrays.asList(1),
                Arrays.asList(2),
            };

        helpProcess(plan, dataManager, expected);
    }

    @Test public void testSidewaysCorrelationBelowAggregation() throws Exception {
        String sql = "select e1 from (SELECT sc.e1 FROM pm1.g1 sc, table(exec pm1.vsp21(sc.e2+1) ) as f ) as x group by e1";

        Command command = helpParse(sql); //$NON-NLS-1$

        CapabilitiesFinder capFinder = TestOptimizer.getGenericFinder();
        HardcodedDataManager dataManager = new HardcodedDataManager();

        dataManager.addData("SELECT g_0.e2, g_0.e1 FROM pm1.g1 AS g_0", //$NON-NLS-1$
                new List[] {
                    Arrays.asList(1, "1"), //$NON-NLS-1$
                    Arrays.asList(2, "2"), //$NON-NLS-1$
                });

        dataManager.addData("SELECT g_0.e1, g_0.e2 FROM pm1.g1 AS g_0", //$NON-NLS-1$
                new List[] {
                    Arrays.asList("2", 2), //$NON-NLS-1$
                });

        ProcessorPlan plan = helpGetPlan(command, RealMetadataFactory.example1Cached(), capFinder);

        List[] expected = new List[] {
                Arrays.asList("1"),
                Arrays.asList("2"),
            };

        helpProcess(plan, dataManager, expected);
    }

    @Test public void testBigIntegerSum() throws Exception {
        String sql = "SELECT sum(x) FROM agg x"; //$NON-NLS-1$

        TransformationMetadata metadata = RealMetadataFactory.fromDDL("create foreign table agg (x biginteger)", "x", "y");
        HardcodedDataManager hdm = new HardcodedDataManager();
        hdm.addData("SELECT y.agg.x FROM y.agg", Arrays.asList(BigInteger.valueOf(1)));
        ProcessorPlan plan = TestProcessor.helpGetPlan(sql, metadata);
        TestProcessor.helpProcess(plan, hdm, new List[] {Arrays.asList(BigInteger.valueOf(1))});
    }

    @Test public void testCorrelatedGroupingColumnExpression() throws Exception {
        // Create query
        String sql = "SELECT A.e2/2, A.e1 FROM pm1.g1 AS A GROUP BY A.e2/2, A.e1 HAVING A.e1 = (SELECT MAX(B.e1) FROM pm1.g1 AS B WHERE A.e2/2 = B.e2)"; //$NON-NLS-1$

        // Create expected results
        List[] expected = new List[] {
                Arrays.asList(new Object[] { 0, "a" })};

        // Construct data manager with data
        FakeDataManager dataManager = new FakeDataManager();
        FakeDataStore.sampleData1(dataManager, RealMetadataFactory.example1Cached());

        // Plan query
        ProcessorPlan plan = helpGetPlan(sql, RealMetadataFactory.example1Cached());

        // Run query
        helpProcess(plan, dataManager, expected);
    }

    @Test public void testCorrelatedGroupingColumnExpressionPushdown() throws Exception {
        // Create query
        String sql = "SELECT A.e2/2, A.e1 FROM pm1.g1 AS A GROUP BY A.e2/2, A.e1 HAVING A.e1 = (SELECT MAX(B.e1) FROM pm1.g1 AS B WHERE A.e2/2 = B.e2)"; //$NON-NLS-1$

        // Plan query
        BasicSourceCapabilities bsc = TestAggregatePushdown.getAggregateCapabilities();
        bsc.setFunctionSupport(SourceSystemFunctions.DIVIDE_OP, true);
        bsc.setCapabilitySupport(Capability.QUERY_SUBQUERIES_CORRELATED, true);
        bsc.setCapabilitySupport(Capability.QUERY_SUBQUERIES_SCALAR, true);
        ProcessorPlan plan = TestOptimizer.helpPlan(sql, RealMetadataFactory.example1Cached(),
                new String[] {"SELECT v_0.c_0, v_0.c_1 FROM (SELECT (g_0.e2 / 2) AS c_0, g_0.e1 AS c_1 FROM pm1.g1 AS g_0 WHERE g_0.e1 = (SELECT MAX(g_1.e1) FROM pm1.g1 AS g_1 WHERE g_1.e2 = (g_0.e2 / 2))) AS v_0 GROUP BY v_0.c_0, v_0.c_1"}, new DefaultCapabilitiesFinder(bsc), ComparisonMode.EXACT_COMMAND_STRING);
        TestOptimizer.checkNodeTypes(plan, TestOptimizer.FULL_PUSHDOWN);

        bsc.setCapabilitySupport(Capability.QUERY_FUNCTIONS_IN_GROUP_BY, true);
        plan = TestOptimizer.helpPlan(sql, RealMetadataFactory.example1Cached(),
                new String[] {"SELECT (g_0.e2 / 2), g_0.e1 FROM pm1.g1 AS g_0 WHERE g_0.e1 = (SELECT MAX(g_1.e1) FROM pm1.g1 AS g_1 WHERE g_1.e2 = (g_0.e2 / 2)) GROUP BY (g_0.e2 / 2), g_0.e1"}, new DefaultCapabilitiesFinder(bsc), ComparisonMode.EXACT_COMMAND_STRING);
        TestOptimizer.checkNodeTypes(plan, TestOptimizer.FULL_PUSHDOWN);
    }

    @Test(expected=FunctionExecutionException.class) public void testSumOverflow() throws Exception {
        String sql = "SELECT sum(x) FROM (select cast(9223372036854775807 as long) as x union all select 1) as x"; //$NON-NLS-1$

        TransformationMetadata metadata = RealMetadataFactory.example1Cached();
        HardcodedDataManager hdm = new HardcodedDataManager();
        ProcessorPlan plan = TestProcessor.helpGetPlan(sql, metadata);
        TestProcessor.helpProcess(plan, TestProcessor.createCommandContext(), hdm, null);
    }

    @Test() public void testAggregateOrderByPushdown() throws Exception {
        String sql = "SELECT string_agg(e1, ' ' order by e1) FROM pm1.g1"; //$NON-NLS-1$

        TransformationMetadata metadata = RealMetadataFactory.example1Cached();
        HardcodedDataManager hdm = new HardcodedDataManager(metadata);

        hdm.addData("SELECT STRING_AGG(g_0.e1, ' ' ORDER BY g_0.e1) FROM g1 AS g_0", Arrays.asList('a'));

        BasicSourceCapabilities bsc = TestAggregatePushdown.getAggregateCapabilities();
        bsc.setCapabilitySupport(Capability.QUERY_AGGREGATES_STRING, true);

        ProcessorPlan plan = TestProcessor.helpGetPlan(sql, metadata, new DefaultCapabilitiesFinder(bsc));
        TestProcessor.helpProcess(plan, TestProcessor.createCommandContext(), hdm, new List[] {Arrays.asList('a')});

        bsc.setSourceProperty(Capability.COLLATION_LOCALE, "foo");

        CommandContext cc = new CommandContext();
        cc.setOptions(new Options().requireTeiidCollation(true));
        CommandContext.pushThreadLocalContext(cc);
        try {
            plan = TestProcessor.helpGetPlan(helpParse(sql), metadata, new DefaultCapabilitiesFinder(bsc), cc);
            TestOptimizer.checkAtomicQueries(new String[] {"SELECT g_0.e1 FROM pm1.g1 AS g_0"}, plan);
        } finally {
            CommandContext.popThreadLocalContext();
        }
    }

    @Test() public void testDistinctOrdering() throws Exception {
        String sql = "select string_agg(DISTINCT col1, ',' ORDER BY col2 DESC) as distinctOrderByDesc from (select 'a' as col1, 1 as col2 union all select 'b', 2) as x";

        TestValidator.helpValidate(sql, new String[] {"string_agg(DISTINCT col1, ',' ORDER BY col2 DESC)"}, RealMetadataFactory.example1Cached());
    }

    @Test() public void testStringAggOrdering() throws Exception {
        String sql = "select string_agg(col1, ',' ORDER BY col1 DESC) as orderByDesc,"
                + " string_agg(col1, ',' ORDER BY col1 ASC) as orderByAsc, "
                + " string_agg(DISTINCT col1, ',' ORDER BY col1 DESC) as distinctOrderByDesc, "
                + "	string_agg(DISTINCT col1, ',' ORDER BY col1 ASC) as distinctOrderByAsc from (select 'a' as col1 union all select 'b' union all select 'b' union all select 'c') as x";

        TransformationMetadata metadata = RealMetadataFactory.example1Cached();
        HardcodedDataManager hdm = new HardcodedDataManager();
        ProcessorPlan plan = TestProcessor.helpGetPlan(sql, metadata);

        TestProcessor.helpProcess(plan, TestProcessor.createCommandContext(), hdm, new List<?>[] { Arrays.asList(new ClobType(new ClobImpl("c,b,b,a")), new ClobType(new ClobImpl("a,b,b,c")), new ClobType(new ClobImpl("c,b,a")), new ClobType(new ClobImpl("a,b,c")))});
    }

    @Test public void testStringAggOverJoin() throws Exception {
        String sql = "select string_agg(pm1.g1.e1, ',') from pm1.g1, pm2.g1 where pm1.g1.e2 = pm2.g1.e2 group by pm2.g1.e3, pm1.g1.e3";

        TransformationMetadata metadata = RealMetadataFactory.example1Cached();
        HardcodedDataManager hdm = new HardcodedDataManager();

        hdm.addData("SELECT g_0.e2 AS c_0, g_0.e3 AS c_1, g_0.e1 AS c_2 FROM pm1.g1 AS g_0 ORDER BY c_0", Arrays.asList(1, true, "a"), Arrays.asList(1, false, "b"));
        hdm.addData("SELECT g_0.e2 AS c_0, g_0.e3 AS c_1 FROM pm2.g1 AS g_0 ORDER BY c_0", Arrays.asList(1, true), Arrays.asList(1, true));

        BasicSourceCapabilities bsc = TestAggregatePushdown.getAggregateCapabilities();
        bsc.setCapabilitySupport(Capability.QUERY_AGGREGATES_STRING, true);

        ProcessorPlan plan = TestProcessor.helpGetPlan(sql, metadata, new DefaultCapabilitiesFinder(bsc));

        TestProcessor.helpProcess(plan, TestProcessor.createCommandContext(),
                hdm, new List<?>[] {Arrays.asList("b,b"), Arrays.asList("a,a")});
    }

    @Test public void testCountConstantWithoutStats() throws Exception {
        String sql = "select count(1) from test_count_1 t1 join test_count_2 t2 on t1.a=t2.a group by t1.a";

        TransformationMetadata metadata = RealMetadataFactory.fromDDL("create foreign table test_count_1 (a string); create foreign table test_count_2 (a string)", "x", "y");
        HardcodedDataManager hdm = new HardcodedDataManager();

        hdm.addData("SELECT g_0.a FROM y.test_count_1 AS g_0", Arrays.asList("a"), Arrays.asList("a"));
        hdm.addData("SELECT g_0.a FROM y.test_count_2 AS g_0", Arrays.asList("a"), Arrays.asList("a"));

        BasicSourceCapabilities bsc = TestAggregatePushdown.getAggregateCapabilities();
        bsc.setCapabilitySupport(Capability.QUERY_FROM_JOIN_INNER, false);
        bsc.setCapabilitySupport(Capability.QUERY_FROM_JOIN_OUTER, false);
        bsc.setCapabilitySupport(Capability.QUERY_ORDERBY, false);
        bsc.setCapabilitySupport(Capability.QUERY_GROUP_BY, false);

        ProcessorPlan plan = TestProcessor.helpGetPlan(sql, metadata, new DefaultCapabilitiesFinder(bsc));
        TestProcessor.helpProcess(plan, TestProcessor.createCommandContext(),
                hdm, new List<?>[] {Arrays.asList(4)});
    }

    @Test public void testCardinalityDependentNotPushed() throws Exception {
        TransformationMetadata metadata = RealMetadataFactory.fromDDL(
                " CREATE FOREIGN TABLE tbl_1 (a integer, b bigdecimal);"
                + " CREATE virtual view v1 as select * from tbl_1; "
                + " CREATE virtual view v2 as select 1 as a, 1 as b;"
                + " create procedure pr() returns (a integer, b integer) as select 1, 1;", "x", "y");

        String sql = "select count(v2.b) from v1 right join v2 on true group by v1.b;";

        HardcodedDataManager hdm = new HardcodedDataManager();
        hdm.addData("SELECT g_0.b FROM y.tbl_1 AS g_0", Arrays.asList(1), Arrays.asList(2), Arrays.asList(2), Arrays.asList(3));

        BasicSourceCapabilities bsc = TestAggregatePushdown.getAggregateCapabilities();
        bsc.setCapabilitySupport(Capability.QUERY_FROM_JOIN_INNER, false);
        bsc.setCapabilitySupport(Capability.QUERY_FROM_JOIN_OUTER, false);
        bsc.setCapabilitySupport(Capability.QUERY_ORDERBY, false);
        bsc.setCapabilitySupport(Capability.QUERY_GROUP_BY, true);

        ProcessorPlan plan = TestProcessor.helpGetPlan(sql, metadata, new DefaultCapabilitiesFinder(bsc));

        TestProcessor.helpProcess(plan, TestProcessor.createCommandContext(),
                hdm, new List<?>[] {Arrays.asList(1), Arrays.asList(2), Arrays.asList(1)});

        sql = "select count(v2.b) from v1 join (call pr()) v2 on true group by v1.b";

        plan = TestProcessor.helpGetPlan(sql, metadata, new DefaultCapabilitiesFinder(bsc));

        TestProcessor.helpProcess(plan, TestProcessor.createCommandContext(),
                hdm, new List<?>[] {Arrays.asList(1), Arrays.asList(2), Arrays.asList(1)});
    }

    @Test public void testGroupByPredicateInCase() throws Exception {
        TransformationMetadata metadata = RealMetadataFactory.fromDDL(
                " CREATE FOREIGN TABLE wdv (id integer, numericvalue bigdecimal);" +
                " CREATE FOREIGN TABLE tv (id integer, varvalue bigdecimal);", "x", "y");

        String sql = "select case when dv.varvalue is null then 'missing' else 'wrong' end as t"
                + " from wdv nv left join tv dv on dv.id = nv.id where (dv.varvalue is null or round(nv.numericvalue,0) <> round(dv.varvalue,0))"
                + " group by dv.varvalue is null";

        HardcodedDataManager hdm = new HardcodedDataManager();
        hdm.addData("SELECT g_1.varvalue, g_0.numericvalue, g_1.varvalue IS NULL FROM y.wdv AS g_0 LEFT OUTER JOIN y.tv AS g_1 ON g_1.id = g_0.id", Arrays.asList(BigDecimal.valueOf(1.0), BigDecimal.valueOf(2.0), false));

        BasicSourceCapabilities bsc = TestAggregatePushdown.getAggregateCapabilities();

        ProcessorPlan plan = TestProcessor.helpGetPlan(sql, metadata, new DefaultCapabilitiesFinder(bsc));

        TestProcessor.helpProcess(plan, TestProcessor.createCommandContext(),
                hdm, new List<?>[] {Arrays.asList("wrong")});
    }

    @Test public void testSumLiteralOverJoin() {
        String sql = "select sum(2) from pm1.g1 a full outer join pm1.g2 b on a.e1 = b.e1"; //$NON-NLS-1$

        ProcessorPlan plan = helpGetPlan(sql, RealMetadataFactory.example1Cached());
        HardcodedDataManager hdm = new HardcodedDataManager();
        hdm.addData("SELECT pm1.g1.e1 FROM pm1.g1", Arrays.asList("a"));
        hdm.addData("SELECT pm1.g2.e1 FROM pm1.g2", Arrays.asList("b"));

        helpProcess(plan, hdm, new List[] {Arrays.asList(4L)});
    }

    @Test public void testAvgLiteralOverJoin() throws QueryMetadataException, TeiidComponentException {
        String sql = "select avg(2) from pm1.g1 a left outer join pm1.g2 b on a.e1 = b.e1"; //$NON-NLS-1$

        TransformationMetadata metadata = RealMetadataFactory.example1();
        RealMetadataFactory.setCardinality("pm1.g1", 500, metadata);
        RealMetadataFactory.setCardinality("pm1.g2", 1000, metadata);
        ProcessorPlan plan = helpGetPlan(sql, metadata);
        HardcodedDataManager hdm = new HardcodedDataManager();
        hdm.addData("SELECT pm1.g1.e1 FROM pm1.g1", Arrays.asList("a"));
        hdm.addData("SELECT pm1.g2.e1 FROM pm1.g2", Arrays.asList("b"));
        helpProcess(plan, hdm, new List[] {Arrays.asList(BigDecimal.valueOf(2))});
    }

    @Test public void testCountBig() throws Exception {
        String sql = "select count_big(e1) from pm1.g1"; //$NON-NLS-1$

        TransformationMetadata metadata = RealMetadataFactory.example1Cached();
        ProcessorPlan plan = helpGetPlan(sql, metadata);
        HardcodedDataManager hdm = new HardcodedDataManager();
        hdm.addData("SELECT pm1.g1.e1 FROM pm1.g1", Arrays.asList("a"));
        helpProcess(plan, hdm, new List[] {Arrays.asList(1L)});
    }

    @Test public void testMultipleCountWithLeftOuter() throws Exception {
        String sql = "select\n" +
                "    count(a.e2)\n" +
                "   , count(b.e1)\n" +
                "from pm1.g1 a\n" +
                "left join pm2.g1 b on a.e1 = b.e1"; //$NON-NLS-1$

        CommandContext cc = TestProcessor.createCommandContext();
        TransformationMetadata metadata = RealMetadataFactory.example1Cached();
        BasicSourceCapabilities aggregateCapabilities = TestAggregatePushdown.getAggregateCapabilities();

        ProcessorPlan plan = helpGetPlan(sql, metadata, new DefaultCapabilitiesFinder(aggregateCapabilities));
        HardcodedDataManager hdm = new HardcodedDataManager(metadata, cc, aggregateCapabilities);

        hdm.addData("SELECT g_0.e1 AS c_0, g_0.e2 AS c_1 FROM g1 AS g_0 ORDER BY c_0", Arrays.asList("a", 1), Arrays.asList("a", 2), Arrays.asList("b", 1));
        hdm.addData("SELECT g_0.e1 AS c_0 FROM g1 AS g_0 ORDER BY c_0", Arrays.asList("a"), Arrays.asList("a"), Arrays.asList("c"));

        TestProcessor.helpProcess(plan, hdm, new List[] {Arrays.asList(5, 4)});

        sql = "select\n" +
                "    count(b.e1)\n" +
                "   , count(a.e2)\n" +
                "from pm1.g1 a\n" +
                "left outer join pm2.g1 b on a.e1 = b.e1"; //$NON-NLS-1$

        plan = helpGetPlan(sql, metadata, new DefaultCapabilitiesFinder(aggregateCapabilities));

        TestProcessor.helpProcess(plan, hdm, new List[] {Arrays.asList(4, 5)});
    }

    @Test public void testMultipleCountWithInner() throws Exception {
        String sql = "select\n" +
                "    count(a.e2)\n" +
                "   , count(b.e1)\n" +
                "from pm1.g1 a\n" +
                "inner join pm2.g1 b on a.e1 = b.e1"; //$NON-NLS-1$

        CommandContext cc = TestProcessor.createCommandContext();
        TransformationMetadata metadata = RealMetadataFactory.example1Cached();
        BasicSourceCapabilities aggregateCapabilities = TestAggregatePushdown.getAggregateCapabilities();

        ProcessorPlan plan = TestOptimizer.helpPlan(sql, metadata, null, new DefaultCapabilitiesFinder(aggregateCapabilities),
                new String[] {"SELECT g_0.e1 AS c_0 FROM pm2.g1 AS g_0 ORDER BY c_0",
                        "SELECT g_0.e1 AS c_0, g_0.e2 AS c_1 FROM pm1.g1 AS g_0 ORDER BY c_0"}, ComparisonMode.EXACT_COMMAND_STRING);
        HardcodedDataManager hdm = new HardcodedDataManager(metadata, cc, aggregateCapabilities);

        hdm.addData("SELECT g_0.e1 AS c_0, g_0.e2 AS c_1 FROM g1 AS g_0 ORDER BY c_0", Arrays.asList("a", 1), Arrays.asList("a", 2), Arrays.asList("b", 1));
        hdm.addData("SELECT g_0.e1 AS c_0 FROM g1 AS g_0 ORDER BY c_0", Arrays.asList("a"), Arrays.asList("a"), Arrays.asList("c"));

        TestProcessor.helpProcess(plan, hdm, new List[] {Arrays.asList(4, 4)});

        sql = "select\n" +
                "    count(b.e1)\n" +
                "   , count(a.e2)\n" +
                "from pm1.g1 a\n" +
                "inner join pm2.g1 b on a.e1 = b.e1"; //$NON-NLS-1$

        plan = helpGetPlan(sql, metadata, new DefaultCapabilitiesFinder(aggregateCapabilities));

        TestProcessor.helpProcess(plan, hdm, new List[] {Arrays.asList(4, 4)});
    }

    @Test public void testSingleCountWithLeftOuter() throws Exception {
        String sql = "select\n" +
                "    count(a.e1)\n" +
                "from pm1.g1 a\n" +
                "left outer join pm2.g1 b on a.e1 = b.e1"; //$NON-NLS-1$

        CommandContext cc = TestProcessor.createCommandContext();
        TransformationMetadata metadata = RealMetadataFactory.example1Cached();
        BasicSourceCapabilities aggregateCapabilities = TestAggregatePushdown.getAggregateCapabilities();

        ProcessorPlan plan = TestOptimizer.helpPlan(sql, metadata, null, new DefaultCapabilitiesFinder(aggregateCapabilities),
                new String[] {"SELECT g_0.e1 AS c_0 FROM pm2.g1 AS g_0 ORDER BY c_0",
                        "SELECT g_0.e1 AS c_0, COUNT(g_0.e1) AS c_1 FROM pm1.g1 AS g_0 GROUP BY g_0.e1 ORDER BY c_0"}, ComparisonMode.EXACT_COMMAND_STRING);
        HardcodedDataManager hdm = new HardcodedDataManager(metadata, cc, aggregateCapabilities);

        hdm.addData("SELECT g_0.e1 AS c_0, COUNT(g_0.e1) AS c_1 FROM g1 AS g_0 GROUP BY g_0.e1 ORDER BY c_0", Arrays.asList("a", 2), Arrays.asList("b", 1));
        hdm.addData("SELECT g_0.e1 AS c_0 FROM g1 AS g_0 ORDER BY c_0", Arrays.asList("a"), Arrays.asList("a"), Arrays.asList("c"));

        TestProcessor.helpProcess(plan, hdm, new List[] {Arrays.asList(5)});

        sql = "select\n" +
                "    count(b.e1)\n" +
                "from pm1.g1 a\n" +
                "left outer join pm2.g1 b on a.e1 = b.e1"; //$NON-NLS-1$

        plan = helpGetPlan(sql, metadata, new DefaultCapabilitiesFinder(aggregateCapabilities));
        hdm = new HardcodedDataManager(metadata, cc, aggregateCapabilities);

        hdm.addData("SELECT g_0.e1 AS c_0, COUNT(g_0.e1) AS c_1 FROM g1 AS g_0 GROUP BY g_0.e1 ORDER BY c_0", Arrays.asList("a", 2), Arrays.asList("c", 1));
        hdm.addData("SELECT g_0.e1 AS c_0 FROM g1 AS g_0 ORDER BY c_0", Arrays.asList("a"), Arrays.asList("a"), Arrays.asList("b"));

        TestProcessor.helpProcess(plan, hdm, new List[] {Arrays.asList(4)});
    }

    @Test public void testNullDependentGroupingExpression() throws Exception {
        String ddl = "create foreign procedure getFiles (name string) returns table (file blob);"
                + " create foreign table vRight (schedule_id string, order_id string, weeks string); "
                + " create view vLeft as\n" +
                "SELECT\n" +
                "\"csv_table\".\"created_at\",\n" +
                "\"csv_table\".\"order_id\",\n" +
                "\"csv_table\".\"store_id\" \n" +
                "FROM\n" +
                "(call getFiles('test1.csv')) f,\n" +
                "    TEXTTABLE(to_chars(f.file,'UTF-8') \n" +
                "        COLUMNS \n" +
                "        \"created_at\" STRING ,\n" +
                "        \"order_id\" STRING ,\n" +
                "        \"store_id\" STRING \n" +
                "        DELIMITER ';' \n" +
                "        QUOTE '\"' \n" +
                "        HEADER 1 \n" +
                "    )\n" +
                "\"csv_table\"";

        QueryMetadataInterface metadata = RealMetadataFactory.fromDDL(ddl, "x", "phy");

        String sql = "select "+
                "count(distinct a.\"order_id\") anzahl_orders," +
                "(case when b.\"order_id\" is not null then 'Direct Rebuy' else 'Standard' end) rebuy_check\n" +
                "FROM vLeft a\n" +
                "left JOIN vRight b on b.order_id=a.order_id\n" +
                "group by\n" +
                "(case when b.\"order_id\" is not null then 'Direct Rebuy' else 'Standard' end) ;"; //$NON-NLS-1$

        DefaultCapabilitiesFinder capFinder = new DefaultCapabilitiesFinder(TestAggregatePushdown.getAggregateCapabilities());

        ProcessorPlan plan = helpGetPlan(sql, metadata, capFinder);

        List<?>[] expected = new List[] {
                Arrays.asList(2, "Direct Rebuy"),
                Arrays.asList(498, "Standard"),
        };

        HardcodedDataManager dataManager = new HardcodedDataManager();
        dataManager.addData("EXEC getFiles('test1.csv')", Arrays.asList(new BlobType(ObjectConverterUtil.convertFileToByteArray(UnitTestUtil.getTestDataFile("5783/test1.csv")))));
        String[] vals = {"1574053", "1574054","1574054","1574055a","1574056b"};

        List<List<?>> rows = Arrays.asList(vals).stream().map(s->Arrays.asList(s)).collect(Collectors.toList());

        dataManager.addData("SELECT DISTINCT g_0.order_id AS c_0 FROM phy.vRight AS g_0 ORDER BY c_0", rows.toArray(new List<?>[0]));
        dataManager.addData("SELECT g_0.order_id AS c_0 FROM phy.vRight AS g_0 ORDER BY c_0", rows.toArray(new List<?>[0]));
        dataManager.addData("SELECT g_0.order_id FROM phy.vRight AS g_0", rows.toArray(new List<?>[0]));
        helpProcess(plan, TestProcessor.createCommandContext(), dataManager, expected);
    }

}

