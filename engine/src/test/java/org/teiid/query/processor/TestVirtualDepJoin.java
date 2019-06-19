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

import java.math.BigDecimal;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

import org.junit.Test;
import org.teiid.common.buffer.BufferManager;
import org.teiid.common.buffer.BufferManagerFactory;
import org.teiid.common.buffer.TupleBuffer;
import org.teiid.core.TeiidComponentException;
import org.teiid.core.TeiidException;
import org.teiid.core.types.DataTypeManager;
import org.teiid.metadata.Column;
import org.teiid.metadata.MetadataStore;
import org.teiid.metadata.Schema;
import org.teiid.metadata.Table;
import org.teiid.query.mapping.relational.QueryNode;
import org.teiid.query.metadata.QueryMetadataInterface;
import org.teiid.query.metadata.TransformationMetadata;
import org.teiid.query.optimizer.TestOptimizer;
import org.teiid.query.optimizer.capabilities.BasicSourceCapabilities;
import org.teiid.query.optimizer.capabilities.FakeCapabilitiesFinder;
import org.teiid.query.optimizer.capabilities.SourceCapabilities.Capability;
import org.teiid.query.sql.lang.Command;
import org.teiid.query.unittest.RealMetadataFactory;
import org.teiid.query.unittest.TimestampUtil;
import org.teiid.query.util.CommandContext;
import org.teiid.query.validator.TestValidator;

@SuppressWarnings("nls")
public class TestVirtualDepJoin {

    private static void setStats(List<Column> elementObjects,
                                 int[] ndvs,
                                 int[] nnvs,
                                 String[] mins,
                                 String[] maxs) {
        for (int i = 0; i < elementObjects.size(); i++) {
            Column obj = elementObjects.get(i);
            if(ndvs != null) {
                obj.setDistinctValues(ndvs[i]);
            }
            if(nnvs != null) {
                obj.setNullValues(nnvs[i]);
            }
            if(mins != null) {
                obj.setMinimumValue(mins[i]);
            }
            if(maxs != null) {
                obj.setMaximumValue(maxs[i]);
            }
        }
    }

    public static TransformationMetadata exampleVirtualDepJoin() {
        MetadataStore metadataStore = new MetadataStore();
        Schema us = RealMetadataFactory.createPhysicalModel("US", metadataStore); //$NON-NLS-1$
        Table usAccts = RealMetadataFactory.createPhysicalGroup("Accounts", us); //$NON-NLS-1$
        usAccts.setCardinality(1000000);
        List<Column> usAcctsElem = RealMetadataFactory.createElements(usAccts,
                                    new String[] { "customer", "account", "txn", "txnid", "pennies" }, //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$ //$NON-NLS-5$
                                    new String[] { DataTypeManager.DefaultDataTypes.LONG, DataTypeManager.DefaultDataTypes.INTEGER, DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.INTEGER, DataTypeManager.DefaultDataTypes.INTEGER });
        setStats(usAcctsElem,
                 new int[] { 1000, 1250, 4, 1000000, 800000}, // NDV per column
                 new int[] { 0, 0, 0, 0, 0}, // NNV per column
                 new String[] {"0", null, null, null, "-10"}, // min per column - use defaults //$NON-NLS-1$ //$NON-NLS-2$
                 new String[] {"1000", null, null, null, "-5"}  // max per column - use defaults //$NON-NLS-1$ //$NON-NLS-2$
        );

        Schema europe = RealMetadataFactory.createPhysicalModel("Europe", metadataStore); //$NON-NLS-1$
        Table euAccts = RealMetadataFactory.createPhysicalGroup("CustAccts", europe); //$NON-NLS-1$
        euAccts.setCardinality(1000000);
        List<Column> euAcctsElem = RealMetadataFactory.createElements(euAccts,
                                    new String[] { "id", "accid", "type", "amount" }, //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
                                    new String[] { DataTypeManager.DefaultDataTypes.LONG, DataTypeManager.DefaultDataTypes.LONG, DataTypeManager.DefaultDataTypes.SHORT, DataTypeManager.DefaultDataTypes.BIG_DECIMAL });
        setStats(euAcctsElem,
                 new int[] { 10000, 1000000, 4, 1000000, 750000}, // NDV per column
                 new int[] { 0, 0, 0, 0, 0}, // NNV per column
                 null, // min per column - use defaults
                 null  // max per column - use defaults
        );

        Schema cust = RealMetadataFactory.createPhysicalModel("CustomerMaster", metadataStore); //$NON-NLS-1$
        Table customers = RealMetadataFactory.createPhysicalGroup("Customers", cust); //$NON-NLS-1$
        customers.setCardinality(1000);
        List<Column> customersElem = RealMetadataFactory.createElements(customers,
                                    new String[] { "id", "first", "last", "birthday" }, //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
                                    new String[] { DataTypeManager.DefaultDataTypes.LONG, DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.DATE });
        setStats(customersElem,
                 new int[] { 1000, 800, 800, 365}, // NDV per column
                 new int[] { 0, 0, 0, 0}, // NNV per column
                 null, // min per column - use defaults
                 null  // max per column - use defaults
        );
        Table locations = RealMetadataFactory.createPhysicalGroup("Locations", cust); //$NON-NLS-1$
        locations.setCardinality(1200);
        List<Column> locationsElem = RealMetadataFactory.createElements(locations,
                                    new String[] { "id", "location" }, //$NON-NLS-1$ //$NON-NLS-2$
                                    new String[] { DataTypeManager.DefaultDataTypes.LONG, DataTypeManager.DefaultDataTypes.STRING });
        setStats(locationsElem,
                 new int[] { 1000, 2}, // NDV per column
                 new int[] { 0, 0, 0, 0}, // NNV per column
                 null, // min per column - use defaults
                 null  // max per column - use defaults
        );

        Schema vAccts = RealMetadataFactory.createVirtualModel("Accounts", metadataStore); //$NON-NLS-1$
        QueryNode accountsPlan = new QueryNode("SELECT customer as customer_id, convert(account, long) as account_id, convert(txnid, long) as transaction_id, case txn when 'DEP' then 1 when 'TFR' then 2 when 'WD' then 3 else -1 end as txn_type, (pennies + convert('0.00', bigdecimal)) / 100 as amount, 'US' as source FROM US.Accounts where txn != 'X'" +  //$NON-NLS-1$
           "UNION ALL " +  //$NON-NLS-1$
           "SELECT id, convert(accid / 10000, long), mod(accid, 10000), convert(\"type\", integer), amount, 'EU' from Europe.CustAccts"); //$NON-NLS-1$
        Table accounts = RealMetadataFactory.createVirtualGroup("Accounts", vAccts, accountsPlan); //$NON-NLS-1$
        RealMetadataFactory.createElements(accounts,
                                            new String[] { "customer_id", "account_id", "transaction_id", "txn_type", "amount", "source" }, //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$ //$NON-NLS-5$ //$NON-NLS-6$
                                            new String[] { DataTypeManager.DefaultDataTypes.LONG, DataTypeManager.DefaultDataTypes.LONG, DataTypeManager.DefaultDataTypes.LONG, DataTypeManager.DefaultDataTypes.INTEGER, DataTypeManager.DefaultDataTypes.BIG_DECIMAL, DataTypeManager.DefaultDataTypes.STRING });

        Schema master = RealMetadataFactory.createVirtualModel("Master", metadataStore); //$NON-NLS-1$
        QueryNode masterPlan = new QueryNode("select id as CustomerID, First, Last, a.account_id as AccountID, transaction_id as TransactionID, txn_type AS TxnCode, Amount from CustomerMaster.Customers c, Accounts.Accounts a where c.id=a.customer_id"); //$NON-NLS-1$
        Table transactions = RealMetadataFactory.createVirtualGroup("Transactions", master, masterPlan); //$NON-NLS-1$
        RealMetadataFactory.createElements(transactions,
                                            new String[] { "CustomerID", "First", "Last", "AccountID", "TransactionID", "TxnCode", "Amount" }, //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$ //$NON-NLS-5$ //$NON-NLS-6$ //$NON-NLS-7$
                                            new String[] { DataTypeManager.DefaultDataTypes.LONG, DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.LONG, DataTypeManager.DefaultDataTypes.LONG, DataTypeManager.DefaultDataTypes.INTEGER, DataTypeManager.DefaultDataTypes.BIG_DECIMAL });

        return RealMetadataFactory.createTransformationMetadata(metadataStore, "virtualDepJoin");
    }

    @Test public void testVirtualDepJoinNoValues() throws Exception {
        // Create query
        String sql = "select first, last, sum(amount) from Europe.CustAccts e join CustomerMaster.Customers c on c.id=e.id where c.first=-9999 group by c.id, first, last"; //$NON-NLS-1$

        // Create expected results
        List[] expected = new List[] {
                };

        // Construct data manager with data
        QueryMetadataInterface metadata = exampleVirtualDepJoin();
        FakeDataManager dataManager = new FakeDataManager();
        sampleDataVirtualDepJoin(dataManager, metadata);

        // Plan query
        CommandContext context = TestProcessor.createCommandContext();

        Command command = TestProcessor.helpParse(sql);
        FakeCapabilitiesFinder finder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = TestOptimizer.getTypicalCapabilities();
        caps.setCapabilitySupport(Capability.QUERY_AGGREGATES, false);
        finder.addCapabilities("Europe", caps);//$NON-NLS-1$
        finder.addCapabilities("CustomerMaster", caps);//$NON-NLS-1$
        ProcessorPlan plan = TestProcessor.helpGetPlan(command, metadata, finder, context);

        TestOptimizer.checkDependentJoinCount(plan, 1);
        // Run query
        TestProcessor.helpProcess(plan, context, dataManager, expected);

        // Second query *will not be run* as no values were passed and dependent side has always false criteria
        // So, the list should contain only the first query
        assertEquals(3, dataManager.getQueries().size());
    }

    public void helpTestVirtualDepJoinSourceSelection(boolean setPushdown) throws Exception {
        // Create query
        String sql = "select c.id as CustomerID, First, Last, a.account_id as AccountID, transaction_id as TransactionID, txn_type AS TxnCode, Amount, source from (CustomerMaster.Customers c join CustomerMaster.Locations l on c.id=l.id) join Accounts.Accounts a on c.id=a.customer_id and l.location=a.source where c.first='Miles' order by accountid option makenotdep c, l"; //$NON-NLS-1$

        // Create expected results
        List[] expected = new List[] {
                        Arrays.asList(new Object[] { new Long(100), "Miles", "Davis", new Long(540), new Long(1002), new Integer(1), new BigDecimal("7.20"), "EU" }), //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
                        Arrays.asList(new Object[] { new Long(100), "Miles", "Davis", new Long(540), new Long(1003), new Integer(2), new BigDecimal("1000.00"), "EU" }), //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
                        Arrays.asList(new Object[] { new Long(100), "Miles", "Davis", new Long(15000), new Long(123), new Integer(1), new BigDecimal("100.00"), "US" }), //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
                        Arrays.asList(new Object[] { new Long(100), "Miles", "Davis", new Long(15000), new Long(127), new Integer(2), new BigDecimal("250.00"), "US" }), //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
                        Arrays.asList(new Object[] { new Long(100), "Miles", "Davis", new Long(15000), new Long(128), new Integer(3), new BigDecimal("1000.00"), "US" }), //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
                        Arrays.asList(new Object[] { new Long(100), "Miles", "Davis", new Long(15001), new Long(134), new Integer(1), new BigDecimal("10.00"), "US" }), //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
                        Arrays.asList(new Object[] { new Long(100), "Miles", "Davis", new Long(15001), new Long(201), new Integer(1), new BigDecimal("10.00"), "US" }), //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
                };

        // Construct data manager with data
        QueryMetadataInterface metadata = exampleVirtualDepJoin();
        FakeDataManager dataManager = new FakeDataManager();
        sampleDataVirtualDepJoin(dataManager, metadata);

        // Plan query
        CommandContext context = TestProcessor.createCommandContext();

        Command command = TestProcessor.helpParse(sql);
        FakeCapabilitiesFinder finder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = TestOptimizer.getTypicalCapabilities();
        caps.setCapabilitySupport(Capability.QUERY_ORDERBY, false);
        caps.setCapabilitySupport(Capability.QUERY_FROM_JOIN_INNER, false);
        caps.setCapabilitySupport(Capability.CRITERIA_ISNULL, false);
        caps.setCapabilitySupport(Capability.CRITERIA_IN, setPushdown);
        finder.addCapabilities("US", caps); //$NON-NLS-1$
        finder.addCapabilities("Europe", caps);//$NON-NLS-1$
        finder.addCapabilities("CustomerMaster", caps);//$NON-NLS-1$
        ProcessorPlan plan = TestProcessor.helpGetPlan(command, metadata, finder, context);

        // Check plan contents
        int selectCount = !setPushdown ? 4 : 1;
        int accessCount = setPushdown ? 1 : 4;
        int depAccessCount = 4 - accessCount;
        TestOptimizer.checkNodeTypes(plan, new int[] {
            accessCount,      // Access
            depAccessCount,      // DependentAccess
            0,      // DependentSelect
            0,      // DependentProject
            0,      // DupRemove
            0,      // Grouping
            0,      // NestedLoopJoinStrategy
            2,      // MergeJoinStrategy
            0,      // Null
            0,      // PlanExecution
            3,      // Project
            selectCount,      // Select
            1,      // Sort
            1       // UnionAll
        });

        // Run query
        TestProcessor.helpProcess(plan, context, dataManager, expected);
    }

    @Test public void testVirtualDepJoinSourceSelectionPushdown() throws Exception {
        helpTestVirtualDepJoinSourceSelection(true);
    }

    @Test public void testVirtualDepJoinSourceSelectionNoPushdown() throws Exception {
        helpTestVirtualDepJoinSourceSelection(false);
    }

    @Test public void testVirtualDepJoinPartialPushdown() throws Exception {
        // Create query
        String sql = "SELECT * from Master.Transactions where last = 'Davis'"; //$NON-NLS-1$

        // Create expected results
        List[] expected = new List[] {
                        Arrays.asList(new Object[] { new Long(100), "Miles", "Davis", new Long(15000), new Long(123), new Integer(1), new BigDecimal("100.00") }), //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
                        Arrays.asList(new Object[] { new Long(100), "Miles", "Davis", new Long(15000), new Long(127), new Integer(2), new BigDecimal("250.00") }), //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
                        Arrays.asList(new Object[] { new Long(100), "Miles", "Davis", new Long(15000), new Long(128), new Integer(3), new BigDecimal("1000.00") }), //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
                        Arrays.asList(new Object[] { new Long(100), "Miles", "Davis", new Long(15001), new Long(134), new Integer(1), new BigDecimal("10.00") }), //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
                        Arrays.asList(new Object[] { new Long(100), "Miles", "Davis", new Long(15001), new Long(201), new Integer(1), new BigDecimal("10.00") }), //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
                        Arrays.asList(new Object[] { new Long(100), "Miles", "Davis", new Long(540), new Long(1002), new Integer(1), new BigDecimal("7.20") }), //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
                        Arrays.asList(new Object[] { new Long(100), "Miles", "Davis", new Long(540), new Long(1003), new Integer(2), new BigDecimal("1000.00") }), //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
                };

        // Construct data manager with data
        QueryMetadataInterface metadata = exampleVirtualDepJoin();
        FakeDataManager dataManager = new FakeDataManager();
        sampleDataVirtualDepJoin(dataManager, metadata);

        // Plan query
        CommandContext context = TestProcessor.createCommandContext();

        Command command = TestProcessor.helpParse(sql);
        FakeCapabilitiesFinder finder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps1 = TestOptimizer.getTypicalCapabilities();
        BasicSourceCapabilities caps2 = TestOptimizer.getTypicalCapabilities();
        caps2.setCapabilitySupport(Capability.CRITERIA_IN, false);
        finder.addCapabilities("US", caps1); //$NON-NLS-1$
        finder.addCapabilities("Europe", caps2);//$NON-NLS-1$
        finder.addCapabilities("CustomerMaster", caps1);//$NON-NLS-1$
        ProcessorPlan plan = TestProcessor.helpGetPlan(command, metadata, finder, context);

        TestOptimizer.checkNodeTypes(plan, new int[] {
            2,      // Access
            1,      // DependentAccess
            0,      // DependentSelect
            0,      // DependentProject
            0,      // DupRemove
            0,      // Grouping
            0,      // NestedLoopJoinStrategy
            1,      // MergeJoinStrategy
            0,      // Null
            0,      // PlanExecution
            3,      // Project
            1,      // Select
            0,      // Sort
            1       // UnionAll
        });

        TestOptimizer.checkDependentJoinCount(plan, 1);
        // Run query
        TestProcessor.helpProcess(plan, context, dataManager, expected);
    }

    @Test public void testVirtualDepJoinOverAggregates() throws Exception {
        // Create query
        String sql = "select first, last, sum(amount) from Europe.CustAccts e join CustomerMaster.Customers c on c.id=e.id where c.first='Miles' group by c.id, first, last"; //$NON-NLS-1$

        // Create expected results
        List[] expected = new List[] {
                        Arrays.asList(new Object[] { "Miles", "Davis", new BigDecimal("1007.20") }), //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
                };

        // Construct data manager with data
        QueryMetadataInterface metadata = exampleVirtualDepJoin();
        FakeDataManager dataManager = new FakeDataManager();
        sampleDataVirtualDepJoin(dataManager, metadata);

        // Plan query
        CommandContext context = TestProcessor.createCommandContext();

        Command command = TestProcessor.helpParse(sql);
        FakeCapabilitiesFinder finder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = TestOptimizer.getTypicalCapabilities();
        caps.setCapabilitySupport(Capability.QUERY_AGGREGATES, false);
        finder.addCapabilities("Europe", caps);//$NON-NLS-1$
        finder.addCapabilities("CustomerMaster", caps);//$NON-NLS-1$
        ProcessorPlan plan = TestProcessor.helpGetPlan(command, metadata, finder, context);

        TestOptimizer.checkNodeTypes(plan, new int[] {
            1,      // Access
            1,      // DependentAccess
            0,      // DependentSelect
            0,      // DependentProject
            0,      // DupRemove
            2,      // Grouping
            0,      // NestedLoopJoinStrategy
            1,      // MergeJoinStrategy
            0,      // Null
            0,      // PlanExecution
            1,      // Project
            0,      // Select
            0,      // Sort
            0       // UnionAll
        });

        TestOptimizer.checkDependentJoinCount(plan, 1);
        // Run query
        TestProcessor.helpProcess(plan, context, dataManager, expected);

        List<String> expectedQueries = new ArrayList<String>(6);
        for (int i = 0; i < 3; i++) {
            expectedQueries.add("SELECT g_0.id AS c_0, g_0.first AS c_1, g_0.last AS c_2 FROM CustomerMaster.Customers AS g_0 WHERE g_0.first = 'Miles' ORDER BY c_0"); //$NON-NLS-1$
            expectedQueries.add("SELECT g_0.id, g_0.amount FROM Europe.CustAccts AS g_0 WHERE g_0.id = 100"); //$NON-NLS-1$
        }

        assertEquals(expectedQueries, dataManager.getQueries());
    }

    @Test public void testVirtualDepJoinSelects() throws Exception {
        helpTestVirtualDepJoin(false);
    }

    @Test public void testVirtualDepJoinPushdown() throws Exception {
        helpTestVirtualDepJoin(true);
    }

    @Test public void testVirtualDepMultipleDependentBatches() throws Exception {
        helpTestMultipleBatches(true);
    }

    @Test public void testVirtualDepMultipleDependentBatchesNonUnique() throws Exception {
        helpTestMultipleBatches(false);
    }

    private void helpTestMultipleBatches(boolean unique) throws Exception,
                                          TeiidComponentException,
                                          TeiidException,
                                          SQLException {
        // Create query
        String sql = "SELECT * from Master.Transactions where last = 'Davis' order by CustomerID, TransactionID"; //$NON-NLS-1$

        List<List<Object>> expected = new LinkedList<List<Object>>();


        // Create expected results
        expected.add(Arrays.asList(new Object[] { new Long(100), "Miles", "Davis", new Long(15000), new Long(123), new Integer(1), new BigDecimal("100.00") })); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
        if (!unique) {
            expected.add(Arrays.asList(new Object[] { new Long(100), "Miles", "Davis", new Long(15000), new Long(123), new Integer(1), new BigDecimal("100.00") })); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
        }
        expected.add(Arrays.asList(new Object[] { new Long(100), "Miles", "Davis", new Long(15000), new Long(127), new Integer(2), new BigDecimal("250.00") })); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
        if (!unique) {
            expected.add(Arrays.asList(new Object[] { new Long(100), "Miles", "Davis", new Long(15000), new Long(127), new Integer(2), new BigDecimal("250.00") })); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
        }
        expected.add(Arrays.asList(new Object[] { new Long(100), "Miles", "Davis", new Long(15000), new Long(128), new Integer(3), new BigDecimal("1000.00") })); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
        if (!unique) {
            expected.add(Arrays.asList(new Object[] { new Long(100), "Miles", "Davis", new Long(15000), new Long(128), new Integer(3), new BigDecimal("1000.00") })); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
        }
        expected.add(Arrays.asList(new Object[] { new Long(100), "Miles", "Davis", new Long(15001), new Long(134), new Integer(1), new BigDecimal("10.00") })); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
        if (!unique) {
            expected.add(Arrays.asList(new Object[] { new Long(100), "Miles", "Davis", new Long(15001), new Long(134), new Integer(1), new BigDecimal("10.00") })); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
        }
        expected.add(Arrays.asList(new Object[] { new Long(100), "Miles", "Davis", new Long(15001), new Long(201), new Integer(1), new BigDecimal("10.00") })); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
        if (!unique) {
            expected.add(Arrays.asList(new Object[] { new Long(100), "Miles", "Davis", new Long(15001), new Long(201), new Integer(1), new BigDecimal("10.00") })); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
        }
        expected.add(Arrays.asList(new Object[] { new Long(100), "Miles", "Davis", new Long(540), new Long(1002), new Integer(1), new BigDecimal("7.20") })); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
        if (!unique) {
            expected.add(Arrays.asList(new Object[] { new Long(100), "Miles", "Davis", new Long(540), new Long(1002), new Integer(1), new BigDecimal("7.20") })); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
        }
        expected.add(Arrays.asList(new Object[] { new Long(100), "Miles", "Davis", new Long(540), new Long(1003), new Integer(2), new BigDecimal("1000.00") })); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
        if (!unique) {
            expected.add(Arrays.asList(new Object[] { new Long(100), "Miles", "Davis", new Long(540), new Long(1003), new Integer(2), new BigDecimal("1000.00") })); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
        }
        expected.add(Arrays.asList(new Object[] { new Long(200), "CloneA", "Davis", new Long(16000), new Long(207), new Integer(3), new BigDecimal("12.34") })); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
        expected.add(Arrays.asList(new Object[] { new Long(200), "CloneA", "Davis", new Long(16000), new Long(299), new Integer(3), new BigDecimal("950.34") })); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
        expected.add(Arrays.asList(new Object[] { new Long(200), "CloneA", "Davis", new Long(550), new Long(1004), new Integer(3), new BigDecimal("542.20") })); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
        expected.add(Arrays.asList(new Object[] { new Long(200), "CloneA", "Davis", new Long(550), new Long(1005), new Integer(1), new BigDecimal("99.99") })); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
        expected.add(Arrays.asList(new Object[] { new Long(300), "CloneB", "Davis", new Long(620), new Long(1006), new Integer(1), new BigDecimal("10000.00") })); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
        expected.add(Arrays.asList(new Object[] { new Long(300), "CloneB", "Davis", new Long(620), new Long(1007), new Integer(2), new BigDecimal("0.75") })); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
        expected.add(Arrays.asList(new Object[] { new Long(300), "CloneB", "Davis", new Long(630), new Long(1008), new Integer(2), new BigDecimal("62.00") })); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$

        // Construct data manager with data
        QueryMetadataInterface metadata = exampleVirtualDepJoin();
        FakeDataManager dataManager = new FakeDataManager();
        sampleDataVirtualDepJoin(dataManager, metadata);
        overrideVirtualDepJoinData(dataManager, metadata, unique);

        // Plan query
        CommandContext context = TestProcessor.createCommandContext();

        Command command = TestProcessor.helpParse(sql);
        FakeCapabilitiesFinder finder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = TestOptimizer.getTypicalCapabilities();
        caps.setSourceProperty(Capability.MAX_IN_CRITERIA_SIZE, new Integer(1));
        finder.addCapabilities("US", caps); //$NON-NLS-1$
        finder.addCapabilities("Europe", caps);//$NON-NLS-1$
        finder.addCapabilities("CustomerMaster", caps);//$NON-NLS-1$
        ProcessorPlan plan = TestProcessor.helpGetPlan(command, metadata, finder, context);

        // Run query
        BufferManager bufferMgr = createCustomBufferMgr(2);
        QueryProcessor processor = new QueryProcessor(plan, context, bufferMgr, dataManager);
        processor.setNonBlocking(true);
        BatchCollector collector = processor.createBatchCollector();
        TupleBuffer id = collector.collectTuples();

        TestProcessor.examineResults(expected.toArray(new List[expected.size()]), bufferMgr, id);
    }

    private BufferManager createCustomBufferMgr(int batchSize) {
        return BufferManagerFactory.getTestBufferManager(200000, batchSize);
    }

    public void helpTestVirtualDepJoin(boolean pushCriteria) throws Exception {
        // Create query
        String sql = "SELECT * from Master.Transactions where last = 'Davis'"; //$NON-NLS-1$

        // Create expected results
        List[] expected = new List[] {
                        Arrays.asList(new Object[] { new Long(100), "Miles", "Davis", new Long(15000), new Long(123), new Integer(1), new BigDecimal("100.00") }), //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
                        Arrays.asList(new Object[] { new Long(100), "Miles", "Davis", new Long(15000), new Long(127), new Integer(2), new BigDecimal("250.00") }), //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
                        Arrays.asList(new Object[] { new Long(100), "Miles", "Davis", new Long(15000), new Long(128), new Integer(3), new BigDecimal("1000.00") }), //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
                        Arrays.asList(new Object[] { new Long(100), "Miles", "Davis", new Long(15001), new Long(134), new Integer(1), new BigDecimal("10.00") }), //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
                        Arrays.asList(new Object[] { new Long(100), "Miles", "Davis", new Long(15001), new Long(201), new Integer(1), new BigDecimal("10.00") }), //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
                        Arrays.asList(new Object[] { new Long(100), "Miles", "Davis", new Long(540), new Long(1002), new Integer(1), new BigDecimal("7.20") }), //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
                        Arrays.asList(new Object[] { new Long(100), "Miles", "Davis", new Long(540), new Long(1003), new Integer(2), new BigDecimal("1000.00") }), //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
                };

        // Construct data manager with data
        QueryMetadataInterface metadata = exampleVirtualDepJoin();
        FakeDataManager dataManager = new FakeDataManager();
        sampleDataVirtualDepJoin(dataManager, metadata);

        // Plan query
        Command command = TestProcessor.helpParse(sql);
        FakeCapabilitiesFinder finder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = TestOptimizer.getTypicalCapabilities();
        caps.setCapabilitySupport(Capability.CRITERIA_IN, pushCriteria);
        finder.addCapabilities("US", caps); //$NON-NLS-1$
        finder.addCapabilities("Europe", caps);//$NON-NLS-1$
        finder.addCapabilities("CustomerMaster", caps);//$NON-NLS-1$
        ProcessorPlan plan = TestProcessor.helpGetPlan(command, metadata, finder);

        // Run query
        CommandContext context = TestProcessor.createCommandContext();
        TestProcessor.helpProcess(plan, context, dataManager, expected);
    }

    private void sampleDataVirtualDepJoin(FakeDataManager dataMgr, QueryMetadataInterface metadata) throws Exception {
        dataMgr.setBlockOnce();

        dataMgr.registerTuples(
            metadata,
            "US.Accounts", new List[] {
                    Arrays.asList(new Object[] { new Long(100), new Integer(15000), "DEP", new Integer(123), new Integer(10000) }), //$NON-NLS-1$
                    Arrays.asList(new Object[] { new Long(100), new Integer(15000), "TFR", new Integer(127), new Integer(25000) }), //$NON-NLS-1$
                    Arrays.asList(new Object[] { new Long(100), new Integer(15000), "WD", new Integer(128), new Integer(100000) }), //$NON-NLS-1$
                    Arrays.asList(new Object[] { new Long(100), new Integer(15001), "DEP", new Integer(134), new Integer(1000) }), //$NON-NLS-1$
                    Arrays.asList(new Object[] { new Long(100), new Integer(15001), "DEP", new Integer(201), new Integer(1000) }), //$NON-NLS-1$
                    Arrays.asList(new Object[] { new Long(200), new Integer(16000), "WD", new Integer(207), new Integer(1234) }), //$NON-NLS-1$
                    Arrays.asList(new Object[] { new Long(200), new Integer(16000), "WD", new Integer(299), new Integer(95034) }), //$NON-NLS-1$
                    Arrays.asList(new Object[] { new Long(200), new Integer(16000), "X", new Integer(301), new Integer(5000) }), //$NON-NLS-1$
                    } );

        dataMgr.registerTuples(
            metadata,
            "Europe.CustAccts", new List[] {
                    Arrays.asList(new Object[] { new Long(100), new Long(5401002), new Short((short)1), new BigDecimal("7.20") }), //$NON-NLS-1$
                    Arrays.asList(new Object[] { new Long(100), new Long(5401003), new Short((short)2), new BigDecimal("1000.00") }), //$NON-NLS-1$
                    Arrays.asList(new Object[] { new Long(200), new Long(5501004), new Short((short)3), new BigDecimal("542.20") }), //$NON-NLS-1$
                    Arrays.asList(new Object[] { new Long(200), new Long(5501005), new Short((short)1), new BigDecimal("99.99") }), //$NON-NLS-1$
                    Arrays.asList(new Object[] { new Long(300), new Long(6201006), new Short((short)1), new BigDecimal("10000.00") }), //$NON-NLS-1$
                    Arrays.asList(new Object[] { new Long(300), new Long(6201007), new Short((short)2), new BigDecimal("0.75") }), //$NON-NLS-1$
                    Arrays.asList(new Object[] { new Long(300), new Long(6301008), new Short((short)2), new BigDecimal("62.00") }), //$NON-NLS-1$
                    } );

        dataMgr.registerTuples(metadata, "CustomerMaster.Customers", new List[] {
                   Arrays.asList(new Object[] { new Long(100), "Miles", "Davis", TimestampUtil.createDate(1926, 4, 25) }), //$NON-NLS-1$ //$NON-NLS-2$
                   Arrays.asList(new Object[] { new Long(200), "John", "Coltrane", TimestampUtil.createDate(1926, 8, 23) }), //$NON-NLS-1$ //$NON-NLS-2$
                   Arrays.asList(new Object[] { new Long(300), "Thelonious", "Monk", TimestampUtil.createDate(1917, 9, 10) }), //$NON-NLS-1$ //$NON-NLS-2$
                   } );

        dataMgr.registerTuples(metadata, "CustomerMaster.Locations", new List[] {
                   Arrays.asList(new Object[] { new Long(100), "US" }), //$NON-NLS-1$
                   Arrays.asList(new Object[] { new Long(100), "EU" }), //$NON-NLS-1$
                   Arrays.asList(new Object[] { new Long(200), "US" }), //$NON-NLS-1$
                   Arrays.asList(new Object[] { new Long(200), "EU" }), //$NON-NLS-1$
                   Arrays.asList(new Object[] { new Long(300), "EU" }), //$NON-NLS-1$
                   } );
    }

    private void overrideVirtualDepJoinData(FakeDataManager dataMgr, QueryMetadataInterface metadata, boolean unique) throws Exception {
        // Group CustomerMaster.Customers
        List<List<?>> data = new LinkedList<List<?>>();

        data.add(Arrays.asList(new Object[] { new Long(100), "Miles", "Davis", TimestampUtil.createDate(1926, 4, 25) })); //$NON-NLS-1$ //$NON-NLS-2$
        if (!unique) {
            data.add(Arrays.asList(new Object[] { new Long(100), "Miles", "Davis", TimestampUtil.createDate(1926, 4, 25) })); //$NON-NLS-1$ //$NON-NLS-2$
        }
        data.add(Arrays.asList(new Object[] { new Long(200), "CloneA", "Davis", TimestampUtil.createDate(1926, 4, 26) })); //$NON-NLS-1$ //$NON-NLS-2$
        data.add(Arrays.asList(new Object[] { new Long(300), "CloneB", "Davis", TimestampUtil.createDate(1926, 4, 27) })); //$NON-NLS-1$ //$NON-NLS-2$
        data.add(Arrays.asList(new Object[] { new Long(400), "CloneC", "Davis", TimestampUtil.createDate(1926, 4, 28) })); //$NON-NLS-1$ //$NON-NLS-2$

        dataMgr.registerTuples(
            metadata,
            "CustomerMaster.Customers", data.toArray(new List[data.size()]));
    }

    @Test public void testVirtualAccessVirtualDep() throws Exception {
        String sql = "SELECT a.e0, b.e2 FROM vTest.vGroup a inner join vTest.vGroup b on (a.e0 = b.e2 and a.e1 = b.e0) where b.e0=1 and b.e1='2'"; //$NON-NLS-1$

        BasicSourceCapabilities caps = TestOptimizer.getTypicalCapabilities();
        caps.setFunctionSupport("convert", true); //$NON-NLS-1$
        FakeCapabilitiesFinder finder = new FakeCapabilitiesFinder();
        finder.addCapabilities("test", caps); //$NON-NLS-1$

        ProcessorPlan plan = TestOptimizer.helpPlan(sql, TestValidator.exampleMetadata4(), null, finder,
                                                    new String[] {"SELECT g_0.e0 AS c_0, convert(g_0.e0, string) AS c_1 FROM test.\"group\" AS g_0 WHERE (g_0.e1 = '1') AND (convert(g_0.e0, string) IN (<dependent values>)) ORDER BY c_1",
            "SELECT g_0.e2 AS c_0 FROM test.\"group\" AS g_0 WHERE (g_0.e0 = 1) AND (g_0.e1 = '2') ORDER BY c_0"}, TestOptimizer.ComparisonMode.EXACT_COMMAND_STRING); //$NON-NLS-1$

        TestOptimizer.checkNodeTypes(plan, new int[] {
            1,      // Access
            1,      // DependentAccess
            0,      // DependentSelect
            0,      // DependentProject
            0,      // DupRemove
            0,      // Grouping
            0,      // NestedLoopJoinStrategy
            1,      // MergeJoinStrategy
            0,      // Null
            0,      // PlanExecution
            1,      // Project
            0,      // Select
            0,      // Sort
            0       // UnionAll
        });
    }

    /**
     * Here the virtual makenotdep hint causes us to throw an exception
     *
     */
    @Test public void testVirtualAccessVirtualDep2() {
        String sql = "SELECT a.e0, b.e2 FROM vTest.vGroup a makenotdep inner join vTest.vGroup b on (a.e0 = b.e2 and a.e1 = b.e0) where b.e0=1 and b.e1='2'"; //$NON-NLS-1$

        BasicSourceCapabilities caps = TestOptimizer.getTypicalCapabilities();
        caps.setFunctionSupport("convert", true); //$NON-NLS-1$
        FakeCapabilitiesFinder finder = new FakeCapabilitiesFinder();
        finder.addCapabilities("test", caps); //$NON-NLS-1$

        TestOptimizer.helpPlan(sql, TestValidator.exampleMetadata4(), null, finder,
                                                    new String[] {}, TestOptimizer.SHOULD_FAIL);

    }

    /**
     *  same as testVirtualDepJoinOverAggregate, but the makenotdep hint prevents the
     *  dependent join from happening
     */
    @Test public void testVirtualDepJoinOverAggregates2() throws Exception {
        // Create query
        String sql = "select first, last, sum(amount) from Europe.CustAccts e makenotdep join CustomerMaster.Customers c on c.id=e.id where c.first='Miles' group by c.id, first, last"; //$NON-NLS-1$

        // Create expected results
        List[] expected = new List[] {
                        Arrays.asList(new Object[] { "Miles", "Davis", new BigDecimal("1007.20") }), //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
                };

        // Construct data manager with data
        QueryMetadataInterface metadata = exampleVirtualDepJoin();
        FakeDataManager dataManager = new FakeDataManager();
        sampleDataVirtualDepJoin(dataManager, metadata);

        // Plan query
        CommandContext context = TestProcessor.createCommandContext();

        Command command = TestProcessor.helpParse(sql);
        FakeCapabilitiesFinder finder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = TestOptimizer.getTypicalCapabilities();
        caps.setCapabilitySupport(Capability.QUERY_AGGREGATES, false);
        finder.addCapabilities("Europe", caps);//$NON-NLS-1$
        finder.addCapabilities("CustomerMaster", caps);//$NON-NLS-1$
        ProcessorPlan plan = TestProcessor.helpGetPlan(command, metadata, finder, context);

        TestOptimizer.checkNodeTypes(plan, new int[] {
            2,      // Access
            0,      // DependentAccess
            0,      // DependentSelect
            0,      // DependentProject
            0,      // DupRemove
            2,      // Grouping
            0,      // NestedLoopJoinStrategy
            1,      // MergeJoinStrategy
            0,      // Null
            0,      // PlanExecution
            1,      // Project
            0,      // Select
            0,      // Sort
            0       // UnionAll
        });

        TestOptimizer.checkDependentJoinCount(plan, 0);
        // Run query
        TestProcessor.helpProcess(plan, context, dataManager, expected);
    }


    @Test public void testVirtualMakeDepHint() throws Exception {
        // Create query
        String sql = "select distinct pm1.g1.e1 from (pm1.g1 inner join pm1.g2 on g1.e1 = g2.e1) makedep inner join pm2.g1 on pm2.g1.e1 = pm1.g1.e1 where pm2.g1.e3 = 1"; //$NON-NLS-1$

        // Create expected results
        List[] expected = new List[] {
                        Arrays.asList(new Object[] { "a" }), //$NON-NLS-1$
                        Arrays.asList(new Object[] { "c" }), //$NON-NLS-1$
                };

        // Construct data manager with data
        QueryMetadataInterface metadata = RealMetadataFactory.example1Cached();
        FakeDataManager dataManager = new FakeDataManager();
        TestProcessor.sampleData1(dataManager);

        // Plan query
        ProcessorPlan plan = TestProcessor.helpGetPlan(sql, metadata);

        TestOptimizer.checkNodeTypes(plan, new int[] {
            3,      // Access
            0,      // DependentAccess
            0,      // DependentSelect
            0,      // DependentProject
            1,      // DupRemove
            0,      // Grouping
            0,      // NestedLoopJoinStrategy
            2,      // MergeJoinStrategy
            0,      // Null
            0,      // PlanExecution
            1,      // Project
            2,      // Select
            0,      // Sort
            0       // UnionAll
        });

        TestOptimizer.checkDependentJoinCount(plan, 1);
        // Run query
        TestProcessor.helpProcess(plan, new CommandContext(), dataManager, expected);
    }


}
