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

package org.teiid.query.optimizer.relational.rules;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;

import org.junit.Ignore;
import org.junit.Test;
import org.teiid.api.exception.query.QueryMetadataException;
import org.teiid.core.TeiidComponentException;
import org.teiid.core.TeiidProcessingException;
import org.teiid.metadata.Column;
import org.teiid.query.metadata.QueryMetadataInterface;
import org.teiid.query.metadata.TransformationMetadata;
import org.teiid.query.optimizer.TestOptimizer;
import org.teiid.query.optimizer.TestOptimizer.ComparisonMode;
import org.teiid.query.optimizer.relational.RelationalPlanner;
import org.teiid.query.optimizer.relational.plantree.NodeConstants;
import org.teiid.query.optimizer.relational.plantree.NodeConstants.Info;
import org.teiid.query.optimizer.relational.plantree.NodeFactory;
import org.teiid.query.optimizer.relational.plantree.PlanNode;
import org.teiid.query.optimizer.relational.rules.NewCalculateCostUtil.ColStats;
import org.teiid.query.parser.QueryParser;
import org.teiid.query.processor.TestVirtualDepJoin;
import org.teiid.query.processor.relational.RelationalPlan;
import org.teiid.query.resolver.QueryResolver;
import org.teiid.query.rewriter.QueryRewriter;
import org.teiid.query.sql.lang.Criteria;
import org.teiid.query.sql.lang.JoinType;
import org.teiid.query.sql.symbol.Constant;
import org.teiid.query.sql.symbol.ElementSymbol;
import org.teiid.query.sql.util.SymbolMap;
import org.teiid.query.unittest.RealMetadataFactory;
import org.teiid.query.util.CommandContext;

@SuppressWarnings("nls")
public class TestCalculateCostUtil {

    // =====================================================================
    // HELPERS
    // =====================================================================

    private static Criteria helpGetCriteria(String critString, QueryMetadataInterface metadata) throws QueryMetadataException, TeiidComponentException, TeiidProcessingException{

        Criteria result = QueryParser.getQueryParser().parseCriteria(critString);
        QueryResolver.resolveCriteria(result, metadata);
        result = QueryRewriter.rewriteCriteria(result, new CommandContext(), metadata);

        return result;
    }

    private static PlanNode helpGetJoinNode(float childCost1, float childCost2, JoinType joinType){
        PlanNode joinNode = NodeFactory.getNewNode(NodeConstants.Types.JOIN);
        PlanNode child1 = NodeFactory.getNewNode(NodeConstants.Types.ACCESS);
        PlanNode child2 = NodeFactory.getNewNode(NodeConstants.Types.ACCESS);

        joinNode.addLastChild(child1);
        joinNode.addLastChild(child2);

        child1.setProperty(NodeConstants.Info.EST_CARDINALITY, new Float(childCost1));
        child2.setProperty(NodeConstants.Info.EST_CARDINALITY, new Float(childCost2));

        joinNode.setProperty(NodeConstants.Info.JOIN_TYPE, joinType);

        return joinNode;
    }

    public static void helpTestEstimateCost(String critString, float childCost, float expectedResult, QueryMetadataInterface metadata) throws Exception {
        Criteria crit = helpGetCriteria(critString, metadata);
        PlanNode select = RelationalPlanner.createSelectNode(crit, false);

        float resultCost = NewCalculateCostUtil.recursiveEstimateCostOfCriteria(childCost, select, crit, metadata);
        assertEquals((int)expectedResult, (int)resultCost);
    }

    // =====================================================================
    // TESTS
    // =====================================================================

    @Test public void testEstimateCostOfCriteria() throws Exception {
        QueryMetadataInterface metadata = RealMetadataFactory.example4();
        String critString = "pm2.g3.e1 = '3' or pm2.g3.e2 = 2"; //$NON-NLS-1$

        helpTestEstimateCost(critString, NewCalculateCostUtil.UNKNOWN_VALUE, NewCalculateCostUtil.UNKNOWN_VALUE, metadata);
    }

    @Test public void testEstimateCostOfCompareCriteria() throws Exception {
        QueryMetadataInterface metadata = RealMetadataFactory.example4();
        String critString = "pm1.g1.e1 = '3'"; //$NON-NLS-1$

        helpTestEstimateCost(critString, NewCalculateCostUtil.UNKNOWN_VALUE, 1, metadata);
    }

    @Test public void testEstimateCostOfCompareCriteria1() throws Exception {
        QueryMetadataInterface metadata = RealMetadataFactory.example4();
        String critString = "pm1.g1.e1 < '3'"; //$NON-NLS-1$

        helpTestEstimateCost(critString, NewCalculateCostUtil.UNKNOWN_VALUE, NewCalculateCostUtil.UNKNOWN_VALUE, metadata);
    }

    /**
     * usesKey = false
     * NOT = false
     */
    @Test public void testEstimateCostOfMatchCriteria1() throws Exception {
        QueryMetadataInterface metadata = RealMetadataFactory.example4();
        String critString = "pm2.g3.e1 LIKE '#%'"; //$NON-NLS-1$

        helpTestEstimateCost(critString, 300, 100, metadata);
    }

    /**
     * usesKey = false
     * NOT = true
     */
    @Test public void testEstimateCostOfMatchCriteria2() throws Exception {
        QueryMetadataInterface metadata = RealMetadataFactory.example4();
        String critString = "pm2.g3.e1 NOT LIKE '#_'"; //$NON-NLS-1$

        helpTestEstimateCost(critString, 300, 241, metadata);
    }

    /**
     * usesKey = true
     * NOT = false
     */
    @Test public void testEstimateCostOfMatchCriteria3() throws Exception {
        QueryMetadataInterface metadata = RealMetadataFactory.example4();
        String critString = "pm1.g1.e1 LIKE '#_'"; //$NON-NLS-1$

        helpTestEstimateCost(critString, 300, 50, metadata);
    }

    /**
     * usesKey = true
     * NOT = true
     */
    @Test public void testEstimateCostOfMatchCriteria4() throws Exception {
        QueryMetadataInterface metadata = RealMetadataFactory.example4();
        String critString = "pm1.g1.e1 NOT LIKE '#_'"; //$NON-NLS-1$

        helpTestEstimateCost(critString, 300, 249, metadata);
    }

    /**
     * usesKey = false
     * NOT = false
     */
    @Test public void testEstimateCostOfIsNullCriteria1() throws Exception {
        QueryMetadataInterface metadata = RealMetadataFactory.example4();
        String critString = "pm2.g3.e1 IS NULL"; //$NON-NLS-1$

        helpTestEstimateCost(critString, 300, 35, metadata);
    }

    /**
     * usesKey = false
     * NOT = true
     */
    @Test public void testEstimateCostOfIsNullCriteria2() throws Exception {
        QueryMetadataInterface metadata = RealMetadataFactory.example4();
        String critString = "pm2.g3.e1 IS NOT NULL"; //$NON-NLS-1$

        helpTestEstimateCost(critString, 300, 264, metadata);
    }

    /**
     * usesKey = true
     * NOT = false
     */
    @Test public void testEstimateCostOfIsNullCriteria3() throws Exception {
        QueryMetadataInterface metadata = RealMetadataFactory.example4();
        String critString = "pm1.g1.e1 IS NULL"; //$NON-NLS-1$

        helpTestEstimateCost(critString, 300, 1, metadata);
    }

    /**
     * usesKey = true
     * NOT = true
     */
    @Test public void testEstimateCostOfIsNullCriteria4() throws Exception {
        QueryMetadataInterface metadata = RealMetadataFactory.example4();
        String critString = "pm1.g1.e1 IS NOT NULL"; //$NON-NLS-1$

        helpTestEstimateCost(critString, 300, 300, metadata);
    }

    /**
     * usesKey = false
     * known child cost = false
     * NOT = false
     */
    @Test public void testEstimateCostOfSetCriteria1() throws Exception {
        QueryMetadataInterface metadata = RealMetadataFactory.example4();
        String critString = "pm2.g3.e1 IN ('2', '3')"; //$NON-NLS-1$

        helpTestEstimateCost(critString, NewCalculateCostUtil.UNKNOWN_VALUE, NewCalculateCostUtil.UNKNOWN_VALUE, metadata);
    }

    /**
     * usesKey = false
     * known child cost = false
     * NOT = true
     */
    @Test public void testEstimateCostOfSetCriteria2() throws Exception {
        QueryMetadataInterface metadata = RealMetadataFactory.example4();
        String critString = "pm2.g3.e1 NOT IN ('2', '3')"; //$NON-NLS-1$

        helpTestEstimateCost(critString, NewCalculateCostUtil.UNKNOWN_VALUE, NewCalculateCostUtil.UNKNOWN_VALUE, metadata);
    }

    /**
     * usesKey = false
     * known child cost = true
     * NOT = false
     */
    @Test public void testEstimateCostOfSetCriteria3() throws Exception {
        QueryMetadataInterface metadata = RealMetadataFactory.example4();
        String critString = "pm2.g3.e1 IN ('2', '3')"; //$NON-NLS-1$

        helpTestEstimateCost(critString, 300, 33, metadata);
    }

    /**
     * usesKey = false
     * known child cost = true
     * NOT = true
     */
    @Test public void testEstimateCostOfSetCriteria4() throws Exception {
        QueryMetadataInterface metadata = RealMetadataFactory.example4();
        String critString = "pm2.g3.e1 NOT IN ('2', '3')"; //$NON-NLS-1$

        helpTestEstimateCost(critString, 300, 266, metadata);
    }

    /**
     * usesKey = true
     * known child cost = false
     * NOT = false
     */
    @Test public void testEstimateCostOfSetCriteria5() throws Exception {
        QueryMetadataInterface metadata = RealMetadataFactory.example4();
        String critString = "pm1.g1.e1 IN ('2', '3')"; //$NON-NLS-1$

        helpTestEstimateCost(critString, NewCalculateCostUtil.UNKNOWN_VALUE, 2, metadata);
    }

    /**
     * usesKey = true
     * known child cost = false
     * NOT = true
     */
    @Test public void testEstimateCostOfSetCriteria6() throws Exception {
        QueryMetadataInterface metadata = RealMetadataFactory.example4();
        String critString = "pm1.g1.e1 NOT IN ('2', '3')"; //$NON-NLS-1$

        helpTestEstimateCost(critString, NewCalculateCostUtil.UNKNOWN_VALUE, NewCalculateCostUtil.UNKNOWN_VALUE, metadata);
    }

    /**
     * usesKey = true
     * known child cost = true
     * NOT = false
     */
    @Test public void testEstimateCostOfSetCriteria7() throws Exception {
        QueryMetadataInterface metadata = RealMetadataFactory.example4();
        String critString = "pm1.g1.e1 IN ('2', '3')"; //$NON-NLS-1$

        helpTestEstimateCost(critString, 200, 2, metadata);
    }

    /**
     * usesKey = true
     * known child cost = true
     * NOT = true
     */
    @Test public void testEstimateCostOfSetCriteria8() throws Exception{
        QueryMetadataInterface metadata = RealMetadataFactory.example4();
        String critString = "pm1.g1.e1 NOT IN ('2', '3')"; //$NON-NLS-1$

        helpTestEstimateCost(critString, 200, 198, metadata);
    }

    @Test public void testEstimateJoinNodeCost() throws Exception {
        QueryMetadataInterface metadata = RealMetadataFactory.example4();
        PlanNode joinNode = helpGetJoinNode(NewCalculateCostUtil.UNKNOWN_VALUE, NewCalculateCostUtil.UNKNOWN_VALUE, JoinType.JOIN_CROSS);

        float cost = NewCalculateCostUtil.computeCostForTree(joinNode, metadata);
        assertTrue(cost == NewCalculateCostUtil.UNKNOWN_VALUE);
    }

    @Ignore("this logic needs to be refined to work better")
    @Test public void testEstimateJoinNodeCostOneUnknown() throws Exception {
        QueryMetadataInterface metadata = RealMetadataFactory.example4();
        PlanNode joinNode = helpGetJoinNode(NewCalculateCostUtil.UNKNOWN_VALUE, 500, JoinType.JOIN_INNER);
        joinNode.setProperty(NodeConstants.Info.JOIN_CRITERIA, Arrays.asList(helpGetCriteria("pm1.g1.e1 = pm1.g2.e1", metadata)));
        float cost = NewCalculateCostUtil.computeCostForTree(joinNode, metadata);
        assertEquals(10000, cost, 0);
    }

    @Test public void testEstimateNdvPostJoin() throws Exception {
        String query = "SELECT account FROM US.Accounts, Europe.CustAccts, CustomerMaster.Customers where account + accid + CustomerMaster.Customers.id = 1000000"; //$NON-NLS-1$

        helpTestQuery(1, query, new String[] {"SELECT g_0.accid FROM Europe.CustAccts AS g_0", "SELECT g_0.id FROM CustomerMaster.Customers AS g_0", "SELECT g_0.account FROM US.Accounts AS g_0"});
    }

    /**
     * cases 2159 and 2160, defect 14998
     *
     * e1 and e2 make up a single compound key
     */
    @Test public void testEstimateCostOfCriteriaCompoundKey() throws Exception {
        QueryMetadataInterface metadata = RealMetadataFactory.example4();
        String critString = "pm4.g1.e1 = '3' and pm4.g1.e2 = 2"; //$NON-NLS-1$

        helpTestEstimateCost(critString, NewCalculateCostUtil.UNKNOWN_VALUE, 1, metadata);
    }

    /**
     * cases 2159 and 2160, defect 14998
     *
     * e1 and e2 make up a single compound key, so an OR criteria cannot be
     * predicted to reduce the cost of the join
     */
    @Test public void testEstimateCostOfCriteriaCompoundKey2() throws Exception {
        QueryMetadataInterface metadata = RealMetadataFactory.example4();
        String critString = "pm4.g1.e1 = '3' or pm4.g1.e2 = 2"; //$NON-NLS-1$

        helpTestEstimateCost(critString, NewCalculateCostUtil.UNKNOWN_VALUE, NewCalculateCostUtil.UNKNOWN_VALUE, metadata);
    }

    /**
     * cases 2159 and 2160, defect 14998
     *
     * e1 and e2 make up a single compound key - this criteria does not
     * lower the cost due to the NOT
     */
    @Test public void testEstimateCostOfCriteriaCompoundKey3() throws Exception {
        QueryMetadataInterface metadata = RealMetadataFactory.example4();
        String critString = "pm4.g1.e1 = '3' and not pm4.g1.e2 = 2"; //$NON-NLS-1$

        helpTestEstimateCost(critString, NewCalculateCostUtil.UNKNOWN_VALUE, NewCalculateCostUtil.UNKNOWN_VALUE, metadata);
    }

    /**
     * cases 2159 and 2160, defect 14998
     *
     * e1 and e2 make up a single compound key - this criteria does not
     * lower the cost due to the 0R
     */
    @Test public void testEstimateCostOfCriteriaCompoundKey4() throws Exception {
        QueryMetadataInterface metadata = RealMetadataFactory.example4();
        String critString = "(pm4.g1.e1 = '3' or pm4.g1.e4 = 2.0) and not pm4.g1.e2 = 2"; //$NON-NLS-1$

        helpTestEstimateCost(critString, NewCalculateCostUtil.UNKNOWN_VALUE, NewCalculateCostUtil.UNKNOWN_VALUE, metadata);
    }

    /**
     * cases 2159 and 2160, defect 14998
     *
     * e1 and e2 make up a single compound key - this criteria does not
     * lower the cost due to the OR
     */
    @Test public void testEstimateCostOfCriteriaCompoundKey5() throws Exception {
        QueryMetadataInterface metadata = RealMetadataFactory.example4();
        String critString = "(pm4.g1.e1 = '3' or pm4.g1.e4 = 2.0) and pm4.g1.e2 = 2"; //$NON-NLS-1$

        helpTestEstimateCost(critString, NewCalculateCostUtil.UNKNOWN_VALUE, NewCalculateCostUtil.UNKNOWN_VALUE, metadata);
    }

    /**
     * cases 2159 and 2160, defect 14998
     *
     * e1 and e2 make up a single compound key - this criteria does not
     * lower the cost due to the OR
     */
    @Test public void testEstimateCostOfCriteriaCompoundKey6() throws Exception {
        QueryMetadataInterface metadata = RealMetadataFactory.example4();
        String critString = "(pm4.g1.e1 = '3' and pm4.g1.e2 = 2) or pm4.g1.e4 = 2.0"; //$NON-NLS-1$

        helpTestEstimateCost(critString, NewCalculateCostUtil.UNKNOWN_VALUE, NewCalculateCostUtil.UNKNOWN_VALUE, metadata);
    }

    /**
     * cases 2159 and 2160, defect 14998
     *
     * e1 and e2 make up a single compound key - this criteria covers that
     * key so the cost should be low
     */
    @Test public void testEstimateCostOfCriteriaCompoundKey8() throws Exception {
        QueryMetadataInterface metadata = RealMetadataFactory.example4();
        String critString = "pm4.g1.e1 LIKE '3%' and pm4.g1.e2 = 2"; //$NON-NLS-1$

        helpTestEstimateCost(critString, NewCalculateCostUtil.UNKNOWN_VALUE, 1, metadata);
    }

    /**
     * cases 2159 and 2160, defect 14998
     *
     * e1 and e2 make up a single compound key - this criteria does not
     * lower the cost due to the NOT
     */
    @Test public void testEstimateCostOfCriteriaCompoundKey9() throws Exception {
        QueryMetadataInterface metadata = RealMetadataFactory.example4();
        String critString = "pm4.g1.e1 NOT LIKE '3%' and pm4.g1.e2 = 2"; //$NON-NLS-1$

        helpTestEstimateCost(critString, NewCalculateCostUtil.UNKNOWN_VALUE, NewCalculateCostUtil.UNKNOWN_VALUE, metadata);
    }

    /**
     * cases 2159 and 2160, defect 14998
     *
     * e1 and e2 make up a single compound key - this criteria covers that
     * key so the cost should be low
     */
    @Test public void testEstimateCostOfCriteriaCompoundKey10() throws Exception {
        QueryMetadataInterface metadata = RealMetadataFactory.example4();
        String critString = "'3' LIKE pm4.g1.e1 and pm4.g1.e2 = 2"; //$NON-NLS-1$

        helpTestEstimateCost(critString, NewCalculateCostUtil.UNKNOWN_VALUE, 1, metadata);
    }

    /**
     * cases 2159 and 2160, defect 14998
     *
     * e1 and e2 make up a single compound key - this criteria covers that
     * key so the cost should be low
     */
    @Test public void testEstimateCostOfCriteriaCompoundKey11() throws Exception {
        QueryMetadataInterface metadata = RealMetadataFactory.example4();
        String critString = "pm4.g1.e1 IS NULL and pm4.g1.e2 = 2"; //$NON-NLS-1$

        helpTestEstimateCost(critString, NewCalculateCostUtil.UNKNOWN_VALUE, 1, metadata);
    }

    /**
     * cases 2159 and 2160, defect 14998
     *
     * e1 and e2 make up a single compound key - this criteria does not
     * lower the cost due to the NOT
     */
    @Test public void testEstimateCostOfCriteriaCompoundKey12() throws Exception {
        QueryMetadataInterface metadata = RealMetadataFactory.example4();
        String critString = "pm4.g1.e1 IS NOT NULL and pm4.g1.e2 = 2"; //$NON-NLS-1$

        helpTestEstimateCost(critString, NewCalculateCostUtil.UNKNOWN_VALUE, NewCalculateCostUtil.UNKNOWN_VALUE, metadata);
    }

    /**
     * cases 2159 and 2160, defect 14998
     *
     * e1 and e2 make up a single compound key - this criteria covers that
     * key so the cost should be low
     */
    @Test public void testEstimateCostOfCriteriaCompoundKey13() throws Exception {
        QueryMetadataInterface metadata = RealMetadataFactory.example4();
        String critString = "pm4.g1.e1 IN ('3', '4') and pm4.g1.e2 = 2"; //$NON-NLS-1$

        helpTestEstimateCost(critString, NewCalculateCostUtil.UNKNOWN_VALUE, 1, metadata);
    }

    /**
     * cases 2159 and 2160, defect 14998
     *
     * e1 and e2 make up a single compound key - this criteria does not
     * lower the cost due to the NOT
     */
    @Test public void testEstimateCostOfCriteriaCompoundKey14() throws Exception {
        QueryMetadataInterface metadata = RealMetadataFactory.example4();
        String critString = "pm4.g1.e1 NOT IN ('3', '4') and pm4.g1.e2 = 2"; //$NON-NLS-1$

        helpTestEstimateCost(critString, NewCalculateCostUtil.UNKNOWN_VALUE, NewCalculateCostUtil.UNKNOWN_VALUE, metadata);
    }

    @Test public void testEstimateCostOfCriteriaCompoundKey15() throws Exception {
        QueryMetadataInterface metadata = RealMetadataFactory.example4();
        String critString = "(pm4.g1.e1 = '3' or pm4.g1.e1 = '2') and (pm4.g1.e2 = 2 or pm4.g1.e2 = 1)"; //$NON-NLS-1$

        helpTestEstimateCost(critString, NewCalculateCostUtil.UNKNOWN_VALUE, 1, metadata);
    }

    /**
     *  usesKey true
     */
    @Test public void testEstimateCostOfCriteriaMultiGroup() throws Exception {
        QueryMetadataInterface metadata = RealMetadataFactory.example4();
        String critString = "pm4.g1.e1 = pm1.g1.e1"; //$NON-NLS-1$

        helpTestEstimateCost(critString, NewCalculateCostUtil.UNKNOWN_VALUE, NewCalculateCostUtil.UNKNOWN_VALUE, metadata);
    }

    /**
     *  usesKey false
     */
    @Test public void testEstimateCostOfCriteriaMultiGroup1() throws Exception {
        QueryMetadataInterface metadata = RealMetadataFactory.example4();
        String critString = "pm2.g3.e1 = pm4.g1.e1"; //$NON-NLS-1$

        helpTestEstimateCost(critString, NewCalculateCostUtil.UNKNOWN_VALUE, NewCalculateCostUtil.UNKNOWN_VALUE, metadata);
    }

    /**
     *  usesKey true
     */
    @Test public void testEstimateCostOfCriteriaMultiGroup2() throws Exception {
        QueryMetadataInterface metadata = RealMetadataFactory.example4();
        String critString = "pm4.g1.e1 = pm1.g1.e1"; //$NON-NLS-1$

        helpTestEstimateCost(critString, 100, 8, metadata);
    }

    /**
     *  usesKey false
     */
    @Test public void testEstimateCostOfCriteriaMultiGroup3() throws Exception {
        QueryMetadataInterface metadata = RealMetadataFactory.example4();
        String critString = "pm2.g3.e1 = pm4.g1.e1"; //$NON-NLS-1$

        helpTestEstimateCost(critString, 100, 10, metadata);
    }

    /**
     *  Date Criteria - Case using valid max and min date strings.  In the case of date,
     *  the valid strings are timestamp format - since that is what our costing sets them as.
     */
    @Test public void testEstimateCostOfCriteriaDate1() throws Exception {
        TransformationMetadata metadata = RealMetadataFactory.example1();
        Column e2 = metadata.getElementID("pm3.g1.e2"); //$NON-NLS-1$
        e2.setMinimumValue("2007-04-03 12:12:12.10"); //$NON-NLS-1$
        e2.setMaximumValue("2007-06-03 12:12:12.10"); //$NON-NLS-1$
        String critString = "pm3.g1.e2 <= {d'2008-04-03'}"; //$NON-NLS-1$

        helpTestEstimateCost(critString, 100, 100, metadata);
    }

    /**
     *  Date Criteria - Case using invalid max and min date strings.  In the case of date,
     *  one example of invalid strings is date format - since our costing sets them to timestamp.
     */
    @Test public void testEstimateCostOfCriteriaDate2() throws Exception {
        TransformationMetadata metadata = RealMetadataFactory.example1();
        Column e2 = metadata.getElementID("pm3.g1.e2"); //$NON-NLS-1$
        e2.setMinimumValue("2007-04-03"); //$NON-NLS-1$
        e2.setMaximumValue("2007-06-03"); //$NON-NLS-1$
        String critString = "pm3.g1.e2 <= {d'2008-04-03'}"; //$NON-NLS-1$

        helpTestEstimateCost(critString, 100, 33, metadata);
    }

    /**
     *  Time Criteria - case using valid max and min time strings.
     */
    @Test public void testEstimateCostOfCriteriaTime1() throws Exception {
        TransformationMetadata metadata = RealMetadataFactory.example1();
        Column e3 = metadata.getElementID("pm3.g1.e3"); //$NON-NLS-1$
        e3.setMinimumValue("12:12:12"); //$NON-NLS-1$
        e3.setMaximumValue("12:13:14"); //$NON-NLS-1$
        String critString = "pm3.g1.e3 <= {t'11:11:11'}"; //$NON-NLS-1$

        helpTestEstimateCost(critString, 100, 1, metadata);
    }

    /**
     *  Time Criteria - case using invalid max and min time strings
     */
    @Test public void testEstimateCostOfCriteriaTime2() throws Exception {
        TransformationMetadata metadata = RealMetadataFactory.example1();
        Column e3 = metadata.getElementID("pm3.g1.e3"); //$NON-NLS-1$
        e3.setMinimumValue("2007-04-03 12:12:12.10"); //$NON-NLS-1$
        e3.setMaximumValue("2007-06-03 12:12:12.10"); //$NON-NLS-1$
        String critString = "pm3.g1.e3 <= {t'11:11:11'}"; //$NON-NLS-1$

        helpTestEstimateCost(critString, 100, 33, metadata);
    }

    /**
     *  Timestamp Criteria - case using valid max and min timestamp strings
     */
    @Test public void testEstimateCostOfCriteriaTimestamp1() throws Exception {
        TransformationMetadata metadata = RealMetadataFactory.example1();
        Column e4 = metadata.getElementID("pm3.g1.e4"); //$NON-NLS-1$
        e4.setMinimumValue("2007-04-03 12:12:12.10"); //$NON-NLS-1$
        e4.setMaximumValue("2007-04-03 12:12:12.10"); //$NON-NLS-1$
        String critString = "pm3.g1.e4 <= {ts'2007-04-03 12:12:12.10'}"; //$NON-NLS-1$

        helpTestEstimateCost(critString, 100, 1, metadata);
    }

    /**
     *  Timestamp Criteria - case using invalid max and min timestamp strings
     */
    @Test public void testEstimateCostOfCriteriaTimestamp2() throws Exception {
        TransformationMetadata metadata = RealMetadataFactory.example1();
        Column e4 = metadata.getElementID("pm3.g1.e4"); //$NON-NLS-1$
        e4.setMinimumValue("2007-04-03"); //$NON-NLS-1$
        e4.setMaximumValue("2007-06-03"); //$NON-NLS-1$
        String critString = "pm3.g1.e4 <= {ts'2007-04-03 12:12:12.10'}"; //$NON-NLS-1$

        helpTestEstimateCost(critString, 100, 33, metadata);
    }

    @Test public void testNDVEstimate() throws Exception {
        String crit = "US.accounts.account = 10"; //$NON-NLS-1$

        helpTestEstimateCost(crit, 1000, 800, TestVirtualDepJoin.exampleVirtualDepJoin());
    }

    @Test public void testNDVEstimate1() throws Exception {
        String crit = "US.accounts.account = US.accounts.customer"; //$NON-NLS-1$

        helpTestEstimateCost(crit, 1000, 894, TestVirtualDepJoin.exampleVirtualDepJoin());
    }

    @Test public void testCompoundCriteriaEstimate() throws Exception {
        String crit = "US.accounts.account = 10 and US.accounts.account = US.accounts.customer"; //$NON-NLS-1$

        helpTestEstimateCost(crit, 1000, 757, TestVirtualDepJoin.exampleVirtualDepJoin());
    }

    @Test public void testCompoundCriteriaEstimate1() throws Exception {
        String crit = "US.accounts.account = 10 or US.accounts.account = US.accounts.customer"; //$NON-NLS-1$

        helpTestEstimateCost(crit, 1000, 1000, TestVirtualDepJoin.exampleVirtualDepJoin());
    }

    @Test public void testNNVEstimate() throws Exception {
        String crit = "US.accounts.account is null"; //$NON-NLS-1$

        helpTestEstimateCost(crit, 1000, 1, TestVirtualDepJoin.exampleVirtualDepJoin());
    }

    @Test public void testNNVEstimate1() throws Exception {
        String crit = "US.accounts.account is null"; //$NON-NLS-1$

        helpTestEstimateCost(crit, NewCalculateCostUtil.UNKNOWN_VALUE, 1, TestVirtualDepJoin.exampleVirtualDepJoin());
    }

    @Test public void testCompoundCriteriaEstimate2() throws Exception {
        String crit = "US.accounts.account is null and US.accounts.account = US.accounts.customer"; //$NON-NLS-1$

        helpTestEstimateCost(crit, 1000, 1, TestVirtualDepJoin.exampleVirtualDepJoin());
    }

    @Test public void testCompoundCriteriaEstimate3() throws Exception {
        String crit = "US.accounts.account is null or US.accounts.account = US.accounts.customer"; //$NON-NLS-1$

        helpTestEstimateCost(crit, 1000, 895, TestVirtualDepJoin.exampleVirtualDepJoin());
    }

    //ensures that the ordering of criteria does not effect the costing calculation
    @Test public void testCompoundCriteriaEstimate4() throws Exception {
        String crit = "US.accounts.account = 10 and US.accounts.account = US.accounts.customer and US.accounts.customer < 100"; //$NON-NLS-1$

        helpTestEstimateCost(crit, 1000, 87, TestVirtualDepJoin.exampleVirtualDepJoin());

        String crit1 = "US.accounts.account = US.accounts.customer and US.accounts.customer < 100 and US.accounts.account = 10"; //$NON-NLS-1$

        helpTestEstimateCost(crit1, 1000, 85, TestVirtualDepJoin.exampleVirtualDepJoin());
    }

    @Test public void testCompoundCriteriaEstimate5() throws Exception {
        String crit = "US.accounts.account is null and US.accounts.account = US.accounts.customer"; //$NON-NLS-1$

        helpTestEstimateCost(crit, NewCalculateCostUtil.UNKNOWN_VALUE, 1, TestVirtualDepJoin.exampleVirtualDepJoin());
    }

    @Test public void testCompoundCriteriaEstimate6() throws Exception {
        String crit = "US.accounts.account is null or US.accounts.account = US.accounts.customer"; //$NON-NLS-1$

        helpTestEstimateCost(crit, NewCalculateCostUtil.UNKNOWN_VALUE, NewCalculateCostUtil.UNKNOWN_VALUE, TestVirtualDepJoin.exampleVirtualDepJoin());
    }

    @Test public void testCompoundCriteriaEstimate7() throws Exception {
        String critString = "US.accounts.account = 10"; //$NON-NLS-1$
        helpTestEstimateCost(critString, 1000, 800, TestVirtualDepJoin.exampleVirtualDepJoin());

        critString = "US.accounts.customer < 100"; //$NON-NLS-1$
        helpTestEstimateCost(critString, 800, 80, TestVirtualDepJoin.exampleVirtualDepJoin());

        String crit = "US.accounts.account = 10 and US.accounts.customer < 100"; //$NON-NLS-1$

        helpTestEstimateCost(crit, 1000, 90, TestVirtualDepJoin.exampleVirtualDepJoin());
    }

    //min and max are not set, so the default estimate is returned
    @Test public void testRangeEstimate() throws Exception {
        String crit = "US.accounts.account < 100"; //$NON-NLS-1$

        helpTestEstimateCost(crit, 1000, 333, TestVirtualDepJoin.exampleVirtualDepJoin());
    }

    @Test public void testRangeEstimate1() throws Exception {
        String crit = "US.accounts.customer < 100"; //$NON-NLS-1$

        helpTestEstimateCost(crit, 1000, 100, TestVirtualDepJoin.exampleVirtualDepJoin());
    }

    @Test public void testRangeEstimate2() throws Exception {
        String crit = "US.accounts.customer > 100"; //$NON-NLS-1$

        helpTestEstimateCost(crit, 1000, 900, TestVirtualDepJoin.exampleVirtualDepJoin());
    }

    @Test public void testRangeEstimate3() throws Exception {
        String crit = "US.accounts.customer >= 1600"; //$NON-NLS-1$

        helpTestEstimateCost(crit, 1000, 1, TestVirtualDepJoin.exampleVirtualDepJoin());
    }

    @Test public void testRangeEstimate4() throws Exception {
        String crit = "US.accounts.customer < -1"; //$NON-NLS-1$

        helpTestEstimateCost(crit, 1000, 1, TestVirtualDepJoin.exampleVirtualDepJoin());
    }

    @Test public void testRangeEstimate5() throws Exception {
        String crit = "US.accounts.customer >= -1"; //$NON-NLS-1$

        helpTestEstimateCost(crit, 1000, 1000, TestVirtualDepJoin.exampleVirtualDepJoin());
    }

    @Test public void testRangeEstimate6() throws Exception {
        String crit = "US.accounts.pennies >= -2"; //$NON-NLS-1$

        helpTestEstimateCost(crit, 1000, 1000, TestVirtualDepJoin.exampleVirtualDepJoin());
    }

    @Test public void testRangeEstimate7() throws Exception {
        String crit = "US.accounts.pennies >= -6"; //$NON-NLS-1$

        helpTestEstimateCost(crit, 1000, 800, TestVirtualDepJoin.exampleVirtualDepJoin());
    }

    @Test public void testLimitWithUnknownChildCardinality() throws Exception {
        String query = "select e1 from pm1.g1 limit 2"; //$NON-NLS-1$

        RelationalPlan plan = (RelationalPlan)TestOptimizer.helpPlan(query, RealMetadataFactory.example1Cached(), new String[] {"SELECT e1 FROM pm1.g1"}); //$NON-NLS-1$

        assertEquals(new Float(2), plan.getRootNode().getEstimateNodeCardinality());
    }

    public void helpTestSetOp(String op, float cost) throws Exception {
        String query = "SELECT customer as customer_id, convert(account, long) as account_id, convert(txnid, long) as transaction_id, case txn when 'DEP' then 1 when 'TFR' then 2 when 'WD' then 3 else -1 end as txn_type, (pennies + convert('0.00', bigdecimal)) / 100 as amount, 'US' as source FROM US.Accounts where txn != 'X'" +  //$NON-NLS-1$
        op +
        "SELECT id, convert(accid / 10000, long), mod(accid, 10000), convert(type, integer), amount, 'EU' from Europe.CustAccts"; //$NON-NLS-1$

        String[] expected = new String[] {"SELECT g_0.customer, g_0.account, g_0.txnid, g_0.txn, g_0.pennies FROM US.Accounts AS g_0 WHERE g_0.txn <> 'X'", "SELECT g_0.id, g_0.accid, g_0.type, g_0.amount FROM Europe.CustAccts AS g_0"};

        helpTestQuery(cost, query, expected);
    }

    private void helpTestQuery(float cost, String query, String[] expected)
            throws TeiidComponentException, TeiidProcessingException {
        RelationalPlan plan = (RelationalPlan)TestOptimizer.helpPlan(query, TestVirtualDepJoin.exampleVirtualDepJoin(), expected, ComparisonMode.EXACT_COMMAND_STRING); //$NON-NLS-1$ //$NON-NLS-2$

        assertEquals(cost, plan.getRootNode().getEstimateNodeCardinality());
    }

    @Test public void testUnion() throws Exception {
        helpTestSetOp("UNION ", 1375000.0f); //$NON-NLS-1$
    }

    @Test public void testUnionALL() throws Exception {
        helpTestSetOp("UNION ALL ", 1750000.0f); //$NON-NLS-1$
    }

    @Test public void testExcept() throws Exception {
        helpTestSetOp("EXCEPT ", 250000.0f); //$NON-NLS-1$
    }

    @Test public void testIntersect() throws Exception {
        helpTestSetOp("INTERSECT ", 375000.0f); //$NON-NLS-1$
    }

    @Test public void testProjectLiteral() throws Exception {
        PlanNode project = NodeFactory.getNewNode(NodeConstants.Types.PROJECT);
        project.setProperty(Info.PROJECT_COLS, Arrays.asList(new Constant(1)));
        PlanNode access = NodeFactory.getNewNode(NodeConstants.Types.ACCESS);
        project.addFirstChild(access);
        access.setProperty(NodeConstants.Info.EST_CARDINALITY, NewCalculateCostUtil.UNKNOWN_VALUE);
        float cost = NewCalculateCostUtil.computeCostForTree(project, RealMetadataFactory.example1Cached());
        assertTrue(cost == NewCalculateCostUtil.UNKNOWN_VALUE);
        ColStats colStats = (ColStats)project.getProperty(Info.EST_COL_STATS);
        assertEquals("{1=[1.0, 1.0, 0.0]}", colStats.toString());

        access.setProperty(NodeConstants.Info.EST_CARDINALITY, 5f);
        NewCalculateCostUtil.updateCardinality(project, RealMetadataFactory.example1Cached());
        cost = NewCalculateCostUtil.computeCostForTree(project, RealMetadataFactory.example1Cached());
        assertTrue(cost == NewCalculateCostUtil.UNKNOWN_VALUE);
        colStats = (ColStats)project.getProperty(Info.EST_COL_STATS);
        assertEquals("{1=[1.0, 1.0, 0.0]}", colStats.toString());
    }

    @Test public void testProjectLiteral1() throws Exception {
        PlanNode project = NodeFactory.getNewNode(NodeConstants.Types.PROJECT);
        project.setProperty(Info.PROJECT_COLS, Arrays.asList(new Constant(1)));
        PlanNode access = NodeFactory.getNewNode(NodeConstants.Types.ACCESS);
        project.addFirstChild(access);
        access.setProperty(NodeConstants.Info.EST_CARDINALITY, 5f);
        float cost = NewCalculateCostUtil.computeCostForTree(project, RealMetadataFactory.example1Cached());
        assertEquals(5f, cost, 0);
        ColStats colStats = (ColStats)project.getProperty(Info.EST_COL_STATS);
        assertEquals("{1=[1.0, 1.0, 0.0]}", colStats.toString());
    }

    @Test public void testProjectLiteral2() throws Exception {
        PlanNode project = NodeFactory.getNewNode(NodeConstants.Types.PROJECT);
        project.setProperty(Info.PROJECT_COLS, Arrays.asList(new Constant(1)));
        PlanNode access = NodeFactory.getNewNode(NodeConstants.Types.ACCESS);
        project.addFirstChild(access);
        access.setProperty(NodeConstants.Info.EST_CARDINALITY, 5f);
        PlanNode source = NodeFactory.getNewNode(NodeConstants.Types.SOURCE);
        source.setProperty(Info.OUTPUT_COLS, Arrays.asList(new ElementSymbol("x")));
        source.addFirstChild(project);
        SymbolMap sm = new SymbolMap();
        sm.addMapping(new ElementSymbol("x"), new Constant(1));
        source.setProperty(Info.SYMBOL_MAP, sm);
        float cost = NewCalculateCostUtil.computeCostForTree(source, RealMetadataFactory.example1Cached());
        assertEquals(5f, cost, 0);
        ColStats colStats = (ColStats)source.getProperty(Info.EST_COL_STATS);
        assertEquals("{x=[1.0, 1.0, 0.0]}", colStats.toString());
    }

    @Test public void testProjectLiteral3() throws Exception {
        PlanNode project = NodeFactory.getNewNode(NodeConstants.Types.PROJECT);
        project.setProperty(Info.PROJECT_COLS, Arrays.asList(new Constant(1)));
        PlanNode access = NodeFactory.getNewNode(NodeConstants.Types.ACCESS);
        project.addFirstChild(access);
        access.setProperty(NodeConstants.Info.EST_CARDINALITY, 5f);
        PlanNode dup = NodeFactory.getNewNode(NodeConstants.Types.DUP_REMOVE);
        dup.addFirstChild(project);
        float cost = NewCalculateCostUtil.computeCostForTree(dup, RealMetadataFactory.example1Cached());
        assertEquals(1f, cost, 0);
        ColStats colStats = (ColStats)project.getProperty(Info.EST_COL_STATS);
        assertEquals("{1=[1.0, 1.0, 0.0]}", colStats.toString());
    }

    @Test public void testProjectLiteral4() throws Exception {
        PlanNode project = NodeFactory.getNewNode(NodeConstants.Types.PROJECT);
        project.setProperty(Info.PROJECT_COLS, Arrays.asList(new Constant(1)));
        PlanNode access = NodeFactory.getNewNode(NodeConstants.Types.ACCESS);
        project.addFirstChild(access);
        access.setProperty(NodeConstants.Info.EST_CARDINALITY, NewCalculateCostUtil.UNKNOWN_VALUE);
        PlanNode dup = NodeFactory.getNewNode(NodeConstants.Types.DUP_REMOVE);
        dup.addFirstChild(project);
        float cost = NewCalculateCostUtil.computeCostForTree(dup, RealMetadataFactory.example1Cached());
        assertEquals(NewCalculateCostUtil.UNKNOWN_VALUE, cost, 0);
        ColStats colStats = (ColStats)dup.getProperty(Info.EST_COL_STATS);
        assertEquals("{1=[1.0, 1.0, 0.0]}", colStats.toString());
    }

}
