/*
 * JBoss, Home of Professional Open Source.
 * See the COPYRIGHT.txt file distributed with this work for information
 * regarding copyright ownership.  Some portions may be licensed
 * to Red Hat, Inc. under one or more contributor license agreements.
 * 
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 2.1 of the License, or (at your option) any later version.
 * 
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 * 
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA
 * 02110-1301 USA.
 */

package com.metamatrix.query.optimizer.relational.rules;

import junit.framework.TestCase;

import com.metamatrix.api.exception.MetaMatrixComponentException;
import com.metamatrix.api.exception.query.QueryMetadataException;
import com.metamatrix.api.exception.query.QueryParserException;
import com.metamatrix.api.exception.query.QueryResolverException;
import com.metamatrix.api.exception.query.QueryValidatorException;
import com.metamatrix.query.metadata.QueryMetadataInterface;
import com.metamatrix.query.optimizer.TestOptimizer;
import com.metamatrix.query.optimizer.relational.GenerateCanonical;
import com.metamatrix.query.optimizer.relational.plantree.NodeConstants;
import com.metamatrix.query.optimizer.relational.plantree.NodeFactory;
import com.metamatrix.query.optimizer.relational.plantree.PlanNode;
import com.metamatrix.query.parser.QueryParser;
import com.metamatrix.query.processor.TestVirtualDepJoin;
import com.metamatrix.query.processor.relational.RelationalPlan;
import com.metamatrix.query.resolver.QueryResolver;
import com.metamatrix.query.rewriter.QueryRewriter;
import com.metamatrix.query.sql.lang.Criteria;
import com.metamatrix.query.sql.lang.JoinType;
import com.metamatrix.query.unittest.FakeMetadataFactory;
import com.metamatrix.query.unittest.FakeMetadataObject;
import com.metamatrix.query.util.CommandContext;

/**
 * Test of {@link CalculateCostUtil}
 */
public class TestCalculateCostUtil extends TestCase {

    /**
     * Constructor for TestCapabilitiesUtil.
     * @param name
     */
    public TestCalculateCostUtil(String name) {
        super(name);
    }
    
    // =====================================================================
    // HELPERS
    // =====================================================================
    
    private static Criteria helpGetCriteria(String critString, QueryMetadataInterface metadata) throws QueryParserException, QueryResolverException, QueryMetadataException, MetaMatrixComponentException, QueryValidatorException{

        Criteria result = QueryParser.getQueryParser().parseCriteria(critString);
        QueryResolver.resolveCriteria(result, metadata);
        result = QueryRewriter.rewriteCriteria(result, null, new CommandContext(), null);

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
    
    void helpTestEstimateCost(String critString, float childCost, float expectedResult, QueryMetadataInterface metadata) throws Exception {
        Criteria crit = helpGetCriteria(critString, metadata);
        PlanNode select = GenerateCanonical.createSelectNode(crit, false);
        
        float resultCost = NewCalculateCostUtil.recursiveEstimateCostOfCriteria(childCost, select, crit, metadata);
        assertEquals((int)expectedResult, (int)resultCost);
    }
    
    // =====================================================================
    // TESTS
    // =====================================================================
    
    /** Merrill ran into a problem with this type of criteria */
    public void testEstimateCostOfCriteria() throws Exception {
        QueryMetadataInterface metadata = FakeMetadataFactory.example4();
        String critString = "pm2.g3.e1 = '3' or pm2.g3.e2 = 2"; //$NON-NLS-1$
        
        helpTestEstimateCost(critString, NewCalculateCostUtil.UNKNOWN_VALUE, NewCalculateCostUtil.UNKNOWN_VALUE, metadata);
    }

    public void testEstimateCostOfCompareCriteria() throws Exception {
        QueryMetadataInterface metadata = FakeMetadataFactory.example4();
        String critString = "pm1.g1.e1 = '3'"; //$NON-NLS-1$
        
        helpTestEstimateCost(critString, NewCalculateCostUtil.UNKNOWN_VALUE, 1, metadata);    
    }    

    /** defect 15045 */ 
    public void testEstimateCostOfCompareCriteria2() throws Exception {
        QueryMetadataInterface metadata = FakeMetadataFactory.example4();
        String critString = "'3' = pm1.g1.e1"; //$NON-NLS-1$
        
        helpTestEstimateCost(critString, NewCalculateCostUtil.UNKNOWN_VALUE, 1, metadata);
    }    
    
    /**
     * usesKey = false
     * NOT = false
     */
    public void testEstimateCostOfMatchCriteria1() throws Exception {
        QueryMetadataInterface metadata = FakeMetadataFactory.example4();
        String critString = "pm2.g3.e1 LIKE '#'"; //$NON-NLS-1$
        
        helpTestEstimateCost(critString, 300, 100, metadata);
    }

    /**
     * usesKey = false
     * NOT = false
     */
    public void testEstimateCostOfMatchCriteria1a() throws Exception {
        QueryMetadataInterface metadata = FakeMetadataFactory.example4();
        String critString = "'#' LIKE pm2.g3.e1"; //$NON-NLS-1$
        
        helpTestEstimateCost(critString, 300, 100, metadata);
    }    
    
    /**
     * usesKey = false
     * NOT = true
     */
    public void testEstimateCostOfMatchCriteria2() throws Exception {
        QueryMetadataInterface metadata = FakeMetadataFactory.example4();
        String critString = "pm2.g3.e1 NOT LIKE '#'"; //$NON-NLS-1$
        
        helpTestEstimateCost(critString, 300, 200, metadata);
    }
    
    /**
     * usesKey = true
     * NOT = false
     */
    public void testEstimateCostOfMatchCriteria3() throws Exception {
        QueryMetadataInterface metadata = FakeMetadataFactory.example4();
        String critString = "pm1.g1.e1 LIKE '#'"; //$NON-NLS-1$
        
        helpTestEstimateCost(critString, 300, 1, metadata);
    }

    /**
     * usesKey = true
     * NOT = false
     */
    public void testEstimateCostOfMatchCriteria3a() throws Exception {
        QueryMetadataInterface metadata = FakeMetadataFactory.example4();
        String critString = "'#' LIKE pm1.g1.e1"; //$NON-NLS-1$
        
        helpTestEstimateCost(critString, 300, 1, metadata);
    }    
    
    /**
     * usesKey = true
     * NOT = true
     */
    public void testEstimateCostOfMatchCriteria4() throws Exception {
        QueryMetadataInterface metadata = FakeMetadataFactory.example4();
        String critString = "pm1.g1.e1 NOT LIKE '#'"; //$NON-NLS-1$
        
        helpTestEstimateCost(critString, 300, 299, metadata);
    }

    /**
     * usesKey = true
     * NOT = true
     */
    public void testEstimateCostOfMatchCriteria4a() throws Exception {
        QueryMetadataInterface metadata = FakeMetadataFactory.example4();
        String critString = "'#' NOT LIKE pm1.g1.e1"; //$NON-NLS-1$
        
        helpTestEstimateCost(critString, 300, 299, metadata);
    }    
    
    /**
     * usesKey = false
     * NOT = false
     */
    public void testEstimateCostOfIsNullCriteria1() throws Exception {
        QueryMetadataInterface metadata = FakeMetadataFactory.example4();
        String critString = "pm2.g3.e1 IS NULL"; //$NON-NLS-1$
        
        helpTestEstimateCost(critString, 300, 100, metadata);
    }
    
    /**
     * usesKey = false
     * NOT = true
     */
    public void testEstimateCostOfIsNullCriteria2() throws Exception {
        QueryMetadataInterface metadata = FakeMetadataFactory.example4();
        String critString = "pm2.g3.e1 IS NOT NULL"; //$NON-NLS-1$
        
        helpTestEstimateCost(critString, 300, 200, metadata);
    }
    
    /**
     * usesKey = true
     * NOT = false
     */
    public void testEstimateCostOfIsNullCriteria3() throws Exception {
        QueryMetadataInterface metadata = FakeMetadataFactory.example4();
        String critString = "pm1.g1.e1 IS NULL"; //$NON-NLS-1$
        
        helpTestEstimateCost(critString, 300, 1, metadata);
    }
    
    /**
     * usesKey = true
     * NOT = true
     */
    public void testEstimateCostOfIsNullCriteria4() throws Exception {
        QueryMetadataInterface metadata = FakeMetadataFactory.example4();
        String critString = "pm1.g1.e1 IS NOT NULL"; //$NON-NLS-1$
        
        helpTestEstimateCost(critString, 300, 299, metadata);
    }
    
    /**
     * usesKey = false
     * known child cost = false
     * NOT = false
     */
    public void testEstimateCostOfSetCriteria1() throws Exception {
        QueryMetadataInterface metadata = FakeMetadataFactory.example4();
        String critString = "pm2.g3.e1 IN ('2', '3')"; //$NON-NLS-1$
        
        helpTestEstimateCost(critString, NewCalculateCostUtil.UNKNOWN_VALUE, NewCalculateCostUtil.UNKNOWN_VALUE, metadata);
    }
    
    /**
     * usesKey = false
     * known child cost = false
     * NOT = true
     */
    public void testEstimateCostOfSetCriteria2() throws Exception {
        QueryMetadataInterface metadata = FakeMetadataFactory.example4();
        String critString = "pm2.g3.e1 NOT IN ('2', '3')"; //$NON-NLS-1$
        
        helpTestEstimateCost(critString, NewCalculateCostUtil.UNKNOWN_VALUE, NewCalculateCostUtil.UNKNOWN_VALUE, metadata);
    }
    
    /**
     * usesKey = false
     * known child cost = true
     * NOT = false
     */
    public void testEstimateCostOfSetCriteria3() throws Exception {
        QueryMetadataInterface metadata = FakeMetadataFactory.example4();
        String critString = "pm2.g3.e1 IN ('2', '3')"; //$NON-NLS-1$
        
        helpTestEstimateCost(critString, 300, 200, metadata);
    }
    
    /**
     * usesKey = false
     * known child cost = true
     * NOT = true
     */
    public void testEstimateCostOfSetCriteria4() throws Exception {
        QueryMetadataInterface metadata = FakeMetadataFactory.example4();
        String critString = "pm2.g3.e1 NOT IN ('2', '3')"; //$NON-NLS-1$
        
        helpTestEstimateCost(critString, 300, 100, metadata);
    }
    
    /**
     * usesKey = true
     * known child cost = false
     * NOT = false
     */
    public void testEstimateCostOfSetCriteria5() throws Exception {
        QueryMetadataInterface metadata = FakeMetadataFactory.example4();
        String critString = "pm1.g1.e1 IN ('2', '3')"; //$NON-NLS-1$
        
        helpTestEstimateCost(critString, NewCalculateCostUtil.UNKNOWN_VALUE, NewCalculateCostUtil.UNKNOWN_VALUE, metadata);
    }
    
    /**
     * usesKey = true
     * known child cost = false
     * NOT = true
     */
    public void testEstimateCostOfSetCriteria6() throws Exception {
        QueryMetadataInterface metadata = FakeMetadataFactory.example4();
        String critString = "pm1.g1.e1 NOT IN ('2', '3')"; //$NON-NLS-1$
        
        helpTestEstimateCost(critString, NewCalculateCostUtil.UNKNOWN_VALUE, NewCalculateCostUtil.UNKNOWN_VALUE, metadata);
    }
    
    /**
     * usesKey = true
     * known child cost = true
     * NOT = false
     */
    public void testEstimateCostOfSetCriteria7() throws Exception {
        QueryMetadataInterface metadata = FakeMetadataFactory.example4();
        String critString = "pm1.g1.e1 IN ('2', '3')"; //$NON-NLS-1$
        
        helpTestEstimateCost(critString, 200, 2, metadata);
    }
    
    /**
     * usesKey = true
     * known child cost = true
     * NOT = true
     */
    public void testEstimateCostOfSetCriteria8() throws Exception{
        QueryMetadataInterface metadata = FakeMetadataFactory.example4();
        String critString = "pm1.g1.e1 NOT IN ('2', '3')"; //$NON-NLS-1$
        
        helpTestEstimateCost(critString, 200, 198, metadata);
    }
    
    public void testEstimateJoinNodeCost() throws Exception {
        QueryMetadataInterface metadata = FakeMetadataFactory.example4();
        PlanNode joinNode = helpGetJoinNode(NewCalculateCostUtil.UNKNOWN_VALUE, NewCalculateCostUtil.UNKNOWN_VALUE, JoinType.JOIN_CROSS);
        
        float cost = NewCalculateCostUtil.computeCostForTree(joinNode, metadata);
        assertTrue(cost == NewCalculateCostUtil.UNKNOWN_VALUE);
    }

    /** 
     * BOA cases 2159 and 2160, defect 14998
     * 
     * e1 and e2 make up a single compound key 
     */
    public void testEstimateCostOfCriteriaCompoundKey() throws Exception {
        QueryMetadataInterface metadata = FakeMetadataFactory.example4();
        String critString = "pm4.g1.e1 = '3' and pm4.g1.e2 = 2"; //$NON-NLS-1$
        
        helpTestEstimateCost(critString, NewCalculateCostUtil.UNKNOWN_VALUE, 1, metadata);
    }    

    /** 
     * BOA cases 2159 and 2160, defect 14998
     * 
     * e1 and e2 make up a single compound key, so an OR criteria cannot be
     * predicted to reduce the cost of the join
     */
    public void testEstimateCostOfCriteriaCompoundKey2() throws Exception {
        QueryMetadataInterface metadata = FakeMetadataFactory.example4();
        String critString = "pm4.g1.e1 = '3' or pm4.g1.e2 = 2"; //$NON-NLS-1$
        
        helpTestEstimateCost(critString, NewCalculateCostUtil.UNKNOWN_VALUE, NewCalculateCostUtil.UNKNOWN_VALUE, metadata);
    }     

    /** 
     * BOA cases 2159 and 2160, defect 14998
     * 
     * e1 and e2 make up a single compound key - this criteria does not
     * lower the cost due to the NOT
     */
    public void testEstimateCostOfCriteriaCompoundKey3() throws Exception {
        QueryMetadataInterface metadata = FakeMetadataFactory.example4();
        String critString = "pm4.g1.e1 = '3' and not pm4.g1.e2 = 2"; //$NON-NLS-1$
        
        helpTestEstimateCost(critString, NewCalculateCostUtil.UNKNOWN_VALUE, NewCalculateCostUtil.UNKNOWN_VALUE, metadata);
    }     

    /** 
     * BOA cases 2159 and 2160, defect 14998
     * 
     * e1 and e2 make up a single compound key - this criteria does not
     * lower the cost due to the 0R
     */
    public void testEstimateCostOfCriteriaCompoundKey4() throws Exception {
        QueryMetadataInterface metadata = FakeMetadataFactory.example4();
        String critString = "(pm4.g1.e1 = '3' or pm4.g1.e4 = 2.0) and not pm4.g1.e2 = 2"; //$NON-NLS-1$
        
        helpTestEstimateCost(critString, NewCalculateCostUtil.UNKNOWN_VALUE, NewCalculateCostUtil.UNKNOWN_VALUE, metadata);
    }      

    /** 
     * BOA cases 2159 and 2160, defect 14998
     * 
     * e1 and e2 make up a single compound key - this criteria does not
     * lower the cost due to the OR
     */
    public void testEstimateCostOfCriteriaCompoundKey5() throws Exception {
        QueryMetadataInterface metadata = FakeMetadataFactory.example4();
        String critString = "(pm4.g1.e1 = '3' or pm4.g1.e4 = 2.0) and pm4.g1.e2 = 2"; //$NON-NLS-1$
        
        helpTestEstimateCost(critString, NewCalculateCostUtil.UNKNOWN_VALUE, NewCalculateCostUtil.UNKNOWN_VALUE, metadata);
    }    

    /** 
     * BOA cases 2159 and 2160, defect 14998
     * 
     * e1 and e2 make up a single compound key - this criteria does not
     * lower the cost due to the OR
     */
    public void testEstimateCostOfCriteriaCompoundKey6() throws Exception {
        QueryMetadataInterface metadata = FakeMetadataFactory.example4();
        String critString = "(pm4.g1.e1 = '3' and pm4.g1.e2 = 2) or pm4.g1.e4 = 2.0"; //$NON-NLS-1$
        
        helpTestEstimateCost(critString, NewCalculateCostUtil.UNKNOWN_VALUE, NewCalculateCostUtil.UNKNOWN_VALUE, metadata);
    } 

    /** 
     * BOA cases 2159 and 2160, defect 14998
     * 
     * e1 and e2 make up a single compound key - this criteria covers that
     * key so the cost should be low
     */
    public void testEstimateCostOfCriteriaCompoundKey7() throws Exception {
        QueryMetadataInterface metadata = FakeMetadataFactory.example4();
        String critString = "(pm4.g1.e1 = '3' and pm4.g1.e2 = 2) and pm4.g1.e4 = 2.0"; //$NON-NLS-1$
        
        helpTestEstimateCost(critString, NewCalculateCostUtil.UNKNOWN_VALUE, 1, metadata);
    }     

    /** 
     * BOA cases 2159 and 2160, defect 14998
     * 
     * e1 and e2 make up a single compound key - this criteria covers that
     * key so the cost should be low
     */
    public void testEstimateCostOfCriteriaCompoundKey8() throws Exception {
        QueryMetadataInterface metadata = FakeMetadataFactory.example4();
        String critString = "pm4.g1.e1 LIKE '3' and pm4.g1.e2 = 2"; //$NON-NLS-1$
        
        helpTestEstimateCost(critString, NewCalculateCostUtil.UNKNOWN_VALUE, 1, metadata);
    }    

    /** 
     * BOA cases 2159 and 2160, defect 14998
     * 
     * e1 and e2 make up a single compound key - this criteria does not
     * lower the cost due to the NOT
     */
    public void testEstimateCostOfCriteriaCompoundKey9() throws Exception {
        QueryMetadataInterface metadata = FakeMetadataFactory.example4();
        String critString = "pm4.g1.e1 NOT LIKE '3' and pm4.g1.e2 = 2"; //$NON-NLS-1$
        
        helpTestEstimateCost(critString, NewCalculateCostUtil.UNKNOWN_VALUE, NewCalculateCostUtil.UNKNOWN_VALUE, metadata);
    }    

    /** 
     * BOA cases 2159 and 2160, defect 14998
     * 
     * e1 and e2 make up a single compound key - this criteria covers that
     * key so the cost should be low
     */
    public void testEstimateCostOfCriteriaCompoundKey10() throws Exception {
        QueryMetadataInterface metadata = FakeMetadataFactory.example4();
        String critString = "'3' LIKE pm4.g1.e1 and pm4.g1.e2 = 2"; //$NON-NLS-1$
        
        helpTestEstimateCost(critString, NewCalculateCostUtil.UNKNOWN_VALUE, 1, metadata);
    }      

    /** 
     * BOA cases 2159 and 2160, defect 14998
     * 
     * e1 and e2 make up a single compound key - this criteria covers that
     * key so the cost should be low
     */
    public void testEstimateCostOfCriteriaCompoundKey11() throws Exception {
        QueryMetadataInterface metadata = FakeMetadataFactory.example4();
        String critString = "pm4.g1.e1 IS NULL and pm4.g1.e2 = 2"; //$NON-NLS-1$
        
        helpTestEstimateCost(critString, NewCalculateCostUtil.UNKNOWN_VALUE, 1, metadata);
    }    

    /** 
     * BOA cases 2159 and 2160, defect 14998
     * 
     * e1 and e2 make up a single compound key - this criteria does not
     * lower the cost due to the NOT
     */
    public void testEstimateCostOfCriteriaCompoundKey12() throws Exception {
        QueryMetadataInterface metadata = FakeMetadataFactory.example4();
        String critString = "pm4.g1.e1 IS NOT NULL and pm4.g1.e2 = 2"; //$NON-NLS-1$
        
        helpTestEstimateCost(critString, NewCalculateCostUtil.UNKNOWN_VALUE, NewCalculateCostUtil.UNKNOWN_VALUE, metadata);
    }      

    /** 
     * BOA cases 2159 and 2160, defect 14998
     * 
     * e1 and e2 make up a single compound key - this criteria covers that
     * key so the cost should be low
     */
    public void testEstimateCostOfCriteriaCompoundKey13() throws Exception {
        QueryMetadataInterface metadata = FakeMetadataFactory.example4();
        String critString = "pm4.g1.e1 IN ('3', '4') and pm4.g1.e2 = 2"; //$NON-NLS-1$
        
        helpTestEstimateCost(critString, NewCalculateCostUtil.UNKNOWN_VALUE, 1, metadata);
    }     

    /** 
     * BOA cases 2159 and 2160, defect 14998
     * 
     * e1 and e2 make up a single compound key - this criteria does not
     * lower the cost due to the NOT
     */
    public void testEstimateCostOfCriteriaCompoundKey14() throws Exception {
        QueryMetadataInterface metadata = FakeMetadataFactory.example4();
        String critString = "pm4.g1.e1 NOT IN ('3', '4') and pm4.g1.e2 = 2"; //$NON-NLS-1$
        
        helpTestEstimateCost(critString, NewCalculateCostUtil.UNKNOWN_VALUE, NewCalculateCostUtil.UNKNOWN_VALUE, metadata);
    }    
    
    /**
     *  usesKey true
     */
    public void testEstimateCostOfCriteriaMultiGroup() throws Exception {
        QueryMetadataInterface metadata = FakeMetadataFactory.example4();
        String critString = "pm4.g1.e1 = pm1.g1.e1"; //$NON-NLS-1$
        
        helpTestEstimateCost(critString, NewCalculateCostUtil.UNKNOWN_VALUE, NewCalculateCostUtil.UNKNOWN_VALUE, metadata);
    }
    
    /**
     *  usesKey false
     */
    public void testEstimateCostOfCriteriaMultiGroup1() throws Exception {
        QueryMetadataInterface metadata = FakeMetadataFactory.example4();
        String critString = "pm2.g3.e1 = pm4.g1.e1"; //$NON-NLS-1$
        
        helpTestEstimateCost(critString, NewCalculateCostUtil.UNKNOWN_VALUE, NewCalculateCostUtil.UNKNOWN_VALUE, metadata);
    }
    
    /**
     *  usesKey true
     */
    public void testEstimateCostOfCriteriaMultiGroup2() throws Exception {
        QueryMetadataInterface metadata = FakeMetadataFactory.example4();
        String critString = "pm4.g1.e1 = pm1.g1.e1"; //$NON-NLS-1$
        
        helpTestEstimateCost(critString, 100, 10, metadata);
    }
    
    /**
     *  usesKey false
     */
    public void testEstimateCostOfCriteriaMultiGroup3() throws Exception {
        QueryMetadataInterface metadata = FakeMetadataFactory.example4();
        String critString = "pm2.g3.e1 = pm4.g1.e1"; //$NON-NLS-1$
        
        helpTestEstimateCost(critString, 100, 33, metadata);
    }
    
    /**
     *  Date Criteria - Case using valid max and min date strings.  In the case of date,
     *  the valid strings are timestamp format - since that is what our costing sets them as.
     */
    public void testEstimateCostOfCriteriaDate1() throws Exception {
        QueryMetadataInterface metadata = FakeMetadataFactory.example1();
        FakeMetadataObject e2 = (FakeMetadataObject)metadata.getElementID("pm3.g1.e2"); //$NON-NLS-1$
        e2.putProperty(FakeMetadataObject.Props.MIN_VALUE,"2007-04-03 12:12:12.10"); //$NON-NLS-1$
        e2.putProperty(FakeMetadataObject.Props.MAX_VALUE,"2007-06-03 12:12:12.10"); //$NON-NLS-1$
        String critString = "pm3.g1.e2 <= {d'2008-04-03'}"; //$NON-NLS-1$
        
        helpTestEstimateCost(critString, 100, 100, metadata);
    }

    /**
     *  Date Criteria - Case using invalid max and min date strings.  In the case of date,
     *  one example of invalid strings is date format - since our costing sets them to timestamp.
     */
    public void testEstimateCostOfCriteriaDate2() throws Exception {
        QueryMetadataInterface metadata = FakeMetadataFactory.example1();
        FakeMetadataObject e2 = (FakeMetadataObject)metadata.getElementID("pm3.g1.e2"); //$NON-NLS-1$
        e2.putProperty(FakeMetadataObject.Props.MIN_VALUE,"2007-04-03"); //$NON-NLS-1$
        e2.putProperty(FakeMetadataObject.Props.MAX_VALUE,"2007-06-03"); //$NON-NLS-1$
        String critString = "pm3.g1.e2 <= {d'2008-04-03'}"; //$NON-NLS-1$
        
        helpTestEstimateCost(critString, 100, 33, metadata);
    }

    /**
     *  Time Criteria - case using valid max and min time strings.
     */
    public void testEstimateCostOfCriteriaTime1() throws Exception {
        QueryMetadataInterface metadata = FakeMetadataFactory.example1();
        FakeMetadataObject e3 = (FakeMetadataObject)metadata.getElementID("pm3.g1.e3"); //$NON-NLS-1$
        e3.putProperty(FakeMetadataObject.Props.MIN_VALUE,"12:12:12"); //$NON-NLS-1$
        e3.putProperty(FakeMetadataObject.Props.MAX_VALUE,"12:13:14"); //$NON-NLS-1$
        String critString = "pm3.g1.e3 <= {t'11:11:11'}"; //$NON-NLS-1$
        
        helpTestEstimateCost(critString, 100, 1, metadata);
    }

    /**
     *  Time Criteria - case using invalid max and min time strings
     */
    public void testEstimateCostOfCriteriaTime2() throws Exception {
        QueryMetadataInterface metadata = FakeMetadataFactory.example1();
        FakeMetadataObject e3 = (FakeMetadataObject)metadata.getElementID("pm3.g1.e3"); //$NON-NLS-1$
        e3.putProperty(FakeMetadataObject.Props.MIN_VALUE,"2007-04-03 12:12:12.10"); //$NON-NLS-1$
        e3.putProperty(FakeMetadataObject.Props.MAX_VALUE,"2007-06-03 12:12:12.10"); //$NON-NLS-1$
        String critString = "pm3.g1.e3 <= {t'11:11:11'}"; //$NON-NLS-1$
        
        helpTestEstimateCost(critString, 100, 33, metadata);
    }
    
    /**
     *  Timestamp Criteria - case using valid max and min timestamp strings
     */
    public void testEstimateCostOfCriteriaTimestamp1() throws Exception {
        QueryMetadataInterface metadata = FakeMetadataFactory.example1();
        FakeMetadataObject e4 = (FakeMetadataObject)metadata.getElementID("pm3.g1.e4"); //$NON-NLS-1$
        e4.putProperty(FakeMetadataObject.Props.MIN_VALUE,"2007-04-03 12:12:12.10"); //$NON-NLS-1$
        e4.putProperty(FakeMetadataObject.Props.MAX_VALUE,"2007-04-03 12:12:12.10"); //$NON-NLS-1$
        String critString = "pm3.g1.e4 <= {ts'2007-04-03 12:12:12.10'}"; //$NON-NLS-1$
        
        helpTestEstimateCost(critString, 100, 1, metadata);
    }

    /**
     *  Timestamp Criteria - case using invalid max and min timestamp strings
     */
    public void testEstimateCostOfCriteriaTimestamp2() throws Exception {
        QueryMetadataInterface metadata = FakeMetadataFactory.example1();
        FakeMetadataObject e4 = (FakeMetadataObject)metadata.getElementID("pm3.g1.e4"); //$NON-NLS-1$
        e4.putProperty(FakeMetadataObject.Props.MIN_VALUE,"2007-04-03"); //$NON-NLS-1$
        e4.putProperty(FakeMetadataObject.Props.MAX_VALUE,"2007-06-03"); //$NON-NLS-1$
        String critString = "pm3.g1.e4 <= {ts'2007-04-03 12:12:12.10'}"; //$NON-NLS-1$
        
        helpTestEstimateCost(critString, 100, 33, metadata);
    }

    public void testNDVEstimate() throws Exception {
        String crit = "US.accounts.account = 10"; //$NON-NLS-1$
        
        helpTestEstimateCost(crit, 1000, 800, TestVirtualDepJoin.exampleVirtualDepJoin());
    }
    
    public void testNDVEstimate1() throws Exception {
        String crit = "US.accounts.account = US.accounts.customer"; //$NON-NLS-1$
        
        helpTestEstimateCost(crit, 1000, 800, TestVirtualDepJoin.exampleVirtualDepJoin());
    }
    
    public void testCompoundCriteriaEstimate() throws Exception {
        String crit = "US.accounts.account = 10 and US.accounts.account = US.accounts.customer"; //$NON-NLS-1$
        
        helpTestEstimateCost(crit, 1000, 640, TestVirtualDepJoin.exampleVirtualDepJoin());
    }

    public void testCompoundCriteriaEstimate1() throws Exception {
        String crit = "US.accounts.account = 10 or US.accounts.account = US.accounts.customer"; //$NON-NLS-1$
        
        helpTestEstimateCost(crit, 1000, 1000, TestVirtualDepJoin.exampleVirtualDepJoin());
    }
    
    public void testNNVEstimate() throws Exception {
        String crit = "US.accounts.account is null"; //$NON-NLS-1$
        
        helpTestEstimateCost(crit, 1000, 1, TestVirtualDepJoin.exampleVirtualDepJoin());
    }
    
    public void testNNVEstimate1() throws Exception {
        String crit = "US.accounts.account is null"; //$NON-NLS-1$
        
        helpTestEstimateCost(crit, NewCalculateCostUtil.UNKNOWN_VALUE, 1, TestVirtualDepJoin.exampleVirtualDepJoin());
    }
    
    public void testCompoundCriteriaEstimate2() throws Exception {
        String crit = "US.accounts.account is null and US.accounts.account = US.accounts.customer"; //$NON-NLS-1$
        
        helpTestEstimateCost(crit, 1000, 1, TestVirtualDepJoin.exampleVirtualDepJoin());
    }
    
    public void testCompoundCriteriaEstimate3() throws Exception {
        String crit = "US.accounts.account is null or US.accounts.account = US.accounts.customer"; //$NON-NLS-1$
        
        helpTestEstimateCost(crit, 1000, 801, TestVirtualDepJoin.exampleVirtualDepJoin());
    }
    
    //ensures that the ordering of criteria does not effect the costing calculation
    public void testCompoundCriteriaEstimate4() throws Exception {
        String crit = "US.accounts.account = 10 and US.accounts.account = US.accounts.customer and US.accounts.account < 100"; //$NON-NLS-1$
        
        helpTestEstimateCost(crit, 1000, 213, TestVirtualDepJoin.exampleVirtualDepJoin());
        
        String crit1 = "US.accounts.account = US.accounts.customer and US.accounts.account < 100 and US.accounts.account = 10"; //$NON-NLS-1$
        
        helpTestEstimateCost(crit1, 1000, 213, TestVirtualDepJoin.exampleVirtualDepJoin());
    }
    
    public void testCompoundCriteriaEstimate5() throws Exception {
        String crit = "US.accounts.account is null and US.accounts.account = US.accounts.customer"; //$NON-NLS-1$
        
        helpTestEstimateCost(crit, NewCalculateCostUtil.UNKNOWN_VALUE, 1, TestVirtualDepJoin.exampleVirtualDepJoin());
    }
    
    public void testCompoundCriteriaEstimate6() throws Exception {
        String crit = "US.accounts.account is null or US.accounts.account = US.accounts.customer"; //$NON-NLS-1$
        
        helpTestEstimateCost(crit, NewCalculateCostUtil.UNKNOWN_VALUE, NewCalculateCostUtil.UNKNOWN_VALUE, TestVirtualDepJoin.exampleVirtualDepJoin());
    }
        
    //min and max are not set, so the default estimate is returned
    public void testRangeEstimate() throws Exception {
        String crit = "US.accounts.account < 100"; //$NON-NLS-1$
        
        helpTestEstimateCost(crit, 1000, 333, TestVirtualDepJoin.exampleVirtualDepJoin());
    }
    
    public void testRangeEstimate1() throws Exception {
        String crit = "US.accounts.customer < 100"; //$NON-NLS-1$
        
        helpTestEstimateCost(crit, 1000, 100, TestVirtualDepJoin.exampleVirtualDepJoin());
    }

    public void testRangeEstimate2() throws Exception {
        String crit = "US.accounts.customer > 100"; //$NON-NLS-1$
        
        helpTestEstimateCost(crit, 1000, 900, TestVirtualDepJoin.exampleVirtualDepJoin());
    }
    
    public void testRangeEstimate3() throws Exception {
        String crit = "US.accounts.customer >= 1600"; //$NON-NLS-1$
        
        helpTestEstimateCost(crit, 1000, 1, TestVirtualDepJoin.exampleVirtualDepJoin());
    }
    
    public void testRangeEstimate4() throws Exception {
        String crit = "US.accounts.customer < -1"; //$NON-NLS-1$
        
        helpTestEstimateCost(crit, 1000, 1, TestVirtualDepJoin.exampleVirtualDepJoin());
    }
    
    public void testRangeEstimate5() throws Exception {
        String crit = "US.accounts.customer >= -1"; //$NON-NLS-1$
        
        helpTestEstimateCost(crit, 1000, 1000, TestVirtualDepJoin.exampleVirtualDepJoin());
    }
    
    public void testRangeEstimate6() throws Exception {
        String crit = "US.accounts.pennies >= -2"; //$NON-NLS-1$
        
        helpTestEstimateCost(crit, 1000, 1000, TestVirtualDepJoin.exampleVirtualDepJoin());
    }
    
    public void testRangeEstimate7() throws Exception {
        String crit = "US.accounts.pennies >= -6"; //$NON-NLS-1$
        
        helpTestEstimateCost(crit, 1000, 800, TestVirtualDepJoin.exampleVirtualDepJoin());
    }
    
    public void testLimitWithUnknownChildCardinality() throws Exception {
        String query = "select e1 from pm1.g1 limit 2"; //$NON-NLS-1$
        
        RelationalPlan plan = (RelationalPlan)TestOptimizer.helpPlan(query, FakeMetadataFactory.example1Cached(), new String[] {"SELECT e1 FROM pm1.g1"}); //$NON-NLS-1$
        
        assertEquals(new Float(2), plan.getRootNode().getEstimateNodeCardinality());
    }

}
