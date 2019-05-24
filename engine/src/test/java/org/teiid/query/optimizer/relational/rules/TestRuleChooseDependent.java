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

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import org.junit.Test;
import org.teiid.api.exception.query.QueryMetadataException;
import org.teiid.api.exception.query.QueryPlannerException;
import org.teiid.core.TeiidComponentException;
import org.teiid.query.analysis.AnalysisRecord;
import org.teiid.query.metadata.QueryMetadataInterface;
import org.teiid.query.optimizer.TestOptimizer;
import org.teiid.query.optimizer.capabilities.FakeCapabilitiesFinder;
import org.teiid.query.optimizer.relational.RuleStack;
import org.teiid.query.optimizer.relational.plantree.NodeConstants;
import org.teiid.query.optimizer.relational.plantree.NodeFactory;
import org.teiid.query.optimizer.relational.plantree.PlanNode;
import org.teiid.query.processor.relational.JoinNode.JoinStrategyType;
import org.teiid.query.resolver.util.ResolverUtil;
import org.teiid.query.rewriter.QueryRewriter;
import org.teiid.query.sql.lang.*;
import org.teiid.query.sql.symbol.Constant;
import org.teiid.query.sql.symbol.ElementSymbol;
import org.teiid.query.sql.symbol.GroupSymbol;
import org.teiid.query.unittest.RealMetadataFactory;
import org.teiid.query.util.CommandContext;

@SuppressWarnings("unchecked")
public class TestRuleChooseDependent {

    /* Make Left Side Dependent */
    private static final int LEFT_SIDE = 1;
    /* Make Right Side Dependent */
    private static final int RIGHT_SIDE = 2;
    /* Make Neither Side Dependent */
    private static final int NEITHER_SIDE = 3;

    private QueryMetadataInterface metadata = RealMetadataFactory.example1Cached();

    // ################################## TEST HELPERS ################################

    public PlanNode createAccessNode(Collection groupSymbols) throws Exception {
        PlanNode accessNode = NodeFactory.getNewNode(NodeConstants.Types.ACCESS);
        PlanNode joinNode = NodeFactory.getNewNode(NodeConstants.Types.JOIN);
        PlanNode sourceNode = NodeFactory.getNewNode(NodeConstants.Types.SOURCE);
        joinNode.setProperty(NodeConstants.Info.JOIN_TYPE, JoinType.JOIN_INNER);
        List crits = new ArrayList();
        crits.add(new CompareCriteria(getElementSymbol(1,1), CompareCriteria.EQ, getElementSymbol(2,1)));
        joinNode.setProperty(NodeConstants.Info.JOIN_CRITERIA, crits);
        joinNode.addFirstChild(accessNode);
        accessNode.addFirstChild(sourceNode);
        Iterator i = groupSymbols.iterator();
        while (i.hasNext()) {
            GroupSymbol gs = (GroupSymbol)i.next();
            accessNode.addGroup(gs);
            sourceNode.addGroup(gs);
        }
        return accessNode;
    }

    public GroupSymbol getVirtualGroup() throws Exception {
        GroupSymbol gs = new GroupSymbol("vm1.g1"); //$NON-NLS-1$
        ResolverUtil.resolveGroup(gs, metadata);
        return gs;
    }

    public GroupSymbol getPhysicalGroup(int num) throws Exception {
        String id = "pm1.g" + num; //$NON-NLS-1$
        GroupSymbol gs = new GroupSymbol(id);
        ResolverUtil.resolveGroup(gs, metadata);
        return gs;
    }

    public GroupSymbol getPhysicalGroup(int modelNum, int num) throws Exception {
        String id = "pm" + modelNum + ".g" + num; //$NON-NLS-1$ //$NON-NLS-2$
        GroupSymbol gs = new GroupSymbol(id);
        ResolverUtil.resolveGroup(gs, metadata);
        return gs;
    }

    public GroupSymbol getPhysicalGroupWithAlias(int num, String alias) throws Exception {
        String id = "pm1.g" + num; //$NON-NLS-1$
        GroupSymbol gs = new GroupSymbol(alias, id);
        ResolverUtil.resolveGroup(gs, metadata);
        return gs;
    }

    public ElementSymbol getElementSymbol(int groupNum, int elementNum) throws Exception {
        String id = "pm1.g" + groupNum + ".e" + elementNum; //$NON-NLS-1$ //$NON-NLS-2$
         ElementSymbol es = new ElementSymbol(id);
         es.setMetadataID(this.metadata.getElementID(id));
         es.setGroupSymbol(getPhysicalGroup(groupNum));
         return es;
    }

    public ElementSymbol getElementSymbol(int modelNum, int groupNum, int elementNum) throws Exception {
        String id = "pm" + modelNum + ".g" + groupNum + ".e" + elementNum; //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
        ElementSymbol es = new ElementSymbol(id);
        es.setMetadataID(this.metadata.getElementID(id));
        es.setGroupSymbol(getPhysicalGroup(modelNum, groupNum));
        return es;
    }

    public ElementSymbol getElementSymbolWithGroupAlias(int groupNum, int elementNum, String alias) throws Exception {
        String id = "pm1.g" + groupNum + ".e" + elementNum; //$NON-NLS-1$ //$NON-NLS-2$
         ElementSymbol es = new ElementSymbol(id);
         es.setMetadataID(this.metadata.getElementID(id));
         es.setGroupSymbol(getPhysicalGroupWithAlias(groupNum, alias));
         return es;
    }

    public Query createBaseQuery() throws Exception {
        Query query = new Query();

        Select select = new Select();
        select.addSymbol(getElementSymbol(1,1));
        query.setSelect(select);

        From from = new From();
        from.addGroup(getPhysicalGroup(1));
        query.setFrom(from);

        return query;
    }

    public void helpTestValidJoin(PlanNode joinNode, PlanNode accessNode, boolean expectedValid) throws QueryMetadataException, TeiidComponentException {
        RuleChooseDependent rule = new RuleChooseDependent();
        RuleChooseJoinStrategy.chooseJoinStrategy(joinNode, metadata);
        boolean isValid = rule.isValidJoin(joinNode, accessNode, AnalysisRecord.createNonRecordingRecord());
        assertEquals("Valid join check is wrong ", expectedValid, isValid);         //$NON-NLS-1$
    }

    /**
     * Tests choosing from two eligible sibling access nodes, and then tests marking
     * the chosen one dependent.  This method sets up a bogus plan tree using a
     * bogus project parent node, an inner join node using the supplied join criteria, and
     * two access nodes using each of the groups and (optional) atomic criteria.  Then
     * this method tests that, if an access node is chosen, it can be marked dependent,
     * and that the chosen one is the one which was expected to be marked dependent.
     * @param atomicRequestGroup1 GroupSymbol to select * from in atomic request 1
     * @param atomicRequestCrit1 optional, may be null
     * @param atomicRequestGroup2 GroupSymbol to select * from in atomic request 2
     * @param atomicRequestCrit2 optional, may be null
     * @param joinCriteria Collection of Criteria to add to join node
     * @param expectedMadeDependent one of the three outcome possibility class constants
     * @throws TeiidComponentException
     * @throws QueryMetadataException
     * @throws QueryPlannerException
     */
    private void helpTestChooseSiblingAndMarkDependent(GroupSymbol atomicRequestGroup1,
                                                        Criteria atomicRequestCrit1,
                                                        GroupSymbol atomicRequestGroup2,
                                                        Criteria atomicRequestCrit2,
                                                        Collection joinCriteria,
                                                        int expectedMadeDependent) throws QueryPlannerException, QueryMetadataException, TeiidComponentException {

        helpTestChooseSiblingAndMarkDependent(atomicRequestGroup1,
        atomicRequestCrit1,
        null,
        null,
        null,
        atomicRequestGroup2,
        atomicRequestCrit2,
        null,
        null,
        null,
        joinCriteria,
        expectedMadeDependent, null, null);
    }

    /**
     * Tests choosing from two eligible sibling access nodes, and then tests marking
     * the chosen one dependent.  This method sets up a bogus plan tree using a
     * bogus project parent node, a join node using the supplied join criteria, and
     * two access nodes using each of the groups and (optional) atomic criteria and
     * join criteria.  Then
     * this method tests that, if an access node is chosen, it is marked dependent,
     * and that the chosen one is the one which was expected to be marked dependent.
     * @param atomicRequestGroup1 GroupSymbol to select from in atomic request 1
     * @param atomicRequestCrit1 optional, may be null
     * @param atomicRequestGroup1a optional, may be null
     * @param atomicRequestCrit1a optional, may be null
     * @param atomicJoinCriteria1 optional, may be null
     * @param atomicRequestGroup2 GroupSymbol to select from in atomic request 2
     * @param atomicRequestCrit2 optional, may be null
     * @param atomicRequestGroup2a optional, may be null
     * @param atomicRequestCrit2a optional, may be null
     * @param atomicJoinCriteria2 optional, may be null
     * @param joinCriteria Collection of Criteria to add to outer join node
     * @param expectedMadeDependent one of the three outcome possibility class constants
     * @throws TeiidComponentException
     * @throws QueryMetadataException
     * @throws QueryPlannerException
     */
    private void helpTestChooseSiblingAndMarkDependent(GroupSymbol atomicRequestGroup1,
                                                        Criteria atomicRequestCrit1,  //optional
                                                        GroupSymbol atomicRequestGroup1a, //optional
                                                        Criteria atomicRequestCrit1a,  //optional
                                                        Collection atomicJoinCriteria1,  //optional
                                                        GroupSymbol atomicRequestGroup2,
                                                        Criteria atomicRequestCrit2,  //optional
                                                        GroupSymbol atomicRequestGroup2a,  //optional
                                                        Criteria atomicRequestCrit2a,  //optional
                                                        Collection atomicJoinCriteria2,  //optional
                                                        Collection joinCriteria,
                                                        int expectedMadeDependent, Number expectedCost1, Number expectedCost2) throws QueryPlannerException, QueryMetadataException, TeiidComponentException {
//EXAMPLE:
//    Project(groups=[])
//      Join(groups=[], props={21=joinCriteria, 23=true, 22=INNER JOIN})
//        Access(groups=[atomicRequestGroup1], props={...})
//          Source(groups=[atomicRequestGroup1])
//        Access(groups=[atomicRequestGroup2, atomicRequestGroup2a], props={...})
//          Join(groups=[atomicRequestGroup2, atomicRequestGroup2a], props={21=[atomicJoinCriteria2], 23=true, 22=INNER JOIN})
//            Select(groups=[atomicRequestGroup2], props={40=atomicRequestCrit2})
//              Source(groups=[atomicRequestGroup2])
//            Source(groups=[atomicRequestGroup2a])

        PlanNode accessNode1 = NodeFactory.getNewNode(NodeConstants.Types.ACCESS);
        accessNode1.addGroup(atomicRequestGroup1);
        if (atomicRequestGroup1a != null){
            accessNode1.addGroup(atomicRequestGroup1a);
        }

        PlanNode accessNode2 = NodeFactory.getNewNode(NodeConstants.Types.ACCESS);
        accessNode2.addGroup(atomicRequestGroup2);
        if (atomicRequestGroup2a != null){
            accessNode2.addGroup(atomicRequestGroup2a);
        }

        PlanNode joinNode = NodeFactory.getNewNode(NodeConstants.Types.JOIN);
        joinNode.setProperty(NodeConstants.Info.JOIN_TYPE, JoinType.JOIN_INNER);
        joinNode.setProperty(NodeConstants.Info.JOIN_CRITERIA, joinCriteria);
        joinNode.setProperty(NodeConstants.Info.JOIN_STRATEGY, JoinStrategyType.NESTED_LOOP);
        joinNode.addLastChild(accessNode1);
        joinNode.addLastChild(accessNode2);

        PlanNode bogusParentNode = NodeFactory.getNewNode(NodeConstants.Types.PROJECT);
        bogusParentNode.addLastChild(joinNode);

        //FIRST (LEFT) BRANCH OF TREE
        PlanNode sourceNode1 = NodeFactory.getNewNode(NodeConstants.Types.SOURCE);
        sourceNode1.addGroup(atomicRequestGroup1);
        if (atomicRequestCrit1 != null){
            PlanNode selectNode1 = NodeFactory.getNewNode(NodeConstants.Types.SELECT);
            selectNode1.setProperty(NodeConstants.Info.SELECT_CRITERIA, atomicRequestCrit1);
            selectNode1.addGroup(atomicRequestGroup1);
            selectNode1.addFirstChild(sourceNode1);
            if (atomicRequestGroup1a != null){
                PlanNode atomicJoinNode1 = NodeFactory.getNewNode(NodeConstants.Types.JOIN);
                if (atomicJoinCriteria1.isEmpty()){
                    atomicJoinNode1.setProperty(NodeConstants.Info.JOIN_TYPE, JoinType.JOIN_CROSS);
                } else {
                    atomicJoinNode1.setProperty(NodeConstants.Info.JOIN_TYPE, JoinType.JOIN_INNER);
                    atomicJoinNode1.setProperty(NodeConstants.Info.JOIN_CRITERIA, atomicJoinCriteria1);
                }
                atomicJoinNode1.addGroup(atomicRequestGroup1);
                atomicJoinNode1.addGroup(atomicRequestGroup1a);
                atomicJoinNode1.addLastChild(selectNode1);

                PlanNode sourceNode1a = NodeFactory.getNewNode(NodeConstants.Types.SOURCE);
                sourceNode1a.addGroup(atomicRequestGroup1a);
                if (atomicRequestCrit1a != null){
                    PlanNode selectNode1a = NodeFactory.getNewNode(NodeConstants.Types.SELECT);
                    selectNode1a.setProperty(NodeConstants.Info.SELECT_CRITERIA, atomicRequestCrit1a);
                    selectNode1a.addGroup(atomicRequestGroup1a);
                    selectNode1a.addFirstChild(sourceNode1a);
                    atomicJoinNode1.addLastChild(selectNode1a);
                } else {
                    atomicJoinNode1.addLastChild(sourceNode1a);
                }
                accessNode1.addLastChild(atomicJoinNode1);
            } else {
                accessNode1.addFirstChild(selectNode1);
            }
        } else {
            accessNode1.addFirstChild(sourceNode1);
        }

        //SECOND (RIGHT) BRANCH OF TREE
        PlanNode sourceNode2 = NodeFactory.getNewNode(NodeConstants.Types.SOURCE);
        sourceNode2.addGroup(atomicRequestGroup2);
        if (atomicRequestCrit2 != null){
            PlanNode selectNode2 = NodeFactory.getNewNode(NodeConstants.Types.SELECT);
            selectNode2.setProperty(NodeConstants.Info.SELECT_CRITERIA, atomicRequestCrit2);
            selectNode2.addGroup(atomicRequestGroup2);
            selectNode2.addFirstChild(sourceNode2);
            if (atomicRequestGroup2a != null){
                PlanNode atomicJoinNode2 = NodeFactory.getNewNode(NodeConstants.Types.JOIN);
                if (atomicJoinCriteria2.isEmpty()){
                    atomicJoinNode2.setProperty(NodeConstants.Info.JOIN_TYPE, JoinType.JOIN_CROSS);
                } else {
                    atomicJoinNode2.setProperty(NodeConstants.Info.JOIN_TYPE, JoinType.JOIN_INNER);
                    atomicJoinNode2.setProperty(NodeConstants.Info.JOIN_CRITERIA, atomicJoinCriteria2);
                }
                atomicJoinNode2.addGroup(atomicRequestGroup2);
                atomicJoinNode2.addGroup(atomicRequestGroup2a);
                atomicJoinNode2.addLastChild(selectNode2);

                PlanNode sourceNode2a = NodeFactory.getNewNode(NodeConstants.Types.SOURCE);
                sourceNode2a.addGroup(atomicRequestGroup2a);
                if (atomicRequestCrit2a != null){
                    PlanNode selectNode2a = NodeFactory.getNewNode(NodeConstants.Types.SELECT);
                    selectNode2a.setProperty(NodeConstants.Info.SELECT_CRITERIA, atomicRequestCrit2a);
                    selectNode2a.addGroup(atomicRequestGroup2a);
                    selectNode2a.addFirstChild(sourceNode2a);
                    atomicJoinNode2.addLastChild(selectNode2a);
                } else {
                    atomicJoinNode2.addLastChild(sourceNode2a);
                }
                accessNode2.addLastChild(atomicJoinNode2);
            } else {
                accessNode2.addFirstChild(selectNode2);
            }
        } else {
            accessNode2.addFirstChild(sourceNode2);
        }

        //Add access pattern(s)
        RulePlaceAccess.addAccessPatternsProperty(accessNode1, metadata);
        RulePlaceAccess.addAccessPatternsProperty(accessNode2, metadata);

        if (DEBUG){
            System.out.println("Before."); //$NON-NLS-1$
            System.out.println(bogusParentNode);
        }

        RuleChooseDependent rule = new RuleChooseDependent();
        RuleChooseJoinStrategy.chooseJoinStrategy(joinNode, metadata);
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        capFinder.addCapabilities("pm1", TestOptimizer.getTypicalCapabilities()); //$NON-NLS-1$
        capFinder.addCapabilities("pm2", TestOptimizer.getTypicalCapabilities()); //$NON-NLS-1$
        capFinder.addCapabilities("pm3", TestOptimizer.getTypicalCapabilities()); //$NON-NLS-1$
        capFinder.addCapabilities("pm4", TestOptimizer.getTypicalCapabilities()); //$NON-NLS-1$

        rule.execute(bogusParentNode, metadata, capFinder, new RuleStack(), null, new CommandContext());

        if (DEBUG){
            System.out.println("Done."); //$NON-NLS-1$
            System.out.println(bogusParentNode);
        }

        Object prop1 = joinNode.getProperty(NodeConstants.Info.DEPENDENT_VALUE_SOURCE);

        if (expectedMadeDependent == LEFT_SIDE){
            assertNotNull("Expected one side to be made dependent", prop1); //$NON-NLS-1$
            assertEquals(accessNode1, FrameUtil.findJoinSourceNode(joinNode.getLastChild()));
        } else if (expectedMadeDependent == RIGHT_SIDE){
            assertNotNull("Expected one side to be made dependent", prop1); //$NON-NLS-1$
            assertEquals(accessNode2, FrameUtil.findJoinSourceNode(joinNode.getLastChild()));
        } else if (expectedMadeDependent == NEITHER_SIDE){
            assertNull("Neither side should be dependent", prop1); //$NON-NLS-1$
        } else {
            fail("Invalid test constant " + expectedMadeDependent); //$NON-NLS-1$
        }

        Float cost1 = (Float)accessNode1.getProperty(NodeConstants.Info.EST_CARDINALITY);
        Float cost2 = (Float)accessNode2.getProperty(NodeConstants.Info.EST_CARDINALITY);
        assertNotNull(cost2);
        assertNotNull(cost1);
        if (expectedCost1 != null) {
            assertEquals(expectedCost1.longValue(), cost1.longValue());
            assertEquals(expectedCost2.longValue(), cost2.longValue());
        }
    }

    // ################################## ACTUAL TESTS ################################

    @Test public void testValidJoin1() throws Exception {
        PlanNode accessNode = NodeFactory.getNewNode(NodeConstants.Types.ACCESS);
        accessNode.addGroup(getPhysicalGroup(1));

        PlanNode joinNode = NodeFactory.getNewNode(NodeConstants.Types.JOIN);
        joinNode.setProperty(NodeConstants.Info.JOIN_TYPE, JoinType.JOIN_CROSS);
        joinNode.addFirstChild(accessNode);

        helpTestValidJoin(joinNode, accessNode, false);
    }

    @Test public void testValidJoin2() throws Exception {
        PlanNode accessNode = NodeFactory.getNewNode(NodeConstants.Types.ACCESS);
        accessNode.addGroup(getPhysicalGroup(1));

        PlanNode joinNode = NodeFactory.getNewNode(NodeConstants.Types.JOIN);
        joinNode.setProperty(NodeConstants.Info.JOIN_TYPE, JoinType.JOIN_FULL_OUTER);
        joinNode.setProperty(NodeConstants.Info.JOIN_CRITERIA, Arrays.asList(QueryRewriter.FALSE_CRITERIA));
        joinNode.addFirstChild(accessNode);

        helpTestValidJoin(joinNode, accessNode, false);
    }

    @Test public void testValidJoin3() throws Exception {
        PlanNode accessNode1 = NodeFactory.getNewNode(NodeConstants.Types.ACCESS);
        PlanNode accessNode2 = NodeFactory.getNewNode(NodeConstants.Types.ACCESS);
        accessNode1.addGroup(getPhysicalGroup(1));
        accessNode2.addGroup(getPhysicalGroup(2));

        PlanNode joinNode = NodeFactory.getNewNode(NodeConstants.Types.JOIN);
        joinNode.setProperty(NodeConstants.Info.JOIN_TYPE, JoinType.JOIN_RIGHT_OUTER);
        List crits = new ArrayList();
        crits.add(new CompareCriteria(getElementSymbol(1,1), CompareCriteria.EQ, getElementSymbol(2,1)));
        joinNode.setProperty(NodeConstants.Info.JOIN_CRITERIA, crits);
        joinNode.addLastChild(accessNode1);
        joinNode.addLastChild(accessNode2);

        helpTestValidJoin(joinNode, accessNode1, true);
    }

    @Test public void testValidJoin4() throws Exception {
        PlanNode accessNode1 = NodeFactory.getNewNode(NodeConstants.Types.ACCESS);
        PlanNode accessNode2 = NodeFactory.getNewNode(NodeConstants.Types.ACCESS);

        PlanNode joinNode = NodeFactory.getNewNode(NodeConstants.Types.JOIN);
        joinNode.setProperty(NodeConstants.Info.JOIN_TYPE, JoinType.JOIN_RIGHT_OUTER);
        List crits = new ArrayList();
        crits.add(new CompareCriteria(getElementSymbol(1,1), CompareCriteria.EQ, getElementSymbol(2,1)));
        joinNode.setProperty(NodeConstants.Info.JOIN_CRITERIA, crits);
        joinNode.addLastChild(accessNode1);
        joinNode.addLastChild(accessNode2);

        helpTestValidJoin(joinNode, accessNode2, false);
    }

    @Test public void testValidJoin5() throws Exception {
        PlanNode accessNode1 = NodeFactory.getNewNode(NodeConstants.Types.ACCESS);
        PlanNode accessNode2 = NodeFactory.getNewNode(NodeConstants.Types.ACCESS);

        PlanNode joinNode = NodeFactory.getNewNode(NodeConstants.Types.JOIN);
        joinNode.setProperty(NodeConstants.Info.JOIN_TYPE, JoinType.JOIN_LEFT_OUTER);
        List crits = new ArrayList();
        crits.add(new CompareCriteria(getElementSymbol(1,1), CompareCriteria.EQ, getElementSymbol(2,1)));
        joinNode.setProperty(NodeConstants.Info.JOIN_CRITERIA, crits);
        joinNode.addLastChild(accessNode1);
        joinNode.addLastChild(accessNode2);

        helpTestValidJoin(joinNode, accessNode1, false);
    }

    @Test public void testValidJoin6() throws Exception {
        PlanNode accessNode1 = NodeFactory.getNewNode(NodeConstants.Types.ACCESS);
        PlanNode accessNode2 = NodeFactory.getNewNode(NodeConstants.Types.ACCESS);
        accessNode1.addGroup(getPhysicalGroup(1));
        accessNode2.addGroup(getPhysicalGroup(2));

        PlanNode joinNode = NodeFactory.getNewNode(NodeConstants.Types.JOIN);
        joinNode.setProperty(NodeConstants.Info.JOIN_TYPE, JoinType.JOIN_LEFT_OUTER);
        List crits = new ArrayList();
        crits.add(new CompareCriteria(getElementSymbol(1,1), CompareCriteria.EQ, getElementSymbol(2,1)));
        joinNode.setProperty(NodeConstants.Info.JOIN_CRITERIA, crits);
        joinNode.addLastChild(accessNode1);
        joinNode.addLastChild(accessNode2);

        helpTestValidJoin(joinNode, accessNode2, true);
    }

    /**
     * Tests that heuristics will take a primary key in the atomic criteria into account when
     * making a dependent join.
     */
    @Test public void testChooseKey() throws Exception {
        //override default metadata
        this.metadata = RealMetadataFactory.example4();

        GroupSymbol group1 = getPhysicalGroup(2,3);
        GroupSymbol group2 = getPhysicalGroup(3,3);

        //Join criteria
        ElementSymbol group1e1 = getElementSymbol(2,3,1);
        ElementSymbol group2e1 = getElementSymbol(3,3,1);
        CompareCriteria crit = new CompareCriteria(group2e1, CompareCriteria.EQ, group1e1);
        ArrayList crits = new ArrayList(1);
        crits.add(crit);

        Criteria atomicCrit1 = null;
        Criteria atomicCrit2 = new CompareCriteria(group2e1, CompareCriteria.EQ, new Constant(new Integer(5)));
        int expected = LEFT_SIDE;
        helpTestChooseSiblingAndMarkDependent(group1, atomicCrit1, group2, atomicCrit2, crits, expected);
        expected = RIGHT_SIDE;
        helpTestChooseSiblingAndMarkDependent(group2, atomicCrit2, group1, atomicCrit1, crits, expected);
    }

    /**
     * Neither side should be chosen since the left side lacks cardinality information and the right is not strong
     */
    @Test public void testChooseKey2() throws Exception {
        //override default metadata
        this.metadata = RealMetadataFactory.example4();

        GroupSymbol group1 = getPhysicalGroup(2,3); //no key
        GroupSymbol group1a = null;
        GroupSymbol group2 = getPhysicalGroup(3,3); //has key
        GroupSymbol group2a = getPhysicalGroup(3,2); //no key

        ElementSymbol group1e1 = getElementSymbol(2,3,1);
        ElementSymbol group2e1 = getElementSymbol(3,3,1);
        ElementSymbol group2e2 = getElementSymbol(3,3,2);
        ElementSymbol group2ae1 = getElementSymbol(3,2,1);

        //Outer Join criteria
        CompareCriteria crit = new CompareCriteria(group2e1, CompareCriteria.EQ, group1e1);
        ArrayList crits = new ArrayList(1);
        crits.add(crit);

        //atomic select criteria
        Criteria atomicCrit1 = null;
        Criteria atomicCrit1a = null;
        Criteria atomicCrit2 = new CompareCriteria(group2e2, CompareCriteria.EQ, new Constant(new Integer(5)));
        Criteria atomicCrit2a = null;

        //atomic Join criteria 1
        Collection atomicJoinCrits1 = null;

        //atomic Join criteria 2
        CompareCriteria atomicJoinCrit2 = new CompareCriteria(group2ae1, CompareCriteria.EQ, group2e1);
        ArrayList atomicJoinCrits2 = new ArrayList(1);
        atomicJoinCrits2.add(atomicJoinCrit2);

        int expected = NEITHER_SIDE;

        helpTestChooseSiblingAndMarkDependent(
         group1,
         atomicCrit1,
         group1a,
         atomicCrit1a,
         atomicJoinCrits1,
         group2,
         atomicCrit2,
         group2a,
         atomicCrit2a,
         atomicJoinCrits2,
         crits,
         expected, -1, 99598);
    }

    /**
     * Tests that heuristics will take cardinality of a group into account when
     * making a dependent join.
     */
    @Test public void testCardinality() throws Exception {
        //override default metadata
        this.metadata = RealMetadataFactory.example4();

        GroupSymbol group1 = getPhysicalGroup(1,2);
        GroupSymbol group2 = getPhysicalGroup(2,2);

        //Join criteria
        ElementSymbol pm1g2e1 = getElementSymbol(1,2,1);
        ElementSymbol pm2g2e1 = getElementSymbol(2,2,1);
        CompareCriteria crit = new CompareCriteria(pm2g2e1, CompareCriteria.EQ, pm1g2e1);
        ArrayList crits = new ArrayList(2);
        crits.add(crit);
        ElementSymbol pm1g2e2 = getElementSymbol(1,2,2);
        ElementSymbol pm2g2e2 = getElementSymbol(2,2,2);
        CompareCriteria crit2 = new CompareCriteria(pm1g2e2, CompareCriteria.EQ, pm2g2e2);
        crits.add(crit2);

        Criteria atomicCrit1 = null;
        Criteria atomicCrit2 = null;
        int expected = RIGHT_SIDE;
        helpTestChooseSiblingAndMarkDependent(group1, atomicCrit1, group2, atomicCrit2, crits, expected);
        expected = LEFT_SIDE;
        helpTestChooseSiblingAndMarkDependent(group2, atomicCrit2, group1, atomicCrit1, crits, expected);
    }

    /**
     * Tests that heuristics will take cardinality of a group into account when
     * making a dependent join, and that this information supercedes a key
     * in the atomic criteria.
     */
    @Test public void testCardinalityAndKey() throws Exception {
        //override default metadata
        this.metadata = RealMetadataFactory.example4();

        GroupSymbol group1 = getPhysicalGroup(1,2);
        GroupSymbol group2 = getPhysicalGroup(2,2);

        //Join criteria
        ElementSymbol pm1g2e1 = getElementSymbol(1,2,1);
        ElementSymbol pm2g2e1 = getElementSymbol(2,2,1);
        CompareCriteria crit = new CompareCriteria(pm2g2e1, CompareCriteria.EQ, pm1g2e1);
        ArrayList crits = new ArrayList(1);
        crits.add(crit);
        ElementSymbol pm1g2e2 = getElementSymbol(1,2,2);
        ElementSymbol pm2g2e2 = getElementSymbol(2,2,2);
        CompareCriteria crit2 = new CompareCriteria(pm1g2e2, CompareCriteria.EQ, pm2g2e2);
        crits.add(crit2);

        Criteria atomicCrit1 = null;
        Criteria atomicCrit2 = null;
        int expected = RIGHT_SIDE;
        helpTestChooseSiblingAndMarkDependent(group1, atomicCrit1, group2, atomicCrit2, crits, expected);
        expected = LEFT_SIDE;
        helpTestChooseSiblingAndMarkDependent(group2, atomicCrit2, group1, atomicCrit1, crits, expected);
    }

    @Test public void testCardinalityAndKeyNestedLoop() throws Exception {
        //override default metadata
        this.metadata = RealMetadataFactory.example4();

        GroupSymbol group1 = getPhysicalGroup(1,2);
        GroupSymbol group2 = getPhysicalGroup(2,2);

        //Join criteria
        ElementSymbol pm1g2e1 = getElementSymbol(1,2,1);
        ElementSymbol pm2g2e1 = getElementSymbol(2,2,1);
        CompareCriteria crit = new CompareCriteria(pm2g2e1, CompareCriteria.EQ, pm1g2e1);
        ArrayList crits = new ArrayList(1);
        crits.add(crit);
        ElementSymbol pm1g2e2 = getElementSymbol(1,2,2);
        ElementSymbol pm2g2e2 = getElementSymbol(2,2,2);
        CompareCriteria crit2 = new CompareCriteria(pm1g2e2, CompareCriteria.LT, pm2g2e2);
        crits.add(crit2);

        Criteria atomicCrit1 = null;
        Criteria atomicCrit2 = null;
        int expected = RIGHT_SIDE;
        helpTestChooseSiblingAndMarkDependent(group1, atomicCrit1, group2, atomicCrit2, crits, expected);
        expected = LEFT_SIDE;
        helpTestChooseSiblingAndMarkDependent(group2, atomicCrit2, group1, atomicCrit1, crits, expected);
    }

    @Test public void testRejectDependentJoin() throws Exception {
        //override default metadata
        this.metadata = RealMetadataFactory.example4();

        GroupSymbol group1 = getPhysicalGroup(3,1);
        GroupSymbol group2 = getPhysicalGroup(3,2);

        //Join criteria
        ElementSymbol pm3g1e2 = getElementSymbol(3,1,2);
        ElementSymbol pm3g2e2 = getElementSymbol(3,2,2);
        CompareCriteria crit = new CompareCriteria(pm3g1e2, CompareCriteria.EQ, pm3g2e2);
        ArrayList crits = new ArrayList(1);
        crits.add(crit);

        Criteria atomicCrit1 = null;
        Criteria atomicCrit2 = null;
        int expected = NEITHER_SIDE;
        helpTestChooseSiblingAndMarkDependent(group1, atomicCrit1, group2, atomicCrit2, crits, expected);
        expected = NEITHER_SIDE;
        helpTestChooseSiblingAndMarkDependent(group2, atomicCrit2, group1, atomicCrit1, crits, expected);
    }

    /**
     * Tests that join side with larger cardinality will still have a lower
     * cost computed because it has a criteria including a primary key
     */
    @Test public void testCardinalityWithKeyCrit() throws Exception {
        //override default metadata
        this.metadata = RealMetadataFactory.example4();

        GroupSymbol group1 = getPhysicalGroup(1,2);
        ElementSymbol g1e2 = getElementSymbol(1,2,2);

        GroupSymbol group2 = getPhysicalGroup(3,3);
        ElementSymbol g2e1 = getElementSymbol(3,3,1);
        ElementSymbol g2e2 = getElementSymbol(3,3,2);

        //Join criteria
        ArrayList crits = new ArrayList(1);
        CompareCriteria crit2 = new CompareCriteria(g1e2, CompareCriteria.EQ, g2e2);
        crits.add(crit2);

        Criteria atomicCrit1 = null;
        Criteria atomicCrit2 = new CompareCriteria(g2e1, CompareCriteria.EQ, new Constant(new Integer(5)));
        int expected = LEFT_SIDE;
        helpTestChooseSiblingAndMarkDependent(group1, atomicCrit1, group2, atomicCrit2, crits, expected);
        expected = RIGHT_SIDE;
        helpTestChooseSiblingAndMarkDependent(group2, atomicCrit2, group1, atomicCrit1, crits, expected);
    }

    /**
     * Tests that join side with larger cardinality will still have a lower
     * cost computed because it has a criteria including a primary key
     */
    @Test public void testCardinalityWithKeyCompoundCritAND() throws Exception {
        //override default metadata
        this.metadata = RealMetadataFactory.example4();

        GroupSymbol group1 = getPhysicalGroup(1,2);
        ElementSymbol g1e2 = getElementSymbol(1,2,2);

        GroupSymbol group2 = getPhysicalGroup(3,3);
        ElementSymbol g2e1 = getElementSymbol(3,3,1);
        ElementSymbol g2e2 = getElementSymbol(3,3,2);

        //Join criteria
        ArrayList crits = new ArrayList(1);
        crits.add(new CompareCriteria(g1e2, CompareCriteria.EQ, g2e2));

        Criteria atomicCrit1 = null;

        Criteria crit1 = new CompareCriteria(g2e1, CompareCriteria.EQ, new Constant(new Integer(5)));
        Criteria crit2 = new CompareCriteria(g2e1, CompareCriteria.EQ, new Constant(new Integer(7)));
        CompoundCriteria atomicCrit2 = new CompoundCriteria(CompoundCriteria.AND, crit1, crit2);

        int expected = LEFT_SIDE;
        helpTestChooseSiblingAndMarkDependent(group1, atomicCrit1, group2, atomicCrit2, crits, expected);
        expected = RIGHT_SIDE;
        helpTestChooseSiblingAndMarkDependent(group2, atomicCrit2, group1, atomicCrit1, crits, expected);
    }

    /**
     * Tests that join side with larger cardinality will still have a lower
     * cost computed because it has a criteria including a primary key.
     * Defect 8445
     */
    @Test public void testCardinalityWithKeyCompoundCritOR() throws Exception {
        //override default metadata
        this.metadata = RealMetadataFactory.example4();

        GroupSymbol group1 = getPhysicalGroup(1,2);
        ElementSymbol g1e2 = getElementSymbol(1,2,2);

        GroupSymbol group2 = getPhysicalGroup(3,3);
        ElementSymbol g2e1 = getElementSymbol(3,3,1);
        ElementSymbol g2e2 = getElementSymbol(3,3,2);

        //Join criteria
        ArrayList crits = new ArrayList(1);
        crits.add(new CompareCriteria(g1e2, CompareCriteria.EQ, g2e2));

        Criteria atomicCrit1 = null;

        Criteria crit1 = new CompareCriteria(g2e1, CompareCriteria.EQ, new Constant(new Integer(5)));
        Criteria crit2 = new CompareCriteria(g2e1, CompareCriteria.EQ, new Constant(new Integer(7)));
        CompoundCriteria atomicCrit2 = new CompoundCriteria(CompoundCriteria.OR, crit1, crit2);

        int expected = LEFT_SIDE;
        helpTestChooseSiblingAndMarkDependent(group1, atomicCrit1, group2, atomicCrit2, crits, expected);
        expected = RIGHT_SIDE;
        helpTestChooseSiblingAndMarkDependent(group2, atomicCrit2, group1, atomicCrit1, crits, expected);
    }

    /**
     * Tests SetCriteria against a key column in the atomic criteria
     */
    @Test public void testCardinalityWithKeySetCrit() throws Exception {
        //override default metadata
        this.metadata = RealMetadataFactory.example4();

        GroupSymbol group1 = getPhysicalGroup(1,2);
        ElementSymbol g1e2 = getElementSymbol(1,2,2);

        GroupSymbol group2 = getPhysicalGroup(3,3);
        ElementSymbol g2e1 = getElementSymbol(3,3,1);
        ElementSymbol g2e2 = getElementSymbol(3,3,2);

        //Join criteria
        ArrayList crits = new ArrayList(1);
        CompareCriteria crit2 = new CompareCriteria(g1e2, CompareCriteria.EQ, g2e2);
        crits.add(crit2);

        Criteria atomicCrit1 = null;
        Collection values = new LinkedList();
        values.add(new Constant(new Integer(3)));
        values.add(new Constant(new Integer(4)));
        values.add(new Constant(new Integer(5)));
        Criteria atomicCrit2 = new SetCriteria(g2e1, values);
        int expected = LEFT_SIDE;
        helpTestChooseSiblingAndMarkDependent(group1, atomicCrit1, group2, atomicCrit2, crits, expected);
        expected = RIGHT_SIDE;
        helpTestChooseSiblingAndMarkDependent(group2, atomicCrit2, group1, atomicCrit1, crits, expected);
    }

    /**
     * Tests SetCriteria in the atomic criteria
     */
    @Test public void testCardinalityWithKeyMatchCrit() throws Exception {
        //override default metadata
        this.metadata = RealMetadataFactory.example4();

        GroupSymbol group1 = getPhysicalGroup(1,2);
        ElementSymbol g1e2 = getElementSymbol(1,2,2);

        GroupSymbol group2 = getPhysicalGroup(3,3);
        ElementSymbol g2e1 = getElementSymbol(3,3,1);
        ElementSymbol g2e2 = getElementSymbol(3,3,2);

        //Join criteria
        ArrayList crits = new ArrayList(1);
        CompareCriteria crit2 = new CompareCriteria(g1e2, CompareCriteria.EQ, g2e2);
        crits.add(crit2);

        Criteria atomicCrit1 = null;
        Criteria atomicCrit2 = new MatchCriteria(g2e1, new Constant(new String("ab%"))); //$NON-NLS-1$
        int expected = RIGHT_SIDE;
        helpTestChooseSiblingAndMarkDependent(group1, atomicCrit1, group2, atomicCrit2, crits, expected);
        expected = LEFT_SIDE;
        helpTestChooseSiblingAndMarkDependent(group2, atomicCrit2, group1, atomicCrit1, crits, expected);
    }

    /**
     * Tests SetCriteria in the atomic criteria
     */
    @Test public void testCardinalityWithKeyIsNullCrit() throws Exception {
        //override default metadata
        this.metadata = RealMetadataFactory.example4();

        GroupSymbol group1 = getPhysicalGroup(1,2);
        ElementSymbol g1e2 = getElementSymbol(1,2,2);

        GroupSymbol group2 = getPhysicalGroup(3,3);
        ElementSymbol g2e1 = getElementSymbol(3,3,1);
        ElementSymbol g2e2 = getElementSymbol(3,3,2);

        //Join criteria
        ArrayList crits = new ArrayList(1);
        CompareCriteria crit2 = new CompareCriteria(g1e2, CompareCriteria.EQ, g2e2);
        crits.add(crit2);

        Criteria atomicCrit1 = null;
        Criteria atomicCrit2 = new IsNullCriteria(g2e1);
        int expected = LEFT_SIDE;
        helpTestChooseSiblingAndMarkDependent(group1, atomicCrit1, group2, atomicCrit2, crits, expected);
        expected = RIGHT_SIDE;
        helpTestChooseSiblingAndMarkDependent(group2, atomicCrit2, group1, atomicCrit1, crits, expected);
    }

    /**
     * Tests NotCriteria in the atomic criteria
     */
    @Test public void testCardinalityWithKeyNotCrit() throws Exception {
        //override default metadata
        this.metadata = RealMetadataFactory.example4();

        GroupSymbol group1 = getPhysicalGroup(1,2);
        ElementSymbol g1e2 = getElementSymbol(1,2,2);

        GroupSymbol group2 = getPhysicalGroup(3,3);
        ElementSymbol g2e1 = getElementSymbol(3,3,1);
        ElementSymbol g2e2 = getElementSymbol(3,3,2);

        //Join criteria
        ArrayList crits = new ArrayList(1);
        CompareCriteria crit2 = new CompareCriteria(g1e2, CompareCriteria.EQ, g2e2);
        crits.add(crit2);

        Criteria atomicCrit1 = null;
        Criteria atomicCrit2 = new CompareCriteria(g2e1, CompareCriteria.EQ, new Constant(new Integer(5)));
        atomicCrit2 = new NotCriteria(atomicCrit2);
        int expected = RIGHT_SIDE;
        helpTestChooseSiblingAndMarkDependent(group1, atomicCrit1, group2, atomicCrit2, crits, expected);
        expected = LEFT_SIDE;
        helpTestChooseSiblingAndMarkDependent(group2, atomicCrit2, group1, atomicCrit1, crits, expected);
    }

    /**
     * Tests that join side with larger cardinality will still have a lower
     * cost computed because it has a criteria including a primary key
     */
    @Test public void testCardinalityWithKeyComplexCrit() throws Exception {
        //override default metadata
        this.metadata = RealMetadataFactory.example4();

        GroupSymbol group1 = getPhysicalGroup(1,2);
        ElementSymbol g1e2 = getElementSymbol(1,2,2);

        GroupSymbol group2 = getPhysicalGroup(3,3);
        ElementSymbol g2e1 = getElementSymbol(3,3,1);
        ElementSymbol g2e2 = getElementSymbol(3,3,2);

        //Join criteria
        ArrayList crits = new ArrayList(1);
        crits.add(new CompareCriteria(g1e2, CompareCriteria.EQ, g2e2));

        Criteria atomicCrit1 = null;

        Criteria crit1 = new CompareCriteria(g2e1, CompareCriteria.EQ, new Constant(new Integer(5)));
        Criteria crit2 = new CompareCriteria(g2e1, CompareCriteria.EQ, new Constant(new Integer(7)));
        CompoundCriteria atomicCrit2 = new CompoundCriteria(CompoundCriteria.AND, crit1, crit2);
        Criteria crit3 = new MatchCriteria(g2e1, new Constant(new String("ab"))); //$NON-NLS-1$
        atomicCrit2 = new CompoundCriteria(CompoundCriteria.OR, atomicCrit2, crit3);

        int expected = LEFT_SIDE;
        helpTestChooseSiblingAndMarkDependent(group1, atomicCrit1, group2, atomicCrit2, crits, expected);
        expected = RIGHT_SIDE;
        helpTestChooseSiblingAndMarkDependent(group2, atomicCrit2, group1, atomicCrit1, crits, expected);
    }

    @Test public void testCardinalityWithKeyComplexCrit2() throws Exception {
        //override default metadata
        this.metadata = RealMetadataFactory.example4();

        GroupSymbol group1 = getPhysicalGroup(1,2);
        ElementSymbol g1e2 = getElementSymbol(1,2,2);

        GroupSymbol group2 = getPhysicalGroup(3,3);
        ElementSymbol g2e1 = getElementSymbol(3,3,1);
        ElementSymbol g2e2 = getElementSymbol(3,3,2);

        //Join criteria
        ArrayList crits = new ArrayList(1);
        crits.add(new CompareCriteria(g1e2, CompareCriteria.EQ, g2e2));

        Criteria atomicCrit1 = null;

        Criteria crit1 = new CompareCriteria(g2e1, CompareCriteria.GT, new Constant(new Integer(5)));
        Criteria crit2 = new CompareCriteria(g2e1, CompareCriteria.LT, new Constant(new Integer(7)));
        Criteria atomicCrit2 = new CompoundCriteria(CompoundCriteria.AND, crit1, crit2);
        Criteria crit3 = new MatchCriteria(g2e1, new Constant(new String("cd%"))); //$NON-NLS-1$
        atomicCrit2 = new CompoundCriteria(CompoundCriteria.OR, atomicCrit2, crit3);
        atomicCrit2 = new NotCriteria(atomicCrit2);

        int expected = RIGHT_SIDE;
        helpTestChooseSiblingAndMarkDependent(group1, atomicCrit1, group2, atomicCrit2, crits, expected);
        expected = LEFT_SIDE;
        helpTestChooseSiblingAndMarkDependent(group2, atomicCrit2, group1, atomicCrit1, crits, expected);
    }

    @Test public void testCardinalityWithKeyComplexCrit3() throws Exception {
        //override default metadata
        this.metadata = RealMetadataFactory.example4();

        GroupSymbol group1 = getPhysicalGroup(1,2);
        ElementSymbol g1e2 = getElementSymbol(1,2,2);

        GroupSymbol group2 = getPhysicalGroup(3,3);
        ElementSymbol g2e1 = getElementSymbol(3,3,1);
        ElementSymbol g2e2 = getElementSymbol(3,3,2);

        //Join criteria
        ArrayList crits = new ArrayList(1);
        crits.add(new CompareCriteria(g1e2, CompareCriteria.EQ, g2e2));

        Criteria atomicCrit1 = null;

        Criteria crit1 = new CompareCriteria(g2e1, CompareCriteria.GE, new Constant(new Integer(5)));
        Criteria crit2 = new CompareCriteria(g2e1, CompareCriteria.NE, new Constant(new Integer(7)));
        Criteria atomicCrit2 = new CompoundCriteria(CompoundCriteria.OR, crit1, crit2);
        atomicCrit2 = new NotCriteria(atomicCrit2);
        Criteria crit3 = new CompareCriteria(g2e1, CompareCriteria.LE, new Constant(new Integer(25)));
        atomicCrit2 = new CompoundCriteria(CompoundCriteria.AND, atomicCrit2, crit3);

        int expected = LEFT_SIDE;
        helpTestChooseSiblingAndMarkDependent(group1, atomicCrit1, group2, atomicCrit2, crits, expected);
        expected = RIGHT_SIDE;
        helpTestChooseSiblingAndMarkDependent(group2, atomicCrit2, group1, atomicCrit1, crits, expected);
    }

    /**
     * Tests that join side with larger cardinality and non-key criteria
     * will be made dependent
     */
    @Test public void testCardinalityWithNonKeyCrit() throws Exception {
        //override default metadata
        this.metadata = RealMetadataFactory.example4();

        GroupSymbol group1 = getPhysicalGroup(1,2);
        ElementSymbol g1e1 = getElementSymbol(1,2,1);
//        ElementSymbol g1e2 = getElementSymbol(1,2,2);

        GroupSymbol group2 = getPhysicalGroup(3,3);
        ElementSymbol g2e1 = getElementSymbol(3,3,1);
        ElementSymbol g2e2 = getElementSymbol(3,3,2);

        //Join criteria
        ArrayList crits = new ArrayList(1);
        CompareCriteria crit2 = new CompareCriteria(g1e1, CompareCriteria.EQ, g2e1);
        crits.add(crit2);

        Criteria atomicCrit1 = null;
        Criteria atomicCrit2 = new CompareCriteria(g2e2, CompareCriteria.EQ, new Constant(new Integer(5)));
        int expected = RIGHT_SIDE;
        helpTestChooseSiblingAndMarkDependent(group1, atomicCrit1, group2, atomicCrit2, crits, expected);
        expected = LEFT_SIDE;
        helpTestChooseSiblingAndMarkDependent(group2, atomicCrit2, group1, atomicCrit1, crits, expected);
    }

    /**
     * Tests that join side with larger cardinality will still have a lower
     * cost computed because it has a criteria including a primary key
     */
    @Test public void testCardinalityWithCriteriaAndJoin() throws Exception {
        //override default metadata
        this.metadata = RealMetadataFactory.example4();

        GroupSymbol group1 = getPhysicalGroup(2,2); //no key
        GroupSymbol group1a = null;
        GroupSymbol group2 = getPhysicalGroup(3,3); //has key
        GroupSymbol group2a = getPhysicalGroup(3,1); //has key

        ElementSymbol group1e1 = getElementSymbol(2,2,1);
        ElementSymbol group2e1 = getElementSymbol(3,3,1);
        ElementSymbol group2e2 = getElementSymbol(3,3,2);
        ElementSymbol group2ae1 = getElementSymbol(3,1,1);
        ElementSymbol group2ae2 = getElementSymbol(3,1,2);

        //Outer Join criteria
        CompareCriteria crit = new CompareCriteria(group2e1, CompareCriteria.EQ, group1e1);
        ArrayList crits = new ArrayList(1);
        crits.add(crit);

        //atomic select criteria
        Criteria atomicCrit1 = null;
        Criteria atomicCrit1a = null;
        Criteria atomicCrit2 = new CompareCriteria(group2e1, CompareCriteria.EQ, new Constant(new Integer(5)));
        Criteria atomicCrit2a = new CompareCriteria(group2ae1, CompareCriteria.EQ, new Constant(new Integer(5)));

        //atomic Join criteria 1
        Collection atomicJoinCrits1 = null;

        //atomic Join criteria 2
        CompareCriteria atomicJoinCrit2 = new CompareCriteria(group2ae2, CompareCriteria.EQ, group2e2);
        ArrayList atomicJoinCrits2 = new ArrayList(1);
        atomicJoinCrits2.add(atomicJoinCrit2);

        int expected = LEFT_SIDE;

        helpTestChooseSiblingAndMarkDependent(
         group1,
         atomicCrit1,
         group1a,
         atomicCrit1a,
         atomicJoinCrits1,
         group2,
         atomicCrit2,
         group2a,
         atomicCrit2a,
         atomicJoinCrits2,
         crits,
         expected, 1000, 1);
    }

    @Test public void testCardinalityWithAtomicCrossJoin() throws Exception {
        //override default metadata
        this.metadata = RealMetadataFactory.example4();

        GroupSymbol group1 = getPhysicalGroup(2,2); //no key
        GroupSymbol group1a = null;
        GroupSymbol group2 = getPhysicalGroup(3,3); //has key
        GroupSymbol group2a = getPhysicalGroup(3,1); //has key

        ElementSymbol group1e1 = getElementSymbol(2,2,1);
        ElementSymbol group2e1 = getElementSymbol(3,3,1);

        //Outer Join criteria
        CompareCriteria crit = new CompareCriteria(group2e1, CompareCriteria.EQ, group1e1);
        ArrayList crits = new ArrayList(1);
        crits.add(crit);

        //atomic select criteria
        Criteria atomicCrit1 = null;
        Criteria atomicCrit1a = null;
        Criteria atomicCrit2 = new CompareCriteria(group2e1, CompareCriteria.EQ, new Constant(new Integer(5)));
        Criteria atomicCrit2a = null;

        //atomic Join criteria 1
        Collection atomicJoinCrits1 = null;

        //atomic Join criteria 2
        List atomicJoinCrits2 = Collections.EMPTY_LIST; //INDICATES CROSS JOIN

        int expected = NEITHER_SIDE;

        helpTestChooseSiblingAndMarkDependent(
         group1,
         atomicCrit1,
         group1a,
         atomicCrit1a,
         atomicJoinCrits1,
         group2,
         atomicCrit2,
         group2a,
         atomicCrit2a,
         atomicJoinCrits2,
         crits,
         expected, 1000, 1E5);
    }

    @Test public void testCardinalityWithAtomicCrossJoin2() throws Exception {
        //override default metadata
        this.metadata = RealMetadataFactory.example4();

        GroupSymbol group1 = getPhysicalGroup(2,2); //no key
        GroupSymbol group1a = null;
        GroupSymbol group2 = getPhysicalGroup(3,3); //has key
        GroupSymbol group2a = getPhysicalGroup(3,1); //has key

        ElementSymbol group1e1 = getElementSymbol(2,2,1);
        ElementSymbol group2e1 = getElementSymbol(3,3,1);

        //Outer Join criteria
        CompareCriteria crit = new CompareCriteria(group2e1, CompareCriteria.EQ, group1e1);
        ArrayList crits = new ArrayList(1);
        crits.add(crit);

        //atomic select criteria
        Criteria atomicCrit1 = null;
        Criteria atomicCrit1a = null;
        Criteria atomicCrit2 = new CompareCriteria(group2e1, CompareCriteria.NE, new Constant(new Integer(5)));
        Criteria atomicCrit2a = null;

        //atomic Join criteria 1
        Collection atomicJoinCrits1 = null;

        //atomic Join criteria 2
        List atomicJoinCrits2 = Collections.EMPTY_LIST; //INDICATES CROSS JOIN

        int expected = RIGHT_SIDE;

        helpTestChooseSiblingAndMarkDependent(
         group1,
         atomicCrit1,
         group1a,
         atomicCrit1a,
         atomicJoinCrits1,
         group2,
         atomicCrit2,
         group2a,
         atomicCrit2a,
         atomicJoinCrits2,
         crits,
         expected, 1000, 10000000000L);
    }

    // ################################## TEST SUITE ################################

    private static final boolean DEBUG = false;
}
