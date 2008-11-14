/*
 * JBoss, Home of Professional Open Source.
 * Copyright (C) 2008 Red Hat, Inc.
 * Copyright (C) 2000-2007 MetaMatrix, Inc.
 * Licensed to Red Hat, Inc. under one or more contributor 
 * license agreements.  See the copyright.txt file in the
 * distribution for a full listing of individual contributors.
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

import java.util.ArrayList;
import java.util.List;

import junit.framework.*;

import com.metamatrix.api.exception.MetaMatrixComponentException;
import com.metamatrix.api.exception.MetaMatrixException;
import com.metamatrix.api.exception.query.QueryMetadataException;
import com.metamatrix.query.optimizer.capabilities.*;
import com.metamatrix.query.optimizer.capabilities.SourceCapabilities.Capability;
import com.metamatrix.query.parser.QueryParser;
import com.metamatrix.query.resolver.QueryResolver;
import com.metamatrix.query.sql.ReservedWords;
import com.metamatrix.query.sql.lang.Command;
import com.metamatrix.query.sql.lang.JoinType;
import com.metamatrix.query.sql.lang.SetQuery.Operation;
import com.metamatrix.query.sql.symbol.*;
import com.metamatrix.query.unittest.*;

/**
 */
public class TestCapabilitiesUtil extends TestCase {

    /**
     * Constructor for TestCapabilitiesUtil.
     * @param name
     */
    public TestCapabilitiesUtil(String name) {
        super(name);
    }
    
    public void helpTestSupportsSelfJoin(boolean supportsSelfJoin, boolean supportsGroupAlias, boolean expectedValue) {
        // Set up metadata
        FakeMetadataFacade metadata = FakeMetadataFactory.example1();
        FakeMetadataObject modelID = metadata.getStore().findObject("pm1", FakeMetadataObject.MODEL); //$NON-NLS-1$

        // Set up capabilities
        FakeCapabilitiesFinder finder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities sourceCaps = new BasicSourceCapabilities();
        sourceCaps.setCapabilitySupport(Capability.QUERY_FROM_JOIN_SELFJOIN, supportsSelfJoin); 
        sourceCaps.setCapabilitySupport(Capability.QUERY_FROM_GROUP_ALIAS, supportsGroupAlias); 
        finder.addCapabilities("pm1", sourceCaps); //$NON-NLS-1$

        // Test capabilities util
        try { 
            boolean actual = CapabilitiesUtil.supportsSelfJoins(modelID, metadata, finder);
            assertEquals("Got wrong answer for supports", expectedValue, actual); //$NON-NLS-1$
        } catch(MetaMatrixException e) {
            fail(e.getMessage());
        }        
    }
    
    public void testSupportsSelfJoin1() {
        helpTestSupportsSelfJoin(false, true, false);
    }

    public void testSupportsSelfJoin2() {
        helpTestSupportsSelfJoin(true, false, false);
    }

    public void testSupportsSelfJoin3() {
        helpTestSupportsSelfJoin(true, true, true);
    }

    public void testSupportsSelfJoin4() {        
        // Set up metadata
        FakeMetadataFacade metadata = FakeMetadataFactory.example1();
        FakeMetadataObject modelID = metadata.getStore().findObject("pm1", FakeMetadataObject.MODEL); //$NON-NLS-1$
        
        // Test capabilities util
        try { 
            boolean actual = CapabilitiesUtil.supportsSelfJoins(modelID, metadata, new DefaultCapabilitiesFinder());
            assertEquals("Got wrong answer for supports", false, actual); //$NON-NLS-1$
        } catch(MetaMatrixException e) {
            fail(e.getMessage());
        }        
    }

    
    public void helpTestSupportsOuterJoin(boolean metadataSupportsOuterJoin, boolean capsSupportsOuterJoin, boolean capsSupportsFullOuterJoin, JoinType joinType, boolean expectedValue) throws QueryMetadataException, MetaMatrixComponentException {
        // Set up metadata
        FakeMetadataFacade metadata = FakeMetadataFactory.example1();
        FakeMetadataObject modelID = metadata.getStore().findObject("pm1", FakeMetadataObject.MODEL); //$NON-NLS-1$
        modelID.putProperty(FakeMetadataObject.Props.OUTER_JOIN, new Boolean(metadataSupportsOuterJoin));
        
        // Set up capabilities
        FakeCapabilitiesFinder finder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities sourceCaps = new BasicSourceCapabilities();
        sourceCaps.setCapabilitySupport(Capability.QUERY_FROM_JOIN_OUTER, capsSupportsOuterJoin); 
        sourceCaps.setCapabilitySupport(Capability.QUERY_FROM_JOIN_OUTER_FULL, capsSupportsFullOuterJoin); 
        finder.addCapabilities("pm1", sourceCaps); //$NON-NLS-1$

        // Test capabilities util
        boolean actual = CapabilitiesUtil.supportsOuterJoin(modelID, joinType, metadata, finder);
        assertEquals("Got wrong answer for supports", expectedValue, actual); //$NON-NLS-1$
    }
    
    // Test where capabilities don't support outer joins
    public void testSupportsOuterJoinFail1() throws Exception {        
        helpTestSupportsOuterJoin(true, false, false, JoinType.JOIN_RIGHT_OUTER, false); 
    }

    // Test where capabilities support outer joins but metadata does not
    public void testSupportsOuterJoinFail2() throws Exception {        
        helpTestSupportsOuterJoin(false, true, false, JoinType.JOIN_RIGHT_OUTER, false); 
    }
    
    // Test where capabilities don't support full outer joins 
    public void testSupportsOuterJoinFail3() throws Exception {        
        helpTestSupportsOuterJoin(true, true, false, JoinType.JOIN_FULL_OUTER, false); 
    }

    // Test where capabilities support outer joins 
    public void testSupportsOuterJoin1() throws Exception {        
        helpTestSupportsOuterJoin(true, true, false, JoinType.JOIN_RIGHT_OUTER, true); 
    }

    // Test where capabilities support full outer joins 
    public void testSupportsOuterJoin2() throws Exception {        
        helpTestSupportsOuterJoin(true, true, true, JoinType.JOIN_FULL_OUTER, true); 
    }

    public void helpTestSupportsAggregates(boolean capsSupportsAggregates, boolean supportsFunctionInGroupBy, List groupCols) {
        // Set up metadata
        FakeMetadataFacade metadata = FakeMetadataFactory.example1();
        FakeMetadataObject modelID = metadata.getStore().findObject("pm1", FakeMetadataObject.MODEL); //$NON-NLS-1$
        
        // Set up capabilities
        FakeCapabilitiesFinder finder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities sourceCaps = new BasicSourceCapabilities();
        sourceCaps.setCapabilitySupport(Capability.QUERY_AGGREGATES, capsSupportsAggregates); 
        sourceCaps.setCapabilitySupport(Capability.QUERY_FUNCTIONS_IN_GROUP_BY, supportsFunctionInGroupBy);
        finder.addCapabilities("pm1", sourceCaps); //$NON-NLS-1$

        // Test capabilities util
        try { 
            boolean actual = CapabilitiesUtil.supportsAggregates(groupCols, modelID, metadata, finder);
            assertEquals("Got wrong answer for supports", capsSupportsAggregates, actual); //$NON-NLS-1$
        } catch(MetaMatrixException e) {
            fail(e.getMessage());
        }        
    }
    
    // Test where capabilities supports aggregates
    public void testSupportsAggregates1() {        
        helpTestSupportsAggregates(true, true, null); 
    }
    
    // Test where capabilities don't support aggregates
    public void testSupportsAggregates2() {        
        helpTestSupportsAggregates(false, true, null); 
    }

    // Test where no capabilities exist
    public void testSupportsAggregates3() {        
        // Set up metadata
        FakeMetadataFacade metadata = FakeMetadataFactory.example1();
        FakeMetadataObject modelID = metadata.getStore().findObject("pm1", FakeMetadataObject.MODEL); //$NON-NLS-1$
        
        // Test capabilities util
        try { 
            boolean actual = CapabilitiesUtil.supportsAggregates(null, modelID, metadata, new DefaultCapabilitiesFinder());
            assertEquals("Got wrong answer for supports", false, actual); //$NON-NLS-1$
        } catch(MetaMatrixException e) {
            fail(e.getMessage());
        }        
    }
    
    /**
     * Supports functions in group by is misleading.  It should actually
     * be called supports expression in group by.  Thus the example below
     * is not supported.
     */
    public void testSupportsFunctionInGroupBy() {
        Function f = new Function("concat", new Expression[] { new Constant("a"), new Constant("b") }); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
        ExpressionSymbol expr = new ExpressionSymbol("e", f); //$NON-NLS-1$
        List cols = new ArrayList();
        cols.add(expr);
        helpTestSupportsAggregates(false, false, cols);
    }

    public void helpTestSupportsAggregateFunction(SourceCapabilities caps, AggregateSymbol aggregate, boolean expectedValue) {
        // Set up metadata
        FakeMetadataFacade metadata = FakeMetadataFactory.example1();
        FakeMetadataObject modelID = metadata.getStore().findObject("pm1", FakeMetadataObject.MODEL); //$NON-NLS-1$
        
        // Set up capabilities
        FakeCapabilitiesFinder finder = new FakeCapabilitiesFinder();
        finder.addCapabilities("pm1", caps); //$NON-NLS-1$

        // Test capabilities util
        try { 
            boolean actual = CapabilitiesUtil.supportsAggregateFunction(modelID, aggregate, metadata, finder);
            assertEquals("Got wrong answer for supports", expectedValue, actual); //$NON-NLS-1$
        } catch(MetaMatrixException e) {
            fail(e.getMessage());
        }        
    }
    
    // Test where capabilities don't support aggregate functions
    public void testSupportsAggregate1() {        
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setCapabilitySupport(Capability.QUERY_AGGREGATES, false);
        
        AggregateSymbol aggregate = new AggregateSymbol("expr", ReservedWords.COUNT, false, null); //$NON-NLS-1$
        
        helpTestSupportsAggregateFunction(caps, aggregate, false); 
    }    
    
    // Test where capabilities don't support COUNT
    public void testSupportsAggregate2() {        
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setCapabilitySupport(Capability.QUERY_AGGREGATES, true);
        caps.setCapabilitySupport(Capability.QUERY_AGGREGATES_COUNT, false);
        caps.setCapabilitySupport(Capability.QUERY_AGGREGATES_COUNT_STAR, false);
        
        AggregateSymbol aggregate = new AggregateSymbol("expr", ReservedWords.COUNT, false, null); //$NON-NLS-1$
        
        helpTestSupportsAggregateFunction(caps, aggregate, false); 
    }    

    // Test where capabilities support only COUNT(*)
    public void testSupportsAggregate3() {        
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setCapabilitySupport(Capability.QUERY_AGGREGATES, true);
        caps.setCapabilitySupport(Capability.QUERY_AGGREGATES_COUNT, false);
        caps.setCapabilitySupport(Capability.QUERY_AGGREGATES_COUNT_STAR, true);
        
        AggregateSymbol aggregate = new AggregateSymbol("expr", ReservedWords.COUNT, false, null); //$NON-NLS-1$
        
        helpTestSupportsAggregateFunction(caps, aggregate, true); 
    }    

    // Test where capabilities support only COUNT(*)
    public void testSupportsAggregate4() {        
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setCapabilitySupport(Capability.QUERY_AGGREGATES, true);
        caps.setCapabilitySupport(Capability.QUERY_AGGREGATES_COUNT, false);
        caps.setCapabilitySupport(Capability.QUERY_AGGREGATES_COUNT_STAR, true);
        
        AggregateSymbol aggregate = new AggregateSymbol("expr", ReservedWords.COUNT, false, new ElementSymbol("x")); //$NON-NLS-1$ //$NON-NLS-2$
        
        helpTestSupportsAggregateFunction(caps, aggregate, false); 
    }    

    // Test where capabilities support only COUNT
    public void testSupportsAggregate5() {        
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setCapabilitySupport(Capability.QUERY_AGGREGATES, true);
        caps.setCapabilitySupport(Capability.QUERY_AGGREGATES_COUNT, true);
        caps.setCapabilitySupport(Capability.QUERY_AGGREGATES_COUNT_STAR, false);
        
        AggregateSymbol aggregate = new AggregateSymbol("expr", ReservedWords.COUNT, false, null); //$NON-NLS-1$
        
        helpTestSupportsAggregateFunction(caps, aggregate, false); 
    }    

    // Test where capabilities support only COUNT
    public void testSupportsAggregate6() {        
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setCapabilitySupport(Capability.QUERY_AGGREGATES, true);
        caps.setCapabilitySupport(Capability.QUERY_AGGREGATES_COUNT, true);
        caps.setCapabilitySupport(Capability.QUERY_AGGREGATES_COUNT_STAR, false);
        
        AggregateSymbol aggregate = new AggregateSymbol("expr", ReservedWords.COUNT, false, new ElementSymbol("x")); //$NON-NLS-1$ //$NON-NLS-2$
        
        helpTestSupportsAggregateFunction(caps, aggregate, true); 
    }    

    // Test where capabilities don't support SUM
    public void testSupportsAggregate7() {        
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setCapabilitySupport(Capability.QUERY_AGGREGATES, true);
        caps.setCapabilitySupport(Capability.QUERY_AGGREGATES_SUM, false);
        
        AggregateSymbol aggregate = new AggregateSymbol("expr", ReservedWords.SUM, false, new ElementSymbol("x")); //$NON-NLS-1$ //$NON-NLS-2$
        
        helpTestSupportsAggregateFunction(caps, aggregate, false); 
    }    

    // Test where capabilities support SUM
    public void testSupportsAggregate8() {        
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setCapabilitySupport(Capability.QUERY_AGGREGATES, true);
        caps.setCapabilitySupport(Capability.QUERY_AGGREGATES_SUM, true);
        
        AggregateSymbol aggregate = new AggregateSymbol("expr", ReservedWords.SUM, false, new ElementSymbol("x")); //$NON-NLS-1$ //$NON-NLS-2$
        
        helpTestSupportsAggregateFunction(caps, aggregate, true); 
    }    

    // Test where capabilities don't support AVG
    public void testSupportsAggregate9() {        
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setCapabilitySupport(Capability.QUERY_AGGREGATES, true);
        caps.setCapabilitySupport(Capability.QUERY_AGGREGATES_AVG, false);
        
        AggregateSymbol aggregate = new AggregateSymbol("expr", ReservedWords.AVG, false, new ElementSymbol("x")); //$NON-NLS-1$ //$NON-NLS-2$
        
        helpTestSupportsAggregateFunction(caps, aggregate, false); 
    }    

    // Test where capabilities support AVG
    public void testSupportsAggregate10() {        
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setCapabilitySupport(Capability.QUERY_AGGREGATES, true);
        caps.setCapabilitySupport(Capability.QUERY_AGGREGATES_AVG, true);
        
        AggregateSymbol aggregate = new AggregateSymbol("expr", ReservedWords.AVG, false, new ElementSymbol("x")); //$NON-NLS-1$ //$NON-NLS-2$
        
        helpTestSupportsAggregateFunction(caps, aggregate, true); 
    }    

    // Test where capabilities don't support MIN
    public void testSupportsAggregate11() {        
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setCapabilitySupport(Capability.QUERY_AGGREGATES, true);
        caps.setCapabilitySupport(Capability.QUERY_AGGREGATES_MIN, false);
        
        AggregateSymbol aggregate = new AggregateSymbol("expr", ReservedWords.MIN, false, new ElementSymbol("x")); //$NON-NLS-1$ //$NON-NLS-2$
        
        helpTestSupportsAggregateFunction(caps, aggregate, false); 
    }    

    // Test where capabilities support MIN
    public void testSupportsAggregate12() {        
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setCapabilitySupport(Capability.QUERY_AGGREGATES, true);
        caps.setCapabilitySupport(Capability.QUERY_AGGREGATES_MIN, true);
        
        AggregateSymbol aggregate = new AggregateSymbol("expr", ReservedWords.MIN, false, new ElementSymbol("x")); //$NON-NLS-1$ //$NON-NLS-2$
        
        helpTestSupportsAggregateFunction(caps, aggregate, true); 
    }    

    // Test where capabilities don't support MAX
    public void testSupportsAggregate13() {        
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setCapabilitySupport(Capability.QUERY_AGGREGATES, true);
        caps.setCapabilitySupport(Capability.QUERY_AGGREGATES_MAX, false);
        
        AggregateSymbol aggregate = new AggregateSymbol("expr", ReservedWords.MAX, false, new ElementSymbol("x")); //$NON-NLS-1$ //$NON-NLS-2$
        
        helpTestSupportsAggregateFunction(caps, aggregate, false); 
    }    

    // Test where capabilities support MAX
    public void testSupportsAggregate14() {        
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setCapabilitySupport(Capability.QUERY_AGGREGATES, true);
        caps.setCapabilitySupport(Capability.QUERY_AGGREGATES_MAX, true);
        
        AggregateSymbol aggregate = new AggregateSymbol("expr", ReservedWords.MAX, false, new ElementSymbol("x")); //$NON-NLS-1$ //$NON-NLS-2$
        
        helpTestSupportsAggregateFunction(caps, aggregate, true); 
    }    
    
    // Test where capabilities don't support DISTINCT
    public void testSupportsAggregate15() {        
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setCapabilitySupport(Capability.QUERY_AGGREGATES, true);
        caps.setCapabilitySupport(Capability.QUERY_AGGREGATES_MAX, true);
        caps.setCapabilitySupport(Capability.QUERY_AGGREGATES_DISTINCT, false);
        
        AggregateSymbol aggregate = new AggregateSymbol("expr", ReservedWords.MAX, true, new ElementSymbol("x")); //$NON-NLS-1$ //$NON-NLS-2$
        
        helpTestSupportsAggregateFunction(caps, aggregate, false); 
    }    

    // Test where capabilities support DISTINCT
    public void testSupportsAggregate16() {        
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setCapabilitySupport(Capability.QUERY_AGGREGATES, true);
        caps.setCapabilitySupport(Capability.QUERY_AGGREGATES_MAX, true);
        caps.setCapabilitySupport(Capability.QUERY_AGGREGATES_DISTINCT, true);
        
        AggregateSymbol aggregate = new AggregateSymbol("expr", ReservedWords.MAX, true, new ElementSymbol("x")); //$NON-NLS-1$ //$NON-NLS-2$
        
        helpTestSupportsAggregateFunction(caps, aggregate, true); 
    }    

    public void helpTestSupportsScalar(SourceCapabilities caps, Function function, boolean expectedValue) {
        // Set up metadata
        FakeMetadataFacade metadata = FakeMetadataFactory.example1();
        FakeMetadataObject modelID = metadata.getStore().findObject("pm1", FakeMetadataObject.MODEL); //$NON-NLS-1$
        
        // Set up capabilities
        FakeCapabilitiesFinder finder = new FakeCapabilitiesFinder();
        finder.addCapabilities("pm1", caps); //$NON-NLS-1$

        // Test capabilities util
        try { 
            boolean actual = CapabilitiesUtil.supportsScalarFunction(modelID, function, metadata, finder);
            assertEquals("Got wrong answer for supports", expectedValue, actual); //$NON-NLS-1$
        } catch(MetaMatrixException e) {
            fail(e.getMessage());
        }        
    }

    // Test where capabilities don't support scalar functions
    public void testSupportsScalar1() {        
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setCapabilitySupport(Capability.FUNCTION, false);

        Function func = new Function("+", new Expression[] { new ElementSymbol("x"), new ElementSymbol("y") }); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
        helpTestSupportsScalar(caps, func, false);        
    }    

    // Test where capabilities doesn't support function
    public void testSupportsScalar3() {        
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setCapabilitySupport(Capability.FUNCTION, true);
        caps.setFunctionSupport("now", false); //$NON-NLS-1$

        Function func = new Function("NOW", new Expression[] { }); //$NON-NLS-1$
        helpTestSupportsScalar(caps, func, false);        
    }    

    // Test where capabilities do support function
    public void testSupportsScalar4() {        
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setCapabilitySupport(Capability.FUNCTION, true);
        caps.setFunctionSupport("now", true); //$NON-NLS-1$

        Function func = new Function("NOW", new Expression[] { }); //$NON-NLS-1$
        helpTestSupportsScalar(caps, func, true);        
    }    

    // Test where function is unknown
    public void testSupportsScalar5() {        
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setCapabilitySupport(Capability.FUNCTION, true);

        Function func = new Function("sasquatch", new Expression[] { }); //$NON-NLS-1$
        helpTestSupportsScalar(caps, func, false);        
    }    

    public void testSupportsDistinct1() {        
        // Set up metadata
        FakeMetadataFacade metadata = FakeMetadataFactory.example1();
        FakeMetadataObject modelID = metadata.getStore().findObject("pm1", FakeMetadataObject.MODEL); //$NON-NLS-1$
        modelID.putProperty(FakeMetadataObject.Props.DISTINCT, Boolean.TRUE);

        // Set up capabilities
        FakeCapabilitiesFinder finder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities sourceCaps = new BasicSourceCapabilities();
        sourceCaps.setCapabilitySupport(Capability.QUERY_SELECT_DISTINCT, true); 
        finder.addCapabilities("pm1", sourceCaps); //$NON-NLS-1$

        // Test capabilities util
        try { 
            boolean actual = CapabilitiesUtil.supportsSelectDistinct(modelID, metadata, finder);
            assertEquals("Got wrong answer for supports", true, actual); //$NON-NLS-1$
        } catch(MetaMatrixException e) {
            fail(e.getMessage());
        }        
    }    

    public void testSupportsDistinct2() {        
        // Set up metadata
        FakeMetadataFacade metadata = FakeMetadataFactory.example1();
        FakeMetadataObject modelID = metadata.getStore().findObject("pm1", FakeMetadataObject.MODEL); //$NON-NLS-1$
        modelID.putProperty(FakeMetadataObject.Props.DISTINCT, Boolean.TRUE);

        // Set up capabilities
        FakeCapabilitiesFinder finder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities sourceCaps = new BasicSourceCapabilities();
        sourceCaps.setCapabilitySupport(Capability.QUERY_SELECT_DISTINCT, false); 
        finder.addCapabilities("pm1", sourceCaps); //$NON-NLS-1$

        // Test capabilities util
        try { 
            boolean actual = CapabilitiesUtil.supportsSelectDistinct(modelID, metadata, finder);
            assertEquals("Got wrong answer for supports", false, actual); //$NON-NLS-1$
        } catch(MetaMatrixException e) {
            fail(e.getMessage());
        }        
    }    
    
    public void testSupportsDistinct3() {        
        // Set up metadata
        FakeMetadataFacade metadata = FakeMetadataFactory.example1();
        FakeMetadataObject modelID = metadata.getStore().findObject("pm1", FakeMetadataObject.MODEL); //$NON-NLS-1$
        modelID.putProperty(FakeMetadataObject.Props.DISTINCT, Boolean.FALSE);

        // Set up capabilities
        FakeCapabilitiesFinder finder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities sourceCaps = new BasicSourceCapabilities();
        sourceCaps.setCapabilitySupport(Capability.QUERY_SELECT_DISTINCT, true); 
        finder.addCapabilities("pm1", sourceCaps); //$NON-NLS-1$

        // Test capabilities util
        try { 
            boolean actual = CapabilitiesUtil.supportsSelectDistinct(modelID, metadata, finder);
            assertEquals("Got wrong answer for supports", false, actual); //$NON-NLS-1$
        } catch(MetaMatrixException e) {
            fail(e.getMessage());
        }        
    }    

    public void testSupportsOrderBy1() {        
        // Set up metadata
        FakeMetadataFacade metadata = FakeMetadataFactory.example1();
        FakeMetadataObject modelID = metadata.getStore().findObject("pm1", FakeMetadataObject.MODEL); //$NON-NLS-1$
        modelID.putProperty(FakeMetadataObject.Props.ORDER_BY, Boolean.TRUE);

        // Set up capabilities
        FakeCapabilitiesFinder finder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities sourceCaps = new BasicSourceCapabilities();
        sourceCaps.setCapabilitySupport(Capability.QUERY_ORDERBY, true); 
        finder.addCapabilities("pm1", sourceCaps); //$NON-NLS-1$

        // Test capabilities util
        try { 
            boolean actual = CapabilitiesUtil.supportsOrderBy(modelID, metadata, finder);
            assertEquals("Got wrong answer for supports", true, actual); //$NON-NLS-1$
        } catch(MetaMatrixException e) {
            fail(e.getMessage());
        }        
    }    

    public void testSupportsOrderBy2() {        
        // Set up metadata
        FakeMetadataFacade metadata = FakeMetadataFactory.example1();
        FakeMetadataObject modelID = metadata.getStore().findObject("pm1", FakeMetadataObject.MODEL); //$NON-NLS-1$
        modelID.putProperty(FakeMetadataObject.Props.ORDER_BY, Boolean.TRUE);

        // Set up capabilities
        FakeCapabilitiesFinder finder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities sourceCaps = new BasicSourceCapabilities();
        sourceCaps.setCapabilitySupport(Capability.QUERY_ORDERBY, false); 
        finder.addCapabilities("pm1", sourceCaps); //$NON-NLS-1$

        // Test capabilities util
        try { 
            boolean actual = CapabilitiesUtil.supportsOrderBy(modelID, metadata, finder);
            assertEquals("Got wrong answer for supports", false, actual); //$NON-NLS-1$
        } catch(MetaMatrixException e) {
            fail(e.getMessage());
        }        
    }    
    
    public void testSupportsOrderBy3() {        
        // Set up metadata
        FakeMetadataFacade metadata = FakeMetadataFactory.example1();
        FakeMetadataObject modelID = metadata.getStore().findObject("pm1", FakeMetadataObject.MODEL); //$NON-NLS-1$
        modelID.putProperty(FakeMetadataObject.Props.ORDER_BY, Boolean.FALSE);

        // Set up capabilities
        FakeCapabilitiesFinder finder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities sourceCaps = new BasicSourceCapabilities();
        sourceCaps.setCapabilitySupport(Capability.QUERY_ORDERBY, true); 
        finder.addCapabilities("pm1", sourceCaps); //$NON-NLS-1$

        // Test capabilities util
        try { 
            boolean actual = CapabilitiesUtil.supportsOrderBy(modelID, metadata, finder);
            assertEquals("Got wrong answer for supports", false, actual); //$NON-NLS-1$
        } catch(MetaMatrixException e) {
            fail(e.getMessage());
        }        
    }    

    public void helpTestSupportsScalarSubquery(boolean supportsScalarSubquery, ScalarSubquery subquery, FakeMetadataFacade metadata, boolean expectedValue) {
        // Set up metadata
        FakeMetadataObject modelID = metadata.getStore().findObject("pm1", FakeMetadataObject.MODEL); //$NON-NLS-1$

        // Set up capabilities
        FakeCapabilitiesFinder finder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities sourceCaps = new BasicSourceCapabilities();
        sourceCaps.setCapabilitySupport(Capability.QUERY_SUBQUERIES_SCALAR, supportsScalarSubquery); 
        finder.addCapabilities("pm1", sourceCaps); //$NON-NLS-1$

        // Test capabilities util
        try { 
            boolean actual = CapabilitiesUtil.supportsScalarSubquery(modelID, subquery, metadata, finder);
            assertEquals("Got wrong answer for supports", expectedValue, actual); //$NON-NLS-1$
        } catch(MetaMatrixException e) {
            fail(e.getMessage());
        }        
    }
    
    private ScalarSubquery exampleSubquery1(FakeMetadataFacade metadata) {
        try {
            QueryParser parser = new QueryParser();
            Command command = parser.parseCommand("SELECT e1 FROM pm1.g1");         //$NON-NLS-1$
            QueryResolver.resolveCommand(command, metadata);
            ScalarSubquery ss = new ScalarSubquery(command);
            return ss;
        } catch(MetaMatrixException e) {
            e.printStackTrace();
            fail(e.getMessage());
            return null;
        }
    }

    public void testSupportsScalarSubquery1() {
        FakeMetadataFacade metadata = FakeMetadataFactory.example1();
        ScalarSubquery ss = exampleSubquery1(metadata);
        helpTestSupportsScalarSubquery(false, ss, metadata, false);
    }

    public void testSupportsScalarSubquery2() {
        FakeMetadataFacade metadata = FakeMetadataFactory.example1();
        ScalarSubquery ss = exampleSubquery1(metadata);
        helpTestSupportsScalarSubquery(true, ss, metadata, true);
    }

    public void testSupportsScalarSubquery3() {        
        // Set up metadata
        FakeMetadataFacade metadata = FakeMetadataFactory.example1();
        FakeMetadataObject modelID = metadata.getStore().findObject("pm1", FakeMetadataObject.MODEL); //$NON-NLS-1$
        
        // Test capabilities util
        try { 
            boolean actual = CapabilitiesUtil.supportsScalarSubquery(modelID, exampleSubquery1(metadata), metadata, new DefaultCapabilitiesFinder());
            assertEquals("Got wrong answer for supports", false, actual); //$NON-NLS-1$
        } catch(MetaMatrixException e) {
            fail(e.getMessage());
        }        
    }

    public void helpTtestSupportsUnion(boolean supports) {        
        // Set up metadata
        FakeMetadataFacade metadata = FakeMetadataFactory.example1();
        FakeMetadataObject modelID = metadata.getStore().findObject("pm1", FakeMetadataObject.MODEL); //$NON-NLS-1$
        modelID.putProperty(FakeMetadataObject.Props.ORDER_BY, Boolean.FALSE);

        // Set up capabilities
        FakeCapabilitiesFinder finder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities sourceCaps = new BasicSourceCapabilities();
        sourceCaps.setCapabilitySupport(Capability.QUERY_UNION, supports); 
        finder.addCapabilities("pm1", sourceCaps); //$NON-NLS-1$

        // Test capabilities util
        try { 
            boolean actual = CapabilitiesUtil.supportsSetOp(modelID, Operation.UNION, metadata, finder);
            assertEquals("Got wrong answer for supports", supports, actual); //$NON-NLS-1$
        } catch(MetaMatrixException e) {
            fail(e.getMessage());
        }        
    }    
    
    public void testSupportsUnionTrue() {
        helpTtestSupportsUnion(true);
    }

    public void testSupportsUnionFalse() {
        helpTtestSupportsUnion(false);
    }

    public void helpTtestSupportsLiterals(boolean supports) {        
        // Set up metadata
        FakeMetadataFacade metadata = FakeMetadataFactory.example1();
        FakeMetadataObject modelID = metadata.getStore().findObject("pm1", FakeMetadataObject.MODEL); //$NON-NLS-1$
        modelID.putProperty(FakeMetadataObject.Props.ORDER_BY, Boolean.FALSE);

        // Set up capabilities
        FakeCapabilitiesFinder finder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities sourceCaps = new BasicSourceCapabilities();
        sourceCaps.setCapabilitySupport(Capability.QUERY_SELECT_LITERALS, supports); 
        finder.addCapabilities("pm1", sourceCaps); //$NON-NLS-1$

        // Test capabilities util
        try { 
            boolean actual = CapabilitiesUtil.supportsSelectLiterals(modelID, metadata, finder);
            assertEquals("Got wrong answer for supports", supports, actual); //$NON-NLS-1$
        } catch(MetaMatrixException e) {
            fail(e.getMessage());
        }        
    }    
    
    public void testSupportsLiteralsTrue() {
        helpTtestSupportsLiterals(true);
    }

    public void testSupportsLiteralsFalse() {
        helpTtestSupportsLiterals(false);
    }

    public void helpTtestSupportsCaseExpression(boolean supports, boolean searched) {        
        // Set up metadata
        FakeMetadataFacade metadata = FakeMetadataFactory.example1();
        FakeMetadataObject modelID = metadata.getStore().findObject("pm1", FakeMetadataObject.MODEL); //$NON-NLS-1$
        modelID.putProperty(FakeMetadataObject.Props.ORDER_BY, Boolean.FALSE);

        // Set up capabilities
        FakeCapabilitiesFinder finder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities sourceCaps = new BasicSourceCapabilities();
        if(searched) {
            sourceCaps.setCapabilitySupport(Capability.QUERY_SEARCHED_CASE, supports);            
        } else {
            sourceCaps.setCapabilitySupport(Capability.QUERY_CASE, supports);
        }
        finder.addCapabilities("pm1", sourceCaps); //$NON-NLS-1$

        // Test capabilities util
        try { 
            boolean actual = false;
            if(searched) {
                actual = CapabilitiesUtil.supportsSearchedCaseExpression(modelID, metadata, finder);
            } else {
                actual = CapabilitiesUtil.supportsCaseExpression(modelID, metadata, finder);                
            }
            assertEquals("Got wrong answer for supports", supports, actual); //$NON-NLS-1$
        } catch(MetaMatrixException e) {
            fail(e.getMessage());
        }        
    }    
    
    public void testSupportsCaseTrue() {
        helpTtestSupportsCaseExpression(true, false);
    }

    public void testSupportsCaseFalse() {
        helpTtestSupportsCaseExpression(false, false);
    }

    public void testSupportsSearchedCaseTrue() {
        helpTtestSupportsCaseExpression(true, true);
    }

    public void testSupportsSearchedCaseFalse() {
        helpTtestSupportsCaseExpression(false, true);
    }
    
    private FakeCapabilitiesFinder getFinder(Capability property, boolean supported) {
        // Set up capabilities
        FakeCapabilitiesFinder finder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities sourceCaps = new BasicSourceCapabilities();
        sourceCaps.setCapabilitySupport(property, supported);
        finder.addCapabilities("pm1", sourceCaps); //$NON-NLS-1$
        return finder;
    }

    public void testSupportRowLimit() throws Exception {
        // Set up metadata
        FakeMetadataFacade metadata = FakeMetadataFactory.example1();
        FakeMetadataObject modelID = metadata.getStore().findObject("pm1", FakeMetadataObject.MODEL); //$NON-NLS-1$
        // Set up capabilities
        FakeCapabilitiesFinder finder = getFinder(Capability.ROW_LIMIT, false);
        // Test capabilities util
        assertEquals(false, CapabilitiesUtil.supportsRowLimit(modelID, metadata, finder));
        
        finder = getFinder(Capability.ROW_LIMIT, true);
        // Test capabilities util
        assertEquals(true, CapabilitiesUtil.supportsRowLimit(modelID, metadata, finder));
    }
    
    public void testSupportRowOffset() throws Exception {
        // Set up metadata
        FakeMetadataFacade metadata = FakeMetadataFactory.example1();
        FakeMetadataObject modelID = metadata.getStore().findObject("pm1", FakeMetadataObject.MODEL); //$NON-NLS-1$
        // Set up capabilities
        FakeCapabilitiesFinder finder = getFinder(Capability.ROW_OFFSET, false);
        // Test capabilities util
        assertEquals(false, CapabilitiesUtil.supportsRowOffset(modelID, metadata, finder));
        
        finder = getFinder(Capability.ROW_OFFSET, true);
        // Test capabilities util
        assertEquals(true, CapabilitiesUtil.supportsRowOffset(modelID, metadata, finder));
    }
}
