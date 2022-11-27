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

import java.util.ArrayList;
import java.util.List;

import org.junit.Test;
import org.teiid.api.exception.query.QueryMetadataException;
import org.teiid.api.exception.query.QueryResolverException;
import org.teiid.core.TeiidComponentException;
import org.teiid.language.SQLConstants.NonReserved;
import org.teiid.metadata.Schema;
import org.teiid.query.metadata.TransformationMetadata;
import org.teiid.query.optimizer.capabilities.BasicSourceCapabilities;
import org.teiid.query.optimizer.capabilities.DefaultCapabilitiesFinder;
import org.teiid.query.optimizer.capabilities.FakeCapabilitiesFinder;
import org.teiid.query.optimizer.capabilities.SourceCapabilities;
import org.teiid.query.optimizer.capabilities.SourceCapabilities.Capability;
import org.teiid.query.resolver.util.ResolverVisitor;
import org.teiid.query.sql.lang.JoinType;
import org.teiid.query.sql.lang.SetQuery.Operation;
import org.teiid.query.sql.symbol.AggregateSymbol;
import org.teiid.query.sql.symbol.Constant;
import org.teiid.query.sql.symbol.ElementSymbol;
import org.teiid.query.sql.symbol.Expression;
import org.teiid.query.sql.symbol.ExpressionSymbol;
import org.teiid.query.sql.symbol.Function;
import org.teiid.query.unittest.RealMetadataFactory;


/**
 */
@SuppressWarnings("nls")
public class TestCapabilitiesUtil {

    public void helpTestSupportsSelfJoin(boolean supportsSelfJoin, boolean supportsGroupAlias, boolean expectedValue) throws QueryMetadataException, TeiidComponentException {
        // Set up metadata
        TransformationMetadata metadata = RealMetadataFactory.example1Cached();
        Schema modelID = metadata.getMetadataStore().getSchema("PM1");

        // Set up capabilities
        FakeCapabilitiesFinder finder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities sourceCaps = new BasicSourceCapabilities();
        sourceCaps.setCapabilitySupport(Capability.QUERY_FROM_JOIN_SELFJOIN, supportsSelfJoin);
        sourceCaps.setCapabilitySupport(Capability.QUERY_FROM_GROUP_ALIAS, supportsGroupAlias);
        finder.addCapabilities("pm1", sourceCaps); //$NON-NLS-1$

        // Test capabilities util
        boolean actual = CapabilitiesUtil.supportsSelfJoins(modelID, metadata, finder);
        assertEquals("Got wrong answer for supports", expectedValue, actual); //$NON-NLS-1$
    }

    @Test public void testSupportsSelfJoin1() throws Exception {
        helpTestSupportsSelfJoin(false, true, false);
    }

    @Test public void testSupportsSelfJoin2() throws Exception {
        helpTestSupportsSelfJoin(true, false, false);
    }

    @Test public void testSupportsSelfJoin3() throws Exception {
        helpTestSupportsSelfJoin(true, true, true);
    }

    @Test public void testSupportsSelfJoin4() throws Exception {
        // Set up metadata
        TransformationMetadata metadata = RealMetadataFactory.example1Cached();
        Schema modelID = metadata.getMetadataStore().getSchema("PM1");

        // Test capabilities util
        boolean actual = CapabilitiesUtil.supportsSelfJoins(modelID, metadata, new DefaultCapabilitiesFinder());
        assertEquals("Got wrong answer for supports", false, actual); //$NON-NLS-1$
    }


    public void helpTestSupportsOuterJoin(boolean capsSupportsOuterJoin, boolean capsSupportsFullOuterJoin, JoinType joinType, boolean expectedValue) throws QueryMetadataException, TeiidComponentException {
        // Set up metadata
        TransformationMetadata metadata = RealMetadataFactory.example1Cached();
        Schema modelID = metadata.getMetadataStore().getSchema("PM1");

        // Set up capabilities
        FakeCapabilitiesFinder finder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities sourceCaps = new BasicSourceCapabilities();
        sourceCaps.setCapabilitySupport(Capability.QUERY_FROM_JOIN_OUTER, capsSupportsOuterJoin);
        sourceCaps.setCapabilitySupport(Capability.QUERY_FROM_JOIN_OUTER_FULL, capsSupportsFullOuterJoin);
        finder.addCapabilities("pm1", sourceCaps); //$NON-NLS-1$

        // Test capabilities util
        boolean actual = CapabilitiesUtil.supportsJoin(modelID, joinType, metadata, finder);
        assertEquals("Got wrong answer for supports", expectedValue, actual); //$NON-NLS-1$
    }

    // Test where capabilities don't support outer joins
    @Test public void testSupportsOuterJoinFail1() throws Exception {
        helpTestSupportsOuterJoin(false, false, JoinType.JOIN_RIGHT_OUTER, false);
    }

    // Test where capabilities don't support full outer joins
    @Test public void testSupportsOuterJoinFail3() throws Exception {
        helpTestSupportsOuterJoin(true, false, JoinType.JOIN_FULL_OUTER, false);
    }

    // Test where capabilities support outer joins
    @Test public void testSupportsOuterJoin1() throws Exception {
        helpTestSupportsOuterJoin(true, false, JoinType.JOIN_RIGHT_OUTER, true);
    }

    // Test where capabilities support full outer joins
    @Test public void testSupportsOuterJoin2() throws Exception {
        helpTestSupportsOuterJoin(true, true, JoinType.JOIN_FULL_OUTER, true);
    }

    public void helpTestSupportsAggregates(boolean capsSupportsAggregates, boolean supportsFunctionInGroupBy, List groupCols) throws QueryMetadataException, TeiidComponentException {
        // Set up metadata
        TransformationMetadata metadata = RealMetadataFactory.example1Cached();
        Schema modelID = metadata.getMetadataStore().getSchema("PM1");

        // Set up capabilities
        FakeCapabilitiesFinder finder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities sourceCaps = new BasicSourceCapabilities();
        sourceCaps.setCapabilitySupport(Capability.QUERY_AGGREGATES, capsSupportsAggregates);
        sourceCaps.setCapabilitySupport(Capability.QUERY_FUNCTIONS_IN_GROUP_BY, supportsFunctionInGroupBy);
        finder.addCapabilities("pm1", sourceCaps); //$NON-NLS-1$

        // Test capabilities util
        boolean actual = CapabilitiesUtil.supportsAggregates(groupCols, modelID, metadata, finder);
        assertEquals("Got wrong answer for supports", capsSupportsAggregates, actual); //$NON-NLS-1$
    }

    // Test where capabilities supports aggregates
    @Test public void testSupportsAggregates1() throws Exception {
        helpTestSupportsAggregates(true, true, null);
    }

    /**
     * Supports functions in group by is misleading.  It should actually
     * be called supports expression in group by.  Thus the example below
     * is not supported.
     */
    @Test public void testSupportsFunctionInGroupBy() throws Exception {
        Function f = new Function("concat", new Expression[] { new Constant("a"), new Constant("b") }); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
        ExpressionSymbol expr = new ExpressionSymbol("e", f); //$NON-NLS-1$
        List cols = new ArrayList();
        cols.add(expr);
        helpTestSupportsAggregates(false, false, cols);
    }

    public void helpTestSupportsAggregateFunction(SourceCapabilities caps, AggregateSymbol aggregate, boolean expectedValue) throws QueryMetadataException, TeiidComponentException {
        // Set up metadata
        TransformationMetadata metadata = RealMetadataFactory.example1Cached();
        Schema modelID = metadata.getMetadataStore().getSchema("PM1");

        // Set up capabilities
        FakeCapabilitiesFinder finder = new FakeCapabilitiesFinder();
        finder.addCapabilities("pm1", caps); //$NON-NLS-1$

        // Test capabilities util
        boolean actual = CapabilitiesUtil.supportsAggregateFunction(modelID, aggregate, metadata, finder);
        assertEquals("Got wrong answer for supports", expectedValue, actual); //$NON-NLS-1$
    }

    // Test where capabilities don't support aggregate functions
    @Test public void testSupportsAggregate1() throws Exception {
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setCapabilitySupport(Capability.QUERY_AGGREGATES, false);

        AggregateSymbol aggregate = new AggregateSymbol(NonReserved.COUNT, false, null); //$NON-NLS-1$

        helpTestSupportsAggregateFunction(caps, aggregate, false);
    }

    // Test where capabilities don't support COUNT
    @Test public void testSupportsAggregate2() throws Exception {
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setCapabilitySupport(Capability.QUERY_AGGREGATES, true);
        caps.setCapabilitySupport(Capability.QUERY_AGGREGATES_COUNT, false);
        caps.setCapabilitySupport(Capability.QUERY_AGGREGATES_COUNT_STAR, false);

        AggregateSymbol aggregate = new AggregateSymbol(NonReserved.COUNT, false, null); //$NON-NLS-1$

        helpTestSupportsAggregateFunction(caps, aggregate, false);
    }

    // Test where capabilities support only COUNT(*)
    @Test public void testSupportsAggregate3() throws Exception {
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setCapabilitySupport(Capability.QUERY_AGGREGATES, true);
        caps.setCapabilitySupport(Capability.QUERY_AGGREGATES_COUNT, false);
        caps.setCapabilitySupport(Capability.QUERY_AGGREGATES_COUNT_STAR, true);

        AggregateSymbol aggregate = new AggregateSymbol(NonReserved.COUNT, false, null); //$NON-NLS-1$

        helpTestSupportsAggregateFunction(caps, aggregate, true);
    }

    // Test where capabilities support only COUNT(*)
    @Test public void testSupportsAggregate4() throws Exception {
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setCapabilitySupport(Capability.QUERY_AGGREGATES, true);
        caps.setCapabilitySupport(Capability.QUERY_AGGREGATES_COUNT, false);
        caps.setCapabilitySupport(Capability.QUERY_AGGREGATES_COUNT_STAR, true);

        AggregateSymbol aggregate = new AggregateSymbol(NonReserved.COUNT, false, new ElementSymbol("x")); //$NON-NLS-1$ //$NON-NLS-2$

        helpTestSupportsAggregateFunction(caps, aggregate, false);
    }

    // Test where capabilities support only COUNT
    @Test public void testSupportsAggregate5() throws Exception {
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setCapabilitySupport(Capability.QUERY_AGGREGATES, true);
        caps.setCapabilitySupport(Capability.QUERY_AGGREGATES_COUNT, true);
        caps.setCapabilitySupport(Capability.QUERY_AGGREGATES_COUNT_STAR, false);

        AggregateSymbol aggregate = new AggregateSymbol(NonReserved.COUNT, false, null); //$NON-NLS-1$

        helpTestSupportsAggregateFunction(caps, aggregate, false);
    }

    // Test where capabilities support only COUNT
    @Test public void testSupportsAggregate6() throws Exception {
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setCapabilitySupport(Capability.QUERY_AGGREGATES, true);
        caps.setCapabilitySupport(Capability.QUERY_AGGREGATES_COUNT, true);
        caps.setCapabilitySupport(Capability.QUERY_AGGREGATES_COUNT_STAR, false);

        AggregateSymbol aggregate = new AggregateSymbol(NonReserved.COUNT, false, new ElementSymbol("x")); //$NON-NLS-1$ //$NON-NLS-2$

        helpTestSupportsAggregateFunction(caps, aggregate, true);
    }

    // Test where capabilities don't support SUM
    @Test public void testSupportsAggregate7() throws Exception {
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setCapabilitySupport(Capability.QUERY_AGGREGATES, true);
        caps.setCapabilitySupport(Capability.QUERY_AGGREGATES_SUM, false);

        AggregateSymbol aggregate = new AggregateSymbol(NonReserved.SUM, false, new ElementSymbol("x")); //$NON-NLS-1$ //$NON-NLS-2$

        helpTestSupportsAggregateFunction(caps, aggregate, false);
    }

    // Test where capabilities support SUM
    @Test public void testSupportsAggregate8() throws Exception {
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setCapabilitySupport(Capability.QUERY_AGGREGATES, true);
        caps.setCapabilitySupport(Capability.QUERY_AGGREGATES_SUM, true);

        AggregateSymbol aggregate = new AggregateSymbol(NonReserved.SUM, false, new ElementSymbol("x")); //$NON-NLS-1$ //$NON-NLS-2$

        helpTestSupportsAggregateFunction(caps, aggregate, true);
    }

    // Test where capabilities don't support AVG
    @Test public void testSupportsAggregate9() throws Exception {
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setCapabilitySupport(Capability.QUERY_AGGREGATES, true);
        caps.setCapabilitySupport(Capability.QUERY_AGGREGATES_AVG, false);

        AggregateSymbol aggregate = new AggregateSymbol(NonReserved.AVG, false, new ElementSymbol("x")); //$NON-NLS-1$ //$NON-NLS-2$

        helpTestSupportsAggregateFunction(caps, aggregate, false);
    }

    // Test where capabilities support AVG
    @Test public void testSupportsAggregate10() throws Exception {
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setCapabilitySupport(Capability.QUERY_AGGREGATES, true);
        caps.setCapabilitySupport(Capability.QUERY_AGGREGATES_AVG, true);

        AggregateSymbol aggregate = new AggregateSymbol(NonReserved.AVG, false, new ElementSymbol("x")); //$NON-NLS-1$ //$NON-NLS-2$

        helpTestSupportsAggregateFunction(caps, aggregate, true);
    }

    // Test where capabilities don't support MIN
    @Test public void testSupportsAggregate11() throws Exception {
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setCapabilitySupport(Capability.QUERY_AGGREGATES, true);
        caps.setCapabilitySupport(Capability.QUERY_AGGREGATES_MIN, false);

        AggregateSymbol aggregate = new AggregateSymbol(NonReserved.MIN, false, new ElementSymbol("x")); //$NON-NLS-1$ //$NON-NLS-2$

        helpTestSupportsAggregateFunction(caps, aggregate, false);
    }

    // Test where capabilities support MIN
    @Test public void testSupportsAggregate12() throws Exception {
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setCapabilitySupport(Capability.QUERY_AGGREGATES, true);
        caps.setCapabilitySupport(Capability.QUERY_AGGREGATES_MIN, true);

        AggregateSymbol aggregate = new AggregateSymbol(NonReserved.MIN, false, new ElementSymbol("x")); //$NON-NLS-1$ //$NON-NLS-2$

        helpTestSupportsAggregateFunction(caps, aggregate, true);
    }

    // Test where capabilities don't support MAX
    @Test public void testSupportsAggregate13() throws Exception {
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setCapabilitySupport(Capability.QUERY_AGGREGATES, true);
        caps.setCapabilitySupport(Capability.QUERY_AGGREGATES_MAX, false);

        AggregateSymbol aggregate = new AggregateSymbol(NonReserved.MAX, false, new ElementSymbol("x")); //$NON-NLS-1$ //$NON-NLS-2$

        helpTestSupportsAggregateFunction(caps, aggregate, false);
    }

    // Test where capabilities support MAX
    @Test public void testSupportsAggregate14() throws Exception {
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setCapabilitySupport(Capability.QUERY_AGGREGATES, true);
        caps.setCapabilitySupport(Capability.QUERY_AGGREGATES_MAX, true);

        AggregateSymbol aggregate = new AggregateSymbol(NonReserved.MAX, false, new ElementSymbol("x")); //$NON-NLS-1$ //$NON-NLS-2$

        helpTestSupportsAggregateFunction(caps, aggregate, true);
    }

    // Test where capabilities don't support DISTINCT
    @Test public void testSupportsAggregate15() throws Exception {
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setCapabilitySupport(Capability.QUERY_AGGREGATES, true);
        caps.setCapabilitySupport(Capability.QUERY_AGGREGATES_MAX, true);
        caps.setCapabilitySupport(Capability.QUERY_AGGREGATES_DISTINCT, false);

        AggregateSymbol aggregate = new AggregateSymbol(NonReserved.MAX, true, new ElementSymbol("x")); //$NON-NLS-1$ //$NON-NLS-2$

        helpTestSupportsAggregateFunction(caps, aggregate, false);
    }

    @Test public void testSupportsAggregate16() throws Exception {
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setCapabilitySupport(Capability.QUERY_AGGREGATES, true);
        caps.setCapabilitySupport(Capability.QUERY_AGGREGATES_MAX, true);
        caps.setCapabilitySupport(Capability.QUERY_AGGREGATES_DISTINCT, true);

        AggregateSymbol aggregate = new AggregateSymbol(NonReserved.MAX, true, new ElementSymbol("x")); //$NON-NLS-1$ //$NON-NLS-2$

        helpTestSupportsAggregateFunction(caps, aggregate, true);
    }

    public void helpTestSupportsScalar(SourceCapabilities caps, Function function, boolean expectedValue) throws QueryMetadataException, TeiidComponentException, QueryResolverException {
        // Set up metadata
        TransformationMetadata metadata = RealMetadataFactory.example1Cached();
        Schema modelID = metadata.getMetadataStore().getSchema("PM1");

        // Set up capabilities
        FakeCapabilitiesFinder finder = new FakeCapabilitiesFinder();
        finder.addCapabilities("pm1", caps); //$NON-NLS-1$
        ResolverVisitor.resolveLanguageObject(function, metadata);
        // Test capabilities util
        boolean actual = CapabilitiesUtil.supportsScalarFunction(modelID, function, metadata, finder);
        assertEquals("Got wrong answer for supports", expectedValue, actual); //$NON-NLS-1$
    }

    // Test where capabilities don't support scalar functions
    @Test public void testSupportsScalar1() throws Exception {
        BasicSourceCapabilities caps = new BasicSourceCapabilities();

        Function func = new Function("+", new Expression[] { new Constant(1), new Constant(2) }); //$NON-NLS-1$
        helpTestSupportsScalar(caps, func, false);
    }

    // Test where capabilities doesn't support function
    @Test public void testSupportsScalar3() throws Exception {
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setFunctionSupport("now", false); //$NON-NLS-1$

        Function func = new Function("NOW", new Expression[] { }); //$NON-NLS-1$
        helpTestSupportsScalar(caps, func, false);
    }

    // Test where capabilities do support function
    @Test public void testSupportsScalar4() throws Exception {
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setFunctionSupport("now", true); //$NON-NLS-1$

        Function func = new Function("NOW", new Expression[] { }); //$NON-NLS-1$
        helpTestSupportsScalar(caps, func, true);
    }

    @Test public void testSupportsDistinct1() throws Exception {
        // Set up metadata
        TransformationMetadata metadata = RealMetadataFactory.example1Cached();
        Schema modelID = metadata.getMetadataStore().getSchema("PM1");

        // Set up capabilities
        FakeCapabilitiesFinder finder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities sourceCaps = new BasicSourceCapabilities();
        sourceCaps.setCapabilitySupport(Capability.QUERY_SELECT_DISTINCT, true);
        finder.addCapabilities("pm1", sourceCaps); //$NON-NLS-1$

        assertTrue(CapabilitiesUtil.supportsSelectDistinct(modelID, metadata, finder));
    }

    @Test public void testSupportsDistinct2() throws Exception {
        // Set up metadata
        TransformationMetadata metadata = RealMetadataFactory.example1Cached();
        Schema modelID = metadata.getMetadataStore().getSchema("PM1");

        // Set up capabilities
        FakeCapabilitiesFinder finder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities sourceCaps = new BasicSourceCapabilities();
        sourceCaps.setCapabilitySupport(Capability.QUERY_SELECT_DISTINCT, false);
        finder.addCapabilities("pm1", sourceCaps); //$NON-NLS-1$

        // Test capabilities util
        boolean actual = CapabilitiesUtil.supportsSelectDistinct(modelID, metadata, finder);
        assertEquals("Got wrong answer for supports", false, actual); //$NON-NLS-1$
    }

    @Test public void testSupportsOrderBy1() throws Exception {
        // Set up metadata
        TransformationMetadata metadata = RealMetadataFactory.example1Cached();
        Schema modelID = metadata.getMetadataStore().getSchema("PM1");

        // Set up capabilities
        FakeCapabilitiesFinder finder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities sourceCaps = new BasicSourceCapabilities();
        sourceCaps.setCapabilitySupport(Capability.QUERY_ORDERBY, true);
        finder.addCapabilities("pm1", sourceCaps); //$NON-NLS-1$

        // Test capabilities util
        boolean actual = CapabilitiesUtil.supportsOrderBy(modelID, metadata, finder);
        assertEquals("Got wrong answer for supports", true, actual); //$NON-NLS-1$
    }

    @Test public void testSupportsOrderBy2() throws Exception {
        // Set up metadata
        TransformationMetadata metadata = RealMetadataFactory.example1Cached();
        Schema modelID = metadata.getMetadataStore().getSchema("PM1");

        // Set up capabilities
        FakeCapabilitiesFinder finder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities sourceCaps = new BasicSourceCapabilities();
        sourceCaps.setCapabilitySupport(Capability.QUERY_ORDERBY, false);
        finder.addCapabilities("pm1", sourceCaps); //$NON-NLS-1$

        // Test capabilities util
        boolean actual = CapabilitiesUtil.supportsOrderBy(modelID, metadata, finder);
        assertEquals("Got wrong answer for supports", false, actual); //$NON-NLS-1$
    }

    public void helpTestSupportsUnion(boolean supports) throws QueryMetadataException, TeiidComponentException {
        // Set up metadata
        TransformationMetadata metadata = RealMetadataFactory.example1Cached();
        Schema modelID = metadata.getMetadataStore().getSchema("PM1");

        // Set up capabilities
        FakeCapabilitiesFinder finder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities sourceCaps = new BasicSourceCapabilities();
        sourceCaps.setCapabilitySupport(Capability.QUERY_UNION, supports);
        finder.addCapabilities("pm1", sourceCaps); //$NON-NLS-1$

        // Test capabilities util
        boolean actual = CapabilitiesUtil.supportsSetOp(modelID, Operation.UNION, metadata, finder);
        assertEquals("Got wrong answer for supports", supports, actual); //$NON-NLS-1$
    }

    @Test public void testSupportsUnionTrue() throws Exception {
        helpTestSupportsUnion(true);
    }

    @Test public void testSupportsUnionFalse() throws Exception {
        helpTestSupportsUnion(false);
    }

    public void helpTestSupportsLiterals(boolean supports) throws QueryMetadataException, TeiidComponentException {
        // Set up metadata
        TransformationMetadata metadata = RealMetadataFactory.example1Cached();
        Schema modelID = metadata.getMetadataStore().getSchema("PM1");

        // Set up capabilities
        FakeCapabilitiesFinder finder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities sourceCaps = new BasicSourceCapabilities();
        sourceCaps.setCapabilitySupport(Capability.QUERY_SELECT_EXPRESSION, supports);
        finder.addCapabilities("pm1", sourceCaps); //$NON-NLS-1$

        // Test capabilities util
        boolean actual = CapabilitiesUtil.supportsSelectExpression(modelID, metadata, finder);
        assertEquals("Got wrong answer for supports", supports, actual); //$NON-NLS-1$
    }

    @Test public void testSupportsLiteralsTrue() throws Exception {
        helpTestSupportsLiterals(true);
    }

    @Test public void testSupportsLiteralsFalse() throws Exception {
        helpTestSupportsLiterals(false);
    }

    public void helpTtestSupportsCaseExpression(boolean supports, boolean searched) throws QueryMetadataException, TeiidComponentException {
        // Set up metadata
        TransformationMetadata metadata = RealMetadataFactory.example1Cached();
        Schema modelID = metadata.getMetadataStore().getSchema("PM1");

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
        boolean actual = false;
        if(searched) {
            actual = CapabilitiesUtil.supportsSearchedCaseExpression(modelID, metadata, finder);
        } else {
            actual = CapabilitiesUtil.supportsCaseExpression(modelID, metadata, finder);
        }
        assertEquals("Got wrong answer for supports", supports, actual); //$NON-NLS-1$
    }

    @Test public void testSupportsCaseTrue() throws Exception {
        helpTtestSupportsCaseExpression(true, false);
    }

    @Test public void testSupportsCaseFalse() throws Exception {
        helpTtestSupportsCaseExpression(false, false);
    }

    @Test public void testSupportsSearchedCaseTrue() throws Exception {
        helpTtestSupportsCaseExpression(true, true);
    }

    @Test public void testSupportsSearchedCaseFalse() throws Exception {
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

    @Test public void testSupportRowLimit() throws Exception {
        // Set up metadata
        TransformationMetadata metadata = RealMetadataFactory.example1Cached();
        Schema modelID = metadata.getMetadataStore().getSchema("PM1");
        // Set up capabilities
        FakeCapabilitiesFinder finder = getFinder(Capability.ROW_LIMIT, false);
        // Test capabilities util
        assertEquals(false, CapabilitiesUtil.supportsRowLimit(modelID, metadata, finder));

        finder = getFinder(Capability.ROW_LIMIT, true);
        // Test capabilities util
        assertEquals(true, CapabilitiesUtil.supportsRowLimit(modelID, metadata, finder));
    }

    @Test public void testSupportRowOffset() throws Exception {
        // Set up metadata
        TransformationMetadata metadata = RealMetadataFactory.example1Cached();
        Schema modelID = metadata.getMetadataStore().getSchema("PM1");
        // Set up capabilities
        FakeCapabilitiesFinder finder = getFinder(Capability.ROW_OFFSET, false);
        // Test capabilities util
        assertEquals(false, CapabilitiesUtil.supportsRowOffset(modelID, metadata, finder));

        finder = getFinder(Capability.ROW_OFFSET, true);
        // Test capabilities util
        assertEquals(true, CapabilitiesUtil.supportsRowOffset(modelID, metadata, finder));
    }
}
