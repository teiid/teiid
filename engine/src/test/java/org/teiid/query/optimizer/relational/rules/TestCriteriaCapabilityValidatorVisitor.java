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

import org.junit.Test;
import org.teiid.api.exception.query.QueryMetadataException;
import org.teiid.core.TeiidException;
import org.teiid.metadata.Column;
import org.teiid.metadata.Column.SearchType;
import org.teiid.metadata.Schema;
import org.teiid.query.metadata.TransformationMetadata;
import org.teiid.query.optimizer.capabilities.BasicSourceCapabilities;
import org.teiid.query.optimizer.capabilities.CapabilitiesFinder;
import org.teiid.query.optimizer.capabilities.FakeCapabilitiesFinder;
import org.teiid.query.optimizer.capabilities.SourceCapabilities.Capability;
import org.teiid.query.parser.QueryParser;
import org.teiid.query.resolver.QueryResolver;
import org.teiid.query.sql.lang.Command;
import org.teiid.query.sql.lang.Criteria;
import org.teiid.query.unittest.RealMetadataFactory;
import org.teiid.translator.SourceSystemFunctions;


/**
 */
@SuppressWarnings("nls")
public class TestCriteriaCapabilityValidatorVisitor {

    public void helpTestVisitor(String sql, Object modelID, TransformationMetadata metadata, CapabilitiesFinder capFinder, boolean isValid, boolean expectException) throws Exception {
        try {
            Criteria criteria = QueryParser.getQueryParser().parseCriteria(sql);

            QueryResolver.resolveCriteria(criteria, metadata);

            assertEquals("Got incorrect isValid flag", isValid, CriteriaCapabilityValidatorVisitor.canPushLanguageObject(criteria, modelID, metadata, capFinder, null)); //$NON-NLS-1$
        } catch(QueryMetadataException e) {
            if (!expectException) {
                throw new RuntimeException(e);
            }
        }
    }

    // Assume there is a wrapped command - this will allow subqueries to be properly resolved
    public void helpTestVisitorWithCommand(String sql, Object modelID, TransformationMetadata metadata, CapabilitiesFinder capFinder, boolean isValid, boolean expectException) {
        try {
            Command command = QueryParser.getQueryParser().parseCommand(sql);

            QueryResolver.resolveCommand(command, metadata);

            assertEquals("Got incorrect isValid flag", isValid, CriteriaCapabilityValidatorVisitor.canPushLanguageObject(command, modelID, metadata, capFinder, null)); //$NON-NLS-1$
        } catch(QueryMetadataException e) {
            if (!expectException) {
                throw new RuntimeException(e);
            }
        } catch(TeiidException e) {
            throw new RuntimeException(e);
        }
    }

    // has all capabilities
    @Test public void testCompareCriteriaSuccess() throws Exception {
        TransformationMetadata metadata = RealMetadataFactory.example1Cached();
        Object modelID = metadata.getMetadataStore().getSchema("PM1");

        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setCapabilitySupport(Capability.CRITERIA_COMPARE_EQ, true);
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$

        helpTestVisitor("pm1.g1.e1 = 'x'", modelID, metadata, capFinder, true, false);         //$NON-NLS-1$
    }

    // does not have where capability
    @Test public void testCompareCriteriaCapFail1() throws Exception {
        TransformationMetadata metadata = RealMetadataFactory.example1Cached();
        Object modelID = metadata.getMetadataStore().getSchema("PM1");

        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setCapabilitySupport(Capability.CRITERIA_COMPARE_EQ, false);
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$

        helpTestVisitor("pm1.g1.e1 = 'x'", modelID, metadata, capFinder, false, false);
    }

    // does not have = capability
    @Test public void testCompareCriteriaOpCapFail1() throws Exception {
        TransformationMetadata metadata = RealMetadataFactory.example1Cached();
        Object modelID = metadata.getMetadataStore().getSchema("PM1");

        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setCapabilitySupport(Capability.CRITERIA_COMPARE_EQ, false);
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$

        helpTestVisitor("pm1.g1.e1 = 'x'", modelID, metadata, capFinder, false, false);
    }

    // does not have <> capability
    @Test public void testCompareCriteriaOpCapFail2() throws Exception {
        TransformationMetadata metadata = RealMetadataFactory.example1Cached();
        Object modelID = metadata.getMetadataStore().getSchema("PM1");

        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setCapabilitySupport(Capability.CRITERIA_COMPARE_EQ, false);
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$

        helpTestVisitor("pm1.g1.e1 <> 'x'", modelID, metadata, capFinder, false, false);
    }

    // does not have < capability
    @Test public void testCompareCriteriaOpCapFail3() throws Exception {
        TransformationMetadata metadata = RealMetadataFactory.example1Cached();
        Object modelID = metadata.getMetadataStore().getSchema("PM1");

        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setCapabilitySupport(Capability.CRITERIA_COMPARE_ORDERED, false);
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$

        helpTestVisitor("pm1.g1.e1 < 'x'", modelID, metadata, capFinder, false, false);
    }

    // does not have <= capability
    @Test public void testCompareCriteriaOpCapFail4() throws Exception {
        TransformationMetadata metadata = RealMetadataFactory.example1Cached();
        Object modelID = metadata.getMetadataStore().getSchema("PM1");

        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setCapabilitySupport(Capability.CRITERIA_COMPARE_ORDERED, false);
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$

        helpTestVisitor("pm1.g1.e1 <= 'x'", modelID, metadata, capFinder, false, false);
    }

    // does not have > capability
    @Test public void testCompareCriteriaOpCapFail5() throws Exception {
        TransformationMetadata metadata = RealMetadataFactory.example1Cached();
        Object modelID = metadata.getMetadataStore().getSchema("PM1");

        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setCapabilitySupport(Capability.CRITERIA_COMPARE_ORDERED, false);
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$

        helpTestVisitor("pm1.g1.e1 > 'x'", modelID, metadata, capFinder, false, false);
    }

    // does not have >= capability
    @Test public void testCompareCriteriaOpCapFail6() throws Exception {
        TransformationMetadata metadata = RealMetadataFactory.example1Cached();
        Object modelID = metadata.getMetadataStore().getSchema("PM1");

        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setCapabilitySupport(Capability.CRITERIA_COMPARE_ORDERED, false);
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$

        helpTestVisitor("pm1.g1.e1 >= 'x'", modelID, metadata, capFinder, false, false);
    }

    // element not searchable
    @Test public void testCompareCriteriaSearchableFail() throws Exception {
        TransformationMetadata metadata = RealMetadataFactory.example1();
        Object modelID = metadata.getMetadataStore().getSchema("PM1");
        Column e1 = metadata.getElementID("pm1.g1.e1");
        e1.setSearchType(SearchType.Like_Only);

        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setCapabilitySupport(Capability.CRITERIA_COMPARE_EQ, true);
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$

        helpTestVisitor("pm1.g1.e1 = 'x'", modelID, metadata, capFinder, false, false);
    }

    // no caps
    @Test public void testCompareCriteriaNoCaps() throws Exception {
        TransformationMetadata metadata = RealMetadataFactory.example1Cached();
        Object modelID = metadata.getMetadataStore().getSchema("PM1");

        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();

        helpTestVisitor("pm1.g1.e1 = 'x'", modelID, metadata, capFinder, true, false);
    }

    @Test public void testCompoundCriteriaAnd1() throws Exception {
        TransformationMetadata metadata = RealMetadataFactory.example1Cached();
        Object modelID = metadata.getMetadataStore().getSchema("PM1");

        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setCapabilitySupport(Capability.CRITERIA_COMPARE_EQ, true);
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$

        helpTestVisitor("pm1.g1.e1 = 'x' AND 0 = 1", modelID, metadata, capFinder, true, false);
    }

    @Test public void testCompoundCriteriaAnd4() throws Exception {
        TransformationMetadata metadata = RealMetadataFactory.example1Cached();
        Object modelID = metadata.getMetadataStore().getSchema("PM1");

        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();

        helpTestVisitor("pm1.g1.e1 = 'x' AND 0 = 1", modelID, metadata, capFinder, true, false);
    }

    @Test public void testCompoundCriteriaOr1() throws Exception {
        TransformationMetadata metadata = RealMetadataFactory.example1Cached();
        Object modelID = metadata.getMetadataStore().getSchema("PM1");

        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setCapabilitySupport(Capability.CRITERIA_COMPARE_EQ, true);
        caps.setCapabilitySupport(Capability.CRITERIA_OR, true);
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$

        helpTestVisitor("pm1.g1.e1 = 'x' OR 0 = 1", modelID, metadata, capFinder, true, false);
    }

    @Test public void testCompoundCriteriaOr2() throws Exception {
        TransformationMetadata metadata = RealMetadataFactory.example1Cached();
        Object modelID = metadata.getMetadataStore().getSchema("PM1");

        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setCapabilitySupport(Capability.CRITERIA_COMPARE_EQ, true);
        caps.setCapabilitySupport(Capability.CRITERIA_OR, false);
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$

        helpTestVisitor("pm1.g1.e1 = 'x' OR 0 = 1", modelID, metadata, capFinder, false, false);
    }

    @Test public void testCompoundCriteriaOr4() throws Exception {
        TransformationMetadata metadata = RealMetadataFactory.example1Cached();
        Object modelID = metadata.getMetadataStore().getSchema("PM1");

        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();

        helpTestVisitor("pm1.g1.e1 = 'x' OR 0 = 1", modelID, metadata, capFinder, true, false);
    }

    @Test public void testScalarFunction1() throws Exception {
        TransformationMetadata metadata = RealMetadataFactory.example1Cached();
        Object modelID = metadata.getMetadataStore().getSchema("PM1");

        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setCapabilitySupport(Capability.CRITERIA_COMPARE_EQ, true);
        caps.setFunctionSupport("curtime", true); //$NON-NLS-1$
        capFinder.addCapabilities("pm1", caps);

        helpTestVisitor("curtime() = {t'10:00:00'}", modelID, metadata, capFinder, true, false);
    }

    /**
     * Since this will always get pre-evaluated, this should also be true
     *
     */
    @Test public void testScalarFunction2() throws Exception {
        TransformationMetadata metadata = RealMetadataFactory.example1Cached();
        Object modelID = metadata.getMetadataStore().getSchema("PM1");

        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setCapabilitySupport(Capability.CRITERIA_COMPARE_EQ, true);
        caps.setFunctionSupport("+", false); //$NON-NLS-1$
        capFinder.addCapabilities("pm1", caps);

        helpTestVisitor("1 + 1 = 2", modelID, metadata, capFinder, true, false);
    }

    /**
     * since curtime is command deterministic and not supported, it will be evaluated
     */
    @Test public void testScalarFunction2a() throws Exception {
        TransformationMetadata metadata = RealMetadataFactory.example1Cached();
        Object modelID = metadata.getMetadataStore().getSchema("PM1");

        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setCapabilitySupport(Capability.CRITERIA_COMPARE_EQ, true);
        caps.setFunctionSupport("curtime", false); //$NON-NLS-1$
        capFinder.addCapabilities("pm1", caps);

        helpTestVisitor("curtime() = {t'10:00:00'}", modelID, metadata, capFinder, true, false);
    }

    /**
     * since rand is non-deterministic and not supported, it will be evaluated for every row
     */
    @Test public void testScalarFunction2b() throws Exception {
        TransformationMetadata metadata = RealMetadataFactory.example1Cached();
        Object modelID = metadata.getMetadataStore().getSchema("PM1");

        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setCapabilitySupport(Capability.CRITERIA_COMPARE_EQ, true);
        caps.setFunctionSupport("rand", false); //$NON-NLS-1$
        capFinder.addCapabilities("pm1", caps);

        helpTestVisitor("rand() = '1.0'", modelID, metadata, capFinder, false, false);
    }

    /**
     * don't push seed with seed argument
     */
    @Test public void testRandWithSeed() throws Exception {
        TransformationMetadata metadata = RealMetadataFactory.example1Cached();
        Object modelID = metadata.getMetadataStore().getSchema("PM1");

        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setCapabilitySupport(Capability.CRITERIA_COMPARE_EQ, true);
        caps.setFunctionSupport(SourceSystemFunctions.RAND, true); //$NON-NLS-1$
        capFinder.addCapabilities("pm1", caps);

        helpTestVisitor("rand(5) = '1.0'", modelID, metadata, capFinder, false, false);
    }

    @Test public void testIsNull1() throws Exception {
        TransformationMetadata metadata = RealMetadataFactory.example1Cached();
        Object modelID = metadata.getMetadataStore().getSchema("PM1");

        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setCapabilitySupport(Capability.CRITERIA_ISNULL, true);
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$

        helpTestVisitor("pm1.g1.e1 IS NULL", modelID, metadata, capFinder, true, false);
    }

    @Test public void testIsNull2() throws Exception {
        TransformationMetadata metadata = RealMetadataFactory.example1Cached();
        Object modelID = metadata.getMetadataStore().getSchema("PM1");

        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setCapabilitySupport(Capability.CRITERIA_ISNULL, false);
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$

        helpTestVisitor("pm1.g1.e1 IS NULL", modelID, metadata, capFinder, false, false);
    }

    @Test public void testIsNull3() throws Exception {
        TransformationMetadata metadata = RealMetadataFactory.example1Cached();
        Object modelID = metadata.getMetadataStore().getSchema("PM1");

        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();

        helpTestVisitor("pm1.g1.e1 IS NULL", modelID, metadata, capFinder, true, false);
    }

    /**
     * Is null is not a comparison operation
     */
    @Test public void testIsNull4() throws Exception {
        TransformationMetadata metadata = RealMetadataFactory.example1();
        Object modelID = metadata.getMetadataStore().getSchema("PM1");
        Column e1 = metadata.getElementID("pm1.g1.e1");
        e1.setSearchType(SearchType.Like_Only);

        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setCapabilitySupport(Capability.CRITERIA_ISNULL, true);
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$

        helpTestVisitor("pm1.g1.e1 IS NULL", modelID, metadata, capFinder, true, false);
    }

    @Test public void testIsNull6() throws Exception {
        TransformationMetadata metadata = RealMetadataFactory.example1Cached();
        Object modelID = metadata.getMetadataStore().getSchema("PM1");

        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setCapabilitySupport(Capability.CRITERIA_ISNULL, true);
        caps.setCapabilitySupport(Capability.CRITERIA_NOT, true);
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$

        helpTestVisitor("pm1.g1.e1 IS NOT NULL", modelID, metadata, capFinder, true, false);
    }

    @Test public void testIsNull6fails() throws Exception {
        TransformationMetadata metadata = RealMetadataFactory.example1Cached();
        Object modelID = metadata.getMetadataStore().getSchema("PM1");

        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setCapabilitySupport(Capability.CRITERIA_ISNULL, true);
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$

        helpTestVisitor("pm1.g1.e1 IS NOT NULL", modelID, metadata, capFinder, false, false);
    }

    // has all capabilities
    @Test public void testMatchCriteriaSuccess() throws Exception {
        TransformationMetadata metadata = RealMetadataFactory.example1Cached();
        Object modelID = metadata.getMetadataStore().getSchema("PM1");

        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setCapabilitySupport(Capability.CRITERIA_LIKE, true);
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$

        helpTestVisitor("pm1.g1.e1 LIKE 'x'", modelID, metadata, capFinder, true, false);
    }

    @Test public void testMatchCriteriaSuccess2() throws Exception {
        TransformationMetadata metadata = RealMetadataFactory.example1Cached();
        Object modelID = metadata.getMetadataStore().getSchema("PM1");

        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setCapabilitySupport(Capability.CRITERIA_LIKE, true);
        caps.setCapabilitySupport(Capability.CRITERIA_LIKE_ESCAPE, true);
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$

        helpTestVisitor("pm1.g1.e1 LIKE 'x' ESCAPE '#'", modelID, metadata, capFinder, true, false);
    }

    // Test for NOT LIKE
    @Test public void testMatchCriteriaSuccess3() throws Exception {
        TransformationMetadata metadata = RealMetadataFactory.example1Cached();
        Object modelID = metadata.getMetadataStore().getSchema("PM1");

        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setCapabilitySupport(Capability.CRITERIA_LIKE, true);
        caps.setCapabilitySupport(Capability.CRITERIA_NOT, true);
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$

        helpTestVisitor("pm1.g1.e1 NOT LIKE 'x'", modelID, metadata, capFinder, true, false);
    }

    @Test public void testMatchCriteriaSuccess3fails() throws Exception {
        TransformationMetadata metadata = RealMetadataFactory.example1Cached();
        Object modelID = metadata.getMetadataStore().getSchema("PM1");

        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setCapabilitySupport(Capability.CRITERIA_LIKE, true);
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$

        helpTestVisitor("pm1.g1.e1 NOT LIKE 'x'", modelID, metadata, capFinder, false, false);         //$NON-NLS-1$
    }

    // does not have compare capability
    @Test public void testMatchCriteriaCapFail1() throws Exception {
        TransformationMetadata metadata = RealMetadataFactory.example1Cached();
        Object modelID = metadata.getMetadataStore().getSchema("PM1");

        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setCapabilitySupport(Capability.CRITERIA_LIKE, false);
        caps.setCapabilitySupport(Capability.CRITERIA_LIKE_ESCAPE, true);
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$

        helpTestVisitor("pm1.g1.e1 LIKE 'x'", modelID, metadata, capFinder, false, false);         //$NON-NLS-1$
    }

    // does not have escape char capability
    @Test public void testMatchCriteriaCapFail2() throws Exception {
        TransformationMetadata metadata = RealMetadataFactory.example1Cached();
        Object modelID = metadata.getMetadataStore().getSchema("PM1");

        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setCapabilitySupport(Capability.CRITERIA_LIKE, true);
        caps.setCapabilitySupport(Capability.CRITERIA_LIKE_ESCAPE, false);
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$

        helpTestVisitor("pm1.g1.e1 LIKE 'x' ESCAPE '#'", modelID, metadata, capFinder, false, false);         //$NON-NLS-1$
    }

    // element not searchable
    @Test public void testMatchCriteriaMatchableFail() throws Exception {
        TransformationMetadata metadata = RealMetadataFactory.example1();
        Object modelID = metadata.getMetadataStore().getSchema("PM1");
        Column e1 = metadata.getElementID("pm1.g1.e1");
        e1.setSearchType(SearchType.All_Except_Like);

        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setCapabilitySupport(Capability.CRITERIA_LIKE, true);
        caps.setCapabilitySupport(Capability.CRITERIA_LIKE_ESCAPE, true);
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$

        helpTestVisitor("pm1.g1.e1 LIKE 'x'", modelID, metadata, capFinder, false, false);         //$NON-NLS-1$
    }

    // no caps
    @Test public void testMatchCriteriaNoCaps() throws Exception {
        TransformationMetadata metadata = RealMetadataFactory.example1Cached();
        Object modelID = metadata.getMetadataStore().getSchema("PM1");

        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();

        helpTestVisitor("pm1.g1.e1 LIKE 'x'", modelID, metadata, capFinder, true, false);         //$NON-NLS-1$
    }

    @Test public void testNotCriteria1() throws Exception {
        TransformationMetadata metadata = RealMetadataFactory.example1Cached();
        Object modelID = metadata.getMetadataStore().getSchema("PM1");

        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setCapabilitySupport(Capability.CRITERIA_NOT, true);
        caps.setCapabilitySupport(Capability.CRITERIA_COMPARE_EQ, true);
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$

        helpTestVisitor("NOT pm1.g1.e1 = 'x'", modelID, metadata, capFinder, true, false);                 //$NON-NLS-1$
    }

    @Test public void testNotCriteria2() throws Exception {
        TransformationMetadata metadata = RealMetadataFactory.example1Cached();
        Object modelID = metadata.getMetadataStore().getSchema("PM1");

        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setCapabilitySupport(Capability.CRITERIA_NOT, false);
        caps.setCapabilitySupport(Capability.CRITERIA_COMPARE_EQ, true);
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$

        helpTestVisitor("NOT pm1.g1.e1 = 'x'", modelID, metadata, capFinder, false, false);                 //$NON-NLS-1$
    }

    @Test public void testSetCriteria1() throws Exception {
        TransformationMetadata metadata = RealMetadataFactory.example1Cached();
        Object modelID = metadata.getMetadataStore().getSchema("PM1");

        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setCapabilitySupport(Capability.CRITERIA_IN, true);
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$

        helpTestVisitor("pm1.g1.e1 IN ('x')", modelID, metadata, capFinder, true, false);                 //$NON-NLS-1$
    }

    @Test public void testSetCriteria2() throws Exception {
        TransformationMetadata metadata = RealMetadataFactory.example1Cached();
        Object modelID = metadata.getMetadataStore().getSchema("PM1");

        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setCapabilitySupport(Capability.CRITERIA_IN, false);
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$

        helpTestVisitor("pm1.g1.e1 IN ('x')", modelID, metadata, capFinder, false, false);                 //$NON-NLS-1$
    }

    @Test public void testSetCriteria3() throws Exception {
        TransformationMetadata metadata = RealMetadataFactory.example1Cached();
        Object modelID = metadata.getMetadataStore().getSchema("PM1");

        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();

        helpTestVisitor("pm1.g1.e1 IN ('x')", modelID, metadata, capFinder, true, false);                 //$NON-NLS-1$
    }

    @Test public void testSetCriteria5() throws Exception {
        TransformationMetadata metadata = RealMetadataFactory.example1();
        Object modelID = metadata.getMetadataStore().getSchema("PM1");
        Column elementID = metadata.getElementID("pm1.g1.e1");
        elementID.setSearchType(SearchType.Like_Only);

        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setCapabilitySupport(Capability.CRITERIA_IN, true);
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$

        helpTestVisitor("pm1.g1.e1 IN ('x')", modelID, metadata, capFinder, false, false);                 //$NON-NLS-1$
    }

    //Test for success NOT IN
    @Test public void testSetCriteria7() throws Exception {
        TransformationMetadata metadata = RealMetadataFactory.example1Cached();
        Object modelID = metadata.getMetadataStore().getSchema("PM1");

        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setCapabilitySupport(Capability.CRITERIA_IN, true);
        caps.setCapabilitySupport(Capability.CRITERIA_NOT, true);
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$

        helpTestVisitor("pm1.g1.e1 NOT IN ('x')", modelID, metadata, capFinder, true, false);                 //$NON-NLS-1$
    }

    @Test public void testSetCriteria7fails() throws Exception {
        TransformationMetadata metadata = RealMetadataFactory.example1Cached();
        Object modelID = metadata.getMetadataStore().getSchema("PM1");

        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setCapabilitySupport(Capability.CRITERIA_IN, true);
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$

        helpTestVisitor("pm1.g1.e1 NOT IN ('x')", modelID, metadata, capFinder, false, false);                 //$NON-NLS-1$
    }

    @Test public void testSetCriteria8() throws Exception {
        TransformationMetadata metadata = RealMetadataFactory.example1Cached();
        Object modelID = metadata.getMetadataStore().getSchema("PM1");

        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setCapabilitySupport(Capability.CRITERIA_IN, true);
        caps.setSourceProperty(Capability.MAX_IN_CRITERIA_SIZE, new Integer(2));
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$

        helpTestVisitor("pm1.g1.e1 IN ('x', 'y', 'z')", modelID, metadata, capFinder, true, false);                 //$NON-NLS-1$

        caps.setSourceProperty(Capability.MAX_DEPENDENT_PREDICATES, 1);

        helpTestVisitor("pm1.g1.e1 IN ('x', 'y', 'z')", modelID, metadata, capFinder, false, false);                 //$NON-NLS-1$
    }

    @Test public void testSetCriteria9() throws Exception {
        TransformationMetadata metadata = RealMetadataFactory.example1Cached();
        Object modelID = metadata.getMetadataStore().getSchema("PM1");

        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setCapabilitySupport(Capability.CRITERIA_IN, true);
        caps.setSourceProperty(Capability.MAX_IN_CRITERIA_SIZE, new Integer(2));
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$

        helpTestVisitor("pm1.g1.e1 IN ('x', 'y')", modelID, metadata, capFinder, true, false);                 //$NON-NLS-1$
    }

    @Test public void testSubquerySetCriteria() throws Exception {
        TransformationMetadata metadata = RealMetadataFactory.example1Cached();
        Object modelID = metadata.getMetadataStore().getSchema("PM1");

        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setCapabilitySupport(Capability.CRITERIA_IN, true);
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$

        helpTestVisitor("pm1.g1.e1 IN (SELECT 'xyz' FROM pm1.g1)", modelID, metadata, capFinder, false, false);                 //$NON-NLS-1$
    }

    @Test public void testSearchCase() throws Exception {
        TransformationMetadata metadata = RealMetadataFactory.example1Cached();
        Object modelID = metadata.getMetadataStore().getSchema("PM1");

        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setCapabilitySupport(Capability.QUERY_SEARCHED_CASE, true);
        caps.setCapabilitySupport(Capability.CRITERIA_COMPARE_EQ, true);
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$

        helpTestVisitor("pm1.g1.e1 = case when pm1.g1.e2 = 1 then 1 else 2 end", modelID, metadata, capFinder, false, false);                 //$NON-NLS-1$
    }

    // has all capabilities
    @Test public void testSubqueryCompareCriteriaSuccess() throws Exception {
        TransformationMetadata metadata = RealMetadataFactory.example1Cached();
        Object modelID = metadata.getMetadataStore().getSchema("PM1");

        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setCapabilitySupport(Capability.CRITERIA_COMPARE_EQ, true);
        caps.setCapabilitySupport(Capability.CRITERIA_QUANTIFIED_SOME, true);
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$

        helpTestVisitorWithCommand("SELECT e1 FROM pm1.g1 WHERE pm1.g1.e1 = ANY (SELECT e1 FROM pm1.g2)", modelID, metadata, capFinder, true, false);         //$NON-NLS-1$
    }

    // does not have where capability
    @Test public void testSubqueryCompareCriteriaCapFail1() throws Exception {
        TransformationMetadata metadata = RealMetadataFactory.example1Cached();
        Object modelID = metadata.getMetadataStore().getSchema("PM1");

        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setCapabilitySupport(Capability.CRITERIA_COMPARE_EQ, true);
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$

        helpTestVisitorWithCommand("SELECT e1 FROM pm1.g1 WHERE pm1.g1.e1 = ANY (SELECT e1 FROM pm1.g2)", modelID, metadata, capFinder, false, false);         //$NON-NLS-1$
    }

    // does not have compare capability
    @Test public void testSubqueryCompareCriteriaCapFail2() throws Exception {
        TransformationMetadata metadata = RealMetadataFactory.example1Cached();
        Object modelID = metadata.getMetadataStore().getSchema("PM1");

        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setCapabilitySupport(Capability.CRITERIA_COMPARE_EQ, true);
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$

        helpTestVisitorWithCommand("SELECT e1 FROM pm1.g1 WHERE pm1.g1.e1 = ANY (SELECT e1 FROM pm1.g2)", modelID, metadata, capFinder, false, false);         //$NON-NLS-1$
    }

    // does not have subquery capability
    @Test public void testSubqueryCompareCriteriaFail3() throws Exception {
        TransformationMetadata metadata = RealMetadataFactory.example1Cached();
        Object modelID = metadata.getMetadataStore().getSchema("PM1");

        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setCapabilitySupport(Capability.CRITERIA_QUANTIFIED_SOME, false);
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$

        helpTestVisitorWithCommand("SELECT e1 FROM pm1.g1 WHERE pm1.g1.e1 = ANY (SELECT e1 FROM pm1.g2)", modelID, metadata, capFinder, false, false);         //$NON-NLS-1$
    }

    // does not have quantified subquery comparison capability
    @Test public void testSubqueryCompareCriteriaFail4() throws Exception {
        TransformationMetadata metadata = RealMetadataFactory.example1Cached();
        Object modelID = metadata.getMetadataStore().getSchema("PM1");

        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setCapabilitySupport(Capability.CRITERIA_COMPARE_EQ, true);
        caps.setCapabilitySupport(Capability.QUERY_SUBQUERIES_SCALAR, true);
        caps.setCapabilitySupport(Capability.CRITERIA_QUANTIFIED_SOME, false);
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$

        helpTestVisitorWithCommand("SELECT e1 FROM pm1.g1 WHERE pm1.g1.e1 = ANY (SELECT e1 FROM pm1.g2)", modelID, metadata, capFinder, false, false);         //$NON-NLS-1$
    }

    // does not have quantified subquery comparison capability for ANY
    @Test public void testSubqueryCompareCriteriaFail5() throws Exception {
        TransformationMetadata metadata = RealMetadataFactory.example1Cached();
        Object modelID = metadata.getMetadataStore().getSchema("PM1");

        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setCapabilitySupport(Capability.CRITERIA_COMPARE_EQ, true);
        caps.setCapabilitySupport(Capability.QUERY_SUBQUERIES_SCALAR, true);
        caps.setCapabilitySupport(Capability.CRITERIA_QUANTIFIED_SOME, false);
        caps.setCapabilitySupport(Capability.CRITERIA_QUANTIFIED_ALL, true);
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$

        helpTestVisitorWithCommand("SELECT e1 FROM pm1.g1 WHERE pm1.g1.e1 = ANY (SELECT e1 FROM pm1.g2)", modelID, metadata, capFinder, false, false);         //$NON-NLS-1$
    }

    // does not have quantified subquery comparison capability for ALL
    @Test public void testSubqueryCompareCriteriaFail6() throws Exception {
        TransformationMetadata metadata = RealMetadataFactory.example1Cached();
        Object modelID = metadata.getMetadataStore().getSchema("PM1");

        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setCapabilitySupport(Capability.CRITERIA_COMPARE_EQ, true);
        caps.setCapabilitySupport(Capability.QUERY_SUBQUERIES_SCALAR, true);
        caps.setCapabilitySupport(Capability.CRITERIA_QUANTIFIED_SOME, true);
        caps.setCapabilitySupport(Capability.CRITERIA_QUANTIFIED_ALL, false);
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$

        helpTestVisitorWithCommand("SELECT e1 FROM pm1.g1 WHERE pm1.g1.e1 = ALL (SELECT e1 FROM pm1.g2)", modelID, metadata, capFinder, false, false);         //$NON-NLS-1$
    }

    // does not have = capability
    @Test public void testSubqueryCompareCriteriaOpCapFail1() throws Exception {
        TransformationMetadata metadata = RealMetadataFactory.example1Cached();
        Object modelID = metadata.getMetadataStore().getSchema("PM1");

        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setCapabilitySupport(Capability.CRITERIA_COMPARE_EQ, false);
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$

        helpTestVisitorWithCommand("SELECT e1 FROM pm1.g1 WHERE pm1.g1.e1 = ANY (SELECT e1 FROM pm1.g2)", modelID, metadata, capFinder, false, false);         //$NON-NLS-1$
    }

    // does not have <> capability
    @Test public void testSubqueryCompareCriteriaOpCapFail2() throws Exception {
        TransformationMetadata metadata = RealMetadataFactory.example1Cached();
        Object modelID = metadata.getMetadataStore().getSchema("PM1");

        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setCapabilitySupport(Capability.CRITERIA_COMPARE_EQ, false);
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$

        helpTestVisitorWithCommand("SELECT e1 FROM pm1.g1 WHERE pm1.g1.e1 <> ANY (SELECT e1 FROM pm1.g2)", modelID, metadata, capFinder, false, false);         //$NON-NLS-1$
    }

    // does not have < capability
    @Test public void testSubqueryCompareCriteriaOpCapFail3() throws Exception {
        TransformationMetadata metadata = RealMetadataFactory.example1Cached();
        Object modelID = metadata.getMetadataStore().getSchema("PM1");

        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setCapabilitySupport(Capability.CRITERIA_COMPARE_ORDERED, false);
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$

        helpTestVisitorWithCommand("SELECT e1 FROM pm1.g1 WHERE pm1.g1.e1 < ANY (SELECT e1 FROM pm1.g2)", modelID, metadata, capFinder, false, false);         //$NON-NLS-1$
    }

    // does not have <= capability
    @Test public void testSubqueryCompareCriteriaOpCapFail4() throws Exception {
        TransformationMetadata metadata = RealMetadataFactory.example1Cached();
        Object modelID = metadata.getMetadataStore().getSchema("PM1");

        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setCapabilitySupport(Capability.CRITERIA_COMPARE_ORDERED, false);
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$

        helpTestVisitorWithCommand("SELECT e1 FROM pm1.g1 WHERE pm1.g1.e1 <= ANY (SELECT e1 FROM pm1.g2)", modelID, metadata, capFinder, false, false);         //$NON-NLS-1$
    }

    // does not have > capability
    @Test public void testSubqueryCompareCriteriaOpCapFail5() throws Exception {
        TransformationMetadata metadata = RealMetadataFactory.example1Cached();
        Object modelID = metadata.getMetadataStore().getSchema("PM1");

        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setCapabilitySupport(Capability.CRITERIA_COMPARE_ORDERED, false);
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$

        helpTestVisitorWithCommand("SELECT e1 FROM pm1.g1 WHERE pm1.g1.e1 > ANY (SELECT e1 FROM pm1.g2)", modelID, metadata, capFinder, false, false);         //$NON-NLS-1$
    }

    // does not have >= capability
    @Test public void testSubqueryCompareCriteriaOpCapFail6() throws Exception {
        TransformationMetadata metadata = RealMetadataFactory.example1Cached();
        Object modelID = metadata.getMetadataStore().getSchema("PM1");

        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setCapabilitySupport(Capability.CRITERIA_COMPARE_ORDERED, false);
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$

        helpTestVisitorWithCommand("SELECT e1 FROM pm1.g1 WHERE pm1.g1.e1 >= ANY (SELECT e1 FROM pm1.g2)", modelID, metadata, capFinder, false, false);         //$NON-NLS-1$
    }

    // element not searchable
    @Test public void testSubqueryCompareCriteriaSearchableFail() throws Exception {
        TransformationMetadata metadata = RealMetadataFactory.example1();
        Schema modelID = metadata.getMetadataStore().getSchema("PM1");
        Column e1 = metadata.getElementID("pm1.g1.e1");
        e1.setSearchType(SearchType.Like_Only);

        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setCapabilitySupport(Capability.CRITERIA_COMPARE_EQ, true);
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$

        helpTestVisitorWithCommand("SELECT e1 FROM pm1.g1 WHERE pm1.g1.e1 = ANY (SELECT e1 FROM pm1.g2)", modelID, metadata, capFinder, false, false);         //$NON-NLS-1$
    }

    @Test public void testExistsCriteria1() throws Exception {
        TransformationMetadata metadata = RealMetadataFactory.example1Cached();
        Object modelID = metadata.getMetadataStore().getSchema("PM1");

        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setCapabilitySupport(Capability.CRITERIA_EXISTS, true);
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$

        helpTestVisitorWithCommand("SELECT e1 FROM pm1.g1 WHERE EXISTS(SELECT e1 FROM pm1.g2)", modelID, metadata, capFinder, true, false); //$NON-NLS-1$
    }

    @Test public void testExistsCriteria2() throws Exception {
        TransformationMetadata metadata = RealMetadataFactory.example1Cached();
        Object modelID = metadata.getMetadataStore().getSchema("PM1");

        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setCapabilitySupport(Capability.CRITERIA_EXISTS, true);
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$

        helpTestVisitorWithCommand("SELECT e1 FROM pm1.g1 WHERE EXISTS(SELECT e1 FROM pm1.g2 where e2 = pm1.g1.e2)", modelID, metadata, capFinder, true, false); //$NON-NLS-1$
    }

    @Test public void testExistsPreeval() throws Exception {
        TransformationMetadata metadata = RealMetadataFactory.example1Cached();
        Object modelID = metadata.getMetadataStore().getSchema("PM1");

        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setCapabilitySupport(Capability.CRITERIA_EXISTS, false);
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$

        helpTestVisitorWithCommand("SELECT e1 FROM pm1.g1 WHERE EXISTS(SELECT e1 FROM pm1.g2)", modelID, metadata, capFinder, true, false); //$NON-NLS-1$
    }

    @Test public void testExistsCriteria5() throws Exception {
        TransformationMetadata metadata = RealMetadataFactory.example1Cached();
        Object modelID = metadata.getMetadataStore().getSchema("PM1");

        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setCapabilitySupport(Capability.CRITERIA_EXISTS, true);
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$

        helpTestVisitorWithCommand("SELECT e1 FROM pm1.g1 WHERE EXISTS(SELECT e1 FROM pm1.g2)", modelID, metadata, capFinder, true, false); //$NON-NLS-1$
    }

    @Test public void testEvaluatableCriteria() throws Exception {
        TransformationMetadata metadata = RealMetadataFactory.example1();
        Object modelID = metadata.getMetadataStore().getSchema("PM1");

        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$

        helpTestVisitor("now() IS NULL", modelID, metadata, capFinder, true, false);
    }

    @Test public void testEvaluatableCriteria1() throws Exception {
        TransformationMetadata metadata = RealMetadataFactory.example1();
        Object modelID = metadata.getMetadataStore().getSchema("PM1");

        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$

        helpTestVisitor("pm1.g1.e1 is null or now() IS NULL", modelID, metadata, capFinder, false, false);
    }
}
