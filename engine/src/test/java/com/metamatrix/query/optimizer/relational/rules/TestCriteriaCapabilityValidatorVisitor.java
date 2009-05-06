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

import org.junit.Test;

import static org.junit.Assert.*;

import com.metamatrix.api.exception.MetaMatrixException;
import com.metamatrix.api.exception.query.QueryMetadataException;
import com.metamatrix.query.optimizer.capabilities.BasicSourceCapabilities;
import com.metamatrix.query.optimizer.capabilities.CapabilitiesFinder;
import com.metamatrix.query.optimizer.capabilities.FakeCapabilitiesFinder;
import com.metamatrix.query.optimizer.capabilities.SourceCapabilities.Capability;
import com.metamatrix.query.parser.QueryParser;
import com.metamatrix.query.resolver.QueryResolver;
import com.metamatrix.query.sql.lang.Command;
import com.metamatrix.query.sql.lang.Criteria;
import com.metamatrix.query.unittest.FakeMetadataFacade;
import com.metamatrix.query.unittest.FakeMetadataFactory;
import com.metamatrix.query.unittest.FakeMetadataObject;

/**
 */
public class TestCriteriaCapabilityValidatorVisitor {

    public void helpTestVisitor(String sql, Object modelID, FakeMetadataFacade metadata, CapabilitiesFinder capFinder, boolean isValid, boolean expectException) {
        try {
            Criteria criteria = QueryParser.getQueryParser().parseCriteria(sql);
            
            QueryResolver.resolveCriteria(criteria, metadata);
                        
            assertEquals("Got incorrect isValid flag", isValid, CriteriaCapabilityValidatorVisitor.canPushLanguageObject(criteria, modelID, metadata, capFinder)); //$NON-NLS-1$
        } catch(QueryMetadataException e) {
        	if (!expectException) {
        		throw new RuntimeException(e);
        	}
        } catch(MetaMatrixException e) {
        	throw new RuntimeException(e);
        }
    }

    // Assume there is a wrapped command - this will allow subqueries to be properly resolved
    public void helpTestVisitorWithCommand(String sql, Object modelID, FakeMetadataFacade metadata, CapabilitiesFinder capFinder, boolean isValid, boolean expectException) {
        try {
            Command command = QueryParser.getQueryParser().parseCommand(sql);
            
            QueryResolver.resolveCommand(command, metadata);
                        
            assertEquals("Got incorrect isValid flag", isValid, CriteriaCapabilityValidatorVisitor.canPushLanguageObject(command, modelID, metadata, capFinder)); //$NON-NLS-1$
        } catch(QueryMetadataException e) {
        	if (!expectException) {
        		throw new RuntimeException(e);
        	}
        } catch(MetaMatrixException e) {
        	throw new RuntimeException(e);
        }
    }
    
    // has all capabilities
    @Test public void testCompareCriteriaSuccess() {
        FakeMetadataFacade metadata = FakeMetadataFactory.example1Cached();
        Object modelID = metadata.getStore().findObject("pm1", FakeMetadataObject.MODEL); //$NON-NLS-1$
        
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setCapabilitySupport(Capability.CRITERIA_COMPARE_EQ, true);
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$
        
        helpTestVisitor("pm1.g1.e1 = 'x'", modelID, metadata, capFinder, true, false);         //$NON-NLS-1$
    }

    // does not have where capability
    @Test public void testCompareCriteriaCapFail1() {
        FakeMetadataFacade metadata = FakeMetadataFactory.example1Cached();
        Object modelID = metadata.getStore().findObject("pm1", FakeMetadataObject.MODEL); //$NON-NLS-1$
        
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setCapabilitySupport(Capability.CRITERIA_COMPARE_EQ, false);
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$
        
        helpTestVisitor("pm1.g1.e1 = 'x'", modelID, metadata, capFinder, false, false);         //$NON-NLS-1$
    }

    // does not have = capability
    @Test public void testCompareCriteriaOpCapFail1() {
        FakeMetadataFacade metadata = FakeMetadataFactory.example1Cached();
        Object modelID = metadata.getStore().findObject("pm1", FakeMetadataObject.MODEL); //$NON-NLS-1$
        
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setCapabilitySupport(Capability.CRITERIA_COMPARE_EQ, false);
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$
        
        helpTestVisitor("pm1.g1.e1 = 'x'", modelID, metadata, capFinder, false, false);         //$NON-NLS-1$
    }
    
    // does not have <> capability
    @Test public void testCompareCriteriaOpCapFail2() {
        FakeMetadataFacade metadata = FakeMetadataFactory.example1Cached();
        Object modelID = metadata.getStore().findObject("pm1", FakeMetadataObject.MODEL); //$NON-NLS-1$
        
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setCapabilitySupport(Capability.CRITERIA_COMPARE_EQ, false);
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$
        
        helpTestVisitor("pm1.g1.e1 <> 'x'", modelID, metadata, capFinder, false, false);         //$NON-NLS-1$
    }

    // does not have < capability
    @Test public void testCompareCriteriaOpCapFail3() {
        FakeMetadataFacade metadata = FakeMetadataFactory.example1Cached();
        Object modelID = metadata.getStore().findObject("pm1", FakeMetadataObject.MODEL); //$NON-NLS-1$
        
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setCapabilitySupport(Capability.CRITERIA_COMPARE_ORDERED, false);
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$
        
        helpTestVisitor("pm1.g1.e1 < 'x'", modelID, metadata, capFinder, false, false);         //$NON-NLS-1$
    }

    // does not have <= capability
    @Test public void testCompareCriteriaOpCapFail4() {
        FakeMetadataFacade metadata = FakeMetadataFactory.example1Cached();
        Object modelID = metadata.getStore().findObject("pm1", FakeMetadataObject.MODEL); //$NON-NLS-1$
        
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setCapabilitySupport(Capability.CRITERIA_COMPARE_ORDERED, false);
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$
        
        helpTestVisitor("pm1.g1.e1 <= 'x'", modelID, metadata, capFinder, false, false);         //$NON-NLS-1$
    }

    // does not have > capability
    @Test public void testCompareCriteriaOpCapFail5() {
        FakeMetadataFacade metadata = FakeMetadataFactory.example1Cached();
        Object modelID = metadata.getStore().findObject("pm1", FakeMetadataObject.MODEL); //$NON-NLS-1$
        
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setCapabilitySupport(Capability.CRITERIA_COMPARE_ORDERED, false);
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$
        
        helpTestVisitor("pm1.g1.e1 > 'x'", modelID, metadata, capFinder, false, false);         //$NON-NLS-1$
    }

    // does not have >= capability
    @Test public void testCompareCriteriaOpCapFail6() {
        FakeMetadataFacade metadata = FakeMetadataFactory.example1Cached();
        Object modelID = metadata.getStore().findObject("pm1", FakeMetadataObject.MODEL); //$NON-NLS-1$
        
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setCapabilitySupport(Capability.CRITERIA_COMPARE_ORDERED, false);
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$
        
        helpTestVisitor("pm1.g1.e1 >= 'x'", modelID, metadata, capFinder, false, false);         //$NON-NLS-1$
    }
    
    // element not searchable
    @Test public void testCompareCriteriaSearchableFail() {
        FakeMetadataFacade metadata = FakeMetadataFactory.example1();
        Object modelID = metadata.getStore().findObject("pm1", FakeMetadataObject.MODEL); //$NON-NLS-1$
        FakeMetadataObject e1 = metadata.getStore().findObject("pm1.g1.e1", FakeMetadataObject.ELEMENT); //$NON-NLS-1$
        e1.putProperty(FakeMetadataObject.Props.SEARCHABLE_COMPARE, Boolean.FALSE);
                
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setCapabilitySupport(Capability.CRITERIA_COMPARE_EQ, true);
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$
        
        helpTestVisitor("pm1.g1.e1 = 'x'", modelID, metadata, capFinder, false, false);         //$NON-NLS-1$
    }

    // no caps
    @Test public void testCompareCriteriaNoCaps() {
        FakeMetadataFacade metadata = FakeMetadataFactory.example1Cached();
        Object modelID = metadata.getStore().findObject("pm1", FakeMetadataObject.MODEL); //$NON-NLS-1$
                
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        
        helpTestVisitor("pm1.g1.e1 = 'x'", modelID, metadata, capFinder, true, false);         //$NON-NLS-1$
    }
    
    @Test public void testCompoundCriteriaAnd1() {
        FakeMetadataFacade metadata = FakeMetadataFactory.example1Cached();
        Object modelID = metadata.getStore().findObject("pm1", FakeMetadataObject.MODEL); //$NON-NLS-1$
                
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setCapabilitySupport(Capability.CRITERIA_COMPARE_EQ, true);
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$
        
        helpTestVisitor("pm1.g1.e1 = 'x' AND 0 = 1", modelID, metadata, capFinder, true, false);                 //$NON-NLS-1$
    }

    @Test public void testCompoundCriteriaAnd4() {
        FakeMetadataFacade metadata = FakeMetadataFactory.example1Cached();
        Object modelID = metadata.getStore().findObject("pm1", FakeMetadataObject.MODEL); //$NON-NLS-1$
                
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        
        helpTestVisitor("pm1.g1.e1 = 'x' AND 0 = 1", modelID, metadata, capFinder, true, false);                 //$NON-NLS-1$
    }

    @Test public void testCompoundCriteriaOr1() {
        FakeMetadataFacade metadata = FakeMetadataFactory.example1Cached();
        Object modelID = metadata.getStore().findObject("pm1", FakeMetadataObject.MODEL); //$NON-NLS-1$
                
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setCapabilitySupport(Capability.CRITERIA_COMPARE_EQ, true);
        caps.setCapabilitySupport(Capability.CRITERIA_OR, true);
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$
        
        helpTestVisitor("pm1.g1.e1 = 'x' OR 0 = 1", modelID, metadata, capFinder, true, false);                 //$NON-NLS-1$
    }

    @Test public void testCompoundCriteriaOr2() {
        FakeMetadataFacade metadata = FakeMetadataFactory.example1Cached();
        Object modelID = metadata.getStore().findObject("pm1", FakeMetadataObject.MODEL); //$NON-NLS-1$
                
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setCapabilitySupport(Capability.CRITERIA_COMPARE_EQ, true);
        caps.setCapabilitySupport(Capability.CRITERIA_OR, false);
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$
        
        helpTestVisitor("pm1.g1.e1 = 'x' OR 0 = 1", modelID, metadata, capFinder, false, false);                 //$NON-NLS-1$
    }

    @Test public void testCompoundCriteriaOr4() {
        FakeMetadataFacade metadata = FakeMetadataFactory.example1Cached();
        FakeMetadataObject modelID = metadata.getStore().findObject("pm1", FakeMetadataObject.MODEL); //$NON-NLS-1$
                
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        
        helpTestVisitor("pm1.g1.e1 = 'x' OR 0 = 1", modelID, metadata, capFinder, true, false);                 //$NON-NLS-1$
    }

    @Test public void testScalarFunction1() {
        FakeMetadataFacade metadata = FakeMetadataFactory.example1Cached();
        Object modelID = metadata.getStore().findObject("pm1", FakeMetadataObject.MODEL); //$NON-NLS-1$
        
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setCapabilitySupport(Capability.CRITERIA_COMPARE_EQ, true);
        caps.setFunctionSupport("curtime", true); //$NON-NLS-1$
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$
        
        helpTestVisitor("curtime() = {t'10:00:00'}", modelID, metadata, capFinder, true, false); //$NON-NLS-1$
    }

    /** 
     * Since this will always get pre-evaluated, this should also be true 
     *  
     */ 
    @Test public void testScalarFunction2() {
        FakeMetadataFacade metadata = FakeMetadataFactory.example1Cached();
        Object modelID = metadata.getStore().findObject("pm1", FakeMetadataObject.MODEL); //$NON-NLS-1$
        
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setCapabilitySupport(Capability.CRITERIA_COMPARE_EQ, true);
        caps.setFunctionSupport("+", false); //$NON-NLS-1$
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$
        
        helpTestVisitor("1 + 1 = 2", modelID, metadata, capFinder, true, false); //$NON-NLS-1$
    }
    
    /**
     * since curtime is command deterministic and not supported, it will be evaluated
     */
    @Test public void testScalarFunction2a() {
        FakeMetadataFacade metadata = FakeMetadataFactory.example1Cached();
        Object modelID = metadata.getStore().findObject("pm1", FakeMetadataObject.MODEL); //$NON-NLS-1$
        
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setCapabilitySupport(Capability.CRITERIA_COMPARE_EQ, true);
        caps.setFunctionSupport("curtime", false); //$NON-NLS-1$
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$
        
        helpTestVisitor("curtime() = '{t'10:00:00'}", modelID, metadata, capFinder, true, false); //$NON-NLS-1$
    }
    
    /**
     * since rand is non-deterministic and not supported, it will be evaluated for every row
     */
    @Test public void testScalarFunction2b() {
        FakeMetadataFacade metadata = FakeMetadataFactory.example1Cached();
        Object modelID = metadata.getStore().findObject("pm1", FakeMetadataObject.MODEL); //$NON-NLS-1$
        
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setCapabilitySupport(Capability.CRITERIA_COMPARE_EQ, true);
        caps.setFunctionSupport("rand", false); //$NON-NLS-1$
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$
        
        helpTestVisitor("rand() = '1.0'", modelID, metadata, capFinder, false, false); //$NON-NLS-1$
    }

    
    @Test public void testIsNull1() {
        FakeMetadataFacade metadata = FakeMetadataFactory.example1Cached();
        Object modelID = metadata.getStore().findObject("pm1", FakeMetadataObject.MODEL); //$NON-NLS-1$
        
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setCapabilitySupport(Capability.CRITERIA_ISNULL, true);
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$
        
        helpTestVisitor("pm1.g1.e1 IS NULL", modelID, metadata, capFinder, true, false); //$NON-NLS-1$
    }

    @Test public void testIsNull2() {
        FakeMetadataFacade metadata = FakeMetadataFactory.example1Cached();
        Object modelID = metadata.getStore().findObject("pm1", FakeMetadataObject.MODEL); //$NON-NLS-1$
        
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setCapabilitySupport(Capability.CRITERIA_ISNULL, false);
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$
        
        helpTestVisitor("pm1.g1.e1 IS NULL", modelID, metadata, capFinder, false, false); //$NON-NLS-1$
    }

    @Test public void testIsNull3() {
        FakeMetadataFacade metadata = FakeMetadataFactory.example1Cached();
        Object modelID = metadata.getStore().findObject("pm1", FakeMetadataObject.MODEL); //$NON-NLS-1$
        
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        
        helpTestVisitor("pm1.g1.e1 IS NULL", modelID, metadata, capFinder, true, false); //$NON-NLS-1$
    }

    /**
     * Is null is not a comparison operation
     */
    @Test public void testIsNull4() {
        FakeMetadataFacade metadata = FakeMetadataFactory.example1();
        Object modelID = metadata.getStore().findObject("pm1", FakeMetadataObject.MODEL); //$NON-NLS-1$
        FakeMetadataObject e1 = metadata.getStore().findObject("pm1.g1.e1", FakeMetadataObject.ELEMENT); //$NON-NLS-1$
        e1.putProperty(FakeMetadataObject.Props.SEARCHABLE_COMPARE, Boolean.FALSE);
        
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setCapabilitySupport(Capability.CRITERIA_ISNULL, true);
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$
        
        helpTestVisitor("pm1.g1.e1 IS NULL", modelID, metadata, capFinder, true, false); //$NON-NLS-1$
    }
    
    @Test public void testIsNull6() {
        FakeMetadataFacade metadata = FakeMetadataFactory.example1Cached();
        Object modelID = metadata.getStore().findObject("pm1", FakeMetadataObject.MODEL); //$NON-NLS-1$
        
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setCapabilitySupport(Capability.CRITERIA_ISNULL, true);
        caps.setCapabilitySupport(Capability.CRITERIA_NOT, true);
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$
        
        helpTestVisitor("pm1.g1.e1 IS NOT NULL", modelID, metadata, capFinder, true, false); //$NON-NLS-1$
    }
    
    @Test public void testIsNull6fails() {
        FakeMetadataFacade metadata = FakeMetadataFactory.example1Cached();
        Object modelID = metadata.getStore().findObject("pm1", FakeMetadataObject.MODEL); //$NON-NLS-1$
        
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setCapabilitySupport(Capability.CRITERIA_ISNULL, true);
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$
        
        helpTestVisitor("pm1.g1.e1 IS NOT NULL", modelID, metadata, capFinder, false, false); //$NON-NLS-1$
    }

    // has all capabilities
    @Test public void testMatchCriteriaSuccess() {
        FakeMetadataFacade metadata = FakeMetadataFactory.example1Cached();
        Object modelID = metadata.getStore().findObject("pm1", FakeMetadataObject.MODEL); //$NON-NLS-1$
        
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setCapabilitySupport(Capability.CRITERIA_LIKE, true);
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$
        
        helpTestVisitor("pm1.g1.e1 LIKE 'x'", modelID, metadata, capFinder, true, false);         //$NON-NLS-1$
    }

    @Test public void testMatchCriteriaSuccess2() {
        FakeMetadataFacade metadata = FakeMetadataFactory.example1Cached();
        Object modelID = metadata.getStore().findObject("pm1", FakeMetadataObject.MODEL); //$NON-NLS-1$
        
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setCapabilitySupport(Capability.CRITERIA_LIKE, true);
        caps.setCapabilitySupport(Capability.CRITERIA_LIKE_ESCAPE, true);
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$
        
        helpTestVisitor("pm1.g1.e1 LIKE 'x' ESCAPE '#'", modelID, metadata, capFinder, true, false);         //$NON-NLS-1$
    }

    // Test for NOT LIKE
    @Test public void testMatchCriteriaSuccess3() {
        FakeMetadataFacade metadata = FakeMetadataFactory.example1Cached();
        Object modelID = metadata.getStore().findObject("pm1", FakeMetadataObject.MODEL); //$NON-NLS-1$
        
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setCapabilitySupport(Capability.CRITERIA_LIKE, true);
        caps.setCapabilitySupport(Capability.CRITERIA_NOT, true);
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$
        
        helpTestVisitor("pm1.g1.e1 NOT LIKE 'x'", modelID, metadata, capFinder, true, false);         //$NON-NLS-1$
    }
    
    @Test public void testMatchCriteriaSuccess3fails() {
        FakeMetadataFacade metadata = FakeMetadataFactory.example1Cached();
        Object modelID = metadata.getStore().findObject("pm1", FakeMetadataObject.MODEL); //$NON-NLS-1$
        
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setCapabilitySupport(Capability.CRITERIA_LIKE, true);
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$
        
        helpTestVisitor("pm1.g1.e1 NOT LIKE 'x'", modelID, metadata, capFinder, false, false);         //$NON-NLS-1$
    }

    // does not have compare capability
    @Test public void testMatchCriteriaCapFail1() {
        FakeMetadataFacade metadata = FakeMetadataFactory.example1Cached();
        Object modelID = metadata.getStore().findObject("pm1", FakeMetadataObject.MODEL); //$NON-NLS-1$
        
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setCapabilitySupport(Capability.CRITERIA_LIKE, false);
        caps.setCapabilitySupport(Capability.CRITERIA_LIKE_ESCAPE, true);
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$
        
        helpTestVisitor("pm1.g1.e1 LIKE 'x'", modelID, metadata, capFinder, false, false);         //$NON-NLS-1$
    }

    // does not have escape char capability
    @Test public void testMatchCriteriaCapFail2() {
        FakeMetadataFacade metadata = FakeMetadataFactory.example1Cached();
        Object modelID = metadata.getStore().findObject("pm1", FakeMetadataObject.MODEL); //$NON-NLS-1$
        
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setCapabilitySupport(Capability.CRITERIA_LIKE, true);
        caps.setCapabilitySupport(Capability.CRITERIA_LIKE_ESCAPE, false);
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$
        
        helpTestVisitor("pm1.g1.e1 LIKE 'x' ESCAPE '#'", modelID, metadata, capFinder, false, false);         //$NON-NLS-1$
    }
        
    // element not searchable
    @Test public void testMatchCriteriaMatchableFail() {
        FakeMetadataFacade metadata = FakeMetadataFactory.example1();
        Object modelID = metadata.getStore().findObject("pm1", FakeMetadataObject.MODEL); //$NON-NLS-1$
        FakeMetadataObject e1 = metadata.getStore().findObject("pm1.g1.e1", FakeMetadataObject.ELEMENT); //$NON-NLS-1$
        e1.putProperty(FakeMetadataObject.Props.SEARCHABLE_LIKE, Boolean.FALSE);
                
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setCapabilitySupport(Capability.CRITERIA_LIKE, true);
        caps.setCapabilitySupport(Capability.CRITERIA_LIKE_ESCAPE, true);
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$
        
        helpTestVisitor("pm1.g1.e1 LIKE 'x'", modelID, metadata, capFinder, false, false);         //$NON-NLS-1$
    }

    // no caps
    @Test public void testMatchCriteriaNoCaps() {
        FakeMetadataFacade metadata = FakeMetadataFactory.example1Cached();
        Object modelID = metadata.getStore().findObject("pm1", FakeMetadataObject.MODEL); //$NON-NLS-1$
                
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        
        helpTestVisitor("pm1.g1.e1 LIKE 'x'", modelID, metadata, capFinder, true, false);         //$NON-NLS-1$
    }    

    @Test public void testNotCriteria1() {
        FakeMetadataFacade metadata = FakeMetadataFactory.example1Cached();
        Object modelID = metadata.getStore().findObject("pm1", FakeMetadataObject.MODEL); //$NON-NLS-1$
                
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setCapabilitySupport(Capability.CRITERIA_NOT, true);
        caps.setCapabilitySupport(Capability.CRITERIA_COMPARE_EQ, true);
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$
        
        helpTestVisitor("NOT pm1.g1.e1 = 'x'", modelID, metadata, capFinder, true, false);                 //$NON-NLS-1$
    }

    @Test public void testNotCriteria2() {
        FakeMetadataFacade metadata = FakeMetadataFactory.example1Cached();
        Object modelID = metadata.getStore().findObject("pm1", FakeMetadataObject.MODEL); //$NON-NLS-1$
                
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setCapabilitySupport(Capability.CRITERIA_NOT, false);
        caps.setCapabilitySupport(Capability.CRITERIA_COMPARE_EQ, true);
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$
        
        helpTestVisitor("NOT pm1.g1.e1 = 'x'", modelID, metadata, capFinder, false, false);                 //$NON-NLS-1$
    }

    @Test public void testSetCriteria1() {
        FakeMetadataFacade metadata = FakeMetadataFactory.example1Cached();
        Object modelID = metadata.getStore().findObject("pm1", FakeMetadataObject.MODEL); //$NON-NLS-1$
                
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setCapabilitySupport(Capability.CRITERIA_IN, true);
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$
        
        helpTestVisitor("pm1.g1.e1 IN ('x')", modelID, metadata, capFinder, true, false);                 //$NON-NLS-1$
    }

    @Test public void testSetCriteria2() {
        FakeMetadataFacade metadata = FakeMetadataFactory.example1Cached();
        Object modelID = metadata.getStore().findObject("pm1", FakeMetadataObject.MODEL); //$NON-NLS-1$
                
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setCapabilitySupport(Capability.CRITERIA_IN, false);
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$
        
        helpTestVisitor("pm1.g1.e1 IN ('x')", modelID, metadata, capFinder, false, false);                 //$NON-NLS-1$
    }

    @Test public void testSetCriteria3() {
        FakeMetadataFacade metadata = FakeMetadataFactory.example1Cached();
        Object modelID = metadata.getStore().findObject("pm1", FakeMetadataObject.MODEL); //$NON-NLS-1$
                
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        
        helpTestVisitor("pm1.g1.e1 IN ('x')", modelID, metadata, capFinder, true, false);                 //$NON-NLS-1$
    }

    @Test public void testSetCriteria5() {
        FakeMetadataFacade metadata = FakeMetadataFactory.example1();
        FakeMetadataObject modelID = metadata.getStore().findObject("pm1", FakeMetadataObject.MODEL); //$NON-NLS-1$
        FakeMetadataObject elementID = metadata.getStore().findObject("pm1.g1.e1" , FakeMetadataObject.ELEMENT); //$NON-NLS-1$
        elementID.putProperty(FakeMetadataObject.Props.SEARCHABLE_COMPARE, Boolean.FALSE);
                
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setCapabilitySupport(Capability.CRITERIA_IN, true);
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$
        
        helpTestVisitor("pm1.g1.e1 IN ('x')", modelID, metadata, capFinder, false, false);                 //$NON-NLS-1$
    }

    //Test for success NOT IN
    @Test public void testSetCriteria7() {
        FakeMetadataFacade metadata = FakeMetadataFactory.example1Cached();
        Object modelID = metadata.getStore().findObject("pm1", FakeMetadataObject.MODEL); //$NON-NLS-1$
                
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setCapabilitySupport(Capability.CRITERIA_IN, true);
        caps.setCapabilitySupport(Capability.CRITERIA_NOT, true);
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$
        
        helpTestVisitor("pm1.g1.e1 NOT IN ('x')", modelID, metadata, capFinder, true, false);                 //$NON-NLS-1$
    }
    
    @Test public void testSetCriteria7fails() {
        FakeMetadataFacade metadata = FakeMetadataFactory.example1Cached();
        Object modelID = metadata.getStore().findObject("pm1", FakeMetadataObject.MODEL); //$NON-NLS-1$
                
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setCapabilitySupport(Capability.CRITERIA_IN, true);
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$
        
        helpTestVisitor("pm1.g1.e1 NOT IN ('x')", modelID, metadata, capFinder, false, false);                 //$NON-NLS-1$
    }
    
    @Test public void testSetCriteria8() {
        FakeMetadataFacade metadata = FakeMetadataFactory.example1Cached();
        Object modelID = metadata.getStore().findObject("pm1", FakeMetadataObject.MODEL); //$NON-NLS-1$
                
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setCapabilitySupport(Capability.CRITERIA_IN, true);
        caps.setSourceProperty(Capability.MAX_IN_CRITERIA_SIZE, new Integer(2));
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$
        
        helpTestVisitor("pm1.g1.e1 IN ('x', 'y', 'z')", modelID, metadata, capFinder, false, false);                 //$NON-NLS-1$
    }
    
    @Test public void testSetCriteria9() {
        FakeMetadataFacade metadata = FakeMetadataFactory.example1Cached();
        Object modelID = metadata.getStore().findObject("pm1", FakeMetadataObject.MODEL); //$NON-NLS-1$
                
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setCapabilitySupport(Capability.CRITERIA_IN, true);
        caps.setSourceProperty(Capability.MAX_IN_CRITERIA_SIZE, new Integer(2));
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$
        
        helpTestVisitor("pm1.g1.e1 IN ('x', 'y')", modelID, metadata, capFinder, true, false);                 //$NON-NLS-1$
    }

    @Test public void testSubquerySetCriteria() {
        FakeMetadataFacade metadata = FakeMetadataFactory.example1Cached();
        Object modelID = metadata.getStore().findObject("pm1", FakeMetadataObject.MODEL); //$NON-NLS-1$
                
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setCapabilitySupport(Capability.CRITERIA_IN, true);
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$
        
        helpTestVisitor("pm1.g1.e1 IN (SELECT 'xyz' FROM pm1.g1)", modelID, metadata, capFinder, false, false);                 //$NON-NLS-1$
    }
    
    @Test public void testSearchCase() {
        FakeMetadataFacade metadata = FakeMetadataFactory.example1Cached();
        Object modelID = metadata.getStore().findObject("pm1", FakeMetadataObject.MODEL); //$NON-NLS-1$
                
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setCapabilitySupport(Capability.QUERY_SEARCHED_CASE, true);
        caps.setCapabilitySupport(Capability.CRITERIA_COMPARE_EQ, true);
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$
        
        helpTestVisitor("pm1.g1.e1 = case when pm1.g1.e2 = 1 then 1 else 2 end", modelID, metadata, capFinder, false, false);                 //$NON-NLS-1$
    }
    
    // has all capabilities
    @Test public void testSubqueryCompareCriteriaSuccess() {
        FakeMetadataFacade metadata = FakeMetadataFactory.example1Cached();
        Object modelID = metadata.getStore().findObject("pm1", FakeMetadataObject.MODEL); //$NON-NLS-1$
        
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setCapabilitySupport(Capability.CRITERIA_COMPARE_EQ, true);        
        caps.setCapabilitySupport(Capability.CRITERIA_QUANTIFIED_SOME, true);
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$
        
        helpTestVisitorWithCommand("SELECT e1 FROM pm1.g1 WHERE pm1.g1.e1 = ANY (SELECT e1 FROM pm1.g2)", modelID, metadata, capFinder, true, false);         //$NON-NLS-1$
    }

    // does not have where capability
    @Test public void testSubqueryCompareCriteriaCapFail1() {
        FakeMetadataFacade metadata = FakeMetadataFactory.example1Cached();
        Object modelID = metadata.getStore().findObject("pm1", FakeMetadataObject.MODEL); //$NON-NLS-1$
        
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setCapabilitySupport(Capability.CRITERIA_COMPARE_EQ, true);
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$
        
        helpTestVisitorWithCommand("SELECT e1 FROM pm1.g1 WHERE pm1.g1.e1 = ANY (SELECT e1 FROM pm1.g2)", modelID, metadata, capFinder, false, false);         //$NON-NLS-1$
    }

    // does not have compare capability
    @Test public void testSubqueryCompareCriteriaCapFail2() {
        FakeMetadataFacade metadata = FakeMetadataFactory.example1Cached();
        Object modelID = metadata.getStore().findObject("pm1", FakeMetadataObject.MODEL); //$NON-NLS-1$
        
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setCapabilitySupport(Capability.CRITERIA_COMPARE_EQ, true);
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$
        
        helpTestVisitorWithCommand("SELECT e1 FROM pm1.g1 WHERE pm1.g1.e1 = ANY (SELECT e1 FROM pm1.g2)", modelID, metadata, capFinder, false, false);         //$NON-NLS-1$
    }

    // does not have subquery capability
    @Test public void testSubqueryCompareCriteriaFail3() {
        FakeMetadataFacade metadata = FakeMetadataFactory.example1Cached();
        Object modelID = metadata.getStore().findObject("pm1", FakeMetadataObject.MODEL); //$NON-NLS-1$
        
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setCapabilitySupport(Capability.CRITERIA_QUANTIFIED_SOME, false);
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$
        
        helpTestVisitorWithCommand("SELECT e1 FROM pm1.g1 WHERE pm1.g1.e1 = ANY (SELECT e1 FROM pm1.g2)", modelID, metadata, capFinder, false, false);         //$NON-NLS-1$
    }

    // does not have quantified subquery comparison capability
    @Test public void testSubqueryCompareCriteriaFail4() {
        FakeMetadataFacade metadata = FakeMetadataFactory.example1Cached();
        Object modelID = metadata.getStore().findObject("pm1", FakeMetadataObject.MODEL); //$NON-NLS-1$
        
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setCapabilitySupport(Capability.CRITERIA_COMPARE_EQ, true);
        caps.setCapabilitySupport(Capability.QUERY_SUBQUERIES_SCALAR, true);
        caps.setCapabilitySupport(Capability.CRITERIA_QUANTIFIED_SOME, false);
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$
        
        helpTestVisitorWithCommand("SELECT e1 FROM pm1.g1 WHERE pm1.g1.e1 = ANY (SELECT e1 FROM pm1.g2)", modelID, metadata, capFinder, false, false);         //$NON-NLS-1$
    }

    // does not have quantified subquery comparison capability for ANY
    @Test public void testSubqueryCompareCriteriaFail5() {
        FakeMetadataFacade metadata = FakeMetadataFactory.example1Cached();
        Object modelID = metadata.getStore().findObject("pm1", FakeMetadataObject.MODEL); //$NON-NLS-1$
        
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
    @Test public void testSubqueryCompareCriteriaFail6() {
        FakeMetadataFacade metadata = FakeMetadataFactory.example1Cached();
        Object modelID = metadata.getStore().findObject("pm1", FakeMetadataObject.MODEL); //$NON-NLS-1$
        
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
    @Test public void testSubqueryCompareCriteriaOpCapFail1() {
        FakeMetadataFacade metadata = FakeMetadataFactory.example1Cached();
        Object modelID = metadata.getStore().findObject("pm1", FakeMetadataObject.MODEL); //$NON-NLS-1$
        
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setCapabilitySupport(Capability.CRITERIA_COMPARE_EQ, false);
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$
        
        helpTestVisitorWithCommand("SELECT e1 FROM pm1.g1 WHERE pm1.g1.e1 = ANY (SELECT e1 FROM pm1.g2)", modelID, metadata, capFinder, false, false);         //$NON-NLS-1$
    }
    
    // does not have <> capability
    @Test public void testSubqueryCompareCriteriaOpCapFail2() {
        FakeMetadataFacade metadata = FakeMetadataFactory.example1Cached();
        Object modelID = metadata.getStore().findObject("pm1", FakeMetadataObject.MODEL); //$NON-NLS-1$
        
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setCapabilitySupport(Capability.CRITERIA_COMPARE_EQ, false);
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$
        
        helpTestVisitorWithCommand("SELECT e1 FROM pm1.g1 WHERE pm1.g1.e1 <> ANY (SELECT e1 FROM pm1.g2)", modelID, metadata, capFinder, false, false);         //$NON-NLS-1$
    }

    // does not have < capability
    @Test public void testSubqueryCompareCriteriaOpCapFail3() {
        FakeMetadataFacade metadata = FakeMetadataFactory.example1Cached();
        Object modelID = metadata.getStore().findObject("pm1", FakeMetadataObject.MODEL); //$NON-NLS-1$
        
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setCapabilitySupport(Capability.CRITERIA_COMPARE_ORDERED, false);
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$
        
        helpTestVisitorWithCommand("SELECT e1 FROM pm1.g1 WHERE pm1.g1.e1 < ANY (SELECT e1 FROM pm1.g2)", modelID, metadata, capFinder, false, false);         //$NON-NLS-1$
    }

    // does not have <= capability
    @Test public void testSubqueryCompareCriteriaOpCapFail4() {
        FakeMetadataFacade metadata = FakeMetadataFactory.example1Cached();
        Object modelID = metadata.getStore().findObject("pm1", FakeMetadataObject.MODEL); //$NON-NLS-1$
        
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setCapabilitySupport(Capability.CRITERIA_COMPARE_ORDERED, false);
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$
        
        helpTestVisitorWithCommand("SELECT e1 FROM pm1.g1 WHERE pm1.g1.e1 <= ANY (SELECT e1 FROM pm1.g2)", modelID, metadata, capFinder, false, false);         //$NON-NLS-1$
    }

    // does not have > capability
    @Test public void testSubqueryCompareCriteriaOpCapFail5() {
        FakeMetadataFacade metadata = FakeMetadataFactory.example1Cached();
        Object modelID = metadata.getStore().findObject("pm1", FakeMetadataObject.MODEL); //$NON-NLS-1$
        
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setCapabilitySupport(Capability.CRITERIA_COMPARE_ORDERED, false);
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$
        
        helpTestVisitorWithCommand("SELECT e1 FROM pm1.g1 WHERE pm1.g1.e1 > ANY (SELECT e1 FROM pm1.g2)", modelID, metadata, capFinder, false, false);         //$NON-NLS-1$
    }

    // does not have >= capability
    @Test public void testSubqueryCompareCriteriaOpCapFail6() {
        FakeMetadataFacade metadata = FakeMetadataFactory.example1Cached();
        Object modelID = metadata.getStore().findObject("pm1", FakeMetadataObject.MODEL); //$NON-NLS-1$
        
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setCapabilitySupport(Capability.CRITERIA_COMPARE_ORDERED, false);
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$
        
        helpTestVisitorWithCommand("SELECT e1 FROM pm1.g1 WHERE pm1.g1.e1 >= ANY (SELECT e1 FROM pm1.g2)", modelID, metadata, capFinder, false, false);         //$NON-NLS-1$
    }
    
    // element not searchable
    @Test public void testSubqueryCompareCriteriaSearchableFail() {
        FakeMetadataFacade metadata = FakeMetadataFactory.example1();
        Object modelID = metadata.getStore().findObject("pm1", FakeMetadataObject.MODEL); //$NON-NLS-1$
        FakeMetadataObject e1 = metadata.getStore().findObject("pm1.g1.e1", FakeMetadataObject.ELEMENT); //$NON-NLS-1$
        e1.putProperty(FakeMetadataObject.Props.SEARCHABLE_COMPARE, Boolean.FALSE);
                
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setCapabilitySupport(Capability.CRITERIA_COMPARE_EQ, true);
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$
        
        helpTestVisitorWithCommand("SELECT e1 FROM pm1.g1 WHERE pm1.g1.e1 = ANY (SELECT e1 FROM pm1.g2)", modelID, metadata, capFinder, false, false);         //$NON-NLS-1$
    }

    @Test public void testExistsCriteria1() {
        FakeMetadataFacade metadata = FakeMetadataFactory.example1Cached();
        Object modelID = metadata.getStore().findObject("pm1", FakeMetadataObject.MODEL); //$NON-NLS-1$
        
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setCapabilitySupport(Capability.CRITERIA_EXISTS, true);
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$
        
        helpTestVisitorWithCommand("SELECT e1 FROM pm1.g1 WHERE EXISTS(SELECT e1 FROM pm1.g2)", modelID, metadata, capFinder, true, false); //$NON-NLS-1$
    }

    @Test public void testExistsCriteria2() {
        FakeMetadataFacade metadata = FakeMetadataFactory.example1Cached();
        Object modelID = metadata.getStore().findObject("pm1", FakeMetadataObject.MODEL); //$NON-NLS-1$
        
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setCapabilitySupport(Capability.CRITERIA_EXISTS, false);
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$
        
        helpTestVisitorWithCommand("SELECT e1 FROM pm1.g1 WHERE EXISTS(SELECT e1 FROM pm1.g2)", modelID, metadata, capFinder, false, false); //$NON-NLS-1$
    }

    @Test public void testExistsCriteria5() {
        FakeMetadataFacade metadata = FakeMetadataFactory.example1Cached();
        Object modelID = metadata.getStore().findObject("pm1", FakeMetadataObject.MODEL); //$NON-NLS-1$
        
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setCapabilitySupport(Capability.CRITERIA_EXISTS, true);
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$
        
        helpTestVisitorWithCommand("SELECT e1 FROM pm1.g1 WHERE EXISTS(SELECT e1 FROM pm1.g2)", modelID, metadata, capFinder, true, false); //$NON-NLS-1$
    }    
}
