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

import junit.framework.TestCase;

import com.metamatrix.api.exception.MetaMatrixException;
import com.metamatrix.query.optimizer.capabilities.BasicSourceCapabilities;
import com.metamatrix.query.optimizer.capabilities.CapabilitiesFinder;
import com.metamatrix.query.optimizer.capabilities.FakeCapabilitiesFinder;
import com.metamatrix.query.optimizer.capabilities.SourceCapabilities.Capability;
import com.metamatrix.query.parser.QueryParser;
import com.metamatrix.query.resolver.QueryResolver;
import com.metamatrix.query.sql.lang.Criteria;
import com.metamatrix.query.sql.lang.Query;
import com.metamatrix.query.sql.navigator.PreOrderNavigator;
import com.metamatrix.query.unittest.FakeMetadataFacade;
import com.metamatrix.query.unittest.FakeMetadataFactory;
import com.metamatrix.query.unittest.FakeMetadataObject;

/**
 */
public class TestCriteriaCapabilityValidatorVisitor extends TestCase {

    /**
     * Constructor for TestCriteriaCapabilityValidatorVisitor.
     * @param name
     */
    public TestCriteriaCapabilityValidatorVisitor(String name) {
        super(name);
    }


    public void helpTestVisitor(String sql, Object modelID, FakeMetadataFacade metadata, CapabilitiesFinder capFinder, boolean isValid, boolean expectException) {
        try {
            Criteria criteria = QueryParser.getQueryParser().parseCriteria(sql);
            
            QueryResolver.resolveCriteria(criteria, metadata);
                        
            CriteriaCapabilityValidatorVisitor visitor = new CriteriaCapabilityValidatorVisitor(modelID, metadata, capFinder);
            PreOrderNavigator.doVisit(criteria, visitor);
            
            assertEquals("Got incorrect isValid flag", isValid, visitor.isValid()); //$NON-NLS-1$
            assertEquals("Got incorrect exception", expectException, visitor.getException() != null); //$NON-NLS-1$
            
        } catch(MetaMatrixException e) {
            fail(e.getMessage());    
        }
    }

    // Assume there is a wrapped command - this will allow subqueries to be properly resolved
    public void helpTestVisitorWithCommand(String sql, Object modelID, FakeMetadataFacade metadata, CapabilitiesFinder capFinder, boolean isValid, boolean expectException) {
        try {
            QueryParser parser = new QueryParser();
            Query query = (Query) parser.parseCommand(sql);
            
            QueryResolver.resolveCommand(query, metadata);
                        
            CriteriaCapabilityValidatorVisitor visitor = new CriteriaCapabilityValidatorVisitor(modelID, metadata, capFinder);
            PreOrderNavigator.doVisit(query.getCriteria(), visitor);
            
            assertEquals("Got incorrect isValid flag", isValid, visitor.isValid()); //$NON-NLS-1$
            assertEquals("Got incorrect exception", expectException, visitor.getException() != null); //$NON-NLS-1$
            
        } catch(MetaMatrixException e) {
            fail(e.getMessage());    
        }
    }
    
    // has all capabilities
    public void testCompareCriteriaSuccess() {
        FakeMetadataFacade metadata = FakeMetadataFactory.example1Cached();
        Object modelID = metadata.getStore().findObject("pm1", FakeMetadataObject.MODEL); //$NON-NLS-1$
        
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setCapabilitySupport(Capability.QUERY_WHERE, true);
        caps.setCapabilitySupport(Capability.QUERY_WHERE_COMPARE, true);
        caps.setCapabilitySupport(Capability.QUERY_WHERE_COMPARE_EQ, true);
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$
        
        helpTestVisitor("pm1.g1.e1 = 'x'", modelID, metadata, capFinder, true, false);         //$NON-NLS-1$
    }

    // does not have where capability
    public void testCompareCriteriaCapFail1() {
        FakeMetadataFacade metadata = FakeMetadataFactory.example1Cached();
        Object modelID = metadata.getStore().findObject("pm1", FakeMetadataObject.MODEL); //$NON-NLS-1$
        
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setCapabilitySupport(Capability.QUERY_WHERE, false);
        caps.setCapabilitySupport(Capability.QUERY_WHERE_COMPARE, true);
        caps.setCapabilitySupport(Capability.QUERY_WHERE_COMPARE_EQ, true);
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$
        
        helpTestVisitor("pm1.g1.e1 = 'x'", modelID, metadata, capFinder, false, false);         //$NON-NLS-1$
    }

    // does not have compare capability
    public void testCompareCriteriaCapFail2() {
        FakeMetadataFacade metadata = FakeMetadataFactory.example1Cached();
        Object modelID = metadata.getStore().findObject("pm1", FakeMetadataObject.MODEL); //$NON-NLS-1$
        
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setCapabilitySupport(Capability.QUERY_WHERE, true);
        caps.setCapabilitySupport(Capability.QUERY_WHERE_COMPARE, false);
        caps.setCapabilitySupport(Capability.QUERY_WHERE_COMPARE_EQ, true);
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$
        
        helpTestVisitor("pm1.g1.e1 = 'x'", modelID, metadata, capFinder, false, false);         //$NON-NLS-1$
    }

    // does not have = capability
    public void testCompareCriteriaOpCapFail1() {
        FakeMetadataFacade metadata = FakeMetadataFactory.example1Cached();
        Object modelID = metadata.getStore().findObject("pm1", FakeMetadataObject.MODEL); //$NON-NLS-1$
        
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setCapabilitySupport(Capability.QUERY_WHERE, true);
        caps.setCapabilitySupport(Capability.QUERY_WHERE_COMPARE, true);
        caps.setCapabilitySupport(Capability.QUERY_WHERE_COMPARE_EQ, false);
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$
        
        helpTestVisitor("pm1.g1.e1 = 'x'", modelID, metadata, capFinder, false, false);         //$NON-NLS-1$
    }
    
    // does not have <> capability
    public void testCompareCriteriaOpCapFail2() {
        FakeMetadataFacade metadata = FakeMetadataFactory.example1Cached();
        Object modelID = metadata.getStore().findObject("pm1", FakeMetadataObject.MODEL); //$NON-NLS-1$
        
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setCapabilitySupport(Capability.QUERY_WHERE, true);
        caps.setCapabilitySupport(Capability.QUERY_WHERE_COMPARE, true);
        caps.setCapabilitySupport(Capability.QUERY_WHERE_COMPARE_NE, false);
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$
        
        helpTestVisitor("pm1.g1.e1 <> 'x'", modelID, metadata, capFinder, false, false);         //$NON-NLS-1$
    }

    // does not have < capability
    public void testCompareCriteriaOpCapFail3() {
        FakeMetadataFacade metadata = FakeMetadataFactory.example1Cached();
        Object modelID = metadata.getStore().findObject("pm1", FakeMetadataObject.MODEL); //$NON-NLS-1$
        
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setCapabilitySupport(Capability.QUERY_WHERE, true);
        caps.setCapabilitySupport(Capability.QUERY_WHERE_COMPARE, true);
        caps.setCapabilitySupport(Capability.QUERY_WHERE_COMPARE_LT, false);
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$
        
        helpTestVisitor("pm1.g1.e1 < 'x'", modelID, metadata, capFinder, false, false);         //$NON-NLS-1$
    }

    // does not have <= capability
    public void testCompareCriteriaOpCapFail4() {
        FakeMetadataFacade metadata = FakeMetadataFactory.example1Cached();
        Object modelID = metadata.getStore().findObject("pm1", FakeMetadataObject.MODEL); //$NON-NLS-1$
        
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setCapabilitySupport(Capability.QUERY_WHERE, true);
        caps.setCapabilitySupport(Capability.QUERY_WHERE_COMPARE, true);
        caps.setCapabilitySupport(Capability.QUERY_WHERE_COMPARE_LE, false);
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$
        
        helpTestVisitor("pm1.g1.e1 <= 'x'", modelID, metadata, capFinder, false, false);         //$NON-NLS-1$
    }

    // does not have > capability
    public void testCompareCriteriaOpCapFail5() {
        FakeMetadataFacade metadata = FakeMetadataFactory.example1Cached();
        Object modelID = metadata.getStore().findObject("pm1", FakeMetadataObject.MODEL); //$NON-NLS-1$
        
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setCapabilitySupport(Capability.QUERY_WHERE, true);
        caps.setCapabilitySupport(Capability.QUERY_WHERE_COMPARE, true);
        caps.setCapabilitySupport(Capability.QUERY_WHERE_COMPARE_GT, false);
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$
        
        helpTestVisitor("pm1.g1.e1 > 'x'", modelID, metadata, capFinder, false, false);         //$NON-NLS-1$
    }

    // does not have >= capability
    public void testCompareCriteriaOpCapFail6() {
        FakeMetadataFacade metadata = FakeMetadataFactory.example1Cached();
        Object modelID = metadata.getStore().findObject("pm1", FakeMetadataObject.MODEL); //$NON-NLS-1$
        
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setCapabilitySupport(Capability.QUERY_WHERE, true);
        caps.setCapabilitySupport(Capability.QUERY_WHERE_COMPARE, true);
        caps.setCapabilitySupport(Capability.QUERY_WHERE_COMPARE_GE, false);
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$
        
        helpTestVisitor("pm1.g1.e1 >= 'x'", modelID, metadata, capFinder, false, false);         //$NON-NLS-1$
    }
    
    // element not searchable
    public void testCompareCriteriaSearchableFail() {
        FakeMetadataFacade metadata = FakeMetadataFactory.example1();
        Object modelID = metadata.getStore().findObject("pm1", FakeMetadataObject.MODEL); //$NON-NLS-1$
        FakeMetadataObject e1 = metadata.getStore().findObject("pm1.g1.e1", FakeMetadataObject.ELEMENT); //$NON-NLS-1$
        e1.putProperty(FakeMetadataObject.Props.SEARCHABLE_COMPARE, Boolean.FALSE);
                
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setCapabilitySupport(Capability.QUERY_WHERE, true);
        caps.setCapabilitySupport(Capability.QUERY_WHERE_COMPARE, true);
        caps.setCapabilitySupport(Capability.QUERY_WHERE_COMPARE_EQ, true);
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$
        
        helpTestVisitor("pm1.g1.e1 = 'x'", modelID, metadata, capFinder, false, false);         //$NON-NLS-1$
    }

    // no caps
    public void testCompareCriteriaNoCaps() {
        FakeMetadataFacade metadata = FakeMetadataFactory.example1Cached();
        Object modelID = metadata.getStore().findObject("pm1", FakeMetadataObject.MODEL); //$NON-NLS-1$
                
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        
        helpTestVisitor("pm1.g1.e1 = 'x'", modelID, metadata, capFinder, true, false);         //$NON-NLS-1$
    }
    
    public void testCompoundCriteriaAnd1() {
        FakeMetadataFacade metadata = FakeMetadataFactory.example1Cached();
        Object modelID = metadata.getStore().findObject("pm1", FakeMetadataObject.MODEL); //$NON-NLS-1$
                
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setCapabilitySupport(Capability.QUERY_WHERE, true);
        caps.setCapabilitySupport(Capability.QUERY_WHERE_COMPARE, true);
        caps.setCapabilitySupport(Capability.QUERY_WHERE_COMPARE_EQ, true);
        caps.setCapabilitySupport(Capability.QUERY_WHERE_AND, true);
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$
        
        helpTestVisitor("pm1.g1.e1 = 'x' AND 0 = 1", modelID, metadata, capFinder, true, false);                 //$NON-NLS-1$
    }

    public void testCompoundCriteriaAnd2() {
        FakeMetadataFacade metadata = FakeMetadataFactory.example1Cached();
        Object modelID = metadata.getStore().findObject("pm1", FakeMetadataObject.MODEL); //$NON-NLS-1$
                
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setCapabilitySupport(Capability.QUERY_WHERE, true);
        caps.setCapabilitySupport(Capability.QUERY_WHERE_COMPARE, true);
        caps.setCapabilitySupport(Capability.QUERY_WHERE_COMPARE_EQ, true);
        caps.setCapabilitySupport(Capability.QUERY_WHERE_AND, false);
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$
        
        helpTestVisitor("pm1.g1.e1 = 'x' AND 0 = 1", modelID, metadata, capFinder, false, false);                 //$NON-NLS-1$
    }

    public void testCompoundCriteriaAnd4() {
        FakeMetadataFacade metadata = FakeMetadataFactory.example1Cached();
        Object modelID = metadata.getStore().findObject("pm1", FakeMetadataObject.MODEL); //$NON-NLS-1$
                
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        
        helpTestVisitor("pm1.g1.e1 = 'x' AND 0 = 1", modelID, metadata, capFinder, true, false);                 //$NON-NLS-1$
    }

    public void testCompoundCriteriaOr1() {
        FakeMetadataFacade metadata = FakeMetadataFactory.example1Cached();
        Object modelID = metadata.getStore().findObject("pm1", FakeMetadataObject.MODEL); //$NON-NLS-1$
                
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setCapabilitySupport(Capability.QUERY_WHERE, true);
        caps.setCapabilitySupport(Capability.QUERY_WHERE_COMPARE, true);
        caps.setCapabilitySupport(Capability.QUERY_WHERE_COMPARE_EQ, true);
        caps.setCapabilitySupport(Capability.QUERY_WHERE_OR, true);
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$
        
        helpTestVisitor("pm1.g1.e1 = 'x' OR 0 = 1", modelID, metadata, capFinder, true, false);                 //$NON-NLS-1$
    }

    public void testCompoundCriteriaOr2() {
        FakeMetadataFacade metadata = FakeMetadataFactory.example1Cached();
        Object modelID = metadata.getStore().findObject("pm1", FakeMetadataObject.MODEL); //$NON-NLS-1$
                
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setCapabilitySupport(Capability.QUERY_WHERE, true);
        caps.setCapabilitySupport(Capability.QUERY_WHERE_COMPARE, true);
        caps.setCapabilitySupport(Capability.QUERY_WHERE_COMPARE_EQ, true);
        caps.setCapabilitySupport(Capability.QUERY_WHERE_OR, false);
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$
        
        helpTestVisitor("pm1.g1.e1 = 'x' OR 0 = 1", modelID, metadata, capFinder, false, false);                 //$NON-NLS-1$
    }

    public void testCompoundCriteriaOr4() {
        FakeMetadataFacade metadata = FakeMetadataFactory.example1Cached();
        FakeMetadataObject modelID = metadata.getStore().findObject("pm1", FakeMetadataObject.MODEL); //$NON-NLS-1$
                
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        
        helpTestVisitor("pm1.g1.e1 = 'x' OR 0 = 1", modelID, metadata, capFinder, true, false);                 //$NON-NLS-1$
    }

    public void testScalarFunction1() {
        FakeMetadataFacade metadata = FakeMetadataFactory.example1Cached();
        Object modelID = metadata.getStore().findObject("pm1", FakeMetadataObject.MODEL); //$NON-NLS-1$
        
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setCapabilitySupport(Capability.QUERY_WHERE, true);
        caps.setCapabilitySupport(Capability.QUERY_WHERE_COMPARE, true);
        caps.setCapabilitySupport(Capability.QUERY_WHERE_COMPARE_EQ, true);
        caps.setCapabilitySupport(Capability.FUNCTION, true);
        caps.setFunctionSupport("curtime", true); //$NON-NLS-1$
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$
        
        helpTestVisitor("curtime() = {t'10:00:00'}", modelID, metadata, capFinder, true, false); //$NON-NLS-1$
    }

    /** 
     * Since this will always get pre-evaluated, this should also be true 
     *  
     */ 
    public void testScalarFunction2() {
        FakeMetadataFacade metadata = FakeMetadataFactory.example1Cached();
        Object modelID = metadata.getStore().findObject("pm1", FakeMetadataObject.MODEL); //$NON-NLS-1$
        
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setCapabilitySupport(Capability.QUERY_WHERE, true);
        caps.setCapabilitySupport(Capability.QUERY_WHERE_COMPARE, true);
        caps.setCapabilitySupport(Capability.QUERY_WHERE_COMPARE_EQ, true);
        caps.setCapabilitySupport(Capability.FUNCTION, true);
        caps.setFunctionSupport("+", false); //$NON-NLS-1$
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$
        
        helpTestVisitor("1 + 1 = 2", modelID, metadata, capFinder, true, false); //$NON-NLS-1$
    }
    
    /**
     * since curtime is non-deterministic and not supported, it will not be pushed down.
     */
    public void testScalarFunction2a() {
        FakeMetadataFacade metadata = FakeMetadataFactory.example1Cached();
        Object modelID = metadata.getStore().findObject("pm1", FakeMetadataObject.MODEL); //$NON-NLS-1$
        
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setCapabilitySupport(Capability.QUERY_WHERE, true);
        caps.setCapabilitySupport(Capability.QUERY_WHERE_COMPARE, true);
        caps.setCapabilitySupport(Capability.QUERY_WHERE_COMPARE_EQ, true);
        caps.setCapabilitySupport(Capability.FUNCTION, true);
        caps.setFunctionSupport("curtime", false); //$NON-NLS-1$
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$
        
        helpTestVisitor("curtime() = '{t'10:00:00'}", modelID, metadata, capFinder, false, false); //$NON-NLS-1$
    }
    
    /**
     * since rand is command deterministic and not supported, it will be evaluated
     */
    public void testScalarFunction2b() {
        FakeMetadataFacade metadata = FakeMetadataFactory.example1Cached();
        Object modelID = metadata.getStore().findObject("pm1", FakeMetadataObject.MODEL); //$NON-NLS-1$
        
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setCapabilitySupport(Capability.QUERY_WHERE, true);
        caps.setCapabilitySupport(Capability.QUERY_WHERE_COMPARE, true);
        caps.setCapabilitySupport(Capability.QUERY_WHERE_COMPARE_EQ, true);
        caps.setCapabilitySupport(Capability.FUNCTION, true);
        caps.setFunctionSupport("rand", true); //$NON-NLS-1$
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$
        
        helpTestVisitor("rand() = '1.0'", modelID, metadata, capFinder, true, false); //$NON-NLS-1$
    }

    
    public void testIsNull1() {
        FakeMetadataFacade metadata = FakeMetadataFactory.example1Cached();
        Object modelID = metadata.getStore().findObject("pm1", FakeMetadataObject.MODEL); //$NON-NLS-1$
        
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setCapabilitySupport(Capability.QUERY_WHERE, true);
        caps.setCapabilitySupport(Capability.QUERY_WHERE_ISNULL, true);
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$
        
        helpTestVisitor("pm1.g1.e1 IS NULL", modelID, metadata, capFinder, true, false); //$NON-NLS-1$
    }

    public void testIsNull2() {
        FakeMetadataFacade metadata = FakeMetadataFactory.example1Cached();
        Object modelID = metadata.getStore().findObject("pm1", FakeMetadataObject.MODEL); //$NON-NLS-1$
        
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setCapabilitySupport(Capability.QUERY_WHERE, true);
        caps.setCapabilitySupport(Capability.QUERY_WHERE_ISNULL, false);
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$
        
        helpTestVisitor("pm1.g1.e1 IS NULL", modelID, metadata, capFinder, false, false); //$NON-NLS-1$
    }

    public void testIsNull3() {
        FakeMetadataFacade metadata = FakeMetadataFactory.example1Cached();
        Object modelID = metadata.getStore().findObject("pm1", FakeMetadataObject.MODEL); //$NON-NLS-1$
        
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        
        helpTestVisitor("pm1.g1.e1 IS NULL", modelID, metadata, capFinder, true, false); //$NON-NLS-1$
    }

    public void testIsNull4() {
        FakeMetadataFacade metadata = FakeMetadataFactory.example1();
        Object modelID = metadata.getStore().findObject("pm1", FakeMetadataObject.MODEL); //$NON-NLS-1$
        FakeMetadataObject e1 = metadata.getStore().findObject("pm1.g1.e1", FakeMetadataObject.ELEMENT); //$NON-NLS-1$
        e1.putProperty(FakeMetadataObject.Props.SEARCHABLE_COMPARE, Boolean.FALSE);
        
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setCapabilitySupport(Capability.QUERY_WHERE, true);
        caps.setCapabilitySupport(Capability.QUERY_WHERE_ISNULL, true);
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$
        
        helpTestVisitor("pm1.g1.e1 IS NULL", modelID, metadata, capFinder, false, false); //$NON-NLS-1$
    }
    
    public void testIsNull5() {
        FakeMetadataFacade metadata = FakeMetadataFactory.example1Cached();
        Object modelID = metadata.getStore().findObject("pm1", FakeMetadataObject.MODEL); //$NON-NLS-1$
        
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setCapabilitySupport(Capability.QUERY_WHERE, false);
        caps.setCapabilitySupport(Capability.QUERY_WHERE_ISNULL, true);
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$
        
        helpTestVisitor("pm1.g1.e1 IS NULL", modelID, metadata, capFinder, false, false); //$NON-NLS-1$
    }

    public void testIsNull6() {
        FakeMetadataFacade metadata = FakeMetadataFactory.example1Cached();
        Object modelID = metadata.getStore().findObject("pm1", FakeMetadataObject.MODEL); //$NON-NLS-1$
        
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setCapabilitySupport(Capability.QUERY_WHERE, true);
        caps.setCapabilitySupport(Capability.QUERY_WHERE_ISNULL, true);
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$
        
        helpTestVisitor("pm1.g1.e1 IS NOT NULL", modelID, metadata, capFinder, true, false); //$NON-NLS-1$
    }

    // has all capabilities
    public void testMatchCriteriaSuccess() {
        FakeMetadataFacade metadata = FakeMetadataFactory.example1Cached();
        Object modelID = metadata.getStore().findObject("pm1", FakeMetadataObject.MODEL); //$NON-NLS-1$
        
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setCapabilitySupport(Capability.QUERY_WHERE, true);
        caps.setCapabilitySupport(Capability.QUERY_WHERE_LIKE, true);
        caps.setCapabilitySupport(Capability.QUERY_WHERE_LIKE_ESCAPE, true);
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$
        
        helpTestVisitor("pm1.g1.e1 LIKE 'x'", modelID, metadata, capFinder, true, false);         //$NON-NLS-1$
    }

    // does not have escape char capability
    public void testMatchCriteriaSuccess2() {
        FakeMetadataFacade metadata = FakeMetadataFactory.example1Cached();
        Object modelID = metadata.getStore().findObject("pm1", FakeMetadataObject.MODEL); //$NON-NLS-1$
        
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setCapabilitySupport(Capability.QUERY_WHERE, true);
        caps.setCapabilitySupport(Capability.QUERY_WHERE_LIKE, true);
        caps.setCapabilitySupport(Capability.QUERY_WHERE_LIKE_ESCAPE, true);
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$
        
        helpTestVisitor("pm1.g1.e1 LIKE 'x' ESCAPE '#'", modelID, metadata, capFinder, true, false);         //$NON-NLS-1$
    }

    // Test for NOT LIKE
    public void testMatchCriteriaSuccess3() {
        FakeMetadataFacade metadata = FakeMetadataFactory.example1Cached();
        Object modelID = metadata.getStore().findObject("pm1", FakeMetadataObject.MODEL); //$NON-NLS-1$
        
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setCapabilitySupport(Capability.QUERY_WHERE, true);
        caps.setCapabilitySupport(Capability.QUERY_WHERE_LIKE, true);
        caps.setCapabilitySupport(Capability.QUERY_WHERE_NOT, true);
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$
        
        helpTestVisitor("pm1.g1.e1 NOT LIKE 'x'", modelID, metadata, capFinder, true, false);         //$NON-NLS-1$
    }

    // does not have compare capability
    public void testMatchCriteriaCapFail1() {
        FakeMetadataFacade metadata = FakeMetadataFactory.example1Cached();
        Object modelID = metadata.getStore().findObject("pm1", FakeMetadataObject.MODEL); //$NON-NLS-1$
        
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setCapabilitySupport(Capability.QUERY_WHERE, true);
        caps.setCapabilitySupport(Capability.QUERY_WHERE_LIKE, false);
        caps.setCapabilitySupport(Capability.QUERY_WHERE_LIKE_ESCAPE, true);
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$
        
        helpTestVisitor("pm1.g1.e1 LIKE 'x'", modelID, metadata, capFinder, false, false);         //$NON-NLS-1$
    }

    // does not have escape char capability
    public void testMatchCriteriaCapFail2() {
        FakeMetadataFacade metadata = FakeMetadataFactory.example1Cached();
        Object modelID = metadata.getStore().findObject("pm1", FakeMetadataObject.MODEL); //$NON-NLS-1$
        
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setCapabilitySupport(Capability.QUERY_WHERE, true);
        caps.setCapabilitySupport(Capability.QUERY_WHERE_LIKE, true);
        caps.setCapabilitySupport(Capability.QUERY_WHERE_LIKE_ESCAPE, false);
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$
        
        helpTestVisitor("pm1.g1.e1 LIKE 'x' ESCAPE '#'", modelID, metadata, capFinder, false, false);         //$NON-NLS-1$
    }
        
    // element not searchable
    public void testMatchCriteriaMatchableFail() {
        FakeMetadataFacade metadata = FakeMetadataFactory.example1();
        Object modelID = metadata.getStore().findObject("pm1", FakeMetadataObject.MODEL); //$NON-NLS-1$
        FakeMetadataObject e1 = metadata.getStore().findObject("pm1.g1.e1", FakeMetadataObject.ELEMENT); //$NON-NLS-1$
        e1.putProperty(FakeMetadataObject.Props.SEARCHABLE_LIKE, Boolean.FALSE);
                
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setCapabilitySupport(Capability.QUERY_WHERE, true);
        caps.setCapabilitySupport(Capability.QUERY_WHERE_LIKE, true);
        caps.setCapabilitySupport(Capability.QUERY_WHERE_LIKE_ESCAPE, true);
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$
        
        helpTestVisitor("pm1.g1.e1 LIKE 'x'", modelID, metadata, capFinder, false, false);         //$NON-NLS-1$
    }

    // no caps
    public void testMatchCriteriaNoCaps() {
        FakeMetadataFacade metadata = FakeMetadataFactory.example1Cached();
        Object modelID = metadata.getStore().findObject("pm1", FakeMetadataObject.MODEL); //$NON-NLS-1$
                
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        
        helpTestVisitor("pm1.g1.e1 LIKE 'x'", modelID, metadata, capFinder, true, false);         //$NON-NLS-1$
    }    

    public void testMatchCriteriaNoCrit() {
        FakeMetadataFacade metadata = FakeMetadataFactory.example1Cached();
        Object modelID = metadata.getStore().findObject("pm1", FakeMetadataObject.MODEL); //$NON-NLS-1$
        
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setCapabilitySupport(Capability.QUERY_WHERE, false);
        caps.setCapabilitySupport(Capability.QUERY_WHERE_LIKE, true);
        caps.setCapabilitySupport(Capability.QUERY_WHERE_LIKE_ESCAPE, true);
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$
        
        helpTestVisitor("pm1.g1.e1 LIKE 'x'", modelID, metadata, capFinder, false, false);         //$NON-NLS-1$
    }

    public void testNotCriteria1() {
        FakeMetadataFacade metadata = FakeMetadataFactory.example1Cached();
        Object modelID = metadata.getStore().findObject("pm1", FakeMetadataObject.MODEL); //$NON-NLS-1$
                
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setCapabilitySupport(Capability.QUERY_WHERE, true);
        caps.setCapabilitySupport(Capability.QUERY_WHERE_NOT, true);
        caps.setCapabilitySupport(Capability.QUERY_WHERE_COMPARE, true);
        caps.setCapabilitySupport(Capability.QUERY_WHERE_COMPARE_EQ, true);
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$
        
        helpTestVisitor("NOT pm1.g1.e1 = 'x'", modelID, metadata, capFinder, true, false);                 //$NON-NLS-1$
    }

    public void testNotCriteria2() {
        FakeMetadataFacade metadata = FakeMetadataFactory.example1Cached();
        Object modelID = metadata.getStore().findObject("pm1", FakeMetadataObject.MODEL); //$NON-NLS-1$
                
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setCapabilitySupport(Capability.QUERY_WHERE, true);
        caps.setCapabilitySupport(Capability.QUERY_WHERE_NOT, false);
        caps.setCapabilitySupport(Capability.QUERY_WHERE_COMPARE, true);
        caps.setCapabilitySupport(Capability.QUERY_WHERE_COMPARE_EQ, true);
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$
        
        helpTestVisitor("NOT pm1.g1.e1 = 'x'", modelID, metadata, capFinder, false, false);                 //$NON-NLS-1$
    }

    public void testSetCriteria1() {
        FakeMetadataFacade metadata = FakeMetadataFactory.example1Cached();
        Object modelID = metadata.getStore().findObject("pm1", FakeMetadataObject.MODEL); //$NON-NLS-1$
                
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setCapabilitySupport(Capability.QUERY_WHERE, true);
        caps.setCapabilitySupport(Capability.QUERY_WHERE_IN, true);
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$
        
        helpTestVisitor("pm1.g1.e1 IN ('x')", modelID, metadata, capFinder, true, false);                 //$NON-NLS-1$
    }

    public void testSetCriteria2() {
        FakeMetadataFacade metadata = FakeMetadataFactory.example1Cached();
        Object modelID = metadata.getStore().findObject("pm1", FakeMetadataObject.MODEL); //$NON-NLS-1$
                
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setCapabilitySupport(Capability.QUERY_WHERE, true);
        caps.setCapabilitySupport(Capability.QUERY_WHERE_IN, false);
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$
        
        helpTestVisitor("pm1.g1.e1 IN ('x')", modelID, metadata, capFinder, false, false);                 //$NON-NLS-1$
    }

    public void testSetCriteria3() {
        FakeMetadataFacade metadata = FakeMetadataFactory.example1Cached();
        Object modelID = metadata.getStore().findObject("pm1", FakeMetadataObject.MODEL); //$NON-NLS-1$
                
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        
        helpTestVisitor("pm1.g1.e1 IN ('x')", modelID, metadata, capFinder, true, false);                 //$NON-NLS-1$
    }

    public void testSetCriteria5() {
        FakeMetadataFacade metadata = FakeMetadataFactory.example1();
        FakeMetadataObject modelID = metadata.getStore().findObject("pm1", FakeMetadataObject.MODEL); //$NON-NLS-1$
        FakeMetadataObject elementID = metadata.getStore().findObject("pm1.g1.e1" , FakeMetadataObject.ELEMENT); //$NON-NLS-1$
        elementID.putProperty(FakeMetadataObject.Props.SEARCHABLE_COMPARE, Boolean.FALSE);
                
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setCapabilitySupport(Capability.QUERY_WHERE, true);
        caps.setCapabilitySupport(Capability.QUERY_WHERE_IN, true);
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$
        
        helpTestVisitor("pm1.g1.e1 IN ('x')", modelID, metadata, capFinder, false, false);                 //$NON-NLS-1$
    }

    public void testSetCriteria6() {
        FakeMetadataFacade metadata = FakeMetadataFactory.example1Cached();
        Object modelID = metadata.getStore().findObject("pm1", FakeMetadataObject.MODEL); //$NON-NLS-1$
                
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setCapabilitySupport(Capability.QUERY_WHERE, false);
        caps.setCapabilitySupport(Capability.QUERY_WHERE_IN, true);
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$
        
        helpTestVisitor("pm1.g1.e1 IN ('x')", modelID, metadata, capFinder, false, false);                 //$NON-NLS-1$
    }
    
    //Test for success NOT IN
    public void testSetCriteria7() {
        FakeMetadataFacade metadata = FakeMetadataFactory.example1Cached();
        Object modelID = metadata.getStore().findObject("pm1", FakeMetadataObject.MODEL); //$NON-NLS-1$
                
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setCapabilitySupport(Capability.QUERY_WHERE, true);
        caps.setCapabilitySupport(Capability.QUERY_WHERE_IN, true);
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$
        
        helpTestVisitor("pm1.g1.e1 NOT IN ('x')", modelID, metadata, capFinder, true, false);                 //$NON-NLS-1$
    }
    
    public void testSetCriteria8() {
        FakeMetadataFacade metadata = FakeMetadataFactory.example1Cached();
        Object modelID = metadata.getStore().findObject("pm1", FakeMetadataObject.MODEL); //$NON-NLS-1$
                
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setCapabilitySupport(Capability.QUERY_WHERE, true);
        caps.setCapabilitySupport(Capability.QUERY_WHERE_IN, true);
        caps.setSourceProperty(Capability.MAX_IN_CRITERIA_SIZE, new Integer(2));
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$
        
        helpTestVisitor("pm1.g1.e1 IN ('x', 'y', 'z')", modelID, metadata, capFinder, false, false);                 //$NON-NLS-1$
    }
    
    public void testSetCriteria9() {
        FakeMetadataFacade metadata = FakeMetadataFactory.example1Cached();
        Object modelID = metadata.getStore().findObject("pm1", FakeMetadataObject.MODEL); //$NON-NLS-1$
                
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setCapabilitySupport(Capability.QUERY_WHERE, true);
        caps.setCapabilitySupport(Capability.QUERY_WHERE_IN, true);
        caps.setSourceProperty(Capability.MAX_IN_CRITERIA_SIZE, new Integer(2));
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$
        
        helpTestVisitor("pm1.g1.e1 IN ('x', 'y')", modelID, metadata, capFinder, true, false);                 //$NON-NLS-1$
    }

    public void testSubquerySetCriteria() {
        FakeMetadataFacade metadata = FakeMetadataFactory.example1Cached();
        Object modelID = metadata.getStore().findObject("pm1", FakeMetadataObject.MODEL); //$NON-NLS-1$
                
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setCapabilitySupport(Capability.QUERY_WHERE, true);
        caps.setCapabilitySupport(Capability.QUERY_WHERE_IN, true);
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$
        
        helpTestVisitor("pm1.g1.e1 IN (SELECT 'xyz' FROM pm1.g1)", modelID, metadata, capFinder, false, false);                 //$NON-NLS-1$
    }
    
    
    // has all capabilities
    public void testSubqueryCompareCriteriaSuccess() {
        FakeMetadataFacade metadata = FakeMetadataFactory.example1Cached();
        Object modelID = metadata.getStore().findObject("pm1", FakeMetadataObject.MODEL); //$NON-NLS-1$
        
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setCapabilitySupport(Capability.QUERY_WHERE, true);
        caps.setCapabilitySupport(Capability.QUERY_WHERE_COMPARE, true);
        caps.setCapabilitySupport(Capability.QUERY_WHERE_COMPARE_EQ, true);
        caps.setCapabilitySupport(Capability.QUERY_SUBQUERIES_SCALAR, true);
        caps.setCapabilitySupport(Capability.QUERY_WHERE_QUANTIFIED_COMPARISON, true);
        caps.setCapabilitySupport(Capability.QUERY_WHERE_QUANTIFIED_SOME, true);
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$
        
        helpTestVisitorWithCommand("SELECT e1 FROM pm1.g1 WHERE pm1.g1.e1 = ANY (SELECT e1 FROM pm1.g2)", modelID, metadata, capFinder, true, false);         //$NON-NLS-1$
    }

    // does not have where capability
    public void testSubqueryCompareCriteriaCapFail1() {
        FakeMetadataFacade metadata = FakeMetadataFactory.example1Cached();
        Object modelID = metadata.getStore().findObject("pm1", FakeMetadataObject.MODEL); //$NON-NLS-1$
        
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setCapabilitySupport(Capability.QUERY_WHERE, false);
        caps.setCapabilitySupport(Capability.QUERY_WHERE_COMPARE, true);
        caps.setCapabilitySupport(Capability.QUERY_WHERE_COMPARE_EQ, true);
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$
        
        helpTestVisitorWithCommand("SELECT e1 FROM pm1.g1 WHERE pm1.g1.e1 = ANY (SELECT e1 FROM pm1.g2)", modelID, metadata, capFinder, false, false);         //$NON-NLS-1$
    }

    // does not have compare capability
    public void testSubqueryCompareCriteriaCapFail2() {
        FakeMetadataFacade metadata = FakeMetadataFactory.example1Cached();
        Object modelID = metadata.getStore().findObject("pm1", FakeMetadataObject.MODEL); //$NON-NLS-1$
        
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setCapabilitySupport(Capability.QUERY_WHERE, true);
        caps.setCapabilitySupport(Capability.QUERY_WHERE_COMPARE, false);
        caps.setCapabilitySupport(Capability.QUERY_WHERE_COMPARE_EQ, true);
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$
        
        helpTestVisitorWithCommand("SELECT e1 FROM pm1.g1 WHERE pm1.g1.e1 = ANY (SELECT e1 FROM pm1.g2)", modelID, metadata, capFinder, false, false);         //$NON-NLS-1$
    }

    // does not have subquery capability
    public void testSubqueryCompareCriteriaFail3() {
        FakeMetadataFacade metadata = FakeMetadataFactory.example1Cached();
        Object modelID = metadata.getStore().findObject("pm1", FakeMetadataObject.MODEL); //$NON-NLS-1$
        
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setCapabilitySupport(Capability.QUERY_WHERE, true);
        caps.setCapabilitySupport(Capability.QUERY_WHERE_COMPARE, true);
        caps.setCapabilitySupport(Capability.QUERY_WHERE_COMPARE_EQ, true);
        caps.setCapabilitySupport(Capability.QUERY_SUBQUERIES_SCALAR, false);
        caps.setCapabilitySupport(Capability.QUERY_WHERE_QUANTIFIED_COMPARISON, false);
        caps.setCapabilitySupport(Capability.QUERY_WHERE_QUANTIFIED_SOME, false);
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$
        
        helpTestVisitorWithCommand("SELECT e1 FROM pm1.g1 WHERE pm1.g1.e1 = ANY (SELECT e1 FROM pm1.g2)", modelID, metadata, capFinder, false, false);         //$NON-NLS-1$
    }

    // does not have quantified subquery comparison capability
    public void testSubqueryCompareCriteriaFail4() {
        FakeMetadataFacade metadata = FakeMetadataFactory.example1Cached();
        Object modelID = metadata.getStore().findObject("pm1", FakeMetadataObject.MODEL); //$NON-NLS-1$
        
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setCapabilitySupport(Capability.QUERY_WHERE, true);
        caps.setCapabilitySupport(Capability.QUERY_WHERE_COMPARE, true);
        caps.setCapabilitySupport(Capability.QUERY_WHERE_COMPARE_EQ, true);
        caps.setCapabilitySupport(Capability.QUERY_SUBQUERIES_SCALAR, true);
        caps.setCapabilitySupport(Capability.QUERY_WHERE_QUANTIFIED_COMPARISON, false);
        caps.setCapabilitySupport(Capability.QUERY_WHERE_QUANTIFIED_SOME, false);
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$
        
        helpTestVisitorWithCommand("SELECT e1 FROM pm1.g1 WHERE pm1.g1.e1 = ANY (SELECT e1 FROM pm1.g2)", modelID, metadata, capFinder, false, false);         //$NON-NLS-1$
    }

    // does not have quantified subquery comparison capability for ANY
    public void testSubqueryCompareCriteriaFail5() {
        FakeMetadataFacade metadata = FakeMetadataFactory.example1Cached();
        Object modelID = metadata.getStore().findObject("pm1", FakeMetadataObject.MODEL); //$NON-NLS-1$
        
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setCapabilitySupport(Capability.QUERY_WHERE, true);
        caps.setCapabilitySupport(Capability.QUERY_WHERE_COMPARE, true);
        caps.setCapabilitySupport(Capability.QUERY_WHERE_COMPARE_EQ, true);
        caps.setCapabilitySupport(Capability.QUERY_SUBQUERIES_SCALAR, true);
        caps.setCapabilitySupport(Capability.QUERY_WHERE_QUANTIFIED_COMPARISON, true);
        caps.setCapabilitySupport(Capability.QUERY_WHERE_QUANTIFIED_SOME, false);
        caps.setCapabilitySupport(Capability.QUERY_WHERE_QUANTIFIED_ALL, true);
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$
        
        helpTestVisitorWithCommand("SELECT e1 FROM pm1.g1 WHERE pm1.g1.e1 = ANY (SELECT e1 FROM pm1.g2)", modelID, metadata, capFinder, false, false);         //$NON-NLS-1$
    }

    // does not have quantified subquery comparison capability for ALL
    public void testSubqueryCompareCriteriaFail6() {
        FakeMetadataFacade metadata = FakeMetadataFactory.example1Cached();
        Object modelID = metadata.getStore().findObject("pm1", FakeMetadataObject.MODEL); //$NON-NLS-1$
        
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setCapabilitySupport(Capability.QUERY_WHERE, true);
        caps.setCapabilitySupport(Capability.QUERY_WHERE_COMPARE, true);
        caps.setCapabilitySupport(Capability.QUERY_WHERE_COMPARE_EQ, true);
        caps.setCapabilitySupport(Capability.QUERY_SUBQUERIES_SCALAR, true);
        caps.setCapabilitySupport(Capability.QUERY_WHERE_QUANTIFIED_COMPARISON, true);
        caps.setCapabilitySupport(Capability.QUERY_WHERE_QUANTIFIED_SOME, true);
        caps.setCapabilitySupport(Capability.QUERY_WHERE_QUANTIFIED_ALL, false);
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$
        
        helpTestVisitorWithCommand("SELECT e1 FROM pm1.g1 WHERE pm1.g1.e1 = ALL (SELECT e1 FROM pm1.g2)", modelID, metadata, capFinder, false, false);         //$NON-NLS-1$
    }

    // does not have = capability
    public void testSubqueryCompareCriteriaOpCapFail1() {
        FakeMetadataFacade metadata = FakeMetadataFactory.example1Cached();
        Object modelID = metadata.getStore().findObject("pm1", FakeMetadataObject.MODEL); //$NON-NLS-1$
        
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setCapabilitySupport(Capability.QUERY_WHERE, true);
        caps.setCapabilitySupport(Capability.QUERY_WHERE_COMPARE, true);
        caps.setCapabilitySupport(Capability.QUERY_WHERE_COMPARE_EQ, false);
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$
        
        helpTestVisitorWithCommand("SELECT e1 FROM pm1.g1 WHERE pm1.g1.e1 = ANY (SELECT e1 FROM pm1.g2)", modelID, metadata, capFinder, false, false);         //$NON-NLS-1$
    }
    
    // does not have <> capability
    public void testSubqueryCompareCriteriaOpCapFail2() {
        FakeMetadataFacade metadata = FakeMetadataFactory.example1Cached();
        Object modelID = metadata.getStore().findObject("pm1", FakeMetadataObject.MODEL); //$NON-NLS-1$
        
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setCapabilitySupport(Capability.QUERY_WHERE, true);
        caps.setCapabilitySupport(Capability.QUERY_WHERE_COMPARE, true);
        caps.setCapabilitySupport(Capability.QUERY_WHERE_COMPARE_NE, false);
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$
        
        helpTestVisitorWithCommand("SELECT e1 FROM pm1.g1 WHERE pm1.g1.e1 <> ANY (SELECT e1 FROM pm1.g2)", modelID, metadata, capFinder, false, false);         //$NON-NLS-1$
    }

    // does not have < capability
    public void testSubqueryCompareCriteriaOpCapFail3() {
        FakeMetadataFacade metadata = FakeMetadataFactory.example1Cached();
        Object modelID = metadata.getStore().findObject("pm1", FakeMetadataObject.MODEL); //$NON-NLS-1$
        
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setCapabilitySupport(Capability.QUERY_WHERE, true);
        caps.setCapabilitySupport(Capability.QUERY_WHERE_COMPARE, true);
        caps.setCapabilitySupport(Capability.QUERY_WHERE_COMPARE_LT, false);
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$
        
        helpTestVisitorWithCommand("SELECT e1 FROM pm1.g1 WHERE pm1.g1.e1 < ANY (SELECT e1 FROM pm1.g2)", modelID, metadata, capFinder, false, false);         //$NON-NLS-1$
    }

    // does not have <= capability
    public void testSubqueryCompareCriteriaOpCapFail4() {
        FakeMetadataFacade metadata = FakeMetadataFactory.example1Cached();
        Object modelID = metadata.getStore().findObject("pm1", FakeMetadataObject.MODEL); //$NON-NLS-1$
        
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setCapabilitySupport(Capability.QUERY_WHERE, true);
        caps.setCapabilitySupport(Capability.QUERY_WHERE_COMPARE, true);
        caps.setCapabilitySupport(Capability.QUERY_WHERE_COMPARE_LE, false);
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$
        
        helpTestVisitorWithCommand("SELECT e1 FROM pm1.g1 WHERE pm1.g1.e1 <= ANY (SELECT e1 FROM pm1.g2)", modelID, metadata, capFinder, false, false);         //$NON-NLS-1$
    }

    // does not have > capability
    public void testSubqueryCompareCriteriaOpCapFail5() {
        FakeMetadataFacade metadata = FakeMetadataFactory.example1Cached();
        Object modelID = metadata.getStore().findObject("pm1", FakeMetadataObject.MODEL); //$NON-NLS-1$
        
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setCapabilitySupport(Capability.QUERY_WHERE, true);
        caps.setCapabilitySupport(Capability.QUERY_WHERE_COMPARE, true);
        caps.setCapabilitySupport(Capability.QUERY_WHERE_COMPARE_GT, false);
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$
        
        helpTestVisitorWithCommand("SELECT e1 FROM pm1.g1 WHERE pm1.g1.e1 > ANY (SELECT e1 FROM pm1.g2)", modelID, metadata, capFinder, false, false);         //$NON-NLS-1$
    }

    // does not have >= capability
    public void testSubqueryCompareCriteriaOpCapFail6() {
        FakeMetadataFacade metadata = FakeMetadataFactory.example1Cached();
        Object modelID = metadata.getStore().findObject("pm1", FakeMetadataObject.MODEL); //$NON-NLS-1$
        
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setCapabilitySupport(Capability.QUERY_WHERE, true);
        caps.setCapabilitySupport(Capability.QUERY_WHERE_COMPARE, true);
        caps.setCapabilitySupport(Capability.QUERY_WHERE_COMPARE_GE, false);
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$
        
        helpTestVisitorWithCommand("SELECT e1 FROM pm1.g1 WHERE pm1.g1.e1 >= ANY (SELECT e1 FROM pm1.g2)", modelID, metadata, capFinder, false, false);         //$NON-NLS-1$
    }
    
    // element not searchable
    public void testSubqueryCompareCriteriaSearchableFail() {
        FakeMetadataFacade metadata = FakeMetadataFactory.example1();
        Object modelID = metadata.getStore().findObject("pm1", FakeMetadataObject.MODEL); //$NON-NLS-1$
        FakeMetadataObject e1 = metadata.getStore().findObject("pm1.g1.e1", FakeMetadataObject.ELEMENT); //$NON-NLS-1$
        e1.putProperty(FakeMetadataObject.Props.SEARCHABLE_COMPARE, Boolean.FALSE);
                
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setCapabilitySupport(Capability.QUERY_WHERE, true);
        caps.setCapabilitySupport(Capability.QUERY_WHERE_COMPARE, true);
        caps.setCapabilitySupport(Capability.QUERY_WHERE_COMPARE_EQ, true);
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$
        
        helpTestVisitorWithCommand("SELECT e1 FROM pm1.g1 WHERE pm1.g1.e1 = ANY (SELECT e1 FROM pm1.g2)", modelID, metadata, capFinder, false, false);         //$NON-NLS-1$
    }

    public void testExistsCriteria1() {
        FakeMetadataFacade metadata = FakeMetadataFactory.example1Cached();
        Object modelID = metadata.getStore().findObject("pm1", FakeMetadataObject.MODEL); //$NON-NLS-1$
        
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setCapabilitySupport(Capability.QUERY_WHERE, true);
        caps.setCapabilitySupport(Capability.QUERY_WHERE_EXISTS, true);
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$
        
        helpTestVisitorWithCommand("SELECT e1 FROM pm1.g1 WHERE EXISTS(SELECT e1 FROM pm1.g2)", modelID, metadata, capFinder, true, false); //$NON-NLS-1$
    }

    public void testExistsCriteria2() {
        FakeMetadataFacade metadata = FakeMetadataFactory.example1Cached();
        Object modelID = metadata.getStore().findObject("pm1", FakeMetadataObject.MODEL); //$NON-NLS-1$
        
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setCapabilitySupport(Capability.QUERY_WHERE, true);
        caps.setCapabilitySupport(Capability.QUERY_WHERE_EXISTS, false);
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$
        
        helpTestVisitorWithCommand("SELECT e1 FROM pm1.g1 WHERE EXISTS(SELECT e1 FROM pm1.g2)", modelID, metadata, capFinder, false, false); //$NON-NLS-1$
    }

    public void testExistsCriteria4() {
        FakeMetadataFacade metadata = FakeMetadataFactory.example1Cached();
        Object modelID = metadata.getStore().findObject("pm1", FakeMetadataObject.MODEL); //$NON-NLS-1$
        
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setCapabilitySupport(Capability.QUERY_WHERE, false);
        caps.setCapabilitySupport(Capability.QUERY_WHERE_EXISTS, true);
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$
        
        helpTestVisitorWithCommand("SELECT e1 FROM pm1.g1 WHERE EXISTS(SELECT e1 FROM pm1.g2)", modelID, metadata, capFinder, false, false); //$NON-NLS-1$
    }

    public void testExistsCriteria5() {
        FakeMetadataFacade metadata = FakeMetadataFactory.example1Cached();
        Object modelID = metadata.getStore().findObject("pm1", FakeMetadataObject.MODEL); //$NON-NLS-1$
        
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setCapabilitySupport(Capability.QUERY_WHERE, true);
        caps.setCapabilitySupport(Capability.QUERY_WHERE_EXISTS, true);
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$
        
        helpTestVisitorWithCommand("SELECT e1 FROM pm1.g1 WHERE EXISTS(SELECT e1 FROM pm1.g2)", modelID, metadata, capFinder, true, false); //$NON-NLS-1$
    }    
}
