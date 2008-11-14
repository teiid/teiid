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

import com.metamatrix.api.exception.MetaMatrixComponentException;
import com.metamatrix.api.exception.query.QueryParserException;
import com.metamatrix.api.exception.query.QueryPlannerException;
import com.metamatrix.api.exception.query.QueryResolverException;
import com.metamatrix.query.optimizer.relational.GenerateCanonical;
import com.metamatrix.query.optimizer.relational.PlanHints;
import com.metamatrix.query.parser.QueryParser;
import com.metamatrix.query.resolver.QueryResolver;
import com.metamatrix.query.sql.lang.Command;
import com.metamatrix.query.sql.lang.CompareCriteria;
import com.metamatrix.query.sql.lang.Insert;
import com.metamatrix.query.sql.lang.Query;
import com.metamatrix.query.sql.symbol.Constant;
import com.metamatrix.query.unittest.FakeMetadataFacade;
import com.metamatrix.query.unittest.FakeMetadataFactory;

public class TestRuleValidateWhereAll extends TestCase {
    
    private static final FakeMetadataFacade METADATA = FakeMetadataFactory.example1();
    private static final QueryParser PARSER = new QueryParser();

    public TestRuleValidateWhereAll(String name) {
        super(name);
    }

    public void testHasNoCriteria1() {
        assertEquals("Got incorrect answer checking for no criteria", false, RuleValidateWhereAll.hasNoCriteria(new Insert())); //$NON-NLS-1$
    }

    public void testHasNoCriteria2() {
        Query query = new Query();
        CompareCriteria crit = new CompareCriteria(new Constant("a"), CompareCriteria.EQ, new Constant("b")); //$NON-NLS-1$ //$NON-NLS-2$
        query.setCriteria(crit);        
        assertEquals("Got incorrect answer checking for no criteria", false, RuleValidateWhereAll.hasNoCriteria(query)); //$NON-NLS-1$
    }

    public void testHasNoCriteria3() {
        assertEquals("Got incorrect answer checking for no criteria", true, RuleValidateWhereAll.hasNoCriteria(new Query())); //$NON-NLS-1$
    }

    private PlanHints buildHints(String command) throws QueryParserException, QueryResolverException, MetaMatrixComponentException, QueryPlannerException {
        Command query = PARSER.parseCommand(command);
        PlanHints planHints = new PlanHints();
        
        QueryResolver.resolveCommand(query, METADATA);
        GenerateCanonical.generatePlan(query, planHints, METADATA);
        return planHints;
    }
    
    public void testWhereAll_noValidationNeeded() throws Exception {
        PlanHints planHints =  buildHints("SELECT * FROM pm1.g1");   //$NON-NLS-1$
        assertFalse(planHints.needsWhereAllValidation);
    } 
    
    public void testWhereAll_validationNeeded() throws Exception  {
        PlanHints planHints =  buildHints("SELECT * FROM pm6.g1");   //$NON-NLS-1$
        assertTrue(planHints.needsWhereAllValidation);
    }    

    public void testWhereAll2__validationNeeded() throws Exception  {
        PlanHints planHints = buildHints("SELECT pm1.g1.e1 FROM pm1.g1, pm6.g1 WHERE pm1.g1.e1=pm6.g1.e1 OPTION MAKEDEP pm6.g1");   //$NON-NLS-1$
        assertTrue(planHints.needsWhereAllValidation);
    }    
        
    public void testWhereAll_virtual_noValidation() throws Exception  {
        PlanHints planHints = buildHints("SELECT * FROM vm1.g38");   //$NON-NLS-1$
        assertFalse(planHints.needsWhereAllValidation);
    }
    
}
