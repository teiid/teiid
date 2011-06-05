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

package org.teiid.query.processor;

import java.util.Arrays;
import java.util.List;

import org.teiid.core.TeiidComponentException;
import org.teiid.query.eval.SecurityFunctionEvaluator;
import org.teiid.query.optimizer.capabilities.DefaultCapabilitiesFinder;
import org.teiid.query.processor.ProcessorPlan;
import org.teiid.query.sql.lang.Command;
import org.teiid.query.unittest.RealMetadataFactory;
import org.teiid.query.util.CommandContext;

import junit.framework.TestCase;


public class TestSecurityFunctions extends TestCase {

    /**
     *  hasRole should be true without a service
     */
    public void testHasRoleWithoutService() throws Exception {
        
        String sql = "select pm1.g1.e2 from pm1.g1 where true = hasRole('data', pm1.g1.e1)";  //$NON-NLS-1$
        
        // Create expected results
        List[] expected = new List[] { 
            Arrays.asList(new Object[] { new Integer(0) }),
        };    
        
        // Construct data manager with data
        HardcodedDataManager dataManager = new HardcodedDataManager();
        
        dataManager.addData("SELECT pm1.g1.e1, pm1.g1.e2 FROM pm1.g1", new List[] { //$NON-NLS-1$
            Arrays.asList(new Object[] { "fooRole", new Integer(0) }), //$NON-NLS-1$  
        }); 
        
        Command command = TestProcessor.helpParse(sql);   
        ProcessorPlan plan = TestProcessor.helpGetPlan(command, RealMetadataFactory.example1Cached());
        
        // Run query
        TestProcessor.helpProcess(plan, dataManager, expected);
    }
    
    public void testHasRoleWithService() throws Exception {
        
        String sql = "select pm1.g1.e2 from pm1.g1 where true = hasRole('data', pm1.g1.e1)";  //$NON-NLS-1$
        
        // Create expected results
        List[] expected = new List[] { };    
        
        // Construct data manager with data
        HardcodedDataManager dataManager = new HardcodedDataManager();
        
        dataManager.addData("SELECT pm1.g1.e1, pm1.g1.e2 FROM pm1.g1", new List[] { //$NON-NLS-1$
            Arrays.asList(new Object[] { "fooRole", new Integer(0) }), //$NON-NLS-1$  
        }); 
        
        CommandContext context = new CommandContext();
        context.setSecurityFunctionEvaluator(new SecurityFunctionEvaluator() {
            public boolean hasRole(String roleType,
                                   String roleName) throws TeiidComponentException {
                return false;
            }});
        
        Command command = TestProcessor.helpParse(sql);   
        ProcessorPlan plan = TestProcessor.helpGetPlan(command, RealMetadataFactory.example1Cached(), new DefaultCapabilitiesFinder(), context);
        
        // Run query
        TestProcessor.helpProcess(plan, context, dataManager, expected);
    }

}
