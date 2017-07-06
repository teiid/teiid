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

package org.teiid.query.processor;

import java.util.Arrays;
import java.util.List;

import org.junit.Test;
import org.teiid.api.exception.query.QueryValidatorException;
import org.teiid.core.TeiidComponentException;
import org.teiid.dqp.internal.process.AuthorizationValidator;
import org.teiid.metadata.AbstractMetadataRecord;
import org.teiid.query.metadata.QueryMetadataInterface;
import org.teiid.query.optimizer.capabilities.DefaultCapabilitiesFinder;
import org.teiid.query.sql.lang.Command;
import org.teiid.query.unittest.RealMetadataFactory;
import org.teiid.query.util.CommandContext;


public class TestSecurityFunctions {

    /**
     *  hasRole should be true without a service
     */
    @Test public void testHasRoleWithoutService() throws Exception {
        
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
    
    @Test public void testHasRoleWithService() throws Exception {
        
        String sql = "select pm1.g1.e2 from pm1.g1 where true = hasRole('data', pm1.g1.e1)";  //$NON-NLS-1$
        
        // Create expected results
        List[] expected = new List[] { };    
        
        // Construct data manager with data
        HardcodedDataManager dataManager = new HardcodedDataManager();
        
        dataManager.addData("SELECT pm1.g1.e1, pm1.g1.e2 FROM pm1.g1", new List[] { //$NON-NLS-1$
            Arrays.asList(new Object[] { "fooRole", new Integer(0) }), //$NON-NLS-1$  
        }); 
        
        CommandContext context = new CommandContext();
        context.setAuthoriziationValidator(new AuthorizationValidator() {

			@Override
			public boolean validate(String[] originalSql, Command command,
					QueryMetadataInterface metadata,
					CommandContext commandContext, CommandType commandType)
					throws QueryValidatorException, TeiidComponentException {
				return false;
			}

			@Override
			public boolean hasRole(String roleName,
					CommandContext commandContext) {
				return false;
			}
			
			@Override
			public boolean isAccessible(AbstractMetadataRecord record,
					CommandContext commandContext) {
				return true;
			}
			
        });
        
        Command command = TestProcessor.helpParse(sql);   
        ProcessorPlan plan = TestProcessor.helpGetPlan(command, RealMetadataFactory.example1Cached(), new DefaultCapabilitiesFinder(), context);
        
        // Run query
        TestProcessor.helpProcess(plan, context, dataManager, expected);
    }
    
    @Test public void testHashes() {
        String sql = "select cast(to_chars(md5(pm1.g1.e1), 'hex') as string), cast(to_chars(sha1(X'61'), 'hex') as string) from pm1.g1";  //$NON-NLS-1$
        
        List<?>[] expected = new List[] { Arrays.asList("0CC175B9C0F1B6A831C399E269772661", "86F7E437FAA5A7FCE15D1DDCB9EAEAEA377667B8")}; //$NON-NLS-1$ //$NON-NLS-2$      
        
        HardcodedDataManager dataManager = new HardcodedDataManager();
        
        dataManager.addData("SELECT pm1.g1.e1 FROM pm1.g1", new List[] { //$NON-NLS-1$
            Arrays.asList(new Object[] { "a" }), //$NON-NLS-1$  
        }); 
        
        Command command = TestProcessor.helpParse(sql);   
        ProcessorPlan plan = TestProcessor.helpGetPlan(command, RealMetadataFactory.example1Cached(), new DefaultCapabilitiesFinder());
        
        TestProcessor.helpProcess(plan, dataManager, expected);
    }

}
