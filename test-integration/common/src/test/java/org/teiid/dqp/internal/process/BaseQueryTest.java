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
package org.teiid.dqp.internal.process;

import java.util.List;

import junit.framework.TestCase;

import org.teiid.metadata.TransformationMetadata;
import org.teiid.metadata.index.VDBMetadataFactory;

import com.metamatrix.dqp.message.RequestID;
import com.metamatrix.query.analysis.AnalysisRecord;
import com.metamatrix.query.metadata.QueryMetadataInterface;
import com.metamatrix.query.optimizer.QueryOptimizer;
import com.metamatrix.query.optimizer.TestOptimizer;
import com.metamatrix.query.optimizer.capabilities.CapabilitiesFinder;
import com.metamatrix.query.processor.ProcessorDataManager;
import com.metamatrix.query.processor.ProcessorPlan;
import com.metamatrix.query.processor.TestProcessor;
import com.metamatrix.query.sql.lang.Command;
import com.metamatrix.query.util.CommandContext;


/** 
 * @since 4.2
 */
public abstract class BaseQueryTest extends TestCase {

    public BaseQueryTest(String name) {
        super(name);
    }
    
    public static TransformationMetadata createMetadata(String vdbFile) {
        return VDBMetadataFactory.getVDBMetadata(vdbFile);
    }
        
    protected void doProcess(QueryMetadataInterface metadata, String sql, CapabilitiesFinder capFinder, ProcessorDataManager dataManager, List[] expectedResults, boolean debug) throws Exception {
    	CommandContext context = createCommandContext();
    	context.setProcessDebug(debug);
        Command command = TestOptimizer.helpGetCommand(sql, metadata, null);

        // plan
        AnalysisRecord analysisRecord = new AnalysisRecord(false, debug, debug);
        ProcessorPlan plan = null;
        try {
            plan = QueryOptimizer.optimizePlan(command, metadata, null, capFinder, analysisRecord, createCommandContext());
        } finally {
            if(debug) {
                System.out.println(analysisRecord.getDebugLog());
            }
        }

    	TestProcessor.doProcess(plan, dataManager, expectedResults, context);
    }

    protected CommandContext createCommandContext() {
        CommandContext context = new CommandContext(new RequestID(), "test", "user", "myvdb", 1); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ 
        return context;
    }       
    
}
