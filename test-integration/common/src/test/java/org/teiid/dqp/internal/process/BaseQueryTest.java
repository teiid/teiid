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

import org.teiid.api.exception.query.QueryMetadataException;
import org.teiid.api.exception.query.QueryPlannerException;
import org.teiid.common.buffer.BufferManagerFactory;
import org.teiid.common.buffer.impl.BufferManagerImpl;
import org.teiid.core.TeiidComponentException;
import org.teiid.core.TeiidProcessingException;
import org.teiid.dqp.message.RequestID;
import org.teiid.metadata.index.VDBMetadataFactory;
import org.teiid.query.analysis.AnalysisRecord;
import org.teiid.query.metadata.QueryMetadataInterface;
import org.teiid.query.metadata.TransformationMetadata;
import org.teiid.query.optimizer.QueryOptimizer;
import org.teiid.query.optimizer.TestOptimizer;
import org.teiid.query.optimizer.capabilities.CapabilitiesFinder;
import org.teiid.query.processor.ProcessorDataManager;
import org.teiid.query.processor.ProcessorPlan;
import org.teiid.query.processor.TestProcessor;
import org.teiid.query.sql.lang.Command;
import org.teiid.query.util.CommandContext;



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
        BufferManagerImpl bm = BufferManagerFactory.createBufferManager();
        bm.setProcessorBatchSize(context.getProcessorBatchSize());
        context.setBufferManager(bm);
        doProcess(metadata, sql, capFinder, dataManager, expectedResults,
				debug, context);
    }

	protected void doProcess(QueryMetadataInterface metadata, String sql,
			CapabilitiesFinder capFinder, ProcessorDataManager dataManager,
			List[] expectedResults, boolean debug, CommandContext context)
			throws TeiidComponentException, TeiidProcessingException,
			QueryMetadataException, QueryPlannerException, Exception {
		Command command = TestOptimizer.helpGetCommand(sql, metadata, null);

        // plan
        AnalysisRecord analysisRecord = new AnalysisRecord(false, debug);
        ProcessorPlan plan = null;
        try {
            plan = QueryOptimizer.optimizePlan(command, metadata, null, capFinder, analysisRecord, context);
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
