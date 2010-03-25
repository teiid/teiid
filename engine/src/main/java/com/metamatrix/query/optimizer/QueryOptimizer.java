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

package com.metamatrix.query.optimizer;

import java.util.Map;

import com.metamatrix.api.exception.MetaMatrixComponentException;
import com.metamatrix.api.exception.query.QueryMetadataException;
import com.metamatrix.api.exception.query.QueryPlannerException;
import com.metamatrix.api.exception.query.QueryResolverException;
import com.metamatrix.core.MetaMatrixRuntimeException;
import com.metamatrix.core.id.IDGenerator;
import com.metamatrix.core.id.IntegerIDFactory;
import com.metamatrix.query.analysis.AnalysisRecord;
import com.metamatrix.query.metadata.QueryMetadataInterface;
import com.metamatrix.query.metadata.TempMetadataAdapter;
import com.metamatrix.query.metadata.TempMetadataStore;
import com.metamatrix.query.optimizer.batch.BatchedUpdatePlanner;
import com.metamatrix.query.optimizer.capabilities.CapabilitiesFinder;
import com.metamatrix.query.optimizer.proc.ProcedurePlanner;
import com.metamatrix.query.optimizer.relational.RelationalPlanner;
import com.metamatrix.query.optimizer.xml.XMLPlanner;
import com.metamatrix.query.optimizer.xquery.XQueryPlanner;
import com.metamatrix.query.processor.ProcessorPlan;
import com.metamatrix.query.resolver.QueryResolver;
import com.metamatrix.query.sql.lang.Command;
import com.metamatrix.query.sql.lang.Query;
import com.metamatrix.query.util.CommandContext;

/**
 * <p>This Class produces a ProcessorPlan object (a plan for query execution) from a 
 * user's command and a source of metadata.</p>
 * 
 * <p>The user's Command object may in fact be a tree of commands and subcommands.
 * This component is architected to defer to the proper 
 * {@link CommandPlanner CommandPlanner} implementation to plan each Command in the
 * tree.</p>
 */
public class QueryOptimizer {
	
	private static final CommandPlanner XML_PLANNER = new XMLPlanner();
	private static final CommandPlanner PROCEDURE_PLANNER = new ProcedurePlanner();
    private static final CommandPlanner XQUERY_PLANNER = new XQueryPlanner();
    private static final CommandPlanner BATCHED_UPDATE_PLANNER = new BatchedUpdatePlanner();

	// Can't construct	
	private QueryOptimizer() {}

	public static ProcessorPlan optimizePlan(Command command, QueryMetadataInterface metadata, IDGenerator idGenerator, CapabilitiesFinder capFinder, AnalysisRecord analysisRecord, CommandContext context)
		throws QueryPlannerException, QueryMetadataException, MetaMatrixComponentException {

		if (analysisRecord == null) {
			analysisRecord = new AnalysisRecord(false, false);
		}
		
		if (context == null) {
			context = new CommandContext();
		}
		
        boolean debug = analysisRecord.recordDebug();
        
        Map tempMetadata = command.getTemporaryMetadata();
        metadata = new TempMetadataAdapter(metadata, new TempMetadataStore(tempMetadata));
                
        // Create an ID generator that can be used for all plans to generate unique data node IDs
        if(idGenerator == null) {
            idGenerator = new IDGenerator();
            idGenerator.setDefaultFactory(new IntegerIDFactory());
        }
        
		if(debug) {
			analysisRecord.println("\n----------------------------------------------------------------------------"); //$NON-NLS-1$
            analysisRecord.println("OPTIMIZE: \n" + command); //$NON-NLS-1$
		}   
                                   
		ProcessorPlan result = null;

		if (command.getType() == Command.TYPE_UPDATE_PROCEDURE){
			result = PROCEDURE_PLANNER.optimize(command, idGenerator, metadata, capFinder, analysisRecord, context);
        } else if (command.getType() == Command.TYPE_XQUERY){
            result = XQUERY_PLANNER.optimize(command, idGenerator, metadata, capFinder, analysisRecord, context);
        } else if (command.getType() == Command.TYPE_BATCHED_UPDATE){
            result = BATCHED_UPDATE_PLANNER.optimize(command, idGenerator, metadata, capFinder, analysisRecord, context);
        } else {
			try {
				if (command.getType() == Command.TYPE_QUERY && command instanceof Query && QueryResolver.isXMLQuery((Query)command, metadata)) {
					result = XML_PLANNER.optimize(command, idGenerator, metadata, capFinder, analysisRecord, context);
				} else {
					result = new RelationalPlanner().optimize(command, idGenerator, metadata, capFinder, analysisRecord, context);
				}
			} catch (QueryResolverException e) {
				throw new MetaMatrixRuntimeException(e);
			}
        }
		
		if(debug) {
            analysisRecord.println("\n----------------------------------------------------------------------------"); //$NON-NLS-1$
            analysisRecord.println("OPTIMIZATION COMPLETE:"); //$NON-NLS-1$
            analysisRecord.println("PROCESSOR PLAN:\n" + result); //$NON-NLS-1$
			analysisRecord.println("============================================================================");		 //$NON-NLS-1$
		}			

		return result;
	}
	
}
