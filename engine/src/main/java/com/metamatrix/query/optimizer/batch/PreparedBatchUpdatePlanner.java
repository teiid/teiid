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

package com.metamatrix.query.optimizer.batch;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.teiid.dqp.internal.process.PreparedStatementRequest;

import com.metamatrix.api.exception.MetaMatrixComponentException;
import com.metamatrix.api.exception.query.ExpressionEvaluationException;
import com.metamatrix.api.exception.query.QueryMetadataException;
import com.metamatrix.api.exception.query.QueryPlannerException;
import com.metamatrix.api.exception.query.QueryResolverException;
import com.metamatrix.core.id.IDGenerator;
import com.metamatrix.core.id.IntegerID;
import com.metamatrix.query.analysis.AnalysisRecord;
import com.metamatrix.query.metadata.QueryMetadataInterface;
import com.metamatrix.query.optimizer.CommandPlanner;
import com.metamatrix.query.optimizer.CommandTreeNode;
import com.metamatrix.query.optimizer.capabilities.CapabilitiesFinder;
import com.metamatrix.query.optimizer.capabilities.SourceCapabilities;
import com.metamatrix.query.processor.ProcessorPlan;
import com.metamatrix.query.processor.batch.BatchedUpdatePlan;
import com.metamatrix.query.processor.batch.PreparedBatchUpdatePlan;
import com.metamatrix.query.processor.relational.BatchedCommandsEvaluator;
import com.metamatrix.query.processor.relational.BatchedUpdateNode;
import com.metamatrix.query.processor.relational.ProjectNode;
import com.metamatrix.query.processor.relational.RelationalPlan;
import com.metamatrix.query.sql.lang.Command;
import com.metamatrix.query.sql.lang.Delete;
import com.metamatrix.query.sql.lang.Insert;
import com.metamatrix.query.sql.lang.PreparedBatchUpdate;
import com.metamatrix.query.sql.lang.StoredProcedure;
import com.metamatrix.query.sql.lang.Update;
import com.metamatrix.query.sql.symbol.GroupSymbol;
import com.metamatrix.query.sql.visitor.EvaluateExpressionVisitor;
import com.metamatrix.query.util.CommandContext;


/** 
 * Planner for PreparedBatchUpdate
 * @since 5.5.2
 */
public class PreparedBatchUpdatePlanner implements CommandPlanner {

    /** 
     * @see com.metamatrix.query.optimizer.CommandPlanner#generateCanonical(com.metamatrix.query.optimizer.CommandTreeNode, com.metamatrix.query.metadata.QueryMetadataInterface, com.metamatrix.query.analysis.AnalysisRecord, CommandContext)
     * @since 5.5.2
     */
    public void generateCanonical(CommandTreeNode rootNode,
                                  QueryMetadataInterface metadata,
                                  AnalysisRecord analysisRecord, CommandContext context)
    throws QueryPlannerException, QueryMetadataException, MetaMatrixComponentException {
        // do nothing. the planner framework takes care of generating the canonical plan for each of the child commands
    }

    /** 
     * If the updates are on a physical source and the source support prepared statement batch update,
     * just use the plan for the update comand. Otherwise, use PreparedBatchUpdatePlan.
     * @see com.metamatrix.query.optimizer.CommandPlanner#optimize(com.metamatrix.query.optimizer.CommandTreeNode, com.metamatrix.core.id.IDGenerator, com.metamatrix.query.metadata.QueryMetadataInterface, com.metamatrix.query.optimizer.capabilities.CapabilitiesFinder, com.metamatrix.query.analysis.AnalysisRecord, CommandContext)
     * @since 5.5.2
     */
    public ProcessorPlan optimize(CommandTreeNode node,
                                  IDGenerator idGenerator,
                                  QueryMetadataInterface metadata,
                                  CapabilitiesFinder capFinder,
                                  AnalysisRecord analysisRecord, CommandContext context)
    throws QueryPlannerException, QueryMetadataException, MetaMatrixComponentException {
    	//should have only one child
    	CommandTreeNode childNode = node.getFirstChild();
    	Command command = childNode.getCommand();
    	boolean supportPreparedBatchUpdate = false;
    	boolean supportBatchedUpdate = false;
    	if (isEligibleForBatching(command, metadata)) {
    		GroupSymbol group = getUpdatedGroup(command);
    		if(group != null){
                Object batchModelID = metadata.getModelID(group.getMetadataID());
                String modelName = metadata.getFullName(batchModelID);
                SourceCapabilities caps = capFinder.findCapabilities(modelName);
                supportPreparedBatchUpdate = caps.supportsCapability(SourceCapabilities.Capability.PREPARED_BATCH_UPDATE);
                supportBatchedUpdate = caps.supportsCapability(SourceCapabilities.Capability.BATCHED_UPDATES);
    		}
    	}
    	
    	ProcessorPlan plan = childNode.getProcessorPlan();
    	if(supportPreparedBatchUpdate){
    		return plan;
    	}
    	
    	List paramValues = context.getPreparedBatchUpdateValues();
    	PreparedBatchUpdate batchUpdate = (PreparedBatchUpdate)childNode.getCommand();
    	if(supportBatchedUpdate && paramValues.size() > 1){
    		List batch = new ArrayList();
    		for(int i=0; i<paramValues.size(); i++ ){
    			batch.add(batchUpdate.clone());
    		}
    		GroupSymbol group = getUpdatedGroup(command);
            Object batchModelID = metadata.getModelID(group.getMetadataID());
            String modelName = metadata.getFullName(batchModelID);
            BatchedUpdateNode batchNode = new BatchedUpdateNode(((IntegerID)idGenerator.create()).getValue(),
                    batch,
                    modelName);
            batchNode.setCommandsEvaluator(
            		new BatchedCommandsEvaluator(){
						public void evaluateExpressions(List commands, CommandContext context) throws ExpressionEvaluationException, QueryResolverException, MetaMatrixComponentException {
		            		PreparedBatchUpdate batchUpdate = (PreparedBatchUpdate)commands.get(0);
		            		List parameters = batchUpdate.getParameterReferences();
		            		Iterator valuesIter = context.getPreparedBatchUpdateValues().iterator();
		            		Iterator commandIter = commands.iterator();
		            		while(valuesIter.hasNext()){
		            			List values = (List)valuesIter.next();
		            			batchUpdate = (PreparedBatchUpdate)commandIter.next();
		            			PreparedStatementRequest.resolveParameterValues(parameters, values, context);
		            			EvaluateExpressionVisitor.replaceExpressions(batchUpdate, true, null, context);
		            		}
						}         			
            		});
            List symbols = Command.getUpdateCommandSymbol();
            batchNode.setElements(symbols);
            ProjectNode projectNode = new ProjectNode(((IntegerID)idGenerator.create()).getValue());
            projectNode.setSelectSymbols(symbols);
            projectNode.setElements(symbols);
            projectNode.addChild(batchNode);
            List childPlans = new ArrayList();
            RelationalPlan rPlan = new RelationalPlan(projectNode);
            rPlan.setOutputElements(symbols);
            childPlans.add(rPlan);
            return new BatchedUpdatePlan(childPlans, paramValues.size());
    	}
    	
    	return new PreparedBatchUpdatePlan(plan, paramValues, batchUpdate.getParameterReferences());
    }
    
    private static GroupSymbol getUpdatedGroup(Command command) {
        int type = command.getType();
        if (type == Command.TYPE_INSERT) {
            return ((Insert)command).getGroup();
        } else if (type == Command.TYPE_UPDATE) {
            return ((Update)command).getGroup();
        } else if (type == Command.TYPE_DELETE) {
            return ((Delete)command).getGroup();
        } else if (type == Command.TYPE_STORED_PROCEDURE) {
            return ((StoredProcedure)command).getGroup();
        } else {
        	return null;
        }
    }
    
    private static boolean isEligibleForBatching(Command command, QueryMetadataInterface metadata) throws QueryMetadataException, MetaMatrixComponentException {
        if (command.getType() == Command.TYPE_QUERY) {
            return false;
        }
        // If the command updates a physical group, it's eligible
        return !metadata.isVirtualGroup(getUpdatedGroup(command).getMetadataID());
    }
}
