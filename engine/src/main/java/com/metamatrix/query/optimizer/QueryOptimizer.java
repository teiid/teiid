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

import java.util.Collection;
import java.util.Iterator;
import java.util.Map;

import com.metamatrix.api.exception.MetaMatrixComponentException;
import com.metamatrix.api.exception.query.QueryMetadataException;
import com.metamatrix.api.exception.query.QueryPlannerException;
import com.metamatrix.core.id.IDGenerator;
import com.metamatrix.core.id.IntegerIDFactory;
import com.metamatrix.query.analysis.AnalysisRecord;
import com.metamatrix.query.metadata.QueryMetadataInterface;
import com.metamatrix.query.metadata.TempMetadataAdapter;
import com.metamatrix.query.metadata.TempMetadataStore;
import com.metamatrix.query.optimizer.batch.BatchedUpdatePlanner;
import com.metamatrix.query.optimizer.capabilities.CapabilitiesFinder;
import com.metamatrix.query.optimizer.proc.ProcedurePlanner;
import com.metamatrix.query.optimizer.relational.MergeTreeNodeProcessor;
import com.metamatrix.query.optimizer.relational.PlanHints;
import com.metamatrix.query.optimizer.relational.RelationalPlanner;
import com.metamatrix.query.optimizer.xml.XMLPlanner;
import com.metamatrix.query.optimizer.xquery.XQueryPlanner;
import com.metamatrix.query.processor.ProcessorPlan;
import com.metamatrix.query.sql.lang.Command;
import com.metamatrix.query.sql.symbol.GroupSymbol;
import com.metamatrix.query.sql.visitor.GroupCollectorVisitor;
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

	private static final CommandPlanner RELATIONAL_PLANNER = new RelationalPlanner();
	private static final CommandPlanner XML_PLANNER = new XMLPlanner();
	private static final CommandPlanner PROCEDURE_PLANNER = new ProcedurePlanner();
    private static final CommandPlanner XQUERY_PLANNER = new XQueryPlanner();
    private static final CommandPlanner BATCHED_UPDATE_PLANNER = new BatchedUpdatePlanner();
	private static final CommandTreeProcessor MERGE_TREE_NODE_PROCESSOR = new MergeTreeNodeProcessor();

	// Can't construct	
	private QueryOptimizer() {}

	/**
	 * <p>This method is intended only for clients with a need to have access to the 
	 * planning state of QueryOptimizer, which it can get to through the 
	 * {@link CommandTreeNode CommandTreeNode} reference passed in.</p>
	 * 
	 * <p>If there's no need to access the planning state, use one of the other
	 * overloaded methods, either
	 * {@link #optimizePlan(Command, boolean, QueryMetadataInterface) optimizePlan}
	 * or
	 * {@link #optimizePlan(Command, boolean, QueryMetadataInterface, boolean) optimizePlan}
	 * </p>
	 * 
	 * @param command fully-resolved user's command object
	 * @param metadata source of metadata
	 * @param idGenerator IDGenerator to be used for data nodes - if null, one will be created
	 * @param context 
	 * @param annotations Collection to add annotations to, null if annotations should be co  
	 * @param debug True if OPTION DEBUG output should be sent to analysisRecord 
	 * @return ProcessorPlan plan for query execution.
	 * @throws QueryPlannerException if there is a problem planning the query plan
	 * @throws QueryMetadataException if there is a problem accessing the metadata source
	 * @throws MetaMatrixComponentException if there is an unexpected exception
	 */
	public static ProcessorPlan optimizePlan(Command command, QueryMetadataInterface metadata, IDGenerator idGenerator, CapabilitiesFinder capabilitiesFinder, AnalysisRecord analysisRecord, CommandContext context)
		throws QueryPlannerException, QueryMetadataException, MetaMatrixComponentException {

        boolean debug = analysisRecord.recordDebug();
		if(debug) {
			analysisRecord.println("\n============================================================================"); //$NON-NLS-1$
            analysisRecord.println("USER COMMAND:\n" + command);		 //$NON-NLS-1$
            analysisRecord.println("\nINITIAL COMMAND STRUCTURE:\n" + command.printCommandTree());             //$NON-NLS-1$
        }

		// Generate canonical plans for each command
        CommandTreeNode root = new CommandTreeNode();
		recursiveGenerateCanonical(root, command, metadata, analysisRecord, context);
                
		// Run preprocessors
        root = MERGE_TREE_NODE_PROCESSOR.process(root, metadata);
		if(debug) {
            analysisRecord.println("\n----------------------------------------------------------------------------"); //$NON-NLS-1$
			analysisRecord.println("COMMAND TREE after merging relational nodes: \n" + root); //$NON-NLS-1$
		}   

        // Create an ID generator that can be used for all plans to generate unique data node IDs
        if(idGenerator == null) {
            idGenerator = new IDGenerator();
            idGenerator.setDefaultFactory(new IntegerIDFactory());
        }
        
        OptimizerContext.getOptimizerContext().reset(context);
        
		// Optimize command tree recursively, bottom-up
		ProcessorPlan plan = recursiveOptimize(root, idGenerator, metadata, capabilitiesFinder, analysisRecord, context);
		
        OptimizerContext.getOptimizerContext().reset(context);

		if(debug) {
            analysisRecord.println("\n----------------------------------------------------------------------------"); //$NON-NLS-1$
            analysisRecord.println("OPTIMIZATION COMPLETE:"); //$NON-NLS-1$
            analysisRecord.println("PLAN TREE:\n" + root); //$NON-NLS-1$
            analysisRecord.println("PROCESSOR PLAN:\n" + plan); //$NON-NLS-1$
			analysisRecord.println("============================================================================");		 //$NON-NLS-1$
		}			

		return plan;
	}
	
	/**
	 * Method recursiveGenerateCanonical.
	 * @param CommandTreeNode
	 * @param command
	 * @param minimizeToSource
	 * @param metadata
	 * @param planNodeIDOffset
	 * @param boundReferencesMap
	 * @param debug
	 */
	private static void recursiveGenerateCanonical(
		CommandTreeNode node,
		Command command,
		QueryMetadataInterface metadata,
		AnalysisRecord analysisRecord,
        CommandContext context) 
	throws QueryPlannerException, QueryMetadataException, MetaMatrixComponentException {
		
        boolean debug = analysisRecord.recordDebug();
		if(debug) {
            analysisRecord.println("\n----------------------------------------------------------------------------"); //$NON-NLS-1$
            analysisRecord.println("GENERATE CANONICAL: \n" + command); //$NON-NLS-1$
		}   
		makeCanonical(node, command, metadata, analysisRecord, context);

		if(debug) {
            analysisRecord.println("\nCANONICAL PLAN: \n" + node.getCanonicalPlan()); //$NON-NLS-1$
		}   
				
		Iterator commands = command.getSubCommands().iterator();
		while (commands.hasNext()) {
			Command subcommand = (Command) commands.next();
			CommandTreeNode child = new CommandTreeNode();
			node.addLastChild(child);
            child.setParent(node);
			recursiveGenerateCanonical(child, subcommand, metadata, analysisRecord, context);
		}
	}

	/**
	 * Method recursiveOptimize.
	 * @param node
	 * @param minimizeToSource
	 * @param metadata
	 * @param boundReferencesMap
	 * @param debug
	 */
	private static ProcessorPlan recursiveOptimize(
		CommandTreeNode node,
        IDGenerator idGenerator,
		QueryMetadataInterface metadata,
        CapabilitiesFinder capFinder,
		AnalysisRecord analysisRecord,
        CommandContext context) 
	throws QueryPlannerException, QueryMetadataException, MetaMatrixComponentException {

				
		Iterator commands = node.getChildren().iterator();
		while (commands.hasNext()) {
			CommandTreeNode child = (CommandTreeNode) commands.next();
			recursiveOptimize(child, idGenerator, metadata, capFinder, analysisRecord, context);
		}
        
        // Create metadata adapter if necessary
        QueryMetadataInterface optMetadata = metadata;
        Map commandMetadata = node.getCommand().getTemporaryMetadata();
        if(commandMetadata != null && commandMetadata.size() > 0) {
            optMetadata = new TempMetadataAdapter(metadata, new TempMetadataStore(commandMetadata));
        }

        boolean debug = analysisRecord.recordDebug();
		if(debug) {
			analysisRecord.println("\n----------------------------------------------------------------------------"); //$NON-NLS-1$
            analysisRecord.println("OPTIMIZE: \n" + node.getCommand()); //$NON-NLS-1$
		}   
                                   
		//optimize step
		ProcessorPlan plan = optimize(node, idGenerator, optMetadata, capFinder, analysisRecord, context);
		node.setProcessorPlan(plan);

		return plan;
	}
	
	/**
	 * Method optimize.
	 * @param node
	 * @param minimizeToSource
	 * @param metadata
	 * @param boundReferencesMap
	 * @return ProcessorPlan
	 */
	private static ProcessorPlan optimize(
		CommandTreeNode node,
        IDGenerator idGenerator,
		QueryMetadataInterface metadata,
        CapabilitiesFinder capFinder,
		AnalysisRecord analysisRecord,
        CommandContext context) 
	throws QueryPlannerException, QueryMetadataException, MetaMatrixComponentException {

		ProcessorPlan result = null;

		if (node.getCommandType() == CommandTreeNode.TYPE_RELATIONAL_COMMAND){
			result = RELATIONAL_PLANNER.optimize(node, idGenerator, metadata, capFinder, analysisRecord, context);
		} else if (node.getCommandType() == CommandTreeNode.TYPE_XML_COMMAND){
			result = XML_PLANNER.optimize(node, idGenerator, metadata, capFinder, analysisRecord, context);
		} else if (node.getCommandType() == CommandTreeNode.TYPE_PROCEDURAL_COMMAND){
			result = PROCEDURE_PLANNER.optimize(node, idGenerator, metadata, capFinder, analysisRecord, context);
        } else if (node.getCommandType() == CommandTreeNode.TYPE_XQUERY_COMMAND){
            result = XQUERY_PLANNER.optimize(node, idGenerator, metadata, capFinder, analysisRecord, context);
        } else if (node.getCommandType() == CommandTreeNode.TYPE_BATCHED_UPDATE_COMMAND){
            result = BATCHED_UPDATE_PLANNER.optimize(node, idGenerator, metadata, capFinder, analysisRecord, context);
        }
		return result;
	}
	
	private static void makeCanonical(CommandTreeNode node, Command command, QueryMetadataInterface metadata, AnalysisRecord analysisRecord, CommandContext context)
	throws QueryPlannerException, QueryMetadataException, MetaMatrixComponentException {

        node.setCommand(command);

        // Use command metadata while checking type                
        QueryMetadataInterface optMetadata = metadata;
        Map tempMetadata = command.getTemporaryMetadata();
        if(tempMetadata != null && !tempMetadata.isEmpty()) {
            optMetadata = new TempMetadataAdapter(metadata, new TempMetadataStore(tempMetadata));
        }
        
        // Easy to detect batched update planner, procedural planner, or XQueryPlanner
        int commandType = command.getType();
        if (commandType == Command.TYPE_BATCHED_UPDATE) {
            node.setCommandType(CommandTreeNode.TYPE_BATCHED_UPDATE_COMMAND);
            BATCHED_UPDATE_PLANNER.generateCanonical(node, optMetadata, analysisRecord, context);
            return;
        } else if (commandType == Command.TYPE_UPDATE_PROCEDURE){
            //it's a procedure command
            node.setCommandType(CommandTreeNode.TYPE_PROCEDURAL_COMMAND);
            PROCEDURE_PLANNER.generateCanonical(node, optMetadata, analysisRecord, context);   
			return;
        } else if (commandType == Command.TYPE_XQUERY){
            node.setCommandType(CommandTreeNode.TYPE_XQUERY_COMMAND);
            XQUERY_PLANNER.generateCanonical(node, optMetadata, analysisRecord, context);   
            return;
        } else if (commandType == Command.TYPE_DYNAMIC){
            node.setCommandType(CommandTreeNode.TYPE_DYNAMIC_COMMAND);
            return;
        }
        
        // Set type
        node.setCommandType(CommandTreeNode.TYPE_RELATIONAL_COMMAND);
        if(commandType == Command.TYPE_QUERY) {
            Collection groups = GroupCollectorVisitor.getGroups(command, true);
            if(groups.size() == 1) {
                GroupSymbol group = (GroupSymbol) groups.iterator().next();
                                     
                if(optMetadata.isXMLGroup(group.getMetadataID())) {
                    node.setCommandType(CommandTreeNode.TYPE_XML_COMMAND);    
                }                    
            }
        }
        
        if(node.getCommandType() == CommandTreeNode.TYPE_RELATIONAL_COMMAND) {    
            PlanHints hints = new PlanHints();
            node.setProperty(RelationalPlanner.HINTS, hints);
            RELATIONAL_PLANNER.generateCanonical(node, optMetadata, analysisRecord, context);
        } else {
            XML_PLANNER.generateCanonical(node, optMetadata, analysisRecord, context);            
        }
        
	}
}
