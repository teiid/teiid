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

package com.metamatrix.query.optimizer.relational;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import com.metamatrix.api.exception.MetaMatrixComponentException;
import com.metamatrix.api.exception.query.QueryMetadataException;
import com.metamatrix.api.exception.query.QueryPlannerException;
import com.metamatrix.core.id.IDGenerator;
import com.metamatrix.core.id.IntegerID;
import com.metamatrix.core.id.IntegerIDFactory;
import com.metamatrix.core.util.Assertion;
import com.metamatrix.query.analysis.AnalysisRecord;
import com.metamatrix.query.execution.QueryExecPlugin;
import com.metamatrix.query.metadata.QueryMetadataInterface;
import com.metamatrix.query.metadata.TempMetadataID;
import com.metamatrix.query.optimizer.capabilities.CapabilitiesFinder;
import com.metamatrix.query.optimizer.capabilities.SourceCapabilities;
import com.metamatrix.query.optimizer.capabilities.SourceCapabilities.Capability;
import com.metamatrix.query.optimizer.relational.plantree.NodeConstants;
import com.metamatrix.query.optimizer.relational.plantree.PlanNode;
import com.metamatrix.query.optimizer.relational.rules.CapabilitiesUtil;
import com.metamatrix.query.processor.ProcessorPlan;
import com.metamatrix.query.processor.relational.AccessNode;
import com.metamatrix.query.processor.relational.DependentAccessNode;
import com.metamatrix.query.processor.relational.DependentProcedureAccessNode;
import com.metamatrix.query.processor.relational.DependentProcedureExecutionNode;
import com.metamatrix.query.processor.relational.GroupingNode;
import com.metamatrix.query.processor.relational.JoinNode;
import com.metamatrix.query.processor.relational.LimitNode;
import com.metamatrix.query.processor.relational.MergeJoinStrategy;
import com.metamatrix.query.processor.relational.NestedLoopJoinStrategy;
import com.metamatrix.query.processor.relational.NullNode;
import com.metamatrix.query.processor.relational.PartitionedSortJoin;
import com.metamatrix.query.processor.relational.PlanExecutionNode;
import com.metamatrix.query.processor.relational.ProjectIntoNode;
import com.metamatrix.query.processor.relational.ProjectNode;
import com.metamatrix.query.processor.relational.RelationalNode;
import com.metamatrix.query.processor.relational.RelationalPlan;
import com.metamatrix.query.processor.relational.SelectNode;
import com.metamatrix.query.processor.relational.SortNode;
import com.metamatrix.query.processor.relational.UnionAllNode;
import com.metamatrix.query.processor.relational.JoinNode.JoinStrategyType;
import com.metamatrix.query.processor.relational.MergeJoinStrategy.SortOption;
import com.metamatrix.query.processor.relational.SortUtility.Mode;
import com.metamatrix.query.resolver.util.ResolverUtil;
import com.metamatrix.query.sql.lang.Command;
import com.metamatrix.query.sql.lang.Criteria;
import com.metamatrix.query.sql.lang.JoinType;
import com.metamatrix.query.sql.lang.Query;
import com.metamatrix.query.sql.lang.StoredProcedure;
import com.metamatrix.query.sql.lang.SetQuery.Operation;
import com.metamatrix.query.sql.symbol.Expression;
import com.metamatrix.query.sql.symbol.GroupSymbol;
import com.metamatrix.query.sql.util.SymbolMap;
import com.metamatrix.query.sql.visitor.EvaluatableVisitor;
import com.metamatrix.query.sql.visitor.GroupCollectorVisitor;
import com.metamatrix.query.util.ErrorMessageKeys;

public class PlanToProcessConverter {
	protected QueryMetadataInterface metadata;
	private IDGenerator idGenerator;
	private AnalysisRecord analysisRecord;
	private CapabilitiesFinder capFinder;
	
	public PlanToProcessConverter(QueryMetadataInterface metadata, IDGenerator idGenerator, AnalysisRecord analysisRecord, CapabilitiesFinder capFinder) {
		this.metadata = metadata;
		this.idGenerator = idGenerator;
		this.analysisRecord = analysisRecord;
		this.capFinder = capFinder;
	}
	
    public RelationalPlan convert(PlanNode planNode)
        throws QueryPlannerException, MetaMatrixComponentException {

        boolean debug = analysisRecord.recordDebug();
        if(debug) {
            analysisRecord.println("\n============================================================================"); //$NON-NLS-1$
            analysisRecord.println("CONVERTING PLAN TREE TO PROCESS TREE"); //$NON-NLS-1$
        }

        // Convert plan tree nodes into process tree nodes
        RelationalNode processNode = convertPlan(planNode);
        if(debug) {
            analysisRecord.println("\nPROCESS PLAN = \n" + processNode); //$NON-NLS-1$
            analysisRecord.println("============================================================================"); //$NON-NLS-1$
        }

        RelationalPlan processPlan = new RelationalPlan(processNode);
        return processPlan;

    }

	private RelationalNode convertPlan(PlanNode planNode)
		throws QueryPlannerException, MetaMatrixComponentException {

		// Convert current node in planTree
		RelationalNode convertedNode = convertNode(planNode);
		
		if(convertedNode == null) {
		    Assertion.assertTrue(planNode.getChildCount() == 1);
	        return convertPlan(planNode.getFirstChild());
		}
		
		RelationalNode nextParent = convertedNode;
		
        // convertedNode may be the head of 1 or more nodes   - go to end of chain
        while(nextParent.getChildren()[0] != null) {
            nextParent = nextParent.getChildren()[0];
        }
		
		// Call convertPlan recursively on children
		for (PlanNode childNode : planNode.getChildren()) {
		    nextParent.addChild(convertPlan(childNode));
		}

        // Return root of tree for top node
		return convertedNode;
	}

    protected int getID() {
        IntegerIDFactory intFactory = (IntegerIDFactory) idGenerator.getDefaultFactory();
        return ((IntegerID) intFactory.create()).getValue();
    }
    
	protected RelationalNode convertNode(PlanNode node)
		throws QueryPlannerException, MetaMatrixComponentException {

		RelationalNode processNode = null;

		switch(node.getType()) {
			case NodeConstants.Types.PROJECT:
                GroupSymbol intoGroup = (GroupSymbol) node.getProperty(NodeConstants.Info.INTO_GROUP);
                if(intoGroup != null) {
                    ProjectIntoNode pinode = new ProjectIntoNode(getID());
                    pinode.setIntoGroup(intoGroup);
                    try {
                        // Figure out what elements should be inserted based on what is being projected 
                        // from child node
                        List allIntoElements = ResolverUtil.resolveElementsInGroup(intoGroup, metadata);
                        
                        pinode.setIntoElements(allIntoElements);

                        Object groupID = intoGroup.getMetadataID();
                        Object modelID = metadata.getModelID(groupID);
                        String modelName = metadata.getFullName(modelID);
                        pinode.setModelName(modelName);
                        if (!metadata.isVirtualGroup(groupID) && !metadata.isTemporaryTable(groupID)) {
                            SourceCapabilities caps = capFinder.findCapabilities(modelName);
                            pinode.setDoBatching(caps.supportsCapability(Capability.BATCHED_UPDATES));
                            pinode.setDoBulkInsert(caps.supportsCapability(Capability.BULK_UPDATE));
                        } else if (metadata.isTemporaryTable(groupID)) {
                            pinode.setDoBulkInsert(true);
                        }
                    } catch(QueryMetadataException e) {
                        throw new MetaMatrixComponentException(e);
                    }

                    processNode = pinode;

                } else {
                    List symbols = (List) node.getProperty(NodeConstants.Info.PROJECT_COLS);
                    
                    ProjectNode pnode = new ProjectNode(getID());
                    pnode.setSelectSymbols(symbols);
            		processNode = pnode;
                }
                break;

			case NodeConstants.Types.JOIN:
                JoinType jtype = (JoinType) node.getProperty(NodeConstants.Info.JOIN_TYPE);
                JoinStrategyType stype = (JoinStrategyType) node.getProperty(NodeConstants.Info.JOIN_STRATEGY);

                JoinNode jnode = new JoinNode(getID());
                jnode.setJoinType(jtype);
                jnode.setLeftDistinct(node.hasBooleanProperty(NodeConstants.Info.IS_LEFT_DISTINCT));
                jnode.setRightDistinct(node.hasBooleanProperty(NodeConstants.Info.IS_RIGHT_DISTINCT));
                List joinCrits = (List) node.getProperty(NodeConstants.Info.JOIN_CRITERIA);
                String depValueSource = (String) node.getProperty(NodeConstants.Info.DEPENDENT_VALUE_SOURCE);
                SortOption leftSort = (SortOption)node.getProperty(NodeConstants.Info.SORT_LEFT);
                if(stype.equals(JoinStrategyType.MERGE) || stype.equals(JoinStrategyType.PARTITIONED_SORT)) {
                	MergeJoinStrategy mjStrategy = null;
                	if (stype.equals(JoinStrategyType.PARTITIONED_SORT)) { 
                		mjStrategy = new PartitionedSortJoin(leftSort, (SortOption)node.getProperty(NodeConstants.Info.SORT_RIGHT));
                	} else {
                		mjStrategy = new MergeJoinStrategy(leftSort, (SortOption)node.getProperty(NodeConstants.Info.SORT_RIGHT), false);
                	}
                    jnode.setJoinStrategy(mjStrategy);
                    List leftExpressions = (List) node.getProperty(NodeConstants.Info.LEFT_EXPRESSIONS);
                    List rightExpressions = (List) node.getProperty(NodeConstants.Info.RIGHT_EXPRESSIONS);
                    jnode.setJoinExpressions(leftExpressions, rightExpressions);
                    joinCrits = (List) node.getProperty(NodeConstants.Info.NON_EQUI_JOIN_CRITERIA);
                } else {
                    NestedLoopJoinStrategy nljStrategy = new NestedLoopJoinStrategy();
                    jnode.setJoinStrategy(nljStrategy);
                } 
                Criteria joinCrit = Criteria.combineCriteria(joinCrits);
                jnode.setJoinCriteria(joinCrit);
                               
                processNode = jnode;
                
                jnode.setDependentValueSource(depValueSource);
                
				break;

			case NodeConstants.Types.ACCESS:
                ProcessorPlan plan = (ProcessorPlan) node.getProperty(NodeConstants.Info.PROCESSOR_PLAN);
                if(plan != null) {
                    
                    PlanExecutionNode peNode = null;
                    
                    Criteria crit = (Criteria)node.getProperty(NodeConstants.Info.PROCEDURE_CRITERIA);
                    
                    if (crit != null) {
                        List references = (List)node.getProperty(NodeConstants.Info.PROCEDURE_INPUTS);
                        List defaults = (List)node.getProperty(NodeConstants.Info.PROCEDURE_DEFAULTS);
                        
                        peNode = new DependentProcedureExecutionNode(getID(), crit, references, defaults);                        
                    } else {
                        peNode = new PlanExecutionNode(getID());
                    }
                    
                    peNode.setProcessorPlan(plan);
                    processNode = peNode;

                } else {
                    AccessNode aNode = null;
                    Command command = (Command) node.getProperty(NodeConstants.Info.ATOMIC_REQUEST);
                    Object modelID = node.getProperty(NodeConstants.Info.MODEL_ID);
                    
                    if(node.hasBooleanProperty(NodeConstants.Info.IS_DEPENDENT_SET)) {
                        if (command instanceof StoredProcedure) {
                            List references = (List)node.getProperty(NodeConstants.Info.PROCEDURE_INPUTS);
                            List defaults = (List)node.getProperty(NodeConstants.Info.PROCEDURE_DEFAULTS);
                            Criteria crit = (Criteria)node.getProperty(NodeConstants.Info.PROCEDURE_CRITERIA);
                            
                            DependentProcedureAccessNode depAccessNode = new DependentProcedureAccessNode(getID(), crit, references, defaults);
                            processNode = depAccessNode;
                            aNode = depAccessNode;
                        } else {
                            //create dependent access node
                            DependentAccessNode depAccessNode = new DependentAccessNode(getID());
                            
                            int maxSetSize = -1;
                            if(modelID != null){
                                try {
                                    // set the max set size for the access node       
                                    maxSetSize = CapabilitiesUtil.getMaxInCriteriaSize(modelID, metadata, capFinder);   
                                }catch(QueryMetadataException e) {
                                    throw new QueryPlannerException(e, QueryExecPlugin.Util.getString(ErrorMessageKeys.OPTIMIZER_0006, modelID));
                                }
                            }
                            depAccessNode.setMaxSetSize(maxSetSize); 
                            processNode = depAccessNode;
                            aNode = depAccessNode;
                        }
                        aNode.setShouldEvaluateExpressions(true);
                    } else {
                        
                        // create access node
                        aNode = new AccessNode(getID());
                        processNode = aNode;
                                                
                        //-- special handling for temp tables. currently they cannot perform projection
                        try {
                            if (command instanceof Query) {
                                processNode = correctProjectionForTempTable(node,
                                                                            aNode);
                            }
                        } catch (QueryMetadataException err) {
                            throw new MetaMatrixComponentException(err);
                        }
                        aNode.setShouldEvaluateExpressions(EvaluatableVisitor.needsProcessingEvaluation(command));
                    }
                    
                    try {
                        command = (Command)command.clone();
                        command.acceptVisitor(new AliasGenerator(modelID != null && CapabilitiesUtil.supportsGroupAliases(modelID, metadata, capFinder)));
                    } catch (QueryMetadataException err) {
                        throw new MetaMatrixComponentException(err);
                    }
                    aNode.setCommand(command);
                    aNode.setModelName(getRoutingName(node));
                }
                break;

			case NodeConstants.Types.SELECT:

				Criteria crit = (Criteria) node.getProperty(NodeConstants.Info.SELECT_CRITERIA);

				SelectNode selnode = new SelectNode(getID());
				selnode.setCriteria(crit);
				processNode = selnode;
                
				break;

			case NodeConstants.Types.SORT:
			case NodeConstants.Types.DUP_REMOVE:
                SortNode sortNode = new SortNode(getID());
                
				List elements = (List) node.getProperty(NodeConstants.Info.SORT_ORDER);
				List sortTypes = (List) node.getProperty(NodeConstants.Info.ORDER_TYPES);
				
				sortNode.setSortElements(elements, sortTypes);
				if (node.getType() == NodeConstants.Types.DUP_REMOVE) {
					sortNode.setMode(Mode.DUP_REMOVE);
				} else if (node.hasBooleanProperty(NodeConstants.Info.IS_DUP_REMOVAL)) {
					sortNode.setMode(Mode.DUP_REMOVE_SORT);
				}

				processNode = sortNode;
				break;
			case NodeConstants.Types.GROUP:
				GroupingNode gnode = new GroupingNode(getID());
				gnode.setGroupingElements( (List) node.getProperty(NodeConstants.Info.GROUP_COLS) );
				gnode.setRemoveDuplicates(node.hasBooleanProperty(NodeConstants.Info.IS_DUP_REMOVAL));
				processNode = gnode;
				break;

			case NodeConstants.Types.SOURCE:
			    SymbolMap symbolMap = (SymbolMap) node.getProperty(NodeConstants.Info.SYMBOL_MAP);
				if(symbolMap != null) {
					PlanNode child = node.getLastChild();

                    if (node.getParent().getType() != NodeConstants.Types.PROJECT || node.getParent().getProperty(NodeConstants.Info.INTO_GROUP) == null) {
                    	if (child.getType() == NodeConstants.Types.PROJECT) {
                    		//update the project cols based upon the original output
                    		child.setProperty(NodeConstants.Info.PROJECT_COLS, child.getProperty(NodeConstants.Info.OUTPUT_COLS));
                    	}
                        child.setProperty(NodeConstants.Info.OUTPUT_COLS, node.getProperty(NodeConstants.Info.OUTPUT_COLS));
                    }
				}
				
				return null;

    		case NodeConstants.Types.SET_OP:
                Operation setOp = (Operation) node.getProperty(NodeConstants.Info.SET_OPERATION);
                boolean useAll = ((Boolean) node.getProperty(NodeConstants.Info.USE_ALL)).booleanValue();
                if(setOp == Operation.UNION) {
                    RelationalNode unionAllNode = new UnionAllNode(getID());

                    if(useAll) {
                        processNode = unionAllNode;
                    } else {
                    	SortNode sNode = new SortNode(getID());
                    	boolean onlyDupRemoval = node.hasBooleanProperty(NodeConstants.Info.IS_DUP_REMOVAL);
                    	sNode.setMode(onlyDupRemoval?Mode.DUP_REMOVE:Mode.DUP_REMOVE_SORT);
                        processNode = sNode;
                        
                        unionAllNode.setElements( (List) node.getProperty(NodeConstants.Info.OUTPUT_COLS) );
                        processNode.addChild(unionAllNode);
                    }
                } else {
                    JoinNode joinAsSet = new JoinNode(getID());
                    joinAsSet.setJoinStrategy(new MergeJoinStrategy(SortOption.SORT_DISTINCT, SortOption.SORT_DISTINCT, true));
                    List leftExpressions = (List) node.getFirstChild().getProperty(NodeConstants.Info.OUTPUT_COLS);
                    List rightExpressions = (List) node.getLastChild().getProperty(NodeConstants.Info.OUTPUT_COLS);
                    joinAsSet.setJoinType(setOp == Operation.EXCEPT ? JoinType.JOIN_ANTI_SEMI : JoinType.JOIN_SEMI);
                    joinAsSet.setJoinExpressions(leftExpressions, rightExpressions);
                    processNode = joinAsSet;
                }

                break;

            case NodeConstants.Types.TUPLE_LIMIT:
                Expression rowLimit = (Expression)node.getProperty(NodeConstants.Info.MAX_TUPLE_LIMIT);
                Expression offset = (Expression)node.getProperty(NodeConstants.Info.OFFSET_TUPLE_COUNT);
                processNode = new LimitNode(getID(), rowLimit, offset);
                break;
                
            case NodeConstants.Types.NULL:
                processNode = new NullNode(getID());
                break;

			default:
                throw new QueryPlannerException(QueryExecPlugin.Util.getString(ErrorMessageKeys.OPTIMIZER_0007, NodeConstants.getNodeTypeString(node.getType())));
		}

		if(processNode != null) {
			processNode = prepareToAdd(node, processNode);
		}

		return processNode;
	}

    private RelationalNode correctProjectionForTempTable(PlanNode node,
                                                                AccessNode aNode) throws QueryMetadataException,
                                                                                                       MetaMatrixComponentException {
        if (node.getGroups().size() != 1) {
            return aNode;
        }
        GroupSymbol group = node.getGroups().iterator().next();
        if (!group.isTempTable()) {
            return aNode;
        }
        List projectSymbols = (List) node.getProperty(NodeConstants.Info.OUTPUT_COLS);
        List acutalColumns = ResolverUtil.resolveElementsInGroup(group, metadata);
        node.setProperty(NodeConstants.Info.OUTPUT_COLS, acutalColumns);
        if (node.getParent().getType() == NodeConstants.Types.PROJECT) {
            //if the parent is already a project, just correcting the output cols is enough
            return aNode;
        }
        ProjectNode pnode = new ProjectNode(getID());
  
        pnode.setSelectSymbols(projectSymbols);
        //if the following cast fails it means that we have a dependent temp table - that is not yet possible
        aNode = (AccessNode)prepareToAdd(node, aNode);
        node.setProperty(NodeConstants.Info.OUTPUT_COLS, projectSymbols);
        pnode.addChild(aNode);
        return pnode;
    }

    private RelationalNode prepareToAdd(PlanNode node,
                                          RelationalNode processNode) {
        // Set the output elements from the plan node
        List cols = (List) node.getProperty(NodeConstants.Info.OUTPUT_COLS);

        processNode.setElements(cols);
        
        // Set the Cost Estimates
        Number estimateNodeCardinality = (Number) node.getProperty(NodeConstants.Info.EST_CARDINALITY);
        processNode.setEstimateNodeCardinality(estimateNodeCardinality);
        Number estimateNodeSetSize = (Number) node.getProperty(NodeConstants.Info.EST_SET_SIZE);
        processNode.setEstimateNodeSetSize(estimateNodeSetSize);
        Number estimateDepAccessCardinality = (Number) node.getProperty(NodeConstants.Info.EST_DEP_CARDINALITY);
        processNode.setEstimateDepAccessCardinality(estimateDepAccessCardinality);
        Number estimateDepJoinCost = (Number) node.getProperty(NodeConstants.Info.EST_DEP_JOIN_COST);
        processNode.setEstimateDepJoinCost(estimateDepJoinCost);
        Number estimateJoinCost = (Number) node.getProperty(NodeConstants.Info.EST_JOIN_COST);
        processNode.setEstimateJoinCost(estimateJoinCost);
       
        return processNode;
    }

	private String getRoutingName(PlanNode node)
		throws QueryPlannerException, MetaMatrixComponentException {

		// Look up connector binding name
		try {
			Object modelID = node.getProperty(NodeConstants.Info.MODEL_ID);
			if(modelID == null || modelID instanceof TempMetadataID) {
				Command command = (Command) node.getProperty(NodeConstants.Info.ATOMIC_REQUEST);
				if(command instanceof StoredProcedure){
					modelID = ((StoredProcedure)command).getModelID();
				}else{
					Collection groups = GroupCollectorVisitor.getGroups(command, true);
					Iterator groupIter = groups.iterator();
					GroupSymbol group = (GroupSymbol) groupIter.next();

					modelID = metadata.getModelID(group.getMetadataID());
				}
			}
			String cbName = metadata.getFullName(modelID);
			return cbName;
		} catch(QueryMetadataException e) {
            throw new QueryPlannerException(e, QueryExecPlugin.Util.getString(ErrorMessageKeys.OPTIMIZER_0009));
		}
	}

}
