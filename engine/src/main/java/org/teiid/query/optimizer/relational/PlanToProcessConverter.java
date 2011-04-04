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

package org.teiid.query.optimizer.relational;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.teiid.api.exception.query.QueryMetadataException;
import org.teiid.api.exception.query.QueryPlannerException;
import org.teiid.core.CoreConstants;
import org.teiid.core.TeiidComponentException;
import org.teiid.core.id.IDGenerator;
import org.teiid.core.id.IntegerID;
import org.teiid.core.id.IntegerIDFactory;
import org.teiid.core.util.Assertion;
import org.teiid.query.QueryPlugin;
import org.teiid.query.analysis.AnalysisRecord;
import org.teiid.query.metadata.QueryMetadataInterface;
import org.teiid.query.metadata.TempMetadataID;
import org.teiid.query.optimizer.capabilities.CapabilitiesFinder;
import org.teiid.query.optimizer.capabilities.SourceCapabilities;
import org.teiid.query.optimizer.capabilities.SourceCapabilities.Capability;
import org.teiid.query.optimizer.relational.plantree.NodeConstants;
import org.teiid.query.optimizer.relational.plantree.PlanNode;
import org.teiid.query.optimizer.relational.plantree.NodeConstants.Info;
import org.teiid.query.optimizer.relational.rules.CapabilitiesUtil;
import org.teiid.query.optimizer.relational.rules.FrameUtil;
import org.teiid.query.processor.ProcessorPlan;
import org.teiid.query.processor.relational.AccessNode;
import org.teiid.query.processor.relational.ArrayTableNode;
import org.teiid.query.processor.relational.DependentAccessNode;
import org.teiid.query.processor.relational.DependentProcedureAccessNode;
import org.teiid.query.processor.relational.DependentProcedureExecutionNode;
import org.teiid.query.processor.relational.GroupingNode;
import org.teiid.query.processor.relational.InsertPlanExecutionNode;
import org.teiid.query.processor.relational.JoinNode;
import org.teiid.query.processor.relational.LimitNode;
import org.teiid.query.processor.relational.MergeJoinStrategy;
import org.teiid.query.processor.relational.NestedLoopJoinStrategy;
import org.teiid.query.processor.relational.NestedTableJoinStrategy;
import org.teiid.query.processor.relational.NullNode;
import org.teiid.query.processor.relational.EnhancedSortMergeJoinStrategy;
import org.teiid.query.processor.relational.PlanExecutionNode;
import org.teiid.query.processor.relational.ProjectIntoNode;
import org.teiid.query.processor.relational.ProjectNode;
import org.teiid.query.processor.relational.RelationalNode;
import org.teiid.query.processor.relational.RelationalPlan;
import org.teiid.query.processor.relational.SelectNode;
import org.teiid.query.processor.relational.SortNode;
import org.teiid.query.processor.relational.TextTableNode;
import org.teiid.query.processor.relational.UnionAllNode;
import org.teiid.query.processor.relational.XMLTableNode;
import org.teiid.query.processor.relational.JoinNode.JoinStrategyType;
import org.teiid.query.processor.relational.MergeJoinStrategy.SortOption;
import org.teiid.query.processor.relational.SortUtility.Mode;
import org.teiid.query.resolver.util.ResolverUtil;
import org.teiid.query.sql.lang.ArrayTable;
import org.teiid.query.sql.lang.Command;
import org.teiid.query.sql.lang.Criteria;
import org.teiid.query.sql.lang.Insert;
import org.teiid.query.sql.lang.JoinType;
import org.teiid.query.sql.lang.OrderBy;
import org.teiid.query.sql.lang.Query;
import org.teiid.query.sql.lang.QueryCommand;
import org.teiid.query.sql.lang.StoredProcedure;
import org.teiid.query.sql.lang.TableFunctionReference;
import org.teiid.query.sql.lang.TextTable;
import org.teiid.query.sql.lang.XMLTable;
import org.teiid.query.sql.lang.SetQuery.Operation;
import org.teiid.query.sql.lang.XMLTable.XMLColumn;
import org.teiid.query.sql.symbol.ElementSymbol;
import org.teiid.query.sql.symbol.Expression;
import org.teiid.query.sql.symbol.GroupSymbol;
import org.teiid.query.sql.util.SymbolMap;
import org.teiid.query.sql.visitor.EvaluatableVisitor;
import org.teiid.query.sql.visitor.GroupCollectorVisitor;


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
        throws QueryPlannerException, TeiidComponentException {

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
		throws QueryPlannerException, TeiidComponentException {

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
			RelationalNode child = convertPlan(childNode);
			if (planNode.getType() == NodeConstants.Types.SET_OP && childNode.getType() == NodeConstants.Types.SET_OP && childNode.hasBooleanProperty(Info.USE_ALL)) {
				for (RelationalNode grandChild : child.getChildren()) {
					if (grandChild != null) {
						nextParent.addChild(grandChild);
					}
				}
			} else {
				nextParent.addChild(child);
			}
		}

        // Return root of tree for top node
		return convertedNode;
	}

    protected int getID() {
        IntegerIDFactory intFactory = (IntegerIDFactory) idGenerator.getDefaultFactory();
        return ((IntegerID) intFactory.create()).getValue();
    }
    
	protected RelationalNode convertNode(PlanNode node)
		throws QueryPlannerException, TeiidComponentException {

		RelationalNode processNode = null;

		switch(node.getType()) {
			case NodeConstants.Types.PROJECT:
                GroupSymbol intoGroup = (GroupSymbol) node.getProperty(NodeConstants.Info.INTO_GROUP);
                if(intoGroup != null) {
                    try {
                    	Insert insert = (Insert)node.getFirstChild().getProperty(Info.VIRTUAL_COMMAND);
                        List<ElementSymbol> allIntoElements = insert.getVariables();
                        
                        Object groupID = intoGroup.getMetadataID();
                        Object modelID = metadata.getModelID(groupID);
                        String modelName = metadata.getFullName(modelID);
                        if (metadata.isVirtualGroup(groupID)) {
                        	InsertPlanExecutionNode ipen = new InsertPlanExecutionNode(getID(), metadata);
                        	ipen.setProcessorPlan((ProcessorPlan)node.getFirstChild().getProperty(Info.PROCESSOR_PLAN));
                        	ipen.setReferences(insert.getValues());
                        	processNode = ipen;
                        } else {
	                        ProjectIntoNode pinode = new ProjectIntoNode(getID());
	                        pinode.setIntoGroup(intoGroup);
	                        pinode.setIntoElements(allIntoElements);
	                        pinode.setModelName(modelName);
	                        processNode = pinode;
                            SourceCapabilities caps = capFinder.findCapabilities(modelName);
                            if (caps.supportsCapability(Capability.INSERT_WITH_ITERATOR)) {
                            	pinode.setMode(org.teiid.query.processor.relational.ProjectIntoNode.Mode.ITERATOR);
                            } else if (caps.supportsCapability(Capability.BULK_UPDATE)) {
                            	pinode.setMode(org.teiid.query.processor.relational.ProjectIntoNode.Mode.BULK);
                            } else if (caps.supportsCapability(Capability.BATCHED_UPDATES)) {
                            	pinode.setMode(org.teiid.query.processor.relational.ProjectIntoNode.Mode.BATCH);
                            } else {
                            	pinode.setMode(org.teiid.query.processor.relational.ProjectIntoNode.Mode.SINGLE);
                            }
                        }
                    } catch(QueryMetadataException e) {
                        throw new TeiidComponentException(e);
                    }

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
                if(stype == JoinStrategyType.MERGE || stype == JoinStrategyType.ENHANCED_SORT) {
                	MergeJoinStrategy mjStrategy = null;
                	if (stype.equals(JoinStrategyType.ENHANCED_SORT)) { 
                		mjStrategy = new EnhancedSortMergeJoinStrategy(leftSort, (SortOption)node.getProperty(NodeConstants.Info.SORT_RIGHT));
                	} else {
                		mjStrategy = new MergeJoinStrategy(leftSort, (SortOption)node.getProperty(NodeConstants.Info.SORT_RIGHT), false);
                	}
                    jnode.setJoinStrategy(mjStrategy);
                    List leftExpressions = (List) node.getProperty(NodeConstants.Info.LEFT_EXPRESSIONS);
                    List rightExpressions = (List) node.getProperty(NodeConstants.Info.RIGHT_EXPRESSIONS);
                    jnode.setJoinExpressions(leftExpressions, rightExpressions);
                    joinCrits = (List) node.getProperty(NodeConstants.Info.NON_EQUI_JOIN_CRITERIA);
                } else if (stype == JoinStrategyType.NESTED_TABLE) {
                	NestedTableJoinStrategy ntjStrategy = new NestedTableJoinStrategy();
                	jnode.setJoinStrategy(ntjStrategy);
                	SymbolMap references = (SymbolMap)FrameUtil.findJoinSourceNode(node.getFirstChild()).getProperty(NodeConstants.Info.CORRELATED_REFERENCES);
            		ntjStrategy.setLeftMap(references);
                	references = (SymbolMap)FrameUtil.findJoinSourceNode(node.getLastChild()).getProperty(NodeConstants.Info.CORRELATED_REFERENCES);
            		ntjStrategy.setRightMap(references);
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
                            
                            if(modelID != null){
                                depAccessNode.setMaxSetSize(CapabilitiesUtil.getMaxInCriteriaSize(modelID, metadata, capFinder));
                                depAccessNode.setMaxPredicates(CapabilitiesUtil.getMaxDependentPredicates(modelID, metadata, capFinder));   
                            }
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
                                processNode = correctProjectionInternalTables(node, aNode);
                            }
                        } catch (QueryMetadataException err) {
                            throw new TeiidComponentException(err);
                        }
                        aNode.setShouldEvaluateExpressions(EvaluatableVisitor.needsProcessingEvaluation(command));
                    }
                    
                    if (command instanceof QueryCommand) {
	                    try {
	                        command = (Command)command.clone();
	                        boolean aliasGroups = modelID != null && CapabilitiesUtil.supportsGroupAliases(modelID, metadata, capFinder);
	                        boolean aliasColumns = modelID != null && CapabilitiesUtil.supports(Capability.QUERY_SELECT_EXPRESSION, modelID, metadata, capFinder);
	                        command.acceptVisitor(new AliasGenerator(aliasGroups, !aliasColumns));
	                    } catch (QueryMetadataException err) {
	                        throw new TeiidComponentException(err);
	                    }
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
                OrderBy orderBy = (OrderBy) node.getProperty(NodeConstants.Info.SORT_ORDER);
				if (orderBy != null) {
					sortNode.setSortElements(orderBy.getOrderByItems());
				}
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
				Object source = node.getProperty(NodeConstants.Info.TABLE_FUNCTION);
				if (source instanceof XMLTable) {
					XMLTable xt = (XMLTable)source;
					XMLTableNode xtn = new XMLTableNode(getID());
					//we handle the projection filtering once here rather than repeating the
					//path analysis on a per plan basis
					updateGroupName(node, xt);
					Map elementMap = RelationalNode.createLookupMap(xt.getProjectedSymbols());
			        List cols = (List) node.getProperty(NodeConstants.Info.OUTPUT_COLS);
					int[] projectionIndexes = RelationalNode.getProjectionIndexes(elementMap, cols);
					ArrayList<XMLColumn> filteredColumns = new ArrayList<XMLColumn>(projectionIndexes.length);
					for (int col : projectionIndexes) {
						filteredColumns.add(xt.getColumns().get(col));
					}
					xt.getXQueryExpression().useDocumentProjection(filteredColumns, analysisRecord);
					xtn.setProjectedColumns(filteredColumns);
					xtn.setTable(xt);
					processNode = xtn;
					break;
				}
				if (source instanceof TextTable) {
					TextTableNode ttn = new TextTableNode(getID());
					TextTable tt = (TextTable)source;
					updateGroupName(node, tt);
					ttn.setTable(tt);
					processNode = ttn;
					break;
				}
				if (source instanceof ArrayTable) {
					ArrayTableNode atn = new ArrayTableNode(getID());
					ArrayTable at = (ArrayTable)source;
					updateGroupName(node, at);
					atn.setTable(at);
					processNode = atn;
					break;
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
                LimitNode ln = new LimitNode(getID(), rowLimit, offset);
                ln.setImplicit(node.hasBooleanProperty(Info.IS_IMPLICIT_LIMIT));
                processNode = ln;
                break;
                
            case NodeConstants.Types.NULL:
                processNode = new NullNode(getID());
                break;

			default:
                throw new QueryPlannerException(QueryPlugin.Util.getString("ERR.015.004.0007", NodeConstants.getNodeTypeString(node.getType()))); //$NON-NLS-1$
		}

		if(processNode != null) {
			processNode = prepareToAdd(node, processNode);
		}

		return processNode;
	}

	private void updateGroupName(PlanNode node, TableFunctionReference tt) {
		String groupName = node.getGroups().iterator().next().getName();
		tt.getGroupSymbol().setName(groupName);
		for (ElementSymbol symbol : tt.getProjectedSymbols()) {
			symbol.setGroupSymbol(new GroupSymbol(groupName));
		}
	}

    private RelationalNode correctProjectionInternalTables(PlanNode node,
                                                                AccessNode aNode) throws QueryMetadataException,
                                                                                                       TeiidComponentException {
        if (node.getGroups().size() != 1) {
            return aNode;
        }
        GroupSymbol group = node.getGroups().iterator().next();
        if (!CoreConstants.SYSTEM_MODEL.equals(metadata.getFullName(metadata.getModelID(group.getMetadataID()))) 
        		&& !CoreConstants.SYSTEM_ADMIN_MODEL.equals(metadata.getFullName(metadata.getModelID(group.getMetadataID())))) {
            return aNode;
        }
        List projectSymbols = (List) node.getProperty(NodeConstants.Info.OUTPUT_COLS);
        List<ElementSymbol> acutalColumns = ResolverUtil.resolveElementsInGroup(group, metadata);
        if (projectSymbols.equals(acutalColumns)) {
        	return aNode;
        }
        node.setProperty(NodeConstants.Info.OUTPUT_COLS, acutalColumns);
        if (node.getParent() != null && node.getParent().getType() == NodeConstants.Types.PROJECT) {
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
		throws QueryPlannerException, TeiidComponentException {

		// Look up connector binding name
		try {
			Object modelID = node.getProperty(NodeConstants.Info.MODEL_ID);
			if(modelID == null || modelID instanceof TempMetadataID) {
				Command command = (Command) node.getProperty(NodeConstants.Info.ATOMIC_REQUEST);
				if(command instanceof StoredProcedure){
					modelID = ((StoredProcedure)command).getModelID();
				}else{
					Collection<GroupSymbol> groups = GroupCollectorVisitor.getGroups(command, true);
					GroupSymbol group = groups.iterator().next();

					modelID = metadata.getModelID(group.getMetadataID());
				}
			}
			String cbName = metadata.getFullName(modelID);
			return cbName;
		} catch(QueryMetadataException e) {
            throw new QueryPlannerException(e, QueryPlugin.Util.getString("ERR.015.004.0009")); //$NON-NLS-1$
		}
	}

}
