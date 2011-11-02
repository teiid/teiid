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

package org.teiid.query.optimizer.relational.rules;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.teiid.api.exception.query.QueryMetadataException;
import org.teiid.api.exception.query.QueryPlannerException;
import org.teiid.core.TeiidComponentException;
import org.teiid.metadata.FunctionMethod.PushDown;
import org.teiid.query.QueryPlugin;
import org.teiid.query.analysis.AnalysisRecord;
import org.teiid.query.metadata.QueryMetadataInterface;
import org.teiid.query.optimizer.capabilities.CapabilitiesFinder;
import org.teiid.query.optimizer.capabilities.SourceCapabilities.Capability;
import org.teiid.query.optimizer.relational.OptimizerRule;
import org.teiid.query.optimizer.relational.RuleStack;
import org.teiid.query.optimizer.relational.plantree.NodeConstants;
import org.teiid.query.optimizer.relational.plantree.NodeEditor;
import org.teiid.query.optimizer.relational.plantree.NodeFactory;
import org.teiid.query.optimizer.relational.plantree.PlanNode;
import org.teiid.query.optimizer.relational.plantree.NodeConstants.Info;
import org.teiid.query.processor.relational.RelationalNode;
import org.teiid.query.sql.lang.Command;
import org.teiid.query.sql.lang.Criteria;
import org.teiid.query.sql.lang.OrderBy;
import org.teiid.query.sql.lang.OrderByItem;
import org.teiid.query.sql.lang.StoredProcedure;
import org.teiid.query.sql.symbol.AggregateSymbol;
import org.teiid.query.sql.symbol.AliasSymbol;
import org.teiid.query.sql.symbol.Constant;
import org.teiid.query.sql.symbol.ElementSymbol;
import org.teiid.query.sql.symbol.Expression;
import org.teiid.query.sql.symbol.ExpressionSymbol;
import org.teiid.query.sql.symbol.Function;
import org.teiid.query.sql.symbol.GroupSymbol;
import org.teiid.query.sql.symbol.SingleElementSymbol;
import org.teiid.query.sql.symbol.WindowFunction;
import org.teiid.query.sql.util.SymbolMap;
import org.teiid.query.sql.visitor.AggregateSymbolCollectorVisitor;
import org.teiid.query.sql.visitor.ElementCollectorVisitor;
import org.teiid.query.sql.visitor.EvaluatableVisitor;
import org.teiid.query.sql.visitor.ExpressionMappingVisitor;
import org.teiid.query.sql.visitor.FunctionCollectorVisitor;
import org.teiid.query.sql.visitor.GroupsUsedByElementsVisitor;
import org.teiid.query.util.CommandContext;


/**
 * <p>This rule is responsible for assigning the output elements to every node in the
 * plan.  The output elements define the columns that are returned from every node.
 * This is generally done by figuring out top-down all the elements required to
 * execute the operation at each node and making sure those elements are selected
 * from the children nodes.  </p>
 */
public final class RuleAssignOutputElements implements OptimizerRule {
	
	private boolean finalRun;
	private boolean checkSymbols;
	
	public RuleAssignOutputElements(boolean finalRun) {
		this.finalRun = finalRun;
	}

    /**
     * Execute the rule.  This rule is executed exactly once during every planning
     * call.  The plan is modified in place - only properties are manipulated, structure
     * is unchanged.
     * @param plan The plan to execute rule on
     * @param metadata The metadata interface
     * @param rules The rule stack, not modified
     * @return The updated plan
     */
	public PlanNode execute(PlanNode plan, QueryMetadataInterface metadata, CapabilitiesFinder capFinder, RuleStack rules, AnalysisRecord analysisRecord, CommandContext context)
		throws QueryPlannerException, QueryMetadataException, TeiidComponentException {

		// Record project node output columns in top node
		PlanNode projectNode = NodeEditor.findNodePreOrder(plan, NodeConstants.Types.PROJECT);

        if(projectNode == null) {
            return plan;
        }

		List<SingleElementSymbol> projectCols = (List<SingleElementSymbol>)projectNode.getProperty(NodeConstants.Info.PROJECT_COLS);

		assignOutputElements(plan, projectCols, metadata, capFinder, rules, analysisRecord, context);

		return plan;
	}

    /**
     * <p>Assign the output elements at a particular node and recurse the tree.  The
     * outputElements needed from above the node have been collected in
     * outputElements.</p>
     *
     * <p>SOURCE nodes:  If we find a SOURCE node, this must define the top
     * of a virtual group.  Physical groups can be identified by ACCESS nodes
     * at this point in the planning stage.  So, we filter the virtual elements
     * in the virtual source based on the required output elements.</p>
     *
     * <p>SET_OP nodes:  If we hit a SET_OP node, this must be a union.  Unions
     * require a lot of special care.  Unions have many branches and the projected
     * elements in each branch are "equivalent" in terms of nodes above the union.
     * This means that any filtering must occur in an identical way in all branches
     * of a union.</p>
     *
     * @param root Node to assign
     * @param outputElements Output elements needed for this node
     * @param metadata Metadata implementation
     */
	private void assignOutputElements(PlanNode root, List<SingleElementSymbol> outputElements, QueryMetadataInterface metadata, CapabilitiesFinder capFinder, RuleStack rules, AnalysisRecord analysisRecord, CommandContext context)
		throws QueryPlannerException, QueryMetadataException, TeiidComponentException {

	    int nodeType = root.getType();
        
		// Update this node's output columns based on parent's columns
		root.setProperty(NodeConstants.Info.OUTPUT_COLS, outputElements);
        
		if (root.getChildCount() == 0) {
            return;
        }

        switch (nodeType) {
		    case NodeConstants.Types.ACCESS:
		        Command command = FrameUtil.getNonQueryCommand(root);
	            if (command instanceof StoredProcedure) {
	                //if the access node represents a stored procedure, then we can't actually change the output symbols
	                root.setProperty(NodeConstants.Info.OUTPUT_COLS, command.getProjectedSymbols());
	            } else if (checkSymbols) {
	            	Object modelId = RuleRaiseAccess.getModelIDFromAccess(root, metadata);
	            	for (SingleElementSymbol symbol : outputElements) {
	                    if(!RuleRaiseAccess.canPushSymbol(symbol, true, modelId, metadata, capFinder, analysisRecord)) {
	                    	throw new QueryPlannerException(QueryPlugin.Util.getString("RuleAssignOutputElements.couldnt_push_expression", symbol, modelId)); //$NON-NLS-1$
	                    } 
					}
	            }
		    case NodeConstants.Types.TUPLE_LIMIT:
		    case NodeConstants.Types.DUP_REMOVE:
		    case NodeConstants.Types.SORT:
		    	if (root.hasBooleanProperty(NodeConstants.Info.UNRELATED_SORT)) {
		    		//add missing sort columns
			    	OrderBy elements = (OrderBy) root.getProperty(NodeConstants.Info.SORT_ORDER);
			    	outputElements = new ArrayList<SingleElementSymbol>(outputElements);
			    	boolean hasUnrelated = false;
			    	for (OrderByItem item : elements.getOrderByItems()) {
			    		if (item.getExpressionPosition() == -1) {
			    			int index = outputElements.indexOf(item.getSymbol());
			    			if (index != -1) {
			    				item.setExpressionPosition(index);
			    			} else {
			    				hasUnrelated = true;
			    				outputElements.add(item.getSymbol());
			    			}
						}
					}
			    	if (!hasUnrelated) {
			    		root.setProperty(NodeConstants.Info.UNRELATED_SORT, false);
			    	}
		    	}
		        assignOutputElements(root.getLastChild(), outputElements, metadata, capFinder, rules, analysisRecord, context);
		        break;
		    case NodeConstants.Types.SOURCE: {
		        outputElements = (List<SingleElementSymbol>)determineSourceOutput(root, outputElements, metadata, capFinder);
	            root.setProperty(NodeConstants.Info.OUTPUT_COLS, outputElements);
	            List<SingleElementSymbol> childElements = filterVirtualElements(root, outputElements, metadata);
	            assignOutputElements(root.getFirstChild(), childElements, metadata, capFinder, rules, analysisRecord, context);
		        break;
		    }
		    case NodeConstants.Types.SET_OP: {
		        for (PlanNode childNode : root.getChildren()) {
		            PlanNode projectNode = NodeEditor.findNodePreOrder(childNode, NodeConstants.Types.PROJECT);
	                List<SingleElementSymbol> projectCols = (List<SingleElementSymbol>)projectNode.getProperty(NodeConstants.Info.PROJECT_COLS);
	                assignOutputElements(childNode, projectCols, metadata, capFinder, rules, analysisRecord, context);
	            }
	            break;
		    }
		    default: {
		    	if (root.getType() == NodeConstants.Types.PROJECT) {
		    		GroupSymbol intoGroup = (GroupSymbol)root.getProperty(NodeConstants.Info.INTO_GROUP);
		            if (intoGroup != null) { //if this is a project into, treat the nodes under the source as a new plan root
		                PlanNode intoRoot = NodeEditor.findNodePreOrder(root, NodeConstants.Types.SOURCE);
		                execute(intoRoot.getFirstChild(), metadata, capFinder, rules, analysisRecord, context);
		                return;
		            }
	            	List<SingleElementSymbol> projectCols = outputElements;
	            	boolean modifiedProject = false;
	            	PlanNode sortNode = NodeEditor.findParent(root, NodeConstants.Types.SORT, NodeConstants.Types.SOURCE);
	            	if (sortNode != null && sortNode.hasBooleanProperty(NodeConstants.Info.UNRELATED_SORT)) {
	            		//if this is the initial rule run, remove unrelated order before changing the project cols
			            if (!finalRun) {
		            		OrderBy elements = (OrderBy) sortNode.getProperty(NodeConstants.Info.SORT_ORDER);
		            		projectCols = new ArrayList<SingleElementSymbol>(projectCols);
		            		for (OrderByItem item : elements.getOrderByItems()) {
		            			if (item.getExpressionPosition() == -1) {
		            				projectCols.remove(item.getSymbol());
		            			}
		            		}
	            		} else {
	            			modifiedProject = true;
	            		}
		            }
		            root.setProperty(NodeConstants.Info.PROJECT_COLS, projectCols);
	            	if (modifiedProject) {
		            	root.getGroups().clear();
		            	root.addGroups(GroupsUsedByElementsVisitor.getGroups(projectCols));
		            	root.addGroups(GroupsUsedByElementsVisitor.getGroups(root.getCorrelatedReferenceElements()));
	            	}
	            	if (root.hasBooleanProperty(Info.HAS_WINDOW_FUNCTIONS)) {
	            		Set<WindowFunction> windowFunctions = getWindowFunctions(projectCols);
	            		if (windowFunctions.isEmpty()) {
	            			root.setProperty(Info.HAS_WINDOW_FUNCTIONS, false);
	            		}
	            	}
		    	}
	            
	            List<SingleElementSymbol> requiredInput = collectRequiredInputSymbols(root);
	            //targeted optimization for unnecessary aggregation
	            if (root.getType() == NodeConstants.Types.GROUP && root.hasBooleanProperty(Info.IS_OPTIONAL) && NodeEditor.findParent(root, NodeConstants.Types.ACCESS) == null) {
	            	PlanNode old = root;
	            	PlanNode next = root.getFirstChild();
	            	NodeEditor.removeChildNode(root.getParent(), root);
	            	
            		SymbolMap symbolMap = (SymbolMap) old.getProperty(NodeConstants.Info.SYMBOL_MAP);
            		if (!symbolMap.asMap().isEmpty()) {
            			FrameUtil.convertFrame(next.getParent(), symbolMap.asMap().keySet().iterator().next().getGroupSymbol(), null, symbolMap.asMap(), metadata);
            		}
    				PlanNode parent = next.getParent();
    				while (parent.getParent() != null && parent.getParent().getType() != NodeConstants.Types.SOURCE) {
    					parent = parent.getParent();
    				}
    				if (!old.hasCollectionProperty(Info.GROUP_COLS)) {
    					//just lob off everything under the projection
    					PlanNode project = NodeEditor.findNodePreOrder(parent, NodeConstants.Types.PROJECT);
    					project.removeAllChildren();
    				} else {
    					PlanNode limit = NodeFactory.getNewNode(NodeConstants.Types.TUPLE_LIMIT);
        				limit.setProperty(Info.MAX_TUPLE_LIMIT, new Constant(1));
	    				if (!rules.contains(RuleConstants.PUSH_LIMIT)) {
	    					rules.push(RuleConstants.PUSH_LIMIT);
	    				}
						parent.getFirstChild().addAsParent(limit);
    				}
	            	execute(parent, metadata, capFinder, rules, analysisRecord, context);
	            	return;
            	}
	            
	            // Call children recursively
	            if(root.getChildCount() == 1) {
	                assignOutputElements(root.getLastChild(), requiredInput, metadata, capFinder, rules, analysisRecord, context);
	            } else {
	                //determine which elements go to each side of the join
	                for (PlanNode childNode : root.getChildren()) {
	                    Set<GroupSymbol> filterGroups = FrameUtil.findJoinSourceNode(childNode).getGroups();
	                    List<SingleElementSymbol> filteredElements = filterElements(requiredInput, filterGroups);

	                    // Call child recursively
	                    assignOutputElements(childNode, filteredElements, metadata, capFinder, rules, analysisRecord, context);
	                }
	            }
		    }
		}
	}

	public static Set<WindowFunction> getWindowFunctions(
			List<SingleElementSymbol> projectCols) {
		LinkedHashSet<WindowFunction> windowFunctions = new LinkedHashSet<WindowFunction>();
		for (SingleElementSymbol singleElementSymbol : projectCols) {
			AggregateSymbolCollectorVisitor.getAggregates(singleElementSymbol, null, null, null, windowFunctions, null);
		}
		return windowFunctions;
	}

    private List<SingleElementSymbol> filterElements(Collection<? extends SingleElementSymbol> requiredInput, Set<GroupSymbol> filterGroups) {
        List<SingleElementSymbol> filteredElements = new ArrayList<SingleElementSymbol>();
        for (SingleElementSymbol element : requiredInput) {
            if(filterGroups.containsAll(GroupsUsedByElementsVisitor.getGroups(element))) {
                filteredElements.add(element);
            }
        }
        return filteredElements;
    }

    /** 
     * A special case to consider is when the virtual group is defined by a
     * UNION (no ALL) or a SELECT DISTINCT.  In this case, the dup removal means 
     * that all columns need to be used to determine duplicates.  So, filtering the
     * columns at all will alter the number of rows flowing through the frame.
     * So, in this case filtering should not occur.  In fact the output columns
     * that were set on root above are filtered, but we actually want all the
     * virtual elements - so just reset it and proceed as before
     * @throws TeiidComponentException 
     * @throws QueryMetadataException 
     * @throws QueryPlannerException 
     */
    static List<? extends SingleElementSymbol> determineSourceOutput(PlanNode root,
                                           List<SingleElementSymbol> outputElements,
                                           QueryMetadataInterface metadata,
                                           CapabilitiesFinder capFinder) throws QueryMetadataException, TeiidComponentException, QueryPlannerException {
        PlanNode virtualRoot = root.getLastChild();
        
        if(hasDupRemoval(virtualRoot)) {
            // Reset the outputColumns for this source node to be all columns for the virtual group
            SymbolMap symbolMap = (SymbolMap) root.getProperty(NodeConstants.Info.SYMBOL_MAP);
            if (!symbolMap.asMap().keySet().containsAll(outputElements)) {
            	outputElements.removeAll(symbolMap.asMap().keySet());
            	throw new QueryPlannerException(QueryPlugin.Util.getString("RuleAssignOutputElements.cannot_introduce_expressions", outputElements)); //$NON-NLS-1$
            }
            return symbolMap.getKeys();
        } 
        PlanNode limit = NodeEditor.findNodePreOrder(root, NodeConstants.Types.TUPLE_LIMIT, NodeConstants.Types.PROJECT);
		if (limit == null) {
			return outputElements;
		}
        //reset the output elements to be the output columns + what's required by the sort
		PlanNode sort = NodeEditor.findNodePreOrder(limit, NodeConstants.Types.SORT, NodeConstants.Types.PROJECT);
        if (sort == null) {
        	return outputElements;
        }
        PlanNode access = NodeEditor.findParent(sort, NodeConstants.Types.ACCESS);
        if (sort.hasBooleanProperty(NodeConstants.Info.UNRELATED_SORT) ||
        		(access != null && capFinder != null && CapabilitiesUtil.supports(Capability.QUERY_ORDERBY_UNRELATED, RuleRaiseAccess.getModelIDFromAccess(access, metadata), metadata, capFinder))) {
    		return outputElements;
        }
        OrderBy sortOrder = (OrderBy)sort.getProperty(NodeConstants.Info.SORT_ORDER);
        List<SingleElementSymbol> topCols = FrameUtil.findTopCols(sort);
        
        SymbolMap symbolMap = (SymbolMap)root.getProperty(NodeConstants.Info.SYMBOL_MAP);
        
        List<ElementSymbol> symbolOrder = symbolMap.getKeys();
        
        for (OrderByItem item : sortOrder.getOrderByItems()) {
            final Expression expr = item.getSymbol();
            int index = topCols.indexOf(expr);
            if (index < 0) {
            	continue;
            }
            ElementSymbol symbol = symbolOrder.get(index);
            if (!outputElements.contains(symbol)) {
                outputElements.add(symbol);
            }
        }
        return outputElements;
    }
    
    /**
     * <p>This method looks at a source node, which defines a virtual group, and filters the
     * virtual elements defined by the group down into just the output elements needed
     * by that source node.  This means, for instance, that the PROJECT node at the top
     * of the virtual group might need to have some elements removed from the project as
     * those elements are no longer needed.  </p>
     *
     * <p>One special case that is handled here is when a virtual group is defined by
     * a UNION ALL.  In this case, the various branches of the union have elements defined
     * and filtering must occur identically in all branches of the union.  </p>
     *
     * @param sourceNode Node to filter
     * @param metadata Metadata implementation
     * @return The filtered list of columns for this node (used in recursing tree)
     * @throws QueryPlannerException 
     */
	static List<SingleElementSymbol> filterVirtualElements(PlanNode sourceNode, List<SingleElementSymbol> outputColumns, QueryMetadataInterface metadata) throws QueryPlannerException {

		PlanNode virtualRoot = sourceNode.getLastChild();

		// Update project cols - typically there is exactly one and that node can
	    // just get the filteredCols determined above.  In the case of one or more
	    // nested set operations (UNION, INTERSECT, EXCEPT) there will be 2 or more
	    // projects.  
	    List<PlanNode> allProjects = NodeEditor.findAllNodes(virtualRoot, NodeConstants.Types.PROJECT, NodeConstants.Types.PROJECT);

        int[] filteredIndex = new int[outputColumns.size()];
        Arrays.fill(filteredIndex, -1);
        
        SymbolMap symbolMap = (SymbolMap)sourceNode.getProperty(NodeConstants.Info.SYMBOL_MAP);
        
        List<ElementSymbol> originalOrder = symbolMap.getKeys();
        
        boolean updateGroups = outputColumns.size() != originalOrder.size();
        boolean[] seenIndex = new boolean[outputColumns.size()];
        boolean newSymbols = false;
        
        for (int i = 0; i < outputColumns.size(); i++) {
            Expression expr = outputColumns.get(i);
            filteredIndex[i] = originalOrder.indexOf(expr);
            if (filteredIndex[i] == -1) {
            	updateGroups = true;
            	//we're adding this symbol, which needs to be updated against respective symbol maps
            	newSymbols = true;
            }
            if (!updateGroups) {
            	seenIndex[filteredIndex[i]] = true;
            }
        }
        
        if (!updateGroups) {
        	for (boolean b : seenIndex) {
				if (!b) {
					updateGroups = true;
					break;
				}
			}
        }
        
        List<SingleElementSymbol> newCols = null;
        for(int i=allProjects.size()-1; i>=0; i--) {
            PlanNode projectNode = allProjects.get(i);
            List<SingleElementSymbol> projectCols = (List<SingleElementSymbol>) projectNode.getProperty(NodeConstants.Info.PROJECT_COLS);

            newCols = RelationalNode.projectTuple(filteredIndex, projectCols, true);
            
            if (newSymbols) {
				SymbolMap childMap = SymbolMap.createSymbolMap(symbolMap.getKeys(), (List) projectNode.getProperty(NodeConstants.Info.PROJECT_COLS));
            	for (int j = 0; j < filteredIndex.length; j++) {
					if (filteredIndex[j] != -1) {
						continue;
					}
					SingleElementSymbol ex = (SingleElementSymbol) outputColumns.get(j).clone();
					ExpressionMappingVisitor.mapExpressions(ex, childMap.asMap());
					newCols.set(j, ex);
					filteredIndex[j] = j;
				}
            }
            
            projectNode.setProperty(NodeConstants.Info.PROJECT_COLS, newCols);
            if (updateGroups) {
	            projectNode.getGroups().clear();
	            projectNode.addGroups(GroupsUsedByElementsVisitor.getGroups(newCols));
	            projectNode.addGroups(GroupsUsedByElementsVisitor.getGroups(projectNode.getCorrelatedReferenceElements()));
            }
        }
        
        if (!updateGroups) {
        	for (int i : filteredIndex) {
				if (i != filteredIndex[i]) {
					updateGroups = true;
					break;
				}
			}
        }
        
        if (updateGroups) {
        	SymbolMap newMap = new SymbolMap();
            List<Expression> originalExpressionOrder = symbolMap.getValues();

        	for (int i = 0; i < filteredIndex.length; i++) {
        		newMap.addMapping(originalOrder.get(filteredIndex[i]), originalExpressionOrder.get(filteredIndex[i]));
			}
        	sourceNode.setProperty(NodeConstants.Info.SYMBOL_MAP, newMap);
        }

		// Create output columns for virtual group project
		return newCols;
	}

    /** 
     * Check all branches for either a dup removal or a non all union.
     *
     * @param node Root of virtual group (node below source node)
     * @return True if the virtual group at this source node does dup removal
     */
	static boolean hasDupRemoval(PlanNode node) {
        
        List<PlanNode> nodes = NodeEditor.findAllNodes(node, NodeConstants.Types.DUP_REMOVE|NodeConstants.Types.SET_OP, NodeConstants.Types.DUP_REMOVE|NodeConstants.Types.PROJECT);
        
        for (PlanNode planNode : nodes) {
            if (planNode.getType() == NodeConstants.Types.DUP_REMOVE
                || (planNode.getType() == NodeConstants.Types.SET_OP && Boolean.FALSE.equals(planNode.getProperty(NodeConstants.Info.USE_ALL)))) {
                return true;
            }
        }
        
        return false;
    }

    /**
     * Collect all required input symbols for a given node.  Input symbols
     * are any symbols that are required in the processing of this node,
     * for instance to create a new element symbol or sort on it, etc.
     * @param node Node to collect for
     */
	private List<SingleElementSymbol> collectRequiredInputSymbols(PlanNode node) {

        Set<SingleElementSymbol> requiredSymbols = new LinkedHashSet<SingleElementSymbol>();
        Set<SingleElementSymbol> createdSymbols = new HashSet<SingleElementSymbol>();
        
        List<SingleElementSymbol> outputCols = (List<SingleElementSymbol>) node.getProperty(NodeConstants.Info.OUTPUT_COLS);

		switch(node.getType()) {
			case NodeConstants.Types.PROJECT:
            {
                List<SingleElementSymbol> projectCols = (List<SingleElementSymbol>) node.getProperty(NodeConstants.Info.PROJECT_COLS);
                for (SingleElementSymbol ss : projectCols) {
                    if(ss instanceof AliasSymbol) {
                        createdSymbols.add(ss);
                        
                        ss = ((AliasSymbol)ss).getSymbol();
                    }
                    
                    if (ss instanceof WindowFunction || (ss instanceof ExpressionSymbol && !(ss instanceof AggregateSymbol))) {
                        createdSymbols.add(ss);
                    }
                    boolean symbolRequired = false;
                    if (finalRun && !(ss instanceof ElementSymbol) && NodeEditor.findParent(node, NodeConstants.Types.ACCESS) == null) {
                    	Collection<Function> functions = FunctionCollectorVisitor.getFunctions(ss, false);
                    	for (Function function : functions) {
							if (function.getFunctionDescriptor().getPushdown() != PushDown.MUST_PUSHDOWN || EvaluatableVisitor.willBecomeConstant(function)) {
								continue;
							}
							//assume we need the whole thing
							requiredSymbols.add(ss);
							symbolRequired = true;
							checkSymbols = true;
							break;
						}
                    }
                    if (!symbolRequired) {
                    	ElementCollectorVisitor.getElements(ss, requiredSymbols);
                    }
                }
				break;
            }
			case NodeConstants.Types.SELECT:
				Criteria selectCriteria = (Criteria) node.getProperty(NodeConstants.Info.SELECT_CRITERIA);
				ElementCollectorVisitor.getElements(selectCriteria, requiredSymbols);
				break;
			case NodeConstants.Types.JOIN:
				List<Criteria> crits = (List) node.getProperty(NodeConstants.Info.JOIN_CRITERIA);
				if(crits != null) {
					for (Criteria joinCriteria : crits) {
						ElementCollectorVisitor.getElements(joinCriteria, requiredSymbols);
					}
				}
				break;
			case NodeConstants.Types.GROUP:
				List<Expression> groupCols = (List<Expression>) node.getProperty(NodeConstants.Info.GROUP_COLS);
				if(groupCols != null) {
				    for (Expression expression : groupCols) {
				    	ElementCollectorVisitor.getElements(expression, requiredSymbols);
                    }
				}
				SymbolMap symbolMap = (SymbolMap) node.getProperty(NodeConstants.Info.SYMBOL_MAP);
				Set<ElementSymbol> usedAggregates = new HashSet<ElementSymbol>();
				
				// Take credit for creating any aggregates that are needed above
				for (SingleElementSymbol outputSymbol : outputCols) {
					if (!(outputSymbol instanceof ElementSymbol)) {
						continue;
					}
					createdSymbols.add(outputSymbol);
					Expression ex = symbolMap.getMappedExpression((ElementSymbol) outputSymbol);
					if(ex instanceof AggregateSymbol) {
					    AggregateSymbol agg = (AggregateSymbol)ex;
	                    Expression aggExpr = agg.getExpression();
	                    if(aggExpr != null) {
	                    	ElementCollectorVisitor.getElements(aggExpr, requiredSymbols);
	                    }
	                    OrderBy orderBy = agg.getOrderBy();
	                    if(orderBy != null) {
	                    	ElementCollectorVisitor.getElements(orderBy, requiredSymbols);
	                    }
	                    Expression condition = agg.getCondition();
	                    if(condition != null) {
	                    	ElementCollectorVisitor.getElements(condition, requiredSymbols);
	                    }
	                    usedAggregates.add((ElementSymbol) outputSymbol);
					}
				}
				//update the aggs in the symbolmap
				for (Map.Entry<ElementSymbol, Expression> entry : new ArrayList<Map.Entry<ElementSymbol, Expression>>(symbolMap.asMap().entrySet())) {
					if (entry.getValue() instanceof AggregateSymbol && !usedAggregates.contains(entry.getKey())) {
						symbolMap.asUpdatableMap().remove(entry.getKey());
					}
				}
				if (requiredSymbols.isEmpty() && usedAggregates.isEmpty()) {
					node.setProperty(Info.IS_OPTIONAL, true);
				}
				break;
		}

        // Gather elements from correlated subquery references;
		for (SymbolMap refs : node.getAllReferences()) {
        	for (Expression expr : refs.asMap().values()) {
        		ElementCollectorVisitor.getElements(expr, requiredSymbols);
            }
        }
        
/*        Set<SingleElementSymbol> tempRequired = requiredSymbols;
        requiredSymbols = new LinkedHashSet<SingleElementSymbol>(outputCols);
        requiredSymbols.removeAll(createdSymbols);
        requiredSymbols.addAll(tempRequired);
*/        
        // Add any columns to required that are in this node's output but were not created here
        for (SingleElementSymbol currentOutputSymbol : outputCols) {
            if(!(createdSymbols.contains(currentOutputSymbol)) ) {
                requiredSymbols.add(currentOutputSymbol);
            }
        }
        
        //further minimize the required symbols based upon underlying expression (accounts for aliasing)
        //TODO: this should depend upon whether the expressions are deterministic
        if (node.getType() == NodeConstants.Types.PROJECT) {
            Set<Expression> expressions = new HashSet<Expression>();
            for (Iterator<SingleElementSymbol> iterator = requiredSymbols.iterator(); iterator.hasNext();) {
                SingleElementSymbol ses = iterator.next();
                if (!expressions.add(SymbolMap.getExpression(ses))) {
                    iterator.remove();
                }
            }
        }
        
        return new ArrayList<SingleElementSymbol>(requiredSymbols);
	}

    /**
     * Get name of the rule
     * @return Name of the rule
     */
	public String toString() {
		return "AssignOutputElements"; //$NON-NLS-1$
	}

}
