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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.teiid.client.plan.Annotation;
import org.teiid.client.plan.Annotation.Priority;
import org.teiid.dqp.internal.process.Request;

import com.metamatrix.api.exception.MetaMatrixComponentException;
import com.metamatrix.api.exception.MetaMatrixProcessingException;
import com.metamatrix.api.exception.query.QueryMetadataException;
import com.metamatrix.api.exception.query.QueryParserException;
import com.metamatrix.api.exception.query.QueryPlannerException;
import com.metamatrix.api.exception.query.QueryResolverException;
import com.metamatrix.common.log.LogConstants;
import com.metamatrix.common.log.LogManager;
import com.metamatrix.core.id.IDGenerator;
import com.metamatrix.query.QueryPlugin;
import com.metamatrix.query.analysis.AnalysisRecord;
import com.metamatrix.query.execution.QueryExecPlugin;
import com.metamatrix.query.mapping.relational.QueryNode;
import com.metamatrix.query.metadata.QueryMetadataInterface;
import com.metamatrix.query.metadata.TempMetadataAdapter;
import com.metamatrix.query.metadata.TempMetadataID;
import com.metamatrix.query.optimizer.CommandPlanner;
import com.metamatrix.query.optimizer.QueryOptimizer;
import com.metamatrix.query.optimizer.capabilities.CapabilitiesFinder;
import com.metamatrix.query.optimizer.relational.plantree.NodeConstants;
import com.metamatrix.query.optimizer.relational.plantree.NodeEditor;
import com.metamatrix.query.optimizer.relational.plantree.NodeFactory;
import com.metamatrix.query.optimizer.relational.plantree.PlanNode;
import com.metamatrix.query.optimizer.relational.plantree.NodeConstants.Info;
import com.metamatrix.query.optimizer.relational.rules.RuleConstants;
import com.metamatrix.query.parser.QueryParser;
import com.metamatrix.query.processor.ProcessorPlan;
import com.metamatrix.query.processor.relational.RelationalPlan;
import com.metamatrix.query.processor.relational.JoinNode.JoinStrategyType;
import com.metamatrix.query.resolver.QueryResolver;
import com.metamatrix.query.resolver.util.BindVariableVisitor;
import com.metamatrix.query.rewriter.QueryRewriter;
import com.metamatrix.query.sql.LanguageObject.Util;
import com.metamatrix.query.sql.lang.Command;
import com.metamatrix.query.sql.lang.Criteria;
import com.metamatrix.query.sql.lang.From;
import com.metamatrix.query.sql.lang.FromClause;
import com.metamatrix.query.sql.lang.GroupBy;
import com.metamatrix.query.sql.lang.Insert;
import com.metamatrix.query.sql.lang.JoinPredicate;
import com.metamatrix.query.sql.lang.JoinType;
import com.metamatrix.query.sql.lang.Limit;
import com.metamatrix.query.sql.lang.Option;
import com.metamatrix.query.sql.lang.OrderBy;
import com.metamatrix.query.sql.lang.ProcedureContainer;
import com.metamatrix.query.sql.lang.Query;
import com.metamatrix.query.sql.lang.QueryCommand;
import com.metamatrix.query.sql.lang.Select;
import com.metamatrix.query.sql.lang.SetQuery;
import com.metamatrix.query.sql.lang.StoredProcedure;
import com.metamatrix.query.sql.lang.SubqueryContainer;
import com.metamatrix.query.sql.lang.SubqueryFromClause;
import com.metamatrix.query.sql.lang.UnaryFromClause;
import com.metamatrix.query.sql.proc.CreateUpdateProcedureCommand;
import com.metamatrix.query.sql.symbol.GroupSymbol;
import com.metamatrix.query.sql.symbol.Reference;
import com.metamatrix.query.sql.symbol.SingleElementSymbol;
import com.metamatrix.query.sql.util.SymbolMap;
import com.metamatrix.query.sql.visitor.AggregateSymbolCollectorVisitor;
import com.metamatrix.query.sql.visitor.CorrelatedReferenceCollectorVisitor;
import com.metamatrix.query.sql.visitor.GroupCollectorVisitor;
import com.metamatrix.query.sql.visitor.GroupsUsedByElementsVisitor;
import com.metamatrix.query.sql.visitor.ValueIteratorProviderCollectorVisitor;
import com.metamatrix.query.util.CommandContext;
import com.metamatrix.query.util.ErrorMessageKeys;
import com.metamatrix.query.validator.ValidationVisitor;

/**
 * This class generates a relational plan for query execution.  The output of
 * this class is a {@link com.metamatrix.query.optimizer.relational.plantree.PlanNode PlanNode}
 * object - this object then becomes the input to
 * {@link PlanToProcessConverter PlanToProcessConverter}
 * to  produce a
 * {@link com.metamatrix.query.processor.relational.RelationalPlan RelationalPlan}.
 */
public class RelationalPlanner implements CommandPlanner {
	
	private AnalysisRecord analysisRecord;
	private Command parentCommand;
	private IDGenerator idGenerator;
	private CommandContext context;
	private CapabilitiesFinder capFinder;
	private QueryMetadataInterface metadata;
	private PlanHints hints = new PlanHints();
	private Option option;

    /**
     * @see com.metamatrix.query.optimizer.CommandPlanner#optimize(com.metamatrix.query.optimizer.CommandTreeNode, java.util.Map, com.metamatrix.query.metadata.QueryMetadataInterface, boolean)
     */
    public ProcessorPlan optimize(
        Command command,
        IDGenerator idGenerator,
        QueryMetadataInterface metadata,
        CapabilitiesFinder capFinder,
        AnalysisRecord analysisRecord, CommandContext context)
        throws
            QueryPlannerException,
            QueryMetadataException,
            MetaMatrixComponentException {
    	initialize(command, idGenerator, metadata, capFinder, analysisRecord, context);

        boolean debug = analysisRecord.recordDebug();
		if(debug) {
            analysisRecord.println("\n----------------------------------------------------------------------------"); //$NON-NLS-1$
            analysisRecord.println("GENERATE CANONICAL: \n" + command); //$NON-NLS-1$
		}   
        PlanNode plan;
		try {
			plan = generatePlan(command);
		} catch (MetaMatrixProcessingException e) {
			throw new QueryPlannerException(e, e.getMessage());
		}

		if(debug) {
            analysisRecord.println("\nCANONICAL PLAN: \n" + plan); //$NON-NLS-1$
		} 

        // Connect ProcessorPlan to SubqueryContainer (if any) of SELECT or PROJECT nodes
		connectSubqueryContainers(plan); //TODO: merge with node creation
        
        // Set top column information on top node
        List<SingleElementSymbol> topCols = Util.deepClone(command.getProjectedSymbols(), SingleElementSymbol.class);

        // Build rule set based on hints
        RuleStack rules = RelationalPlanner.buildRules(hints);

        // Run rule-based optimizer
        plan = executeRules(rules, plan);

        PlanToProcessConverter planToProcessConverter = null;
        if (context != null) {
        	planToProcessConverter = context.getPlanToProcessConverter();
        }
        if (planToProcessConverter == null) {
        	planToProcessConverter = new PlanToProcessConverter(metadata, idGenerator, analysisRecord, capFinder);
        }
        
        RelationalPlan result = planToProcessConverter.convert(plan);
        
        result.setOutputElements(topCols);
        
        return result;
    }

	public void initialize(Command command, IDGenerator idGenerator,
			QueryMetadataInterface metadata, CapabilitiesFinder capFinder,
			AnalysisRecord analysisRecord, CommandContext context) {
		this.parentCommand = command;
    	this.idGenerator = idGenerator;
    	this.metadata = metadata;
    	this.capFinder = capFinder;
    	this.analysisRecord = analysisRecord;
    	this.context = context;
	}

    private void connectSubqueryContainers(PlanNode plan) throws QueryPlannerException, QueryMetadataException, MetaMatrixComponentException {
        Set<GroupSymbol> groupSymbols = getGroupSymbols(plan);

        for (PlanNode node : NodeEditor.findAllNodes(plan, NodeConstants.Types.PROJECT | NodeConstants.Types.SELECT | NodeConstants.Types.JOIN)) {
            List<SubqueryContainer> subqueryContainers = node.getSubqueryContainers();
            if (subqueryContainers.isEmpty()){
            	continue;
            }
            Set<GroupSymbol> localGroupSymbols = groupSymbols;
            if (node.getType() == NodeConstants.Types.JOIN) {
            	localGroupSymbols = getGroupSymbols(node);
            }
            for (SubqueryContainer container : subqueryContainers) {
                ArrayList<Reference> correlatedReferences = new ArrayList<Reference>(); 
                Command subCommand = container.getCommand();
                ProcessorPlan procPlan = QueryOptimizer.optimizePlan(subCommand, metadata, idGenerator, capFinder, analysisRecord, context);
                subCommand.setProcessorPlan(procPlan);
                CorrelatedReferenceCollectorVisitor.collectReferences(subCommand, localGroupSymbols, correlatedReferences);
                if (!correlatedReferences.isEmpty()) {
	                SymbolMap map = new SymbolMap();
	                for (Reference reference : correlatedReferences) {
	    				map.addMapping(reference.getExpression(), reference.getExpression());
	    			}
	                subCommand.setCorrelatedReferences(map);
                }
            }
            node.addGroups(GroupsUsedByElementsVisitor.getGroups(node.getCorrelatedReferenceElements()));
        }
    }

	private static Set<GroupSymbol> getGroupSymbols(PlanNode plan) {
		Set<GroupSymbol> groupSymbols = new HashSet<GroupSymbol>();
        for (PlanNode source : NodeEditor.findAllNodes(plan, NodeConstants.Types.SOURCE)) {
            groupSymbols.addAll(source.getGroups());
        }
		return groupSymbols;
	}

    /**
     * Distribute and "make (not) dependent" hints specified in the query into the
     * fully resolved query plan.  This is done after virtual group resolution so
     * that all groups in the plan are known.  The hint is attached to all SOURCE
     * nodes for each group that should be made dependent/not dependent.
     * @param groups List of groups (Strings) to be made dependent
     * @param plan The canonical plan
     */
    private void distributeDependentHints(Collection<String> groups, PlanNode plan, QueryMetadataInterface metadata, NodeConstants.Info hintProperty)
        throws QueryMetadataException, MetaMatrixComponentException {
    
        if(groups == null || groups.isEmpty()) {
        	return;
        }
        // Get all source nodes
        List<PlanNode> nodes = NodeEditor.findAllNodes(plan, NodeConstants.Types.SOURCE);

        // Walk through each dependent group hint and
        // attach to the correct source node
        for (String groupName : groups) {
            // Walk through nodes and apply hint to all that match group name
            boolean appliedHint = applyHint(nodes, groupName, hintProperty);

            if(! appliedHint) {
                //check if it is partial group name
                Collection groupNames = metadata.getGroupsForPartialName(groupName);
                if(groupNames.size() == 1) {
                    groupName = (String)groupNames.iterator().next();
                    appliedHint = applyHint(nodes, groupName, hintProperty);
                }
                
                if(! appliedHint) {
                	String msg = QueryExecPlugin.Util.getString(ErrorMessageKeys.OPTIMIZER_0010, groupName);
                	if (this.analysisRecord.recordAnnotations()) {
                		this.analysisRecord.addAnnotation(new Annotation(Annotation.HINTS, msg, "ignoring hint", Priority.MEDIUM)); //$NON-NLS-1$
                	}
                }
            }
        }
    }
    
    private static boolean applyHint(List<PlanNode> nodes, String groupName, NodeConstants.Info hintProperty) {
        boolean appliedHint = false;
        for (PlanNode node : nodes) {
            GroupSymbol nodeGroup = node.getGroups().iterator().next();
            
            String sDefinition = nodeGroup.getDefinition();
            
            if (nodeGroup.getName().equalsIgnoreCase(groupName) 
             || (sDefinition != null && sDefinition.equalsIgnoreCase(groupName)) ) {
                node.setProperty(hintProperty, Boolean.TRUE);
                appliedHint = true;
            }
        }
        return appliedHint;
    }

    public static RuleStack buildRules(PlanHints hints) {
        RuleStack rules = new RuleStack();

        rules.push(RuleConstants.COLLAPSE_SOURCE);
        
        rules.push(RuleConstants.PLAN_SORTS);

        if(hints.hasJoin) {
            rules.push(RuleConstants.IMPLEMENT_JOIN_STRATEGY);
        }
        
        rules.push(RuleConstants.ASSIGN_OUTPUT_ELEMENTS);
        
        rules.push(RuleConstants.CALCULATE_COST);
        
        if (hints.hasLimit) {
            rules.push(RuleConstants.PUSH_LIMIT);
        }
        if (hints.hasJoin || hints.hasCriteria) {
            rules.push(RuleConstants.MERGE_CRITERIA);
        }
        if (hints.hasRelationalProc) {
            rules.push(RuleConstants.PLAN_PROCEDURES);
        }
        if(hints.hasJoin) {
            rules.push(RuleConstants.CHOOSE_DEPENDENT);
        }
        if(hints.hasAggregates) {
            rules.push(RuleConstants.PUSH_AGGREGATES);
        }
        if(hints.hasJoin) {
            rules.push(RuleConstants.CHOOSE_JOIN_STRATEGY);
            rules.push(RuleConstants.RAISE_ACCESS);
            //after planning the joins, let the criteria be pushed back into place
            rules.push(RuleConstants.PUSH_SELECT_CRITERIA);
            rules.push(RuleConstants.PLAN_JOINS);
        }
        rules.push(RuleConstants.RAISE_ACCESS);
        if (hints.hasSetQuery) {
            rules.push(RuleConstants.PLAN_UNIONS);
        } 
        if(hints.hasCriteria || hints.hasJoin) {
            //after copy criteria, it is no longer necessary to have phantom criteria nodes, so do some cleaning
            rules.push(RuleConstants.CLEAN_CRITERIA);
        }
        if(hints.hasJoin) {
            rules.push(RuleConstants.COPY_CRITERIA);
            rules.push(RuleConstants.PUSH_NON_JOIN_CRITERIA);
        }
        if(hints.hasVirtualGroups) {
            rules.push(RuleConstants.MERGE_VIRTUAL);
        }
        if(hints.hasCriteria) {
            rules.push(RuleConstants.PUSH_SELECT_CRITERIA);
        }
        if (hints.hasJoin && hints.hasOptionalJoin) {
            rules.push(RuleConstants.REMOVE_OPTIONAL_JOINS);
        }
        rules.push(RuleConstants.PLACE_ACCESS);
        return rules;
    }

    private PlanNode executeRules(RuleStack rules, PlanNode plan)
        throws QueryPlannerException, QueryMetadataException, MetaMatrixComponentException {

        boolean debug = analysisRecord.recordDebug();
        while(! rules.isEmpty()) {
            if(debug) {
                analysisRecord.println("\n============================================================================"); //$NON-NLS-1$
            }

            OptimizerRule rule = rules.pop();
            if(debug) {
                analysisRecord.println("EXECUTING " + rule); //$NON-NLS-1$
            }

            plan = rule.execute(plan, metadata, capFinder, rules, analysisRecord, context);
            if(debug) {
                analysisRecord.println("\nAFTER: \n" + plan); //$NON-NLS-1$
            }
        }
        return plan;
    }
	
	public PlanNode generatePlan(Command cmd) throws MetaMatrixComponentException, MetaMatrixProcessingException {
		//cascade the option clause nocache
		Option savedOption = option;
		option = cmd.getOption();
        if (option == null) {
        	if (savedOption != null) {
        		option = savedOption;
        	} 
        } else if (savedOption != null && savedOption.isNoCache()) { //merge no cache settings
    		if (savedOption.getNoCacheGroups() == null || savedOption.getNoCacheGroups().isEmpty()) {
    			if (option.getNoCacheGroups() != null) {
    				option.getNoCacheGroups().clear(); // full no cache
    			}
    		} else if (option.getNoCacheGroups() != null && !option.getNoCacheGroups().isEmpty()) {
    			for (String noCache : savedOption.getNoCacheGroups()) {
					option.addNoCacheGroup(noCache); // only groups
				}
    		}
    		option.setNoCache(true);
        }
		
		PlanNode result = null;
		switch (cmd.getType()) {
		case Command.TYPE_QUERY:
			result = createQueryPlan((QueryCommand)cmd);
			break;
		case Command.TYPE_INSERT:
		case Command.TYPE_UPDATE:
		case Command.TYPE_DELETE:
		case Command.TYPE_CREATE:
		case Command.TYPE_DROP:
			result = createUpdatePlan(cmd);
			break;
		case Command.TYPE_STORED_PROCEDURE:
			result = createStoredProcedurePlan((StoredProcedure)cmd);
			break;
		default:
			throw new AssertionError("Invalid command type"); //$NON-NLS-1$
		}
        // Distribute make dependent hints as necessary
        if (cmd.getOption() != null) {
	        if(cmd.getOption().getDependentGroups() != null) {
	            distributeDependentHints(cmd.getOption().getDependentGroups(), result, metadata, NodeConstants.Info.MAKE_DEP);
	        }
	        if (cmd.getOption().getNotDependentGroups() != null) {
	            distributeDependentHints(cmd.getOption().getNotDependentGroups(), result, metadata, NodeConstants.Info.MAKE_NOT_DEP);
	        }
        }
        this.option = savedOption;
        return result;
	}

	PlanNode createUpdatePlan(Command command) throws MetaMatrixComponentException, MetaMatrixProcessingException {
        // Create top project node - define output columns for stored query / procedure
        PlanNode projectNode = NodeFactory.getNewNode(NodeConstants.Types.PROJECT);

        Collection<GroupSymbol> groups = GroupCollectorVisitor.getGroups(command, false);
        projectNode.addGroups(groups);

        // Set output columns
        List<SingleElementSymbol> cols = command.getProjectedSymbols();
        projectNode.setProperty(NodeConstants.Info.PROJECT_COLS, cols);

        // Define source of data for stored query / procedure
        PlanNode sourceNode = NodeFactory.getNewNode(NodeConstants.Types.SOURCE);
        sourceNode.setProperty(NodeConstants.Info.ATOMIC_REQUEST, command);
        sourceNode.setProperty(NodeConstants.Info.VIRTUAL_COMMAND, command);
        if (command instanceof ProcedureContainer) {
        	ProcedureContainer container = (ProcedureContainer)command;
        	addNestedProcedure(sourceNode, container);
        }
        sourceNode.addGroups(groups);

        attachLast(projectNode, sourceNode);

        //for INTO query, attach source and project nodes
        if(command instanceof Insert){
        	Insert insert = (Insert)command;
        	if (insert.getQueryExpression() != null) {
	            PlanNode plan = generatePlan(insert.getQueryExpression());
	            attachLast(sourceNode, plan);
	            mergeTempMetadata(insert.getQueryExpression(), insert);
	            projectNode.setProperty(NodeConstants.Info.INTO_GROUP, insert.getGroup());
        	}
        }
        
        return projectNode;
	}

	private void addNestedProcedure(PlanNode sourceNode,
			ProcedureContainer container) throws MetaMatrixComponentException,
			QueryMetadataException, MetaMatrixProcessingException {
		//plan any subqueries in criteria/parameters/values
		for (SubqueryContainer subqueryContainer : ValueIteratorProviderCollectorVisitor.getValueIteratorProviders(container)) {
    		ProcessorPlan plan = QueryOptimizer.optimizePlan(subqueryContainer.getCommand(), metadata, null, capFinder, analysisRecord, context);
    		subqueryContainer.getCommand().setProcessorPlan(plan);
		}
		
		String cacheString = "transformation/" + container.getClass().getSimpleName(); //$NON-NLS-1$
		Command c = (Command)metadata.getFromMetadataCache(container.getGroup().getMetadataID(), cacheString);
		if (c == null) {
			c = QueryResolver.expandCommand(container, metadata, analysisRecord);
			if (c != null) {
		        Request.validateWithVisitor(new ValidationVisitor(), metadata, c);
		        metadata.addToMetadataCache(container.getGroup().getMetadataID(), cacheString, c.clone());
			}
		} else {
			c = (Command)c.clone();
			if (c instanceof CreateUpdateProcedureCommand) {
				((CreateUpdateProcedureCommand)c).setUserCommand(container);
			}
		}
		if (c != null) {
		    c = QueryRewriter.rewrite(c, metadata, context);
		    addNestedCommand(sourceNode, container.getGroup(), container, c, false);
		}
	}

    PlanNode createStoredProcedurePlan(StoredProcedure storedProc) throws QueryMetadataException, MetaMatrixComponentException, MetaMatrixProcessingException {
        // Create top project node - define output columns for stored query / procedure
        PlanNode projectNode = NodeFactory.getNewNode(NodeConstants.Types.PROJECT);

        // Set output columns
        List cols = storedProc.getProjectedSymbols();
        projectNode.setProperty(NodeConstants.Info.PROJECT_COLS, cols);

        // Define source of data for stored query / procedure
        PlanNode sourceNode = NodeFactory.getNewNode(NodeConstants.Types.SOURCE);
        sourceNode.setProperty(NodeConstants.Info.VIRTUAL_COMMAND, storedProc);
    	addNestedProcedure(sourceNode, storedProc);
        
        hints.hasRelationalProc |= storedProc.isProcedureRelational();

        // Set group on source node
        sourceNode.addGroup(storedProc.getGroup());

        attachLast(projectNode, sourceNode);

        return projectNode;
    }

	PlanNode createQueryPlan(QueryCommand command)
		throws MetaMatrixComponentException, MetaMatrixProcessingException {
        // Build canonical plan
    	PlanNode node = null;
        if(command instanceof Query) {
            node = createQueryPlan((Query) command);
        } else {
            hints.hasSetQuery = true;
            SetQuery query = (SetQuery)command;
            PlanNode leftPlan = createQueryPlan( query.getLeftQuery());
            PlanNode rightPlan = createQueryPlan( query.getRightQuery());

            node = NodeFactory.getNewNode(NodeConstants.Types.SET_OP);
            node.setProperty(NodeConstants.Info.SET_OPERATION, query.getOperation());
            node.setProperty(NodeConstants.Info.USE_ALL, query.isAll());
            
            attachLast(node, leftPlan);
            attachLast(node, rightPlan);
        }
        
		if(command.getOrderBy() != null) {
			node = attachSorting(node, command.getOrderBy());
		}

        if (command.getLimit() != null) {
            node = attachTupleLimit(node, command.getLimit(), hints);
        }
        
        return node;
    }

    private PlanNode createQueryPlan(Query query)
		throws QueryMetadataException, MetaMatrixComponentException, MetaMatrixProcessingException {

        PlanNode plan = null;

        if(query.getFrom() != null){
            FromClause fromClause = mergeClauseTrees(query.getFrom());
            
            PlanNode dummyRoot = new PlanNode();
            
    		buildTree(fromClause, dummyRoot);
            
            plan = dummyRoot.getFirstChild();
            
            hints.hasJoin |= plan.getType() == NodeConstants.Types.JOIN;

    		// Attach criteria on top
    		if(query.getCriteria() != null) {
    			plan = attachCriteria(plan, query.getCriteria(), false);
                hints.hasCriteria = true;
    		}

    		// Attach grouping node on top
    		if(query.getGroupBy() != null || query.getHaving() != null || !AggregateSymbolCollectorVisitor.getAggregates(query.getSelect(), false).isEmpty()) {
    			plan = attachGrouping(plan, query, hints);
    		}

    		// Attach having criteria node on top
    		if(query.getHaving() != null) {
    			plan = attachCriteria(plan, query.getHaving(), true);
                hints.hasCriteria = true;
    		}
            
        }

		// Attach project on top
		plan = attachProject(plan, query.getSelect());

		// Attach dup removal on top
		if(query.getSelect().isDistinct()) {
			plan = attachDupRemoval(plan);
		}

		return plan;
	}

    /**
     * Merges the from clause into a single join predicate if there are more than 1 from clauses
     */
    private static FromClause mergeClauseTrees(From from) {
        List clauses = from.getClauses();
        
        while (clauses.size() > 1) {
            FromClause first = (FromClause)from.getClauses().remove(0);
            FromClause second = (FromClause)from.getClauses().remove(0);
            JoinPredicate jp = new JoinPredicate(first, second, JoinType.JOIN_CROSS);
            clauses.add(0, jp);
        }
        
        return (FromClause)clauses.get(0);
    }
    
    /**
     * Build a join plan based on the structure in a clause.  These structures should be
     * essentially the same tree, but with different objects and details.
     * @param clause Clause to build tree from
     * @param parent Parent node to attach join node structure to
     * @param sourceMap Map of group to source node, used for connecting children to join plan
     * @param markJoinsInternal Flag saying whether joins built in this method should be marked
     * as internal
     * @throws MetaMatrixComponentException 
     * @throws QueryMetadataException 
     * @throws MetaMatrixProcessingException 
     */
    void buildTree(FromClause clause, PlanNode parent)
        throws QueryMetadataException, MetaMatrixComponentException, MetaMatrixProcessingException {
        
        PlanNode node = null;
        
        if(clause instanceof UnaryFromClause) {
            // No join required
            UnaryFromClause ufc = (UnaryFromClause)clause;
            GroupSymbol group = ufc.getGroup();
            if (metadata.isVirtualGroup(group.getMetadataID())) {
            	hints.hasVirtualGroups = true;
            }
            Command nestedCommand = ufc.getExpandedCommand();
            if (nestedCommand == null && !group.isTempGroupSymbol() && !group.isProcedure() 
            		&& (!(group.getMetadataID() instanceof TempMetadataID) || metadata.getVirtualPlan(group.getMetadataID()) != null)
        	        && (metadata.isVirtualGroup(group.getMetadataID()))) { 
            	//must be a view layer
            	nestedCommand = resolveVirtualGroup(group);
            }
            node = NodeFactory.getNewNode(NodeConstants.Types.SOURCE);
            node.addGroup(group);
            if (nestedCommand != null) {
            	addNestedCommand(node, group, nestedCommand, nestedCommand, true);
            }
            parent.addLastChild(node);
        } else if(clause instanceof JoinPredicate) {
            JoinPredicate jp = (JoinPredicate) clause;

            // Set up new join node corresponding to this join predicate
            node = NodeFactory.getNewNode(NodeConstants.Types.JOIN);
            node.setProperty(NodeConstants.Info.JOIN_TYPE, jp.getJoinType());
            node.setProperty(NodeConstants.Info.JOIN_STRATEGY, JoinStrategyType.NESTED_LOOP);
            node.setProperty(NodeConstants.Info.JOIN_CRITERIA, jp.getJoinCriteria());
            
            if (jp.getJoinType() == JoinType.JOIN_LEFT_OUTER) {
            	hints.hasOptionalJoin = true;
            }
         
            // Attach join node to parent
            parent.addLastChild(node);

            // Handle each child
            FromClause[] clauses = new FromClause[] {jp.getLeftClause(), jp.getRightClause()};
            for(int i=0; i<2; i++) {
                buildTree(clauses[i], node);

                // Add groups to joinNode
                for (PlanNode child : node.getChildren()) {
                    node.addGroups(child.getGroups());
                }
            }
        } else if (clause instanceof SubqueryFromClause) {
            SubqueryFromClause sfc = (SubqueryFromClause)clause;
            GroupSymbol group = sfc.getGroupSymbol();
            Command nestedCommand = sfc.getCommand();
            node = NodeFactory.getNewNode(NodeConstants.Types.SOURCE);
            node.addGroup(group);
            addNestedCommand(node, group, nestedCommand, nestedCommand, true);
            hints.hasVirtualGroups = true;
            parent.addLastChild(node);
        }
        
        if (clause.isOptional()) {
            node.setProperty(NodeConstants.Info.IS_OPTIONAL, Boolean.TRUE);
            hints.hasOptionalJoin = true;
        }
        
        if (clause.isMakeDep()) {
            node.setProperty(NodeConstants.Info.MAKE_DEP, Boolean.TRUE);
        } else if (clause.isMakeNotDep()) {
            node.setProperty(NodeConstants.Info.MAKE_NOT_DEP, Boolean.TRUE);
        }
    }

	private void addNestedCommand(PlanNode node,
			GroupSymbol group, Command nestedCommand, Command toPlan, boolean merge) throws MetaMatrixComponentException, QueryMetadataException, MetaMatrixProcessingException {
		if (nestedCommand instanceof QueryCommand) {
			//remove unnecessary order by
        	QueryCommand queryCommand = (QueryCommand)nestedCommand;
        	if (queryCommand.getLimit() == null) {
        		queryCommand.setOrderBy(null);
        	}
        }
		node.setProperty(NodeConstants.Info.NESTED_COMMAND, nestedCommand);

		if (merge && nestedCommand instanceof Query && QueryResolver.isXMLQuery((Query)nestedCommand, metadata)) {
			merge = false;
		}

		if (merge) {
			mergeTempMetadata(nestedCommand, parentCommand);
		    PlanNode childRoot = generatePlan(nestedCommand);
		    node.addFirstChild(childRoot);
			List<SingleElementSymbol> projectCols = nestedCommand.getProjectedSymbols();
			node.setProperty(NodeConstants.Info.SYMBOL_MAP, SymbolMap.createSymbolMap(group, projectCols, metadata));
		} else {
			QueryMetadataInterface actualMetadata = metadata;
			if (actualMetadata instanceof TempMetadataAdapter) {
				actualMetadata = ((TempMetadataAdapter)metadata).getMetadata();
			}
			ProcessorPlan plan = QueryOptimizer.optimizePlan(toPlan, actualMetadata, null, capFinder, analysisRecord, context);
		    node.setProperty(NodeConstants.Info.PROCESSOR_PLAN, plan);
		}
	}

	/**
	 * Attach all criteria above the join nodes.  The optimizer will push these
	 * criteria down to the appropriate source.
	 * @param plan Existing plan, which joins all source groups
	 * @param criteria Criteria from query
	 * @return Updated tree
	 */
	private static PlanNode attachCriteria(PlanNode plan, Criteria criteria, boolean isHaving) {
	    List<Criteria> crits = Criteria.separateCriteriaByAnd(criteria);
	    
	    for (Criteria crit : crits) {
            PlanNode critNode = createSelectNode(crit, isHaving);
            attachLast(critNode, plan);
            plan = critNode;
        } 
	    
		return plan;
	}

    public static PlanNode createSelectNode(final Criteria crit, boolean isHaving) {
        PlanNode critNode = NodeFactory.getNewNode(NodeConstants.Types.SELECT);
        critNode.setProperty(NodeConstants.Info.SELECT_CRITERIA, crit);
        if (isHaving && !AggregateSymbolCollectorVisitor.getAggregates(crit, false).isEmpty()) {
            critNode.setProperty(NodeConstants.Info.IS_HAVING, Boolean.TRUE);
        }
        // Add groups to crit node
        critNode.addGroups(GroupsUsedByElementsVisitor.getGroups(crit));
        critNode.addGroups(GroupsUsedByElementsVisitor.getGroups(critNode.getCorrelatedReferenceElements()));
        return critNode;
    }

	/**
	 * Attach a grouping node at top of tree.
	 * @param plan Existing plan
	 * @param groupBy Group by clause, which may be null
	 * @return Updated plan
	 */
	private static PlanNode attachGrouping(PlanNode plan, Query query, PlanHints hints) {
		PlanNode groupNode = NodeFactory.getNewNode(NodeConstants.Types.GROUP);

		GroupBy groupBy = query.getGroupBy();
		if(groupBy != null) {
			groupNode.setProperty(NodeConstants.Info.GROUP_COLS, groupBy.getSymbols());
            groupNode.addGroups(GroupsUsedByElementsVisitor.getGroups(groupBy));
		}

		attachLast(groupNode, plan);
        
        // Mark in hints
        hints.hasAggregates = true;
        
		return groupNode;
	}

    /**
	 * Attach SORT node at top of tree.  The SORT may be pushed down to a source (or sources)
	 * if possible by the optimizer.
	 * @param plan Existing plan
	 * @param orderBy Sort description from the query
	 * @return Updated plan
	 */
	private static PlanNode attachSorting(PlanNode plan, OrderBy orderBy) {
		PlanNode sortNode = NodeFactory.getNewNode(NodeConstants.Types.SORT);
		
		sortNode.setProperty(NodeConstants.Info.SORT_ORDER, orderBy);
		if (orderBy.hasUnrelated()) {
			sortNode.setProperty(Info.UNRELATED_SORT, true);
		}
		sortNode.addGroups(GroupsUsedByElementsVisitor.getGroups(orderBy));

		attachLast(sortNode, plan);
		return sortNode;
	}
    
    private static PlanNode attachTupleLimit(PlanNode plan, Limit limit, PlanHints hints) {
        hints.hasLimit = true;
        PlanNode limitNode = NodeFactory.getNewNode(NodeConstants.Types.TUPLE_LIMIT);
        
        boolean attach = false;
        if (limit.getOffset() != null) {
            limitNode.setProperty(NodeConstants.Info.OFFSET_TUPLE_COUNT, limit.getOffset());
            attach = true;
        }
        if (limit.getRowLimit() != null) {
            limitNode.setProperty(NodeConstants.Info.MAX_TUPLE_LIMIT, limit.getRowLimit());
            attach = true;
        }
        if (attach) {
            attachLast(limitNode, plan);
            plan = limitNode;
        }
        return plan;
    }

	/**
	 * Attach DUP_REMOVE node at top of tree.  The DUP_REMOVE may be pushed down
	 * to a source (or sources) if possible by the optimizer.
	 * @param plan Existing plan
	 * @return Updated plan
	 */
	private static PlanNode attachDupRemoval(PlanNode plan) {
		PlanNode dupNode = NodeFactory.getNewNode(NodeConstants.Types.DUP_REMOVE);
		attachLast(dupNode, plan);
		return dupNode;
	}

	private static PlanNode attachProject(PlanNode plan, Select select) {
		PlanNode projectNode = NodeFactory.getNewNode(NodeConstants.Types.PROJECT);
		projectNode.setProperty(NodeConstants.Info.PROJECT_COLS, select.getProjectedSymbols());

		// Set groups
		projectNode.addGroups(GroupsUsedByElementsVisitor.getGroups(select));

		attachLast(projectNode, plan);
		return projectNode;
	}

	static final void attachLast(PlanNode parent, PlanNode child) {
		if(child != null) {
			parent.addLastChild(child);
		}
	}

    /**
     * Adds temp metadata (if any) of child command to temp metadata
     * (if any) of parent command.
     * @param childCommand 
     * @param parentCommand
     */
    static void mergeTempMetadata(
        Command childCommand,
        Command parentCommand) {
        Map childTempMetadata = childCommand.getTemporaryMetadata();
        if (childTempMetadata != null && !childTempMetadata.isEmpty()){
            // Add to parent temp metadata
            Map parentTempMetadata = parentCommand.getTemporaryMetadata();
            if (parentTempMetadata == null){
                parentCommand.setTemporaryMetadata(new HashMap(childTempMetadata));
            } else {
                parentTempMetadata.putAll(childTempMetadata);
            }
        }
    }
	
    private Command resolveVirtualGroup(GroupSymbol virtualGroup)
    throws QueryMetadataException, MetaMatrixComponentException, MetaMatrixProcessingException {
    	
        QueryNode qnode = null;
        
        Object metadataID = virtualGroup.getMetadataID();
        boolean noCache = false;
        boolean isMaterializedGroup = metadata.hasMaterialization(metadataID);
        String cacheString = "select"; //$NON-NLS-1$
        if( isMaterializedGroup) {
            noCache = isNoCacheGroup(metadata, metadataID, option);
        	if(noCache){
        		//not use cache
        		qnode = metadata.getVirtualPlan(metadataID);
        		String matTableName = metadata.getFullName(metadata.getMaterialization(metadataID));
        		recordMaterializationTableAnnotation(virtualGroup, analysisRecord, matTableName, "SimpleQueryResolver.materialized_table_not_used"); //$NON-NLS-1$
        	}else{
                // Default query for a materialized group - go to cached table
                String groupName = metadata.getFullName(metadataID);
                String matTableName = metadata.getFullName(metadata.getMaterialization(metadataID));
                qnode = new QueryNode(groupName, "SELECT * FROM " + matTableName); //$NON-NLS-1$
                cacheString = "matview"; //$NON-NLS-1$
                recordMaterializationTableAnnotation(virtualGroup, analysisRecord, matTableName, "SimpleQueryResolver.Query_was_redirected_to_Mat_table"); //$NON-NLS-1$                
        	}
        } else {
            // Not a materialized view - query the primary transformation
            qnode = metadata.getVirtualPlan(metadataID);            
        }

        Command result = (Command)metadata.getFromMetadataCache(virtualGroup.getMetadataID(), "transformation/" + cacheString); //$NON-NLS-1$
        if (result != null) {
        	result = (Command)result.clone();
        } else {
        	//parse, resolve, validate
        	result = convertToSubquery(qnode, metadata);
	        QueryResolver.resolveCommand(result, Collections.EMPTY_MAP, metadata, analysisRecord);
	        Request.validateWithVisitor(new ValidationVisitor(), metadata, result);
	        metadata.addToMetadataCache(virtualGroup.getMetadataID(), "transformation/" + cacheString, result.clone()); //$NON-NLS-1$
        }        
        return QueryRewriter.rewrite(result, metadata, context);
    }

    public static boolean isNoCacheGroup(QueryMetadataInterface metadata,
                                          Object metadataID,
                                          Option option) throws QueryMetadataException,
                                                        MetaMatrixComponentException {
        if(option == null || !option.isNoCache()){
            return false;
        }
    	if(option.getNoCacheGroups() == null || option.getNoCacheGroups().isEmpty()){
    		//only OPTION NOCACHE, no group specified
    		return true;
    	}       
    	for (String groupName : option.getNoCacheGroups()) {
            try {
                Object noCacheGroupID = metadata.getGroupID(groupName);
                if(metadataID.equals(noCacheGroupID)){
                    return true;
                }
            } catch (QueryMetadataException e) {
                //log that an unknown groups was used in the no cache
                LogManager.logWarning(LogConstants.CTX_QUERY_RESOLVER, e, QueryPlugin.Util.getString("SimpleQueryResolver.unknown_group_in_nocache", groupName)); //$NON-NLS-1$
            }
        }
        return false;
    }
    
    private static void recordMaterializationTableAnnotation(GroupSymbol virtualGroup,
                                                      AnalysisRecord analysis,
                                                      String matTableName, String msg) {
        if ( analysis.recordAnnotations() ) {
            Object[] params = new Object[] {virtualGroup, matTableName};
            Annotation annotation = new Annotation(Annotation.MATERIALIZED_VIEW, 
                                                         QueryPlugin.Util.getString(msg, params), 
                                                         null, 
                                                         Priority.LOW);
            analysis.addAnnotation(annotation);
        }
    }

    private static Command convertToSubquery(QueryNode qnode, QueryMetadataInterface metadata)
    throws QueryResolverException, MetaMatrixComponentException {

        // Parse this node's command
        Command command = qnode.getCommand();
        
        if (command == null) {
            try {
                command = QueryParser.getQueryParser().parseCommand(qnode.getQuery());
            } catch(QueryParserException e) {
                throw new QueryResolverException(e, ErrorMessageKeys.RESOLVER_0011, QueryPlugin.Util.getString(ErrorMessageKeys.RESOLVER_0011, qnode.getGroupName()));
            }
            
            //Handle bindings and references
            List bindings = qnode.getBindings();
            if (bindings != null){
                BindVariableVisitor.bindReferences(command, bindings, metadata);
            }
        }

        return command;
    }

}