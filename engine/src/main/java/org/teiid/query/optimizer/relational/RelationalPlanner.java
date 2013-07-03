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

import java.util.*;

import org.teiid.api.exception.query.QueryMetadataException;
import org.teiid.api.exception.query.QueryPlannerException;
import org.teiid.client.plan.Annotation;
import org.teiid.client.plan.Annotation.Priority;
import org.teiid.common.buffer.LobManager;
import org.teiid.core.TeiidComponentException;
import org.teiid.core.TeiidProcessingException;
import org.teiid.core.id.IDGenerator;
import org.teiid.core.types.DataTypeManager;
import org.teiid.core.util.HashCodeUtil;
import org.teiid.dqp.internal.process.Request;
import org.teiid.language.SQLConstants;
import org.teiid.metadata.Procedure;
import org.teiid.query.QueryPlugin;
import org.teiid.query.analysis.AnalysisRecord;
import org.teiid.query.function.FunctionDescriptor;
import org.teiid.query.function.FunctionLibrary;
import org.teiid.query.mapping.relational.QueryNode;
import org.teiid.query.metadata.BasicQueryMetadata;
import org.teiid.query.metadata.QueryMetadataInterface;
import org.teiid.query.metadata.TempMetadataAdapter;
import org.teiid.query.metadata.TempMetadataID;
import org.teiid.query.metadata.TempMetadataStore;
import org.teiid.query.optimizer.QueryOptimizer;
import org.teiid.query.optimizer.TriggerActionPlanner;
import org.teiid.query.optimizer.capabilities.CapabilitiesFinder;
import org.teiid.query.optimizer.capabilities.SourceCapabilities.Capability;
import org.teiid.query.optimizer.relational.plantree.NodeConstants;
import org.teiid.query.optimizer.relational.plantree.NodeConstants.Info;
import org.teiid.query.optimizer.relational.plantree.NodeEditor;
import org.teiid.query.optimizer.relational.plantree.NodeFactory;
import org.teiid.query.optimizer.relational.plantree.PlanNode;
import org.teiid.query.optimizer.relational.rules.CapabilitiesUtil;
import org.teiid.query.optimizer.relational.rules.CriteriaCapabilityValidatorVisitor;
import org.teiid.query.optimizer.relational.rules.RuleApplySecurity;
import org.teiid.query.optimizer.relational.rules.RuleAssignOutputElements;
import org.teiid.query.optimizer.relational.rules.RuleCollapseSource;
import org.teiid.query.optimizer.relational.rules.RuleConstants;
import org.teiid.query.optimizer.relational.rules.RuleMergeCriteria;
import org.teiid.query.optimizer.relational.rules.RulePlaceAccess;
import org.teiid.query.optimizer.relational.rules.RulePushAggregates;
import org.teiid.query.processor.ProcessorPlan;
import org.teiid.query.processor.proc.ProcedurePlan;
import org.teiid.query.processor.relational.AccessNode;
import org.teiid.query.processor.relational.JoinNode.JoinStrategyType;
import org.teiid.query.processor.relational.RelationalNode;
import org.teiid.query.processor.relational.RelationalPlan;
import org.teiid.query.resolver.ProcedureContainerResolver;
import org.teiid.query.resolver.QueryResolver;
import org.teiid.query.resolver.util.ResolverUtil;
import org.teiid.query.rewriter.QueryRewriter;
import org.teiid.query.sql.LanguageObject;
import org.teiid.query.sql.LanguageObject.Util;
import org.teiid.query.sql.LanguageVisitor;
import org.teiid.query.sql.lang.*;
import org.teiid.query.sql.navigator.PreOrPostOrderNavigator;
import org.teiid.query.sql.proc.CreateProcedureCommand;
import org.teiid.query.sql.proc.TriggerAction;
import org.teiid.query.sql.symbol.*;
import org.teiid.query.sql.util.SymbolMap;
import org.teiid.query.sql.visitor.AggregateSymbolCollectorVisitor;
import org.teiid.query.sql.visitor.CorrelatedReferenceCollectorVisitor;
import org.teiid.query.sql.visitor.ElementCollectorVisitor;
import org.teiid.query.sql.visitor.ExpressionMappingVisitor;
import org.teiid.query.sql.visitor.FunctionCollectorVisitor;
import org.teiid.query.sql.visitor.GroupCollectorVisitor;
import org.teiid.query.sql.visitor.GroupsUsedByElementsVisitor;
import org.teiid.query.sql.visitor.ValueIteratorProviderCollectorVisitor;
import org.teiid.query.util.CommandContext;
import org.teiid.query.validator.UpdateValidator.UpdateInfo;
import org.teiid.query.validator.ValidationVisitor;


/**
 * This class generates a relational plan for query execution.  The output of
 * this class is a {@link org.teiid.query.optimizer.relational.plantree.PlanNode PlanNode}
 * object - this object then becomes the input to
 * {@link PlanToProcessConverter PlanToProcessConverter}
 * to  produce a
 * {@link org.teiid.query.processor.relational.RelationalPlan RelationalPlan}.
 */
public class RelationalPlanner {
	
	public static final String MAT_PREFIX = "#MAT_"; //$NON-NLS-1$
	
	private AnalysisRecord analysisRecord;
	private Command parentCommand;
	private IDGenerator idGenerator;
	CommandContext context;
	CapabilitiesFinder capFinder;
	QueryMetadataInterface metadata;
	private PlanHints hints = new PlanHints();
	private Option option;
	private SourceHint sourceHint;
	
	private static final Comparator<GroupSymbol> nonCorrelatedComparator = new Comparator<GroupSymbol>() {
		@Override
		public int compare(GroupSymbol arg0, GroupSymbol arg1) {
			return arg0.getNonCorrelationName().compareTo(arg1.getNonCorrelationName());
		}
	};
	
	private static class PlanningStackEntry {
		Command command;
		GroupSymbol group;

		public PlanningStackEntry(Command command, GroupSymbol group) {
			this.command = command;
			this.group = group;
		}

		@Override
		public int hashCode() {
			return HashCodeUtil.hashCode(group.getMetadataID().hashCode(), command.getType());
		}
		
		public boolean equals(Object obj) {
			if (obj == this) {
				return true;
			}
			if (!(obj instanceof PlanningStackEntry)) {
				return false;
			}
			PlanningStackEntry other = (PlanningStackEntry)obj;
			return group.getMetadataID().equals(other.group.getMetadataID())
					&& command.getType() == other.command.getType();
		}
		
		@Override
		public String toString() {
			return command.getClass().getSimpleName() + " " + group.getNonCorrelationName().toString(); //$NON-NLS-1$
		}
	}
	
	private static ThreadLocal<HashSet<PlanningStackEntry>> planningStack = new ThreadLocal<HashSet<PlanningStackEntry>>() {
		protected HashSet<PlanningStackEntry> initialValue() {
			return new LinkedHashSet<PlanningStackEntry>();
		}
	};
	
    public RelationalPlan optimize(
        Command command)
        throws
            QueryPlannerException,
            QueryMetadataException,
            TeiidComponentException {

        boolean debug = analysisRecord.recordDebug();
		if(debug) {
            analysisRecord.println("\n----------------------------------------------------------------------------"); //$NON-NLS-1$
            analysisRecord.println("GENERATE CANONICAL: \n" + command); //$NON-NLS-1$
		}   
		
		SourceHint previous = this.sourceHint;
		this.sourceHint = SourceHint.combine(previous, command.getSourceHint());
		
		PlanToProcessConverter planToProcessConverter = new PlanToProcessConverter(metadata, idGenerator, analysisRecord, capFinder, context);
		
		//plan with
		List<WithQueryCommand> withList = null;
        Map<String, WithQueryCommand> pushdownWith = null;
        Set<GroupSymbol> withGroups = null;
		if (command instanceof QueryCommand) {
			QueryCommand queryCommand = (QueryCommand)command;
			withList = queryCommand.getWith();
			if (withList != null) {
	        	for (WithQueryCommand with : withList) {
	        		context.getGroups().add(with.getGroupSymbol().getName());
	        		QueryCommand subCommand = with.getCommand();
	        		if (subCommand instanceof Query && ((Query)subCommand).getIsXML()) {
	        			ProcessorPlan plan = QueryOptimizer.optimizePlan(subCommand, metadata, idGenerator, capFinder, analysisRecord, context);
	        			subCommand.setProcessorPlan(plan);
	        			continue;
	        		}
	                RelationalPlan plan = optimize(subCommand);
	                subCommand.setProcessorPlan(plan);
	                RelationalPlan procPlan = (RelationalPlan) plan;
	                RelationalNode root = procPlan.getRootNode();
	                Number planCardinality = root.getEstimateNodeCardinality();
	                if (planCardinality != null) {
	                	((TempMetadataID)with.getGroupSymbol().getMetadataID()).setCardinality(planCardinality.intValue());
	                }
	                AccessNode aNode = CriteriaCapabilityValidatorVisitor.getAccessNode(procPlan);
	                if (aNode == null) {
	                	continue;
	                }
                	Object modelID = CriteriaCapabilityValidatorVisitor.validateCommandPushdown(null, metadata, capFinder, aNode);
                	QueryCommand withCommand = CriteriaCapabilityValidatorVisitor.getQueryCommand(aNode);
	                if (modelID == null || withCommand == null) {
	                	continue;
	                }
	        		if (!CapabilitiesUtil.supports(Capability.COMMON_TABLE_EXPRESSIONS, modelID, metadata, capFinder)) {
	        			continue;
	        		}
                	WithQueryCommand wqc = new WithQueryCommand(with.getGroupSymbol(), with.getColumns(), withCommand);
                	with.getGroupSymbol().setModelMetadataId(modelID);
                	if (pushdownWith == null) {
                		pushdownWith = new LinkedHashMap<String, WithQueryCommand>();
                		withGroups = new TreeSet<GroupSymbol>(nonCorrelatedComparator);
                	}
                	pushdownWith.put(with.getGroupSymbol().getName(), wqc);
				}
	        	if (pushdownWith != null) {
	        		addModelIds(command, pushdownWith);
	        	}
	        }
		}
        PlanNode plan;
		try {
			plan = generatePlan(command);
		} catch (TeiidProcessingException e) {
			 throw new QueryPlannerException(e);
		}

		if(debug) {
            analysisRecord.println("\nCANONICAL PLAN: \n" + plan); //$NON-NLS-1$
		} 

        // Connect ProcessorPlan to SubqueryContainer (if any) of SELECT or PROJECT nodes
		connectSubqueryContainers(plan, pushdownWith, withGroups); //TODO: merge with node creation
        
        // Set top column information on top node
        List<Expression> topCols = Util.deepClone(command.getProjectedSymbols(), Expression.class);

        // Build rule set based on hints
        RuleStack rules = buildRules();
        // Run rule-based optimizer
        plan = executeRules(rules, plan);

        RelationalPlan result = planToProcessConverter.convert(plan);
        boolean fullPushdown = false;
        if (pushdownWith != null) {
        	AccessNode aNode = CriteriaCapabilityValidatorVisitor.getAccessNode(result);
         	if (aNode != null) { 
         		QueryCommand queryCommand = CriteriaCapabilityValidatorVisitor.getQueryCommand(aNode);
         		if (queryCommand != null) {
         			fullPushdown = true;
         		}
         	}
     		//distribute the appropriate clauses to the pushdowns
     		assignWithClause(result.getRootNode(), pushdownWith, withGroups);
        }
        if (!fullPushdown) {
        	//generally any with item associated with a pushdown will not be needed as we're converting to a source query
        	result.setWith(withList);
        }
        result.setOutputElements(topCols);
        this.sourceHint = previous;
        return result;
    }
    
    private static void assignWithClause(RelationalNode node, Map<String, WithQueryCommand> pushdownWith, Set<GroupSymbol> groups) {
        if(node instanceof AccessNode) {
            AccessNode accessNode = (AccessNode) node;
            Map<GroupSymbol, RelationalPlan> subplans = accessNode.getSubPlans();
            if (subplans != null) {
            	for (RelationalPlan subplan : subplans.values()) {
    				assignWithClause(subplan.getRootNode(), pushdownWith, groups);
            	}
            }
            Command command = accessNode.getCommand();
            if (command instanceof QueryCommand) {
            	groups.clear();
            	GroupCollectorVisitor.getGroupsIgnoreInlineViews(command, groups);
            	List<WithQueryCommand> with = new ArrayList<WithQueryCommand>();
            	for (GroupSymbol groupSymbol : groups) {
            		WithQueryCommand clause = pushdownWith.get(groupSymbol.getNonCorrelationName());
            		if (clause != null) {
            			with.add(clause.clone());
            			command.setSourceHint(SourceHint.combine(command.getSourceHint(), clause.getCommand().getSourceHint()));
            		}
				}
            	if (!with.isEmpty()) {
            		QueryCommand query = (QueryCommand)command;
            		if (query.getWith() != null) {
            			//we need to accumulate as a with clause could have been used at a lower scope
            			query.getWith().addAll(with);
            		} else {
            			query.setWith(with);
            		}
            	}
            }
        } 
        
        // Recurse through children
        RelationalNode[] children = node.getChildren();
        for(int i=0; i<node.getChildCount(); i++) {
        	assignWithClause(children[i], pushdownWith, groups);
        }
    }

    /**
     * mark all relevant group symbols as being from the modelid
     * @param command
     * @param modelID
     * @param names
     */
	private void addModelIds(Command command, final Map<String, WithQueryCommand> with) {
		PreOrPostOrderNavigator.doVisit(command, new LanguageVisitor() {
			@Override
			public void visit(UnaryFromClause obj) {
				GroupSymbol group = obj.getGroup();
				WithQueryCommand wqc = with.get(group.getNonCorrelationName());
				if (wqc != null) {
					group.setModelMetadataId(wqc.getGroupSymbol().getModelMetadataId());
				}
			}
		}, PreOrPostOrderNavigator.POST_ORDER, true);  
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

    private void connectSubqueryContainers(PlanNode plan, Map<String, WithQueryCommand> pushdownWith, Set<GroupSymbol> groups) throws QueryPlannerException, QueryMetadataException, TeiidComponentException {
        Set<GroupSymbol> groupSymbols = getGroupSymbols(plan);

        for (PlanNode node : NodeEditor.findAllNodes(plan, NodeConstants.Types.PROJECT | NodeConstants.Types.SELECT | NodeConstants.Types.JOIN | NodeConstants.Types.SOURCE | NodeConstants.Types.GROUP)) {
            List<SubqueryContainer<?>> subqueryContainers = node.getSubqueryContainers();
            planSubqueries(pushdownWith, groups, groupSymbols, node, subqueryContainers, false);
            node.addGroups(GroupsUsedByElementsVisitor.getGroups(node.getCorrelatedReferenceElements()));
        }
    }

	public void planSubqueries(
			Map<String, WithQueryCommand> pushdownWith,
			Set<GroupSymbol> groups, Set<GroupSymbol> groupSymbols,
			PlanNode node, List<SubqueryContainer<?>> subqueryContainers, boolean isStackEntry)
			throws QueryMetadataException, TeiidComponentException,
			QueryPlannerException {
        if (subqueryContainers.isEmpty()){
        	return;
        }
		Set<GroupSymbol> localGroupSymbols = groupSymbols;
		if (node != null && node.getType() == NodeConstants.Types.JOIN) {
			localGroupSymbols = getGroupSymbols(node);
		}
		for (SubqueryContainer<?> container : subqueryContainers) {
			if (container.getCommand().getProcessorPlan() != null) {
				continue;
			}
		    //a clone is needed here because the command could get modified during planning
		    Command subCommand = (Command)container.getCommand().clone(); 
			Set<PlanningStackEntry> entries = null;
			PlanningStackEntry stackEntry = null;
			if (isStackEntry) {
				entries = planningStack.get();
				stackEntry = createPlanningStackEntry(groupSymbols.iterator().next(), subCommand, false, entries);
			}
			try {
			    ArrayList<Reference> correlatedReferences = new ArrayList<Reference>();
			    CorrelatedReferenceCollectorVisitor.collectReferences(subCommand, localGroupSymbols, correlatedReferences);
			    ProcessorPlan procPlan = QueryOptimizer.optimizePlan(subCommand, metadata, idGenerator, capFinder, analysisRecord, context);
			    if (procPlan instanceof RelationalPlan && pushdownWith != null) {
			    	Map<String, WithQueryCommand> parentPushdownWith = pushdownWith;
			    	if (subCommand instanceof QueryCommand) {
			    		QueryCommand query = (QueryCommand)subCommand;
			    		List<WithQueryCommand> with = query.getWith();
			    		if (with != null && !with.isEmpty()) {
			    			parentPushdownWith = new HashMap<String, WithQueryCommand>(parentPushdownWith);
			    			for (WithQueryCommand withQueryCommand : with) {
			    				parentPushdownWith.remove(withQueryCommand.getGroupSymbol().getNonCorrelationName());
							}
			    		}
			    	}
			    	assignWithClause(((RelationalPlan) procPlan).getRootNode(), parentPushdownWith, groups);
			    }
			    container.getCommand().setProcessorPlan(procPlan);
			    setCorrelatedReferences(container, correlatedReferences);
			    //update the correlated references to the appropriate grouping symbols
			    if (node != null && node.getType() != NodeConstants.Types.JOIN && node.getType() != NodeConstants.Types.GROUP  && !correlatedReferences.isEmpty()) {
			    	PlanNode grouping = NodeEditor.findNodePreOrder(node, NodeConstants.Types.GROUP, NodeConstants.Types.SOURCE | NodeConstants.Types.JOIN);
			    	if (grouping != null) {
			    		SymbolMap map = (SymbolMap) grouping.getProperty(Info.SYMBOL_MAP);
			    		SymbolMap symbolMap = container.getCommand().getCorrelatedReferences();
			    		for (Map.Entry<ElementSymbol, Expression> entry : map.asMap().entrySet()) {
			    			if (!(entry.getValue() instanceof ElementSymbol)) {
			    				continue; //currently can't be correlated on an aggregate
			    			}
			    			ElementSymbol es = (ElementSymbol)entry.getValue();
			    			if (symbolMap.getMappedExpression(es) != null) {
			    				symbolMap.addMapping(es, entry.getKey());
			    			}
						}
			    	}
			    }
			} finally {
				if (entries != null) {
					entries.remove(stackEntry);
				}
			}
		}
	}

	private void setCorrelatedReferences(SubqueryContainer<?> container,
			List<Reference> correlatedReferences) {
		if (!correlatedReferences.isEmpty()) {
		    SymbolMap map = new SymbolMap();
		    for (Reference reference : correlatedReferences) {
				map.addMapping(reference.getExpression(), reference.getExpression());
			}
		    container.getCommand().setCorrelatedReferences(map);
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
    private void distributeDependentHints(Collection<String> groups, PlanNode plan, NodeConstants.Info hintProperty, Collection<? extends Object> vals)
        throws QueryMetadataException, TeiidComponentException {
    
        if(groups == null || groups.isEmpty()) {
        	return;
        }
        // Get all source nodes
        List<PlanNode> nodes = NodeEditor.findAllNodes(plan, NodeConstants.Types.SOURCE);
        Iterator<? extends Object> valIter = vals.iterator();
        // Walk through each dependent group hint and
        // attach to the correct source node
        for (String groupName : groups) {
        	Object val = valIter.next();
            // Walk through nodes and apply hint to all that match group name
            boolean appliedHint = applyHint(nodes, groupName, hintProperty, val);

            if(! appliedHint) {
                //check if it is partial group name
                Collection groupNames = metadata.getGroupsForPartialName(groupName);
                if(groupNames.size() == 1) {
                    groupName = (String)groupNames.iterator().next();
                    appliedHint = applyHint(nodes, groupName, hintProperty, val);
                }
                
                if(! appliedHint) {
                	String msg = QueryPlugin.Util.getString("ERR.015.004.0010", groupName); //$NON-NLS-1$
                	if (this.analysisRecord.recordAnnotations()) {
                		this.analysisRecord.addAnnotation(new Annotation(Annotation.HINTS, msg, "ignoring hint", Priority.MEDIUM)); //$NON-NLS-1$
                	}
                }
            }
        }
    }
    
    private static boolean applyHint(List<PlanNode> nodes, String groupName, NodeConstants.Info hintProperty, Object value) {
        boolean appliedHint = false;
        for (PlanNode node : nodes) {
            GroupSymbol nodeGroup = node.getGroups().iterator().next();
            
            String sDefinition = nodeGroup.getDefinition();
            
            if (nodeGroup.getName().equalsIgnoreCase(groupName) 
             || (sDefinition != null && sDefinition.equalsIgnoreCase(groupName)) ) {
                node.setProperty(hintProperty, value);
                appliedHint = true;
            }
        }
        return appliedHint;
    }

    public RuleStack buildRules() {
        RuleStack rules = new RuleStack();
        rules.setPlanner(this);
        rules.push(RuleConstants.COLLAPSE_SOURCE);
        
        rules.push(RuleConstants.PLAN_SORTS);
        
        //TODO: update plan sorts to take advantage of semi-join ordering
        if (hints.hasJoin || hints.hasCriteria) {
            rules.push(new RuleMergeCriteria(idGenerator, capFinder, analysisRecord, context, metadata));
        }

        if(hints.hasJoin) {
            rules.push(RuleConstants.IMPLEMENT_JOIN_STRATEGY);
        }
        
        rules.push(RuleConstants.CALCULATE_COST);
        
        rules.push(new RuleAssignOutputElements(true));
        
        if (hints.hasLimit) {
            rules.push(RuleConstants.PUSH_LIMIT);
        }
        if (hints.hasRelationalProc) {
            rules.push(RuleConstants.PLAN_PROCEDURES);
        }
        if (hints.hasJoin) {
        	rules.push(RuleConstants.CHOOSE_DEPENDENT);
        }
        if(hints.hasAggregates) {
            rules.push(new RulePushAggregates(idGenerator));
        }
        if(hints.hasJoin) {
            rules.push(RuleConstants.CHOOSE_JOIN_STRATEGY);
            rules.push(RuleConstants.RAISE_ACCESS);
            //after planning the joins, let the criteria be pushed back into place
            rules.push(RuleConstants.PUSH_SELECT_CRITERIA);
            rules.push(RuleConstants.PLAN_JOINS);
        }
        rules.push(RuleConstants.RAISE_ACCESS);
        if (hints.hasFunctionBasedColumns) {
        	rules.push(RuleConstants.SUBSTITUE_EXPRESSIONS);
        }
        if (hints.hasSetQuery) {
            rules.push(RuleConstants.PLAN_UNIONS);
        } 
        if(hints.hasCriteria || hints.hasJoin || hints.hasVirtualGroups) {
            //after copy criteria, it is no longer necessary to have phantom criteria nodes, so do some cleaning
        	//also remove possible erroneous output elements
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
        if (hints.hasJoin && hints.hasSetQuery) {
            rules.push(RuleConstants.DECOMPOSE_JOIN);
            rules.push(RuleConstants.MERGE_VIRTUAL);
        }
        if (hints.hasJoin && hints.hasOptionalJoin) {
            rules.push(RuleConstants.REMOVE_OPTIONAL_JOINS);
        }
        if (hints.hasRowBasedSecurity) {
        	rules.push(new RuleApplySecurity());
        }
        if (hints.hasVirtualGroups || (hints.hasJoin && hints.hasOptionalJoin) || hints.hasRowBasedSecurity) {
        	//do initial filtering to make merging and optional join logic easier
            rules.push(new RuleAssignOutputElements(false));
        }
        rules.push(RuleConstants.PLACE_ACCESS);
        return rules;
    }

    public PlanNode executeRules(RuleStack rules, PlanNode plan)
        throws QueryPlannerException, QueryMetadataException, TeiidComponentException {

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
                analysisRecord.println("\nAFTER: \n" + plan.nodeToString(true)); //$NON-NLS-1$
            }
        }
        return plan;
    }
	
	public PlanNode generatePlan(Command cmd) throws TeiidComponentException, TeiidProcessingException {
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
	        if(cmd.getOption().getMakeDepOptions() != null) {
	            distributeDependentHints(cmd.getOption().getDependentGroups(), result, NodeConstants.Info.MAKE_DEP, cmd.getOption().getMakeDepOptions());
	        }
	        if (cmd.getOption().getNotDependentGroups() != null) {
	            distributeDependentHints(cmd.getOption().getNotDependentGroups(), result, NodeConstants.Info.MAKE_NOT_DEP, Collections.nCopies(cmd.getOption().getNotDependentGroups().size(), Boolean.TRUE));
	        }
        }
        this.option = savedOption;
        return result;
	}

	PlanNode createUpdatePlan(Command command) throws TeiidComponentException, TeiidProcessingException {
        // Create top project node - define output columns for stored query / procedure
        PlanNode projectNode = NodeFactory.getNewNode(NodeConstants.Types.PROJECT);

        // Set output columns
        List<Expression> cols = command.getProjectedSymbols();
        projectNode.setProperty(NodeConstants.Info.PROJECT_COLS, cols);

        // Define source of data for stored query / procedure
        PlanNode sourceNode = NodeFactory.getNewNode(NodeConstants.Types.SOURCE);
        sourceNode.setProperty(NodeConstants.Info.ATOMIC_REQUEST, command);
        sourceNode.setProperty(NodeConstants.Info.VIRTUAL_COMMAND, command);
        boolean usingTriggerAction = false;
        if (command instanceof ProcedureContainer) {
        	ProcedureContainer container = (ProcedureContainer)command;
        	usingTriggerAction = addNestedProcedure(sourceNode, container, container.getGroup().getMetadataID());
        }
        GroupSymbol target = ((TargetedCommand)command).getGroup();
        sourceNode.addGroup(target);
    	Object id = getTrackableGroup(target, metadata);
    	if (id != null) {
    		context.accessedPlanningObject(id);
    	}
        attachLast(projectNode, sourceNode);

        //for INTO query, attach source and project nodes
        if(!usingTriggerAction && command instanceof Insert){
        	Insert insert = (Insert)command;
        	if (insert.getQueryExpression() != null) {
	            PlanNode plan = generatePlan(insert.getQueryExpression());
	            attachLast(sourceNode, plan);
	            mergeTempMetadata(insert.getQueryExpression(), insert);
	            projectNode.setProperty(NodeConstants.Info.INTO_GROUP, insert.getGroup());
	            if (insert.getConstraint() != null) {
	            	projectNode.setProperty(NodeConstants.Info.CONSTRAINT, insert.getConstraint());
	            }
        	}
        }
        
        return projectNode;
	}

	private boolean addNestedProcedure(PlanNode sourceNode,
			ProcedureContainer container, Object metadataId) throws TeiidComponentException,
			QueryMetadataException, TeiidProcessingException {
		if (container instanceof StoredProcedure) {
			StoredProcedure sp = (StoredProcedure)container;
			if (sp.getProcedureID() instanceof Procedure) {
				context.accessedPlanningObject(sp.getProcedureID());
			}
		}
		for (SubqueryContainer<?> subqueryContainer : ValueIteratorProviderCollectorVisitor.getValueIteratorProviders(container)) {
			if (subqueryContainer.getCommand().getCorrelatedReferences() != null) {
				continue;
			}
			List<Reference> correlatedReferences = new ArrayList<Reference>();
			CorrelatedReferenceCollectorVisitor.collectReferences(subqueryContainer.getCommand(), Arrays.asList(container.getGroup()), correlatedReferences);
			setCorrelatedReferences(subqueryContainer, correlatedReferences);
		}
		String cacheString = "transformation/" + container.getClass().getSimpleName().toUpperCase(); //$NON-NLS-1$
		Command c = (Command)metadata.getFromMetadataCache(metadataId, cacheString);
		if (c == null) {
			c = QueryResolver.expandCommand(container, metadata, analysisRecord);
			if (c != null) {
				if (c instanceof CreateProcedureCommand) {
					//TODO: find a better way to do this
					((CreateProcedureCommand)c).setProjectedSymbols(container.getProjectedSymbols());
				}
		        Request.validateWithVisitor(new ValidationVisitor(), metadata, c);
		        metadata.addToMetadataCache(metadataId, cacheString, c.clone());
			}
		} else {
			c = (Command)c.clone();
			if (c instanceof CreateProcedureCommand) {
				//TODO: find a better way to do this
				((CreateProcedureCommand)c).setProjectedSymbols(container.getProjectedSymbols());
			}
		}
		boolean checkRowBasedSecurity = true;
		if (!container.getGroup().isProcedure() && !metadata.isVirtualGroup(metadataId)) {
			Set<PlanningStackEntry> entries = planningStack.get();
			if (entries.contains(new PlanningStackEntry(container, container.getGroup()))) {
				checkRowBasedSecurity = false;
			}
		}
		if (checkRowBasedSecurity) {
			c = RowBasedSecurityHelper.checkUpdateRowBasedFilters(container, c, this);
		}
		if (c != null) {
			if (c instanceof TriggerAction) {
				TriggerAction ta = (TriggerAction)c;
				ProcessorPlan plan = new TriggerActionPlanner().optimize((ProcedureContainer)container.clone(), ta, idGenerator, metadata, capFinder, analysisRecord, context);
			    sourceNode.setProperty(NodeConstants.Info.PROCESSOR_PLAN, plan);
			    return true;
			}
			if (c.getCacheHint() != null) {
				if (container instanceof StoredProcedure) {
					StoredProcedure sp = (StoredProcedure)container;
					boolean noCache = isNoCacheGroup(metadata, sp.getProcedureID(), option);
					if (!noCache) {
						if (context.isResultSetCacheEnabled() && container.areResultsCachable() && LobManager.getLobIndexes(new ArrayList<ElementSymbol>(sp.getProcedureParameters().keySet())) == null) {
							container.getGroup().setGlobalTable(true);
							container.setCacheHint(c.getCacheHint());
							recordAnnotation(analysisRecord, Annotation.CACHED_PROCEDURE, Priority.LOW, "SimpleQueryResolver.procedure_cache_used", container.getGroup()); //$NON-NLS-1$
							return false;
						}
						recordAnnotation(analysisRecord, Annotation.CACHED_PROCEDURE, Priority.MEDIUM, "SimpleQueryResolver.procedure_cache_not_usable", container.getGroup()); //$NON-NLS-1$
					} else {
						recordAnnotation(analysisRecord, Annotation.CACHED_PROCEDURE, Priority.LOW, "SimpleQueryResolver.procedure_cache_not_used", container.getGroup()); //$NON-NLS-1$
					}
				}
			}
			//skip the rewrite here, we'll do that in the optimizer
			//so that we know what the determinism level is.
			addNestedCommand(sourceNode, container.getGroup(), container, c, false, true);
		}

		List<SubqueryContainer<?>> subqueries = ValueIteratorProviderCollectorVisitor.getValueIteratorProviders(container);

		if (c == null
				/* we cheat with the temp table capabilities a little - it actual supports any non-pushdown required function and non-correlated subquery
				 * we do a specific validation for subqueries below, and otherwise skip the CriteriaCapabilityValidatorVisitor
				 */
				&& (!container.getGroup().isTempTable() || metadata.getModelID(container.getGroup().getMetadataID()) != TempMetadataAdapter.TEMP_MODEL) 
				&& container instanceof FilteredCommand) { //we force the evaluation of procedure params - TODO: inserts are fine except for nonpushdown functions on columns
			//for non-temp source queries, we must pre-plan subqueries to know if they can be pushed down
			planSubqueries(container, c, subqueries, true);
			
			if (!CriteriaCapabilityValidatorVisitor.canPushLanguageObject(container, metadata.getModelID(container.getGroup().getMetadataID()), metadata, capFinder, analysisRecord)) {
				//do a workaround of row-by-row processing for update/delete
				validateRowProcessing(container);
				
				//treat this as an update procedure
				if (container instanceof Update) {
					c = QueryRewriter.createUpdateProcedure((Update)container, metadata, context);
				} else {
					c = QueryRewriter.createDeleteProcedure((Delete)container, metadata, context);
				}
				addNestedCommand(sourceNode, container.getGroup(), container, c, false, true);
				return false;
			}
		}
		//plan any subqueries in criteria/parameters/values
		planSubqueries(container, c, subqueries, false);
		return false;
	}

	private void planSubqueries(ProcedureContainer container, Command c, List<SubqueryContainer<?>> subqueries, boolean initial)
			throws QueryPlannerException, QueryMetadataException,
			TeiidComponentException {
		
		boolean isSourceTemp = c == null && container.getGroup().isTempTable() && metadata.getModelID(container.getGroup().getMetadataID()) == TempMetadataAdapter.TEMP_MODEL;
		
		for (SubqueryContainer<?> subqueryContainer : subqueries) {
			if (isSourceTemp) {
				if (subqueryContainer.getCommand().getCorrelatedReferences() == null) {
					if (subqueryContainer instanceof ScalarSubquery) {
						((ScalarSubquery) subqueryContainer).setShouldEvaluate(true);
					} else if (subqueryContainer instanceof ExistsCriteria) {
						((ExistsCriteria) subqueryContainer).setShouldEvaluate(true);
					} else {
						throw new QueryPlannerException(QueryPlugin.Event.TEIID30253, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID30253, container));
					}
				} else {
					throw new QueryPlannerException(QueryPlugin.Event.TEIID30253, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID30253, container));
				}
    		}
			if (subqueryContainer.getCommand().getProcessorPlan() == null) {
				Command subCommand = initial?(Command) subqueryContainer.getCommand().clone():subqueryContainer.getCommand();
				ProcessorPlan plan = QueryOptimizer.optimizePlan(subCommand, metadata, null, capFinder, analysisRecord, context);
	    		subqueryContainer.getCommand().setProcessorPlan(plan);
			}
    		
    		if (c == null && !initial) {
				RuleCollapseSource.prepareSubquery(subqueryContainer);
			}
		}
	}

	void validateRowProcessing(ProcedureContainer container)
			throws TeiidComponentException, QueryMetadataException,
			QueryPlannerException {
		if (metadata.getUniqueKeysInGroup(container.getGroup().getMetadataID()).isEmpty() 
				|| !CapabilitiesUtil.supports(Capability.CRITERIA_COMPARE_EQ, metadata.getModelID(container.getGroup().getMetadataID()), metadata, capFinder)) {
			 throw new QueryPlannerException(QueryPlugin.Event.TEIID30253, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID30253, container));
		}
	}

    PlanNode createStoredProcedurePlan(StoredProcedure storedProc) throws QueryMetadataException, TeiidComponentException, TeiidProcessingException {
        // Create top project node - define output columns for stored query / procedure
    	PlanNode projectNode = attachProject(null, storedProc.getProjectedSymbols());

        // Define source of data for stored query / procedure
        PlanNode sourceNode = NodeFactory.getNewNode(NodeConstants.Types.SOURCE);
        sourceNode.setProperty(NodeConstants.Info.VIRTUAL_COMMAND, storedProc);
    	addNestedProcedure(sourceNode, storedProc, storedProc.getProcedureID());
        
        hints.hasRelationalProc |= storedProc.isProcedureRelational();
        
        if (!hints.hasRowBasedSecurity && RowBasedSecurityHelper.applyRowSecurity(metadata, storedProc.getGroup(), context)) {
        	hints.hasRowBasedSecurity = true;
        }

        // Set group on source node
        sourceNode.addGroup(storedProc.getGroup());

        attachLast(projectNode, sourceNode);

        return projectNode;
    }

	PlanNode createQueryPlan(QueryCommand command)
		throws TeiidComponentException, TeiidProcessingException {
        // Build canonical plan
    	PlanNode node = null;
        if(command instanceof Query) {
            node = createQueryPlan((Query) command);
        } else {
            hints.hasSetQuery = true;
            SetQuery query = (SetQuery)command;
            SourceHint previous = this.sourceHint;
            this.sourceHint = SourceHint.combine(previous, query.getProjectedQuery().getSourceHint());
            PlanNode leftPlan = createQueryPlan( query.getLeftQuery());
            PlanNode rightPlan = createQueryPlan( query.getRightQuery());
            node = NodeFactory.getNewNode(NodeConstants.Types.SET_OP);
            node.setProperty(NodeConstants.Info.SET_OPERATION, query.getOperation());
            node.setProperty(NodeConstants.Info.USE_ALL, query.isAll());
            this.sourceHint = previous;
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
		throws QueryMetadataException, TeiidComponentException, TeiidProcessingException {

        PlanNode plan = null;

		LinkedHashSet<WindowFunction> windowFunctions = new LinkedHashSet<WindowFunction>();

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
    		LinkedHashSet<AggregateSymbol> aggs = new LinkedHashSet<AggregateSymbol>();
    		AggregateSymbolCollectorVisitor.getAggregates(query.getSelect(), aggs, null, null, windowFunctions, null);
    		boolean hasGrouping = !aggs.isEmpty();
    		if (query.getHaving() != null) {
    			aggs.addAll(AggregateSymbolCollectorVisitor.getAggregates(query.getHaving(), true));
    			hasGrouping = true;
    		}
    		if (query.getGroupBy() != null) {
    			hasGrouping = true;
    		}
    		if(hasGrouping) {
    			plan = attachGrouping(plan, query, aggs);
    		}

    		// Attach having criteria node on top
    		if(query.getHaving() != null) {
    			plan = attachCriteria(plan, query.getHaving(), true);
                hints.hasCriteria = true;
    		}
            
        }

		// Attach project on top
		plan = attachProject(plan, query.getSelect().getProjectedSymbols());
		if (query.getOrderBy() != null) {
			AggregateSymbolCollectorVisitor.getAggregates(query.getOrderBy(), null, null, null, windowFunctions, null);
		}
		if (!windowFunctions.isEmpty()) {
			plan.setProperty(Info.HAS_WINDOW_FUNCTIONS, true);
		}

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
        List<FromClause> clauses = from.getClauses();
        
        while (clauses.size() > 1) {
            FromClause first = from.getClauses().remove(0);
            FromClause second = from.getClauses().remove(0);
            JoinPredicate jp = new JoinPredicate(first, second, JoinType.JOIN_CROSS);
            clauses.add(0, jp);
        }
        
        return clauses.get(0);
    }
    
    /**
     * Build a join plan based on the structure in a clause.  These structures should be
     * essentially the same tree, but with different objects and details.
     * @param clause Clause to build tree from
     * @param parent Parent node to attach join node structure to
     * @param sourceMap Map of group to source node, used for connecting children to join plan
     * @param markJoinsInternal Flag saying whether joins built in this method should be marked
     * as internal
     * @throws TeiidComponentException 
     * @throws QueryMetadataException 
     * @throws TeiidProcessingException 
     */
    void buildTree(FromClause clause, final PlanNode parent)
        throws QueryMetadataException, TeiidComponentException, TeiidProcessingException {
        
        PlanNode node = null;
        
        if(clause instanceof UnaryFromClause) {
            // No join required
            UnaryFromClause ufc = (UnaryFromClause)clause;
            GroupSymbol group = ufc.getGroup();
            if (metadata.isVirtualGroup(group.getMetadataID())) {
            	hints.hasVirtualGroups = true;
            }
            if (!hints.hasRowBasedSecurity && RowBasedSecurityHelper.applyRowSecurity(metadata, group, context)) {
            	hints.hasRowBasedSecurity = true;
            }
            if (metadata.getFunctionBasedExpressions(group.getMetadataID()) != null) {
            	hints.hasFunctionBasedColumns = true;
            }
            Command nestedCommand = ufc.getExpandedCommand();
            if (nestedCommand == null && !group.isProcedure()) {
            	Object id = getTrackableGroup(group, metadata);
            	if (id != null) {
            		context.accessedPlanningObject(id);
            	}
            	if (!group.isTempGroupSymbol() && metadata.isVirtualGroup(group.getMetadataID())) { 
    				nestedCommand = resolveVirtualGroup(group);
        		}
            }
            node = NodeFactory.getNewNode(NodeConstants.Types.SOURCE);
            if (group.getModelMetadataId() != null) {
            	node.setProperty(Info.MODEL_ID, group.getModelMetadataId());
            }
            if (ufc.isNoUnnest()) {
            	node.setProperty(Info.NO_UNNEST, Boolean.TRUE);
            }
            node.addGroup(group);
            if (nestedCommand != null) {
            	UpdateInfo info = ProcedureContainerResolver.getUpdateInfo(group, metadata);
            	if (info != null && info.getPartitionInfo() != null && !info.getPartitionInfo().isEmpty()) {
            		node.setProperty(NodeConstants.Info.PARTITION_INFO, info.getPartitionInfo());
            	}
            	SourceHint previous = this.sourceHint;
            	if (nestedCommand.getSourceHint() != null) {
            		this.sourceHint = SourceHint.combine(previous, nestedCommand.getSourceHint());
            	}
            	addNestedCommand(node, group, nestedCommand, nestedCommand, true, true);
            	this.sourceHint = previous;
            } else if (this.sourceHint != null) {
            	node.setProperty(Info.SOURCE_HINT, this.sourceHint);
            }
            parent.addLastChild(node);
        } else if(clause instanceof JoinPredicate) {
            JoinPredicate jp = (JoinPredicate) clause;

            // Set up new join node corresponding to this join predicate
            node = NodeFactory.getNewNode(NodeConstants.Types.JOIN);
            node.setProperty(NodeConstants.Info.JOIN_TYPE, jp.getJoinType());
            node.setProperty(NodeConstants.Info.JOIN_STRATEGY, JoinStrategyType.NESTED_LOOP);
            node.setProperty(NodeConstants.Info.JOIN_CRITERIA, jp.getJoinCriteria());
            if (jp.isPreserve()) {
            	node.setProperty(Info.PRESERVE, Boolean.TRUE);
            }
            if (jp.getJoinType() == JoinType.JOIN_LEFT_OUTER) {
            	hints.hasOptionalJoin = true;
            }
         
            // Attach join node to parent
            parent.addLastChild(node);

            // Handle each child
            FromClause[] clauses = new FromClause[] {jp.getLeftClause(), jp.getRightClause()};
            for(int i=0; i<2; i++) {
            	if (jp.isPreserve() && clauses[i] instanceof JoinPredicate) {
            		((JoinPredicate)clauses[i]).setPreserve(true);
            	}
                buildTree(clauses[i], node);
                // Add groups to joinNode
            	node.addGroups(node.getLastChild().getGroups());
            }
        } else if (clause instanceof SubqueryFromClause) {
            SubqueryFromClause sfc = (SubqueryFromClause)clause;
            GroupSymbol group = sfc.getGroupSymbol();
            Command nestedCommand = sfc.getCommand();
            node = NodeFactory.getNewNode(NodeConstants.Types.SOURCE);
            if (sfc.isTable()) {
    		    sfc.getCommand().setCorrelatedReferences(getCorrelatedReferences(parent, node, sfc));
            }
            if (sfc.isNoUnnest()) {
            	node.setProperty(Info.NO_UNNEST, Boolean.TRUE);
            }
            SourceHint previous = this.sourceHint;
        	if (nestedCommand.getSourceHint() != null) {
        		this.sourceHint = SourceHint.combine(previous, nestedCommand.getSourceHint());
        	}
            node.addGroup(group);
            addNestedCommand(node, group, nestedCommand, nestedCommand, true, false);
        	this.sourceHint = previous;
			if (nestedCommand instanceof SetQuery) {
				Map<ElementSymbol, List<Set<Constant>>> partitionInfo = PartitionAnalyzer.extractPartionInfo((SetQuery)nestedCommand, ResolverUtil.resolveElementsInGroup(group, metadata));
				if (!partitionInfo.isEmpty()) {
					node.setProperty(NodeConstants.Info.PARTITION_INFO, partitionInfo);
				}
			}
            hints.hasVirtualGroups = true;
            parent.addLastChild(node);
        } else if (clause instanceof TableFunctionReference) {
        	TableFunctionReference tt = (TableFunctionReference)clause;
            GroupSymbol group = tt.getGroupSymbol();
            //special handling to convert array table into a mergable construct
            if (parent.getType() == NodeConstants.Types.JOIN && tt instanceof ArrayTable) {
            	JoinType jt = (JoinType) parent.getProperty(Info.JOIN_TYPE);
            	if (jt != JoinType.JOIN_FULL_OUTER && parent.getChildCount() > 0) {
	            	ArrayTable at = (ArrayTable)tt;
		        	//rewrite if deterministic and free of subqueries
		        	if (FunctionCollectorVisitor.isNonDeterministic(at.getArrayValue())
		        			|| ValueIteratorProviderCollectorVisitor.getValueIteratorProviders(at).isEmpty()) {
		            	List<ElementSymbol> symbols = at.getProjectedSymbols();
		        		FunctionLibrary funcLib = this.metadata.getFunctionLibrary();
		                FunctionDescriptor descriptor = funcLib.findFunction(FunctionLibrary.ARRAY_GET, 
		                		new Class[] { DataTypeManager.DefaultDataClasses.OBJECT, DataTypeManager.DefaultDataClasses.INTEGER });
		                Query query = new Query();
		                Select select = new Select();
		                query.setSelect(select);
		            	for (int i = 0; i < symbols.size(); i++) {
		            		ElementSymbol es = symbols.get(i);
		            		Function f = new Function(FunctionLibrary.ARRAY_GET, new Expression[] {(Expression) at.getArrayValue().clone(), new Constant(i + 1)});
		            		f.setType(DataTypeManager.DefaultDataClasses.OBJECT);
		                    f.setFunctionDescriptor(descriptor);
		                    Expression ex = f;
		            		if (es.getType() != DataTypeManager.DefaultDataClasses.OBJECT) {
		            			ex = ResolverUtil.getConversion(ex, DataTypeManager.DefaultDataTypes.OBJECT, DataTypeManager.getDataTypeName(es.getType()), false, metadata.getFunctionLibrary());
		            		}
		            		select.addSymbol(new AliasSymbol(es.getShortName(), ex));
		            	}
		            	SubqueryFromClause sfc = new SubqueryFromClause(at.getGroupSymbol(), query);
		            	sfc.setTable(true);
		            	buildTree(sfc, parent);
		            	if (!jt.isOuter()) {
		            		//insert is null criteria
		            		IsNullCriteria criteria = new IsNullCriteria((Expression) at.getArrayValue().clone());
		            		if (sfc.getCommand().getCorrelatedReferences() != null) {
			            		RuleMergeCriteria.ReferenceReplacementVisitor rrv = new RuleMergeCriteria.ReferenceReplacementVisitor(sfc.getCommand().getCorrelatedReferences());
			            		PreOrPostOrderNavigator.doVisit(criteria, rrv, PreOrPostOrderNavigator.PRE_ORDER);
		            		}
			            	criteria.setNegated(true);
			            	if (jt == JoinType.JOIN_CROSS) {
			            		parent.setProperty(NodeConstants.Info.JOIN_TYPE, JoinType.JOIN_INNER);
			            	}
			            	List<Criteria> joinCriteria = (List<Criteria>) parent.getProperty(Info.JOIN_CRITERIA); 
			            	if (joinCriteria == null) {
			            		joinCriteria = new ArrayList<Criteria>(2);
			            	}
			            	joinCriteria.add(criteria);
			                parent.setProperty(NodeConstants.Info.JOIN_CRITERIA, joinCriteria);
		            	}
		            	return;
		        	}
            	}
            }
            node = NodeFactory.getNewNode(NodeConstants.Types.SOURCE);
            node.setProperty(NodeConstants.Info.TABLE_FUNCTION, tt);
            tt.setCorrelatedReferences(getCorrelatedReferences(parent, node, tt));
            node.addGroup(group);
            parent.addLastChild(node);
        } else {
        	throw new AssertionError("Unknown Type"); //$NON-NLS-1$
        }
        
        if (clause.isOptional()) {
            node.setProperty(NodeConstants.Info.IS_OPTIONAL, Boolean.TRUE);
            hints.hasOptionalJoin = true;
        }
        
        if (clause.getMakeDep() != null) {
            node.setProperty(NodeConstants.Info.MAKE_DEP, clause.getMakeDep());
        } else if (clause.isMakeNotDep()) {
            node.setProperty(NodeConstants.Info.MAKE_NOT_DEP, Boolean.TRUE);
        }
        if (clause.isMakeInd()) {
        	node.setProperty(NodeConstants.Info.MAKE_IND, Boolean.TRUE);
        }
    }

	public static Object getTrackableGroup(GroupSymbol group, QueryMetadataInterface metadata)
			throws TeiidComponentException, QueryMetadataException {
		if (group.isTempGroupSymbol()) {
			QueryMetadataInterface qmi = metadata.getSessionMetadata();
			try {
				//exclude proc scoped temp tables
				if (group.isGlobalTable() || (qmi != null && qmi.getGroupID(group.getNonCorrelationName()) == group.getMetadataID())) {
					return group.getMetadataID();
				}
			} catch (QueryMetadataException e) {
				//not a session table
			}
		} else {
			return group.getMetadataID();
		}
		return null;
	}

	private SymbolMap getCorrelatedReferences(PlanNode parent, PlanNode node,
			LanguageObject lo) {
		PlanNode rootJoin = parent;
		while (rootJoin.getParent() != null && rootJoin.getParent().getType() == NodeConstants.Types.JOIN && !rootJoin.getParent().getGroups().isEmpty()) {
			rootJoin = rootJoin.getParent();
		}
		List<Reference> correlatedReferences = new ArrayList<Reference>();
		CorrelatedReferenceCollectorVisitor.collectReferences(lo, rootJoin.getGroups(), correlatedReferences);
		
		if (correlatedReferences.isEmpty()) {
			return null;
		}
	    SymbolMap map = new SymbolMap();
	    for (Reference reference : correlatedReferences) {
			map.addMapping(reference.getExpression(), reference.getExpression());
		}
	    node.setProperty(NodeConstants.Info.CORRELATED_REFERENCES, map);
	    return map;
	}

	private void addNestedCommand(PlanNode node,
			GroupSymbol group, Command nestedCommand, Command toPlan, boolean merge, boolean isStackEntry) throws TeiidComponentException, QueryMetadataException, TeiidProcessingException {
		if (nestedCommand instanceof QueryCommand) {
			//remove unnecessary order by
        	QueryCommand queryCommand = (QueryCommand)nestedCommand;
        	if (queryCommand.getLimit() == null) {
        		queryCommand.setOrderBy(null);
        	}
        	if (merge && queryCommand.getWith() != null) {
        		//TODO: should recontext with and merge
        		merge = false;
        	}
        }
		Set<PlanningStackEntry> entries = null;
		PlanningStackEntry entry = null;
		if (isStackEntry) {
			entries = planningStack.get();
			entry = createPlanningStackEntry(group, nestedCommand, toPlan.getType() == Command.TYPE_UPDATE_PROCEDURE, entries);
		}
		try {
			node.setProperty(NodeConstants.Info.NESTED_COMMAND, nestedCommand);
	
			if (merge && nestedCommand instanceof Query && QueryResolver.isXMLQuery((Query)nestedCommand, metadata)) {
				merge = false;
			}
	
			if (merge) {
				mergeTempMetadata(nestedCommand, parentCommand);
			    PlanNode childRoot = generatePlan(nestedCommand);
			    node.addFirstChild(childRoot);
				List<Expression> projectCols = nestedCommand.getProjectedSymbols();
				SymbolMap map = SymbolMap.createSymbolMap(group, projectCols, metadata);
				node.setProperty(NodeConstants.Info.SYMBOL_MAP, map);
			} else {
				QueryMetadataInterface actualMetadata = metadata;
				if (actualMetadata instanceof TempMetadataAdapter) {
					actualMetadata = ((TempMetadataAdapter)metadata).getMetadata();
				}
				ProcessorPlan plan = QueryOptimizer.optimizePlan(toPlan, actualMetadata, idGenerator, capFinder, analysisRecord, context);
				//hack for the optimizer not knowing the containing command when forming the plan
				if (nestedCommand instanceof StoredProcedure && plan instanceof ProcedurePlan) {
					StoredProcedure container = (StoredProcedure)nestedCommand;
					ProcedurePlan pp = (ProcedurePlan)plan;
					pp.setRequiresTransaction(container.getUpdateCount() > 0);
	        		if (container.returnParameters()) {
	        			List<ElementSymbol> outParams = new LinkedList<ElementSymbol>();
	        			for (SPParameter param : container.getParameters()) {
							if (param.getParameterType() == SPParameter.RETURN_VALUE) {
								outParams.add(param.getParameterSymbol());
							}
						}
	        			for (SPParameter param : container.getParameters()) {
							if (param.getParameterType() == SPParameter.INOUT || 
									param.getParameterType() == SPParameter.OUT) {
								outParams.add(param.getParameterSymbol());
							}
						}
	        			if (outParams.size() > 0) {
	        				pp.setOutParams(outParams);
	        			}
	        		}
	        		pp.setParams(container.getProcedureParameters());
				}
			    node.setProperty(NodeConstants.Info.PROCESSOR_PLAN, plan);
			}
		} finally {
			if (entries != null) {
				entries.remove(entry);
			}
		}
	}

	public PlanningStackEntry createPlanningStackEntry(GroupSymbol group,
			Command nestedCommand, boolean isUpdateProcedure,
			Set<PlanningStackEntry> entries) throws TeiidComponentException,
			QueryMetadataException, QueryPlannerException {
		PlanningStackEntry entry;
		entry = new PlanningStackEntry(nestedCommand, group);
		if (!entries.add(entry)) {
			if (isUpdateProcedure && !metadata.isVirtualGroup(group.getMetadataID())) {
				//must be a compensating update/delete
				throw new QueryPlannerException(QueryPlugin.Event.TEIID30254, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID30254, nestedCommand));
			}
			throw new QueryPlannerException(QueryPlugin.Event.TEIID31124, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID31124, nestedCommand.getClass().getSimpleName(), group.getNonCorrelationName(), entries));
		}
		return entry;
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
        if (isHaving && !ElementCollectorVisitor.getAggregates(crit, false).isEmpty()) {
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
	 * @param aggs 
	 * @param groupBy Group by clause, which may be null
	 * @return Updated plan
	 * @throws TeiidComponentException 
	 * @throws QueryMetadataException 
	 */
	private PlanNode attachGrouping(PlanNode plan, Query query, Collection<AggregateSymbol> aggs) throws QueryMetadataException, TeiidComponentException {
		//TODO: correlated agg
		
		GroupBy groupBy = query.getGroupBy();
		List<Expression> groupingCols = null;
		if (groupBy != null) {
			groupingCols = groupBy.getSymbols();
		}
		
		PlanNode groupNode = NodeFactory.getNewNode(NodeConstants.Types.GROUP);
		
		Map<Expression, ElementSymbol> mapping = buildGroupingNode(aggs, groupingCols, groupNode, this.context, this.idGenerator).inserseMapping();

		attachLast(groupNode, plan);
		ExpressionMappingVisitor.mapExpressions(query.getHaving(), mapping);
		ExpressionMappingVisitor.mapExpressions(query.getSelect(), mapping);
		ExpressionMappingVisitor.mapExpressions(query.getOrderBy(), mapping);
        
        // Mark in hints
        hints.hasAggregates = true;
        
		return groupNode;
	}

	/**
	 * Build a grouping node that introduces a anon group (without a inline view source node)
	 */
	public static SymbolMap buildGroupingNode(
			Collection<AggregateSymbol> aggs, List<? extends Expression> groupingCols,
			PlanNode groupNode, CommandContext cc, IDGenerator idGenerator) throws QueryMetadataException, TeiidComponentException {
		SymbolMap map = new SymbolMap();
		aggs = LanguageObject.Util.deepClone(aggs, AggregateSymbol.class);
		groupingCols = LanguageObject.Util.deepClone(groupingCols, Expression.class);
		GroupSymbol group = new GroupSymbol("anon_grp" + idGenerator.nextInt()); //$NON-NLS-1$
		if (!cc.getGroups().add(group.getName())) {
			group = RulePlaceAccess.recontextSymbol(group, cc.getGroups());
		}
		
		TempMetadataStore tms = new TempMetadataStore();
		
		int i = 0;
		
		List<AliasSymbol> symbols = new LinkedList<AliasSymbol>();
		List<Expression> targets = new LinkedList<Expression>();

		if(groupingCols != null) {
			groupNode.setProperty(NodeConstants.Info.GROUP_COLS, groupingCols);
            groupNode.addGroups(GroupsUsedByElementsVisitor.getGroups(groupingCols));
            for (Expression ex : groupingCols) {
            	AliasSymbol as = new AliasSymbol("gcol" + i++, new ExpressionSymbol("expr", ex)); //$NON-NLS-1$ //$NON-NLS-2$
            	targets.add(ex);
            	symbols.add(as);
    		}
		}
		
		i = 0;
		for (AggregateSymbol ex : aggs) {
			AliasSymbol as = new AliasSymbol("agg" + i++, new ExpressionSymbol("expr", ex)); //$NON-NLS-1$ //$NON-NLS-2$
        	targets.add(ex);
        	symbols.add(as);
		}
		
		group.setMetadataID(tms.addTempGroup(group.getName(), symbols, true, false));
		Iterator<Expression> targetIter = targets.iterator();
		for (ElementSymbol es : ResolverUtil.resolveElementsInGroup(group, new TempMetadataAdapter(new BasicQueryMetadata(), tms))) {
			Expression target = targetIter.next();
			es.setAggregate(target instanceof AggregateSymbol);
			map.addMapping(es, target);
		}
		
		groupNode.setProperty(NodeConstants.Info.SYMBOL_MAP, map);
		groupNode.addGroup(group);
		return map;
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
        	if (limit.isImplicit()) {
        		limitNode.setProperty(Info.IS_IMPLICIT_LIMIT, true);
        	}
        	if (!limit.isStrict()) {
        		limitNode.setProperty(Info.IS_NON_STRICT, true);
        	}
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

	private static PlanNode attachProject(PlanNode plan, List<? extends Expression> select) {
		PlanNode projectNode = createProjectNode(select);

		attachLast(projectNode, plan);
		return projectNode;
	}

	public static PlanNode createProjectNode(List<? extends Expression> select) {
		PlanNode projectNode = NodeFactory.getNewNode(NodeConstants.Types.PROJECT);
		projectNode.setProperty(NodeConstants.Info.PROJECT_COLS, select);

		// Set groups
		projectNode.addGroups(GroupsUsedByElementsVisitor.getGroups(select));
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
        TempMetadataStore childTempMetadata = childCommand.getTemporaryMetadata();
        if (childTempMetadata != null && !childTempMetadata.getData().isEmpty()){
            // Add to parent temp metadata
        	TempMetadataStore parentTempMetadata = parentCommand.getTemporaryMetadata();
            if (parentTempMetadata == null){
                parentCommand.setTemporaryMetadata(childTempMetadata);
            } else {
                parentTempMetadata.getData().putAll(childTempMetadata.getData());
            }
        }
    }
	
    private Command resolveVirtualGroup(GroupSymbol virtualGroup)
    throws QueryMetadataException, TeiidComponentException, TeiidProcessingException {
    	
        QueryNode qnode = null;
        
        Object metadataID = virtualGroup.getMetadataID();
        boolean noCache = isNoCacheGroup(metadata, metadataID, option);
        boolean isMaterializedGroup = metadata.hasMaterialization(metadataID);
        String cacheString = SQLConstants.Reserved.SELECT; 
        
        if( isMaterializedGroup) {
        	Object matMetadataId = metadata.getMaterialization(metadataID);
        	String matTableName = null;
        	CacheHint hint = null;
        	boolean isImplicitGlobal = matMetadataId == null;
            if (isImplicitGlobal) {
        		TempMetadataID tid = context.getGlobalTableStore().getGlobalTempTableMetadataId(metadataID);
        		matTableName = tid.getID();
        		hint = tid.getCacheHint();
        		if (hint != null) {
					recordAnnotation(analysisRecord, Annotation.MATERIALIZED_VIEW, Priority.LOW, "SimpleQueryResolver.cache_hint_used", virtualGroup, matTableName, tid); //$NON-NLS-1$
				}        	
        		matMetadataId = tid;
            } else {
            	matTableName = metadata.getFullName(matMetadataId);
            }

        	if(noCache){
        		//not use cache
        		qnode = metadata.getVirtualPlan(metadataID);
        		//TODO: update the table for defaultMat
        		recordAnnotation(analysisRecord, Annotation.MATERIALIZED_VIEW, Priority.LOW, "SimpleQueryResolver.materialized_table_not_used", virtualGroup, matTableName); //$NON-NLS-1$
        	}else{
            	this.context.accessedPlanningObject(matMetadataId);
        		qnode = new QueryNode(null);
        		List<ElementSymbol> symbols = new ArrayList<ElementSymbol>();
        		for (ElementSymbol el : ResolverUtil.resolveElementsInGroup(virtualGroup, metadata)) {
        			symbols.add(new ElementSymbol(el.getShortName()));
        		}
        		Query query = createMatViewQuery(matMetadataId, matTableName, symbols, isImplicitGlobal);
        		query.setCacheHint(hint);
        		qnode.setCommand(query);
                cacheString = "matview"; //$NON-NLS-1$
                recordAnnotation(analysisRecord, Annotation.MATERIALIZED_VIEW, Priority.LOW, "SimpleQueryResolver.Query_was_redirected_to_Mat_table", virtualGroup, matTableName); //$NON-NLS-1$
        	}
        } else {
            // Not a materialized view - query the primary transformation
            qnode = metadata.getVirtualPlan(metadataID); 
        }

        Command result = (Command)QueryResolver.resolveView(virtualGroup, qnode, cacheString, metadata).getCommand().clone();   
        return QueryRewriter.rewrite(result, metadata, context);
    }
    
	public static Query createMatViewQuery(Object matMetadataId, String matTableName, List<? extends Expression> select, boolean isGlobal) {
		Query query = new Query();
		query.setSelect(new Select(select));
		GroupSymbol gs = new GroupSymbol(matTableName);
		gs.setGlobalTable(isGlobal);
		gs.setMetadataID(matMetadataId);
		query.setFrom(new From(Arrays.asList(new UnaryFromClause(gs))));
		return query;
	}

    public static boolean isNoCacheGroup(QueryMetadataInterface metadata,
                                          Object metadataID,
                                          Option option) throws QueryMetadataException,
                                                        TeiidComponentException {
        if(option == null || !option.isNoCache()){
            return false;
        }
    	if(option.getNoCacheGroups() == null || option.getNoCacheGroups().isEmpty()){
    		//only OPTION NOCACHE, no group specified
    		return true;
    	}       
    	String fullName = metadata.getFullName(metadataID);
    	for (String groupName : option.getNoCacheGroups()) {
            if(groupName.equalsIgnoreCase(fullName)){
                return true;
            }
        }
        return false;
    }
    
    private static void recordAnnotation(AnalysisRecord analysis, String type, Priority priority, String msgKey, Object... parts) {
    	if (analysis.recordAnnotations()) {
    		Annotation annotation = new Annotation(type, 
                    QueryPlugin.Util.getString(msgKey, parts), 
                    null, 
                    priority);
    		analysis.addAnnotation(annotation);
    	}
    }

}