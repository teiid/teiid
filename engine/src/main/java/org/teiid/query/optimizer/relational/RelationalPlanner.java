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
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.teiid.api.exception.query.QueryMetadataException;
import org.teiid.api.exception.query.QueryPlannerException;
import org.teiid.api.exception.query.QueryResolverException;
import org.teiid.api.exception.query.QueryValidatorException;
import org.teiid.client.plan.Annotation;
import org.teiid.client.plan.Annotation.Priority;
import org.teiid.common.buffer.LobManager;
import org.teiid.core.TeiidComponentException;
import org.teiid.core.TeiidProcessingException;
import org.teiid.core.id.IDGenerator;
import org.teiid.dqp.internal.process.Request;
import org.teiid.language.SQLConstants;
import org.teiid.query.QueryPlugin;
import org.teiid.query.analysis.AnalysisRecord;
import org.teiid.query.mapping.relational.QueryNode;
import org.teiid.query.metadata.QueryMetadataInterface;
import org.teiid.query.metadata.TempMetadataAdapter;
import org.teiid.query.metadata.TempMetadataID;
import org.teiid.query.metadata.TempMetadataStore;
import org.teiid.query.optimizer.QueryOptimizer;
import org.teiid.query.optimizer.TriggerActionPlanner;
import org.teiid.query.optimizer.capabilities.CapabilitiesFinder;
import org.teiid.query.optimizer.capabilities.SourceCapabilities.Capability;
import org.teiid.query.optimizer.relational.plantree.NodeConstants;
import org.teiid.query.optimizer.relational.plantree.NodeEditor;
import org.teiid.query.optimizer.relational.plantree.NodeFactory;
import org.teiid.query.optimizer.relational.plantree.PlanNode;
import org.teiid.query.optimizer.relational.plantree.NodeConstants.Info;
import org.teiid.query.optimizer.relational.rules.CapabilitiesUtil;
import org.teiid.query.optimizer.relational.rules.CriteriaCapabilityValidatorVisitor;
import org.teiid.query.optimizer.relational.rules.RuleCollapseSource;
import org.teiid.query.optimizer.relational.rules.RuleConstants;
import org.teiid.query.optimizer.relational.rules.RuleMergeCriteria;
import org.teiid.query.processor.ProcessorPlan;
import org.teiid.query.processor.relational.RelationalPlan;
import org.teiid.query.processor.relational.JoinNode.JoinStrategyType;
import org.teiid.query.resolver.ProcedureContainerResolver;
import org.teiid.query.resolver.QueryResolver;
import org.teiid.query.resolver.util.ResolverUtil;
import org.teiid.query.rewriter.QueryRewriter;
import org.teiid.query.sql.LanguageObject;
import org.teiid.query.sql.LanguageVisitor;
import org.teiid.query.sql.LanguageObject.Util;
import org.teiid.query.sql.lang.CacheHint;
import org.teiid.query.sql.lang.Command;
import org.teiid.query.sql.lang.Criteria;
import org.teiid.query.sql.lang.Delete;
import org.teiid.query.sql.lang.ExistsCriteria;
import org.teiid.query.sql.lang.From;
import org.teiid.query.sql.lang.FromClause;
import org.teiid.query.sql.lang.GroupBy;
import org.teiid.query.sql.lang.Insert;
import org.teiid.query.sql.lang.JoinPredicate;
import org.teiid.query.sql.lang.JoinType;
import org.teiid.query.sql.lang.Limit;
import org.teiid.query.sql.lang.Option;
import org.teiid.query.sql.lang.OrderBy;
import org.teiid.query.sql.lang.ProcedureContainer;
import org.teiid.query.sql.lang.Query;
import org.teiid.query.sql.lang.QueryCommand;
import org.teiid.query.sql.lang.Select;
import org.teiid.query.sql.lang.SetQuery;
import org.teiid.query.sql.lang.StoredProcedure;
import org.teiid.query.sql.lang.SubqueryContainer;
import org.teiid.query.sql.lang.SubqueryFromClause;
import org.teiid.query.sql.lang.TableFunctionReference;
import org.teiid.query.sql.lang.TranslatableProcedureContainer;
import org.teiid.query.sql.lang.UnaryFromClause;
import org.teiid.query.sql.lang.Update;
import org.teiid.query.sql.lang.WithQueryCommand;
import org.teiid.query.sql.navigator.PreOrPostOrderNavigator;
import org.teiid.query.sql.proc.CreateUpdateProcedureCommand;
import org.teiid.query.sql.proc.TriggerAction;
import org.teiid.query.sql.symbol.AllSymbol;
import org.teiid.query.sql.symbol.Constant;
import org.teiid.query.sql.symbol.ElementSymbol;
import org.teiid.query.sql.symbol.GroupSymbol;
import org.teiid.query.sql.symbol.Reference;
import org.teiid.query.sql.symbol.ScalarSubquery;
import org.teiid.query.sql.symbol.SelectSymbol;
import org.teiid.query.sql.symbol.SingleElementSymbol;
import org.teiid.query.sql.util.SymbolMap;
import org.teiid.query.sql.visitor.AggregateSymbolCollectorVisitor;
import org.teiid.query.sql.visitor.CorrelatedReferenceCollectorVisitor;
import org.teiid.query.sql.visitor.GroupCollectorVisitor;
import org.teiid.query.sql.visitor.GroupsUsedByElementsVisitor;
import org.teiid.query.sql.visitor.ValueIteratorProviderCollectorVisitor;
import org.teiid.query.util.CommandContext;
import org.teiid.query.validator.ValidationVisitor;
import org.teiid.query.validator.UpdateValidator.UpdateInfo;


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
	private CommandContext context;
	private CapabilitiesFinder capFinder;
	private QueryMetadataInterface metadata;
	private PlanHints hints = new PlanHints();
	private Option option;
	
    public ProcessorPlan optimize(
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
		
		PlanToProcessConverter planToProcessConverter = null;
        if (context != null) {
        	planToProcessConverter = context.getPlanToProcessConverter();
        }
        if (planToProcessConverter == null) {
        	planToProcessConverter = new PlanToProcessConverter(metadata, idGenerator, analysisRecord, capFinder);
        }
		
		//plan with
        List<WithQueryCommand> withList = null;
        Object modelID = null;
		boolean supportsWithPushdown = true;
		List<WithQueryCommand> pushDownWith = null;
		if (command instanceof QueryCommand) {
			QueryCommand queryCommand = (QueryCommand)command;
			final HashSet<String> names = new HashSet<String>();
			if (queryCommand.getWith() != null) {
	        	withList = queryCommand.getWith();
	        	for (WithQueryCommand with : queryCommand.getWith()) {
	        		Command subCommand = with.getCommand();
	                ProcessorPlan procPlan = QueryOptimizer.optimizePlan(subCommand, metadata, idGenerator, capFinder, analysisRecord, context);
	                subCommand.setProcessorPlan(procPlan);
	                QueryCommand withCommand = CriteriaCapabilityValidatorVisitor.getQueryCommand(procPlan);
	                if (withCommand != null && supportsWithPushdown) {
	                	modelID = CriteriaCapabilityValidatorVisitor.validateCommandPushdown(modelID, metadata, capFinder, withCommand);
	            	}
	                if (modelID == null) {
	                	supportsWithPushdown = false;
	                } else {
	                	if (pushDownWith == null) {
	                		pushDownWith = new ArrayList<WithQueryCommand>();
	                	}
	                	WithQueryCommand wqc = new WithQueryCommand(with.getGroupSymbol(), with.getColumns(), withCommand);
	                	pushDownWith.add(wqc);
	                }
		        	names.add(with.getGroupSymbol().getCanonicalName());
				}
	        	if (modelID != null && supportsWithPushdown) {
	        		supportsWithPushdown = CapabilitiesUtil.supports(Capability.COMMON_TABLE_EXPRESSIONS, modelID, metadata, capFinder);
	        	}
				if (supportsWithPushdown) {
					addModelIds(command, modelID, names);
				}
	        }
		}
				
        PlanNode plan;
		try {
			plan = generatePlan(command);
		} catch (TeiidProcessingException e) {
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
        RuleStack rules = buildRules();

        // Run rule-based optimizer
        plan = executeRules(rules, plan);

        RelationalPlan result = planToProcessConverter.convert(plan);
        if (withList != null && supportsWithPushdown) {
        	QueryCommand queryCommand = CriteriaCapabilityValidatorVisitor.getQueryCommand(result);
        	if (queryCommand != null) { 
				if (CriteriaCapabilityValidatorVisitor.validateCommandPushdown(modelID, metadata, capFinder, queryCommand) == null) {
					supportsWithPushdown = false;
				} else {
					queryCommand.setWith(pushDownWith);
				}
        	} else {
        		supportsWithPushdown = false;
        	}
        }
        if (!supportsWithPushdown) {
        	result.setWith(withList);
        }
        result.setOutputElements(topCols);
        
        return result;
    }

    /**
     * mark all relevant group symbols as being from the modelid
     * @param command
     * @param modelID
     * @param names
     */
	private void addModelIds(Command command, final Object modelID,
			final HashSet<String> names) {
		PreOrPostOrderNavigator.doVisit(command, new LanguageVisitor() {
			@Override
			public void visit(UnaryFromClause obj) {
				GroupSymbol group = obj.getGroup();
				if (names.contains(group.getNonCorrelationName().toUpperCase())) {
					group.setModelMetadataId(modelID);
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

    private void connectSubqueryContainers(PlanNode plan) throws QueryPlannerException, QueryMetadataException, TeiidComponentException {
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
                //a clone is needed here because the command could get modified during planning
                Command subCommand = (Command)container.getCommand().clone(); 
                ArrayList<Reference> correlatedReferences = new ArrayList<Reference>();
                CorrelatedReferenceCollectorVisitor.collectReferences(subCommand, localGroupSymbols, correlatedReferences);
                ProcessorPlan procPlan = QueryOptimizer.optimizePlan(subCommand, metadata, idGenerator, capFinder, analysisRecord, context);
                container.getCommand().setProcessorPlan(procPlan);
                if (!correlatedReferences.isEmpty()) {
	                SymbolMap map = new SymbolMap();
	                for (Reference reference : correlatedReferences) {
	    				map.addMapping(reference.getExpression(), reference.getExpression());
	    			}
	                container.getCommand().setCorrelatedReferences(map);
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
    private void distributeDependentHints(Collection<String> groups, PlanNode plan, NodeConstants.Info hintProperty)
        throws QueryMetadataException, TeiidComponentException {
    
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
                	String msg = QueryPlugin.Util.getString("ERR.015.004.0010", groupName); //$NON-NLS-1$
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

    public RuleStack buildRules() {
        RuleStack rules = new RuleStack();

        rules.push(RuleConstants.COLLAPSE_SOURCE);
        
        rules.push(RuleConstants.PLAN_SORTS);
        
        //TODO: update plan sorts to take advantage or semi-join ordering
        if (hints.hasJoin || hints.hasCriteria) {
            rules.push(new RuleMergeCriteria(idGenerator, capFinder, analysisRecord, context, metadata));
        }

        if(hints.hasJoin) {
            rules.push(RuleConstants.IMPLEMENT_JOIN_STRATEGY);
        }
        
        rules.push(RuleConstants.CALCULATE_COST);
        
        rules.push(RuleConstants.ASSIGN_OUTPUT_ELEMENTS);
        
        if (hints.hasLimit) {
            rules.push(RuleConstants.PUSH_LIMIT);
        }
        if (hints.hasRelationalProc) {
            rules.push(RuleConstants.PLAN_PROCEDURES);
        }
        if(hints.hasAggregates) {
            rules.push(RuleConstants.PUSH_AGGREGATES);
        }
        if(hints.hasJoin) {
            rules.push(RuleConstants.CHOOSE_DEPENDENT);
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
        if (hints.hasJoin && hints.hasSetQuery) {
            rules.push(RuleConstants.DECOMPOSE_JOIN);
            rules.push(RuleConstants.MERGE_VIRTUAL);
        }
        if (hints.hasJoin && hints.hasOptionalJoin) {
            rules.push(RuleConstants.REMOVE_OPTIONAL_JOINS);
        }
        if (hints.hasVirtualGroups || (hints.hasJoin && hints.hasOptionalJoin)) {
        	//do initial filtering to make merging and optional join logic easier
            rules.push(RuleConstants.ASSIGN_OUTPUT_ELEMENTS);
        }
        rules.push(RuleConstants.PLACE_ACCESS);
        return rules;
    }

    private PlanNode executeRules(RuleStack rules, PlanNode plan)
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
                analysisRecord.println("\nAFTER: \n" + plan); //$NON-NLS-1$
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
	        if(cmd.getOption().getDependentGroups() != null) {
	            distributeDependentHints(cmd.getOption().getDependentGroups(), result, NodeConstants.Info.MAKE_DEP);
	        }
	        if (cmd.getOption().getNotDependentGroups() != null) {
	            distributeDependentHints(cmd.getOption().getNotDependentGroups(), result, NodeConstants.Info.MAKE_NOT_DEP);
	        }
        }
        this.option = savedOption;
        return result;
	}

	PlanNode createUpdatePlan(Command command) throws TeiidComponentException, TeiidProcessingException {
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
        boolean usingTriggerAction = false;
        if (command instanceof ProcedureContainer) {
        	ProcedureContainer container = (ProcedureContainer)command;
        	usingTriggerAction = addNestedProcedure(sourceNode, container);
        }
        sourceNode.addGroups(groups);

        attachLast(projectNode, sourceNode);

        //for INTO query, attach source and project nodes
        if(!usingTriggerAction && command instanceof Insert){
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

	private boolean addNestedProcedure(PlanNode sourceNode,
			ProcedureContainer container) throws TeiidComponentException,
			QueryMetadataException, TeiidProcessingException {
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
			if (c instanceof TriggerAction) {
				TriggerAction ta = (TriggerAction)c;
				ProcessorPlan plan = new TriggerActionPlanner().optimize(container, ta, idGenerator, metadata, capFinder, analysisRecord, context);
			    sourceNode.setProperty(NodeConstants.Info.PROCESSOR_PLAN, plan);
			    return true;
			}
			if (c.getCacheHint() != null) {
				if (container instanceof StoredProcedure) {
					boolean noCache = isNoCacheGroup(metadata, ((StoredProcedure) container).getProcedureID(), option);
					if (!noCache) {
						if (context.isResultSetCacheEnabled() && container.areResultsCachable() && LobManager.getLobIndexes(new ArrayList<ElementSymbol>(container.getProcedureParameters().keySet())) == null) {
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
			addNestedCommand(sourceNode, container.getGroup(), container, c, false);
		} else if (!container.getGroup().isTempTable() && //we hope for the best, and do a specific validation for subqueries below
				container instanceof TranslatableProcedureContainer //we force the evaluation of procedure params - TODO: inserts are fine except for nonpushdown functions on columns
				&& !CriteriaCapabilityValidatorVisitor.canPushLanguageObject(container, metadata.getModelID(container.getGroup().getMetadataID()), metadata, capFinder, analysisRecord)) {
			//do a workaround of row-by-row processing for update/delete
			if (metadata.getUniqueKeysInGroup(container.getGroup().getMetadataID()).isEmpty() 
					|| !CapabilitiesUtil.supports(Capability.CRITERIA_COMPARE_EQ, metadata.getModelID(container.getGroup().getMetadataID()), metadata, capFinder)) {
				throw new QueryPlannerException(QueryPlugin.Util.getString("RelationalPlanner.nonpushdown_command", container)); //$NON-NLS-1$
			}
			
			//treat this as an update procedure
			if (container instanceof Update) {
				c = QueryRewriter.createUpdateProcedure((Update)container, metadata, context);
			} else {
				c = QueryRewriter.createDeleteProcedure((Delete)container, metadata, context);
			}
			addNestedCommand(sourceNode, container.getGroup(), container, c, false);
			return false;
		}
		
		//plan any subqueries in criteria/parameters/values
		for (SubqueryContainer<?> subqueryContainer : ValueIteratorProviderCollectorVisitor.getValueIteratorProviders(container)) {
			if (c == null && container.getGroup().isTempTable()) {
				if (subqueryContainer.getCommand().getCorrelatedReferences() == null) {
					if (subqueryContainer instanceof ScalarSubquery) {
						((ScalarSubquery) subqueryContainer).setShouldEvaluate(true);
					} else if (subqueryContainer instanceof ExistsCriteria) {
						((ExistsCriteria) subqueryContainer).setShouldEvaluate(true);
					} else {
						throw new QueryPlannerException(QueryPlugin.Util.getString("RelationalPlanner.nonpushdown_command", container)); //$NON-NLS-1$
					}
				} else {
					throw new QueryPlannerException(QueryPlugin.Util.getString("RelationalPlanner.nonpushdown_command", container)); //$NON-NLS-1$
				}
    		}
			ProcessorPlan plan = QueryOptimizer.optimizePlan(subqueryContainer.getCommand(), metadata, null, capFinder, analysisRecord, context);
    		subqueryContainer.getCommand().setProcessorPlan(plan);
    		
    		if (c == null) {
				RuleCollapseSource.prepareSubquery(subqueryContainer);
			}
		}
		return false;
	}

    PlanNode createStoredProcedurePlan(StoredProcedure storedProc) throws QueryMetadataException, TeiidComponentException, TeiidProcessingException {
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
		throws TeiidComponentException, TeiidProcessingException {
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
		throws QueryMetadataException, TeiidComponentException, TeiidProcessingException {

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
    		if(query.hasAggregates()) {
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
     * @throws TeiidComponentException 
     * @throws QueryMetadataException 
     * @throws TeiidProcessingException 
     */
    void buildTree(FromClause clause, PlanNode parent)
        throws QueryMetadataException, TeiidComponentException, TeiidProcessingException {
        
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
            if (group.getModelMetadataId() != null) {
            	node.setProperty(Info.MODEL_ID, group.getModelMetadataId());
            }
            node.addGroup(group);
            if (nestedCommand != null) {
            	UpdateInfo info = ProcedureContainerResolver.getUpdateInfo(group, metadata);
            	if (info != null && info.getPartitionInfo() != null && !info.getPartitionInfo().isEmpty()) {
            		node.setProperty(NodeConstants.Info.PARTITION_INFO, info.getPartitionInfo());
            	}
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
            if (sfc.isTable()) {
    		    sfc.getCommand().setCorrelatedReferences(getCorrelatedReferences(parent, node, sfc));
            }
            node.addGroup(group);
            addNestedCommand(node, group, nestedCommand, nestedCommand, true);
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
            node = NodeFactory.getNewNode(NodeConstants.Types.SOURCE);
            node.setProperty(NodeConstants.Info.TABLE_FUNCTION, tt);
            tt.setCorrelatedReferences(getCorrelatedReferences(parent, node, tt));
            node.addGroup(group);
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
			GroupSymbol group, Command nestedCommand, Command toPlan, boolean merge) throws TeiidComponentException, QueryMetadataException, TeiidProcessingException {
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
		node.setProperty(NodeConstants.Info.NESTED_COMMAND, nestedCommand);

		if (merge && nestedCommand instanceof Query && QueryResolver.isXMLQuery((Query)nestedCommand, metadata)) {
			merge = false;
		}

		if (merge) {
			mergeTempMetadata(nestedCommand, parentCommand);
		    PlanNode childRoot = generatePlan(nestedCommand);
		    node.addFirstChild(childRoot);
			List<SingleElementSymbol> projectCols = nestedCommand.getProjectedSymbols();
			SymbolMap map = SymbolMap.createSymbolMap(group, projectCols, metadata);
			node.setProperty(NodeConstants.Info.SYMBOL_MAP, map);
		} else {
			QueryMetadataInterface actualMetadata = metadata;
			if (actualMetadata instanceof TempMetadataAdapter) {
				actualMetadata = ((TempMetadataAdapter)metadata).getMetadata();
			}
			ProcessorPlan plan = QueryOptimizer.optimizePlan(toPlan, actualMetadata, idGenerator, capFinder, analysisRecord, context);
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
        	if (limit.isImplicit()) {
        		limitNode.setProperty(Info.IS_IMPLICIT_LIMIT, true);
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
    throws QueryMetadataException, TeiidComponentException, TeiidProcessingException {
    	
        QueryNode qnode = null;
        
        Object metadataID = virtualGroup.getMetadataID();
        boolean noCache = isNoCacheGroup(metadata, metadataID, option);
        boolean isMaterializedGroup = metadata.hasMaterialization(metadataID);
        String cacheString = SQLConstants.Reserved.SELECT; 
        String groupName = metadata.getFullName(metadataID);
        
        if( isMaterializedGroup) {
        	Object matMetadataId = metadata.getMaterialization(metadataID);
        	String matTableName = null;
        	CacheHint hint = null;
        	boolean isImplicitGlobal = matMetadataId == null;
            if (isImplicitGlobal) {
        		matTableName = MAT_PREFIX + groupName;
        		matMetadataId = getGlobalTempTableMetadataId(virtualGroup, matTableName, context, metadata, analysisRecord);
        		hint = ((TempMetadataID)matMetadataId).getCacheHint();
            } else {
            	matTableName = metadata.getFullName(matMetadataId);
            }

        	if(noCache){
        		//not use cache
        		qnode = metadata.getVirtualPlan(metadataID);
        		//TODO: update the table for defaultMat
        		recordAnnotation(analysisRecord, Annotation.MATERIALIZED_VIEW, Priority.LOW, "SimpleQueryResolver.materialized_table_not_used", virtualGroup, matTableName); //$NON-NLS-1$
        	}else{
        		qnode = new QueryNode(groupName, null);
        		Query query = createMatViewQuery(matMetadataId, matTableName, Arrays.asList(new AllSymbol()), isImplicitGlobal);
        		query.setCacheHint(hint);
        		qnode.setCommand(query);
                cacheString = "matview"; //$NON-NLS-1$
                recordAnnotation(analysisRecord, Annotation.MATERIALIZED_VIEW, Priority.LOW, "SimpleQueryResolver.Query_was_redirected_to_Mat_table", virtualGroup, matTableName); //$NON-NLS-1$
        	}
        } else {
            // Not a materialized view - query the primary transformation
            qnode = metadata.getVirtualPlan(metadataID);            
        }

        Command result = QueryResolver.resolveView(virtualGroup, qnode, cacheString, metadata);   
        return QueryRewriter.rewrite(result, metadata, context);
    }
    
	public static Query createMatViewQuery(Object matMetadataId, String matTableName, List<? extends SelectSymbol> select, boolean isGlobal) {
		Query query = new Query();
		query.setSelect(new Select(select));
		GroupSymbol gs = new GroupSymbol(matTableName);
		gs.setGlobalTable(isGlobal);
		gs.setMetadataID(matMetadataId);
		query.setFrom(new From(Arrays.asList(new UnaryFromClause(gs))));
		return query;
	}

	public static Object getGlobalTempTableMetadataId(GroupSymbol table, String matTableName, CommandContext context, QueryMetadataInterface metadata, AnalysisRecord analysisRecord)
			throws QueryMetadataException, TeiidComponentException, QueryResolverException, QueryValidatorException {
		TempMetadataStore store = context.getGlobalTableStore().getMetadataStore();
		TempMetadataID id = store.getTempGroupID(matTableName);
		//define the table preserving the primary key
		if (id == null) {
			synchronized (table.getMetadataID()) {
				id = store.getTempGroupID(matTableName);
				if (id == null) {
					//this is really just temporary and will be replaced by the real table
					id = store.addTempGroup(matTableName, ResolverUtil.resolveElementsInGroup(table, metadata), false, true);
					id.setQueryNode(metadata.getVirtualPlan(table.getMetadataID()));
					id.setCardinality(metadata.getCardinality(table.getMetadataID()));
					
					Object pk = metadata.getPrimaryKey(table.getMetadataID());
					if (pk != null) {
						ArrayList<TempMetadataID> primaryKey = resolveIndex(metadata, id, pk);
						id.setPrimaryKey(primaryKey);
					}
					Collection keys = metadata.getUniqueKeysInGroup(table.getMetadataID());
					for (Object key : keys) {
						id.addUniqueKey(resolveIndex(metadata, id, key));
					}
					Collection indexes = metadata.getIndexesInGroup(table.getMetadataID());
					for (Object index : indexes) {
						id.addIndex(resolveIndex(metadata, id, index));
					}
					Command c = QueryResolver.resolveView(table, metadata.getVirtualPlan(table.getMetadataID()), SQLConstants.Reserved.SELECT, metadata);
					CacheHint hint = c.getCacheHint();
					if (hint != null) {
						recordAnnotation(analysisRecord, Annotation.MATERIALIZED_VIEW, Priority.LOW, "SimpleQueryResolver.cache_hint_used", table, matTableName, id.getCacheHint()); //$NON-NLS-1$
					}
					id.setCacheHint(hint);
				}
			}
		} else if (id.getCacheHint() != null) {
			recordAnnotation(analysisRecord, Annotation.MATERIALIZED_VIEW, Priority.LOW, "SimpleQueryResolver.cache_hint_used", table, matTableName, id.getCacheHint()); //$NON-NLS-1$
		}
		return id;
	}

	private static ArrayList<TempMetadataID> resolveIndex(
			QueryMetadataInterface metadata, TempMetadataID id, Object pk)
			throws TeiidComponentException, QueryMetadataException {
		List cols = metadata.getElementIDsInKey(pk);
		ArrayList<TempMetadataID> primaryKey = new ArrayList<TempMetadataID>(cols.size());
		for (Object coldId : cols) {
			int pos = metadata.getPosition(coldId) - 1;
			primaryKey.add(id.getElements().get(pos));
		}
		return primaryKey;
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