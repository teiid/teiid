package org.teiid.query.optimizer.relational;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.teiid.adminapi.DataPolicy;
import org.teiid.adminapi.impl.DataPolicyMetadata;
import org.teiid.adminapi.impl.DataPolicyMetadata.PermissionMetaData;
import org.teiid.api.exception.query.ExpressionEvaluationException;
import org.teiid.api.exception.query.QueryMetadataException;
import org.teiid.api.exception.query.QueryPlannerException;
import org.teiid.api.exception.query.QueryProcessingException;
import org.teiid.api.exception.query.QueryResolverException;
import org.teiid.common.buffer.BlockedException;
import org.teiid.core.TeiidComponentException;
import org.teiid.core.TeiidProcessingException;
import org.teiid.metadata.FunctionMethod.Determinism;
import org.teiid.query.QueryPlugin;
import org.teiid.query.eval.Evaluator;
import org.teiid.query.metadata.QueryMetadataInterface;
import org.teiid.query.parser.QueryParser;
import org.teiid.query.resolver.util.ResolverUtil;
import org.teiid.query.resolver.util.ResolverVisitor;
import org.teiid.query.rewriter.QueryRewriter;
import org.teiid.query.sql.lang.BatchedUpdateCommand;
import org.teiid.query.sql.lang.Command;
import org.teiid.query.sql.lang.CompoundCriteria;
import org.teiid.query.sql.lang.Criteria;
import org.teiid.query.sql.lang.FilteredCommand;
import org.teiid.query.sql.lang.Insert;
import org.teiid.query.sql.lang.ProcedureContainer;
import org.teiid.query.sql.lang.SetClause;
import org.teiid.query.sql.lang.Update;
import org.teiid.query.sql.navigator.PreOrPostOrderNavigator;
import org.teiid.query.sql.symbol.Constant;
import org.teiid.query.sql.symbol.ElementSymbol;
import org.teiid.query.sql.symbol.Expression;
import org.teiid.query.sql.symbol.GroupSymbol;
import org.teiid.query.sql.visitor.EvaluatableVisitor;
import org.teiid.query.sql.visitor.ExpressionMappingVisitor;
import org.teiid.query.util.CommandContext;

public class RowBasedSecurityHelper {
	
    private static final String FILTER_KEY = "filter"; //$NON-NLS-1$
	
	public static Criteria getRowBasedFilters(QueryMetadataInterface metadata,
			final GroupSymbol group, CommandContext cc)
			throws QueryMetadataException, TeiidComponentException, TeiidProcessingException {
		ArrayList<Criteria> crits = new ArrayList<Criteria>(2);
		Map<String, DataPolicy> policies = cc.getAllowedDataPolicies();
		if (policies == null || policies.isEmpty()) {
			return null;
		}
		Criteria filter = (Criteria)metadata.getFromMetadataCache(group.getMetadataID(), FILTER_KEY);
		if (filter == null) {
			for (Map.Entry<String, DataPolicy> entry : policies.entrySet()) {
				DataPolicyMetadata dpm = (DataPolicyMetadata)entry.getValue();
				Object metadataID = group.getMetadataID();
				String fullName = metadata.getFullName(metadataID);
				PermissionMetaData pmd = dpm.getPermissionMap().get(fullName);
				if (pmd == null) {
					continue;
				}
				String filterString = pmd.getCondition();
				try {
					Criteria c = QueryParser.getQueryParser().parseCriteria(filterString);
					GroupSymbol gs = group;
					if (group.getDefinition() != null) {
						gs = new GroupSymbol(fullName);
						gs.setMetadataID(metadataID);
					}
					ResolverVisitor.resolveLanguageObject(c, Arrays.asList(gs), metadata);
					crits.add(c);
				} catch (QueryProcessingException e) {
					throw new QueryMetadataException(QueryPlugin.Event.TEIID31129, e, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID31129, entry.getKey(), fullName));
				}
			}
			if (crits.isEmpty()) {
				metadata.addToMetadataCache(group.getMetadataID(), FILTER_KEY, QueryRewriter.TRUE_CRITERIA);
				return null;
			}
			if (crits.size() == 1) {
				filter = crits.get(0);
			} else {
				filter = new CompoundCriteria(CompoundCriteria.AND, crits);
			}
			metadata.addToMetadataCache(group.getMetadataID(), FILTER_KEY, filter);
		} else if (filter.equals(QueryRewriter.TRUE_CRITERIA)) {
			//we treat this as user deterministic since the data roles won't change.  this may change if the logic becomes dynamic
			cc.setDeterminismLevel(Determinism.USER_DETERMINISTIC);  
			return null;
		}
		filter = (Criteria)filter.clone();
		if (group.getDefinition() != null) {
			ExpressionMappingVisitor emv = new ExpressionMappingVisitor(null) {
				@Override
				public Expression replaceExpression(
						Expression element) {
					if (element instanceof ElementSymbol) {
						ElementSymbol es = (ElementSymbol)element;
						if (es.getGroupSymbol().getDefinition() == null && es.getGroupSymbol().getName().equalsIgnoreCase(group.getDefinition())) {
							es.getGroupSymbol().setDefinition(group.getDefinition());
							es.getGroupSymbol().setName(group.getName());            						}
					}
					return element;
				}
			};
	        PreOrPostOrderNavigator.doVisit(filter, emv, PreOrPostOrderNavigator.PRE_ORDER, true);
		}
		//we treat this as user deterministic since the data roles won't change.  this may change if the logic becomes dynamic 
		cc.setDeterminismLevel(Determinism.USER_DETERMINISTIC);  
		QueryRewriter.rewriteCriteria(filter, cc, metadata);
		return filter;
	}

	public static Command checkUpdateRowBasedFilters(ProcedureContainer container, Command procedure, RelationalPlanner planner)
			throws QueryMetadataException, TeiidComponentException,
			TeiidProcessingException, QueryResolverException {
		Criteria filter = RowBasedSecurityHelper.getRowBasedFilters(planner.metadata, container.getGroup(), planner.context);
		if (filter == null) {
			return procedure;
		}

    	//we won't enforce on the update side through a virtual
    	if (procedure != null) {
        	addFilter(container, planner, filter); 
    		return procedure;
    	}
    	
		//TODO: alter the compensation logic in RelationalPlanner to produce an row-by-row check for insert/update
    	//check constraints
		Map<ElementSymbol, Expression> values = null;
		boolean compensate = false;
		if (container instanceof Update) {
			//check the change set against the filter
			values = new HashMap<ElementSymbol, Expression>();
			Update update = (Update)container;
			for (SetClause clause : update.getChangeList().getClauses()) {
				//TODO: do this is a single eval pass
				if (EvaluatableVisitor.isFullyEvaluatable(clause.getValue(), true)) {
					values.put(clause.getSymbol(), clause.getValue());
				} else if (!compensate && !EvaluatableVisitor.isFullyEvaluatable(clause.getValue(), false)) {
					compensate = true;
				}
			}
		} else if (container instanceof Insert) {
			Insert insert = (Insert)container;

			if (insert.getQueryExpression() == null) {
				values = new HashMap<ElementSymbol, Expression>();
				
    			Collection<ElementSymbol> insertElmnts = ResolverUtil.resolveElementsInGroup(insert.getGroup(), planner.metadata);

    			for (ElementSymbol elementSymbol : insertElmnts) {
    				Expression value = null;
    				int index = insert.getVariables().indexOf(elementSymbol);
    				if (index == -1) {
    					value = ResolverUtil.getDefault(elementSymbol, planner.metadata);
            			values.put(elementSymbol, value);
    				} else {
    					value = (Expression) insert.getValues().get(index);
    					if (EvaluatableVisitor.isFullyEvaluatable(value, true)) {
    						values.put(elementSymbol, value);
        				}
    				}
				}
			} else {
				insert.setConstraint(filter);
			}
		}
		if (values != null) {
			if (!values.isEmpty()) {
				ExpressionMappingVisitor.mapExpressions(filter, values);
				filter = QueryRewriter.rewriteCriteria(filter, planner.context, planner.metadata);
			}
			if (filter != QueryRewriter.TRUE_CRITERIA) {
				if (filter == QueryRewriter.FALSE_CRITERIA || filter == QueryRewriter.UNKNOWN_CRITERIA) {
					throw new TeiidProcessingException(QueryPlugin.Event.TEIID31130, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID31130, container));
				} 
				
				if (container instanceof Update) {
					if (compensate) {
						addFilter(container, planner, filter);
						try {
							planner.validateRowProcessing(container);
						} catch (QueryPlannerException e) {
							throw new TeiidProcessingException(QueryPlugin.Event.TEIID31131, e, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID31131, container));
						}
						return QueryRewriter.createUpdateProcedure((Update)container, planner.metadata, planner.context); 
					}
					((Update)container).setConstraint(filter);
				} else if (container instanceof Insert) {
					((Insert)container).setConstraint(filter);
				}
			} 
		} else {
			addFilter(container, planner, filter);
		}
		return procedure;
	}

	private static void addFilter(ProcedureContainer container,
			RelationalPlanner planner, Criteria filter) {
		if (container instanceof FilteredCommand) {
    		FilteredCommand fc = (FilteredCommand)container;
    		if (fc.getCriteria() == null) {
    			fc.setCriteria(filter);
    		} else {
        		fc.setCriteria(QueryRewriter.optimizeCriteria(new CompoundCriteria(Arrays.asList(fc.getCriteria(), filter)), planner.metadata));
    		}
    	}
	}
	
	public static void checkConstraints(Command atomicCommand, Evaluator eval)
			throws ExpressionEvaluationException, BlockedException,
			TeiidComponentException, QueryProcessingException {
		Criteria constraint = null;
		HashMap<ElementSymbol, Expression> values = null;
		int rows = -1;
		if (atomicCommand instanceof Update) {
			Update update = (Update)atomicCommand;
			constraint = update.getConstraint();
			if (constraint != null) {
				values = new HashMap<ElementSymbol, Expression>();
				for (SetClause clause : update.getChangeList().getClauses()) {
					values.put(clause.getSymbol(), clause.getValue());
					if (rows == -1) {
						rows = getMultiValuedSize(clause.getValue());
					}
				}
			}
		} else if (atomicCommand instanceof Insert) {
			Insert insert = (Insert)atomicCommand;
			constraint = insert.getConstraint();
			if (constraint != null) {
				values = new HashMap<ElementSymbol, Expression>();

				if (insert.getQueryExpression() == null) {
	    			for (int i = 0; i < insert.getVariables().size(); i++) {
	    				ElementSymbol symbol = insert.getVariables().get(i);
	    				Expression value = (Expression) insert.getValues().get(i);
	    				values.put(symbol, value);
	    				if (rows == -1) {
							rows = getMultiValuedSize(value);
						}
					}
				}
			}
		} else if (atomicCommand instanceof BatchedUpdateCommand) {
			BatchedUpdateCommand buc = (BatchedUpdateCommand)atomicCommand;
			List<Command> commands = buc.getUpdateCommands();
			for (Command command : commands) {
				checkConstraints(command, eval);
			}
			return;
		}
		if (constraint == null) {
			return;
		}
		if (!EvaluatableVisitor.isFullyEvaluatable(constraint, false)) {
			throw new QueryProcessingException(QueryPlugin.Event.TEIID31130, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID31130, atomicCommand));
		}
		if (rows != -1) {
			Map<ElementSymbol, Expression> currentValues = new HashMap<ElementSymbol, Expression>();
			for (int i = 0; i < rows; i++) {
				currentValues.clear();
				for (Map.Entry<ElementSymbol, Expression> entry : values.entrySet()) {
					ElementSymbol symbol = entry.getKey();
					Expression value = entry.getValue();
					if (value instanceof Constant && ((Constant)value).isMultiValued()) {
						Object obj =  ((List<?>)((Constant)value).getValue()).get(i);
						value = new Constant(obj, symbol.getType());
					}
					currentValues.put(symbol, value);
				}
				evaluateConstraint(atomicCommand, eval, constraint, currentValues);
			}
		} else {
			evaluateConstraint(atomicCommand, eval, constraint, values);
		}
	}

	private static void evaluateConstraint(Command atomicCommand,
			Evaluator eval, Criteria constraint,
			Map<ElementSymbol, Expression> values)
			throws ExpressionEvaluationException, BlockedException,
			TeiidComponentException, QueryProcessingException {
		Criteria clone = (Criteria) constraint.clone();
		ExpressionMappingVisitor.mapExpressions(clone, values);
		if (!eval.evaluate(clone, null)) {
			throw new QueryProcessingException(QueryPlugin.Event.TEIID31130, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID31130, atomicCommand));
		}
	}

	private static int getMultiValuedSize(Expression value) {
		if (value instanceof Constant && ((Constant)value).isMultiValued()) {
			return ((List<?>)((Constant)value).getValue()).size();
		}
		return 1;
	}

}
