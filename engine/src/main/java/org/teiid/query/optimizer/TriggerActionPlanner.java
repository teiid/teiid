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

package org.teiid.query.optimizer;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.teiid.api.exception.query.QueryMetadataException;
import org.teiid.api.exception.query.QueryPlannerException;
import org.teiid.api.exception.query.QueryResolverException;
import org.teiid.core.TeiidComponentException;
import org.teiid.core.TeiidProcessingException;
import org.teiid.core.id.IDGenerator;
import org.teiid.language.SQLConstants;
import org.teiid.query.analysis.AnalysisRecord;
import org.teiid.query.metadata.QueryMetadataInterface;
import org.teiid.query.optimizer.capabilities.CapabilitiesFinder;
import org.teiid.query.optimizer.relational.rules.RuleChooseJoinStrategy;
import org.teiid.query.optimizer.relational.rules.RuleMergeCriteria;
import org.teiid.query.processor.ProcessorPlan;
import org.teiid.query.processor.proc.ForEachRowPlan;
import org.teiid.query.processor.proc.ProcedurePlan;
import org.teiid.query.processor.relational.RelationalNode;
import org.teiid.query.resolver.QueryResolver;
import org.teiid.query.resolver.util.ResolverUtil;
import org.teiid.query.rewriter.QueryRewriter;
import org.teiid.query.sql.LanguageObject;
import org.teiid.query.sql.lang.*;
import org.teiid.query.sql.navigator.DeepPostOrderNavigator;
import org.teiid.query.sql.proc.CommandStatement;
import org.teiid.query.sql.proc.CreateProcedureCommand;
import org.teiid.query.sql.proc.Statement;
import org.teiid.query.sql.proc.TriggerAction;
import org.teiid.query.sql.symbol.ElementSymbol;
import org.teiid.query.sql.symbol.Expression;
import org.teiid.query.sql.symbol.GroupSymbol;
import org.teiid.query.sql.util.SymbolMap;
import org.teiid.query.sql.visitor.ExpressionMappingVisitor;
import org.teiid.query.sql.visitor.GroupCollectorVisitor;
import org.teiid.query.util.CommandContext;


public final class TriggerActionPlanner {
	
	public ProcessorPlan optimize(ProcedureContainer userCommand, TriggerAction ta, IDGenerator idGenerator, QueryMetadataInterface metadata, CapabilitiesFinder capFinder, AnalysisRecord analysisRecord, CommandContext context)
	throws QueryMetadataException, TeiidComponentException, QueryResolverException, TeiidProcessingException {
		//TODO consider caching the plans without using the changing vars
		QueryRewriter.rewrite(ta, metadata, context, QueryResolver.getVariableValues(userCommand, true, metadata));
		
		QueryCommand query = null;
		Map<ElementSymbol, Expression> params = new HashMap<ElementSymbol, Expression>();
		
		Map<ElementSymbol, Expression> mapping = QueryResolver.getVariableValues(userCommand, false, metadata);
		if (userCommand instanceof Insert) {
			Insert insert = (Insert)userCommand;
			if (insert.getQueryExpression() != null) {
				query = insert.getQueryExpression();
			} else {
				query = new Query();
				((Query)query).setSelect(new Select(RuleChooseJoinStrategy.createExpressionSymbols(insert.getValues())));
			}
			ProcessorPlan plan = rewritePlan(ta, idGenerator, metadata, capFinder, analysisRecord,
					context, query, mapping, insert);
			if (plan != null) {
				return plan;
			}
		} else if (userCommand instanceof Delete) {
			query = createOldQuery(userCommand, ta, metadata, params);
		} else if (userCommand instanceof Update) {
			query = createOldQuery(userCommand, ta, metadata, params);
		} else {
			throw new AssertionError();
		}
		for (Map.Entry<ElementSymbol, Expression> entry : mapping.entrySet()) {
			Expression value = entry.getValue();
			params.put(entry.getKey(), value);
			if (entry.getKey().getGroupSymbol().getShortName().equalsIgnoreCase(SQLConstants.Reserved.NEW) && userCommand instanceof Update) {
				((Query)query).getSelect().addSymbol(value);
			}
		}
		ForEachRowPlan result = new ForEachRowPlan();
		result.setParams(params);
		ProcessorPlan queryPlan = QueryOptimizer.optimizePlan(query, metadata, idGenerator, capFinder, analysisRecord, context);
		result.setQueryPlan(queryPlan);
		result.setLookupMap(RelationalNode.createLookupMap(query.getProjectedSymbols()));
		CreateProcedureCommand command = new CreateProcedureCommand(ta.getBlock());
		command.setVirtualGroup(ta.getView());
		command.setUpdateType(userCommand.getType());
		ProcedurePlan rowProcedure = (ProcedurePlan)QueryOptimizer.optimizePlan(command, metadata, idGenerator, capFinder, analysisRecord, context);
		rowProcedure.setRunInContext(false);
		result.setRowProcedure(rowProcedure);
		return result;
	}

	/**
	 * look for the simple case of a mapping to a single insert statement trigger action - and reconstruct the plan as a single insert
	 * TODO: need internal primitives for delete/update batching in a loop for delete/update cases
	 */
	private ProcessorPlan rewritePlan(TriggerAction ta, IDGenerator idGenerator,
			QueryMetadataInterface metadata, CapabilitiesFinder capFinder,
			AnalysisRecord analysisRecord, CommandContext context,
			QueryCommand query, Map<ElementSymbol, Expression> mapping,
			Insert insert) throws QueryMetadataException,
			QueryResolverException, TeiidComponentException,
			QueryPlannerException {
		if (ta.getBlock().getStatements().size() != 1) {
			return null;
		}
		Statement s = ta.getBlock().getStatements().get(0);
		if (!(s instanceof CommandStatement)) {
			return null;
		}
		CommandStatement cs = (CommandStatement)s;
		if (!(cs.getCommand() instanceof Insert)) {
			return null;
		}
		Insert mapped = (Insert) cs.getCommand();
		if (mapped.getQueryExpression() != null) {
			return null;
		}
		if (insert.getQueryExpression() != null) {
			//use a unique inline view name to make the final remapping easier
			GroupSymbol group = new GroupSymbol("X");
			Collection<GroupSymbol> groups = GroupCollectorVisitor.getGroups(query, true);
			for (int i = 0; groups.contains(group); i++) {
				group.setName("X_" + i);
			}
			
			List<Expression> projectedSymbols = query.getProjectedSymbols();
			Query queryExpression = QueryRewriter.createInlineViewQuery(group, query, metadata, projectedSymbols);
			List<Expression> viewSymbols = new ArrayList<Expression>(queryExpression.getSelect().getSymbols());
			
			//switch to the values
			queryExpression.getSelect().clearSymbols();
			List<Expression> values = mapped.getValues();
			queryExpression.getSelect().addSymbols(values);
			values.clear();
			//map to the query form - changes references back to element form
			SymbolMap queryMapping = new SymbolMap();
			queryMapping.asUpdatableMap().putAll(mapping);
			ExpressionMappingVisitor visitor = new RuleMergeCriteria.ReferenceReplacementVisitor(queryMapping);
			DeepPostOrderNavigator.doVisit(queryExpression.getSelect(), visitor);
			//map to the inline view
			Map<Expression, Expression> viewMapping = new HashMap<Expression, Expression>();
			for (int i = 0; i < projectedSymbols.size(); i++) {
				viewMapping.put(SymbolMap.getExpression(projectedSymbols.get(i)), SymbolMap.getExpression(viewSymbols.get(i)));
			}
			ExpressionMappingVisitor.mapExpressions(queryExpression.getSelect(), viewMapping);
			
			//now we can return a plan based off a single insert statement
			mapped.setQueryExpression(queryExpression);
			return QueryOptimizer.optimizePlan(mapped, metadata, idGenerator, capFinder, analysisRecord, context);
		}
		List<Expression> values = mapped.getValues();
		SymbolMap queryMapping = new SymbolMap();
		queryMapping.asUpdatableMap().putAll(mapping);
		ExpressionMappingVisitor visitor = new RuleMergeCriteria.ReferenceReplacementVisitor(queryMapping);
		Select select = new Select();
		select.addSymbols(values);
		DeepPostOrderNavigator.doVisit(select, visitor);
		values.clear();
		for (Expression ex : select.getSymbols()) {
			values.add(SymbolMap.getExpression(ex));
		}
		return QueryOptimizer.optimizePlan(mapped, metadata, idGenerator, capFinder, analysisRecord, context);
	}

	private QueryCommand createOldQuery(ProcedureContainer userCommand,
			TriggerAction ta, QueryMetadataInterface metadata,
			Map<ElementSymbol, Expression> params)
			throws QueryMetadataException, TeiidComponentException {
		List<ElementSymbol> allSymbols = ResolverUtil.resolveElementsInGroup(ta.getView(), metadata);
		GroupSymbol old = new GroupSymbol(SQLConstants.Reserved.OLD);
		GroupSymbol newGroup = new GroupSymbol(SQLConstants.Reserved.NEW);
		for (ElementSymbol elementSymbol : allSymbols) {
			ElementSymbol es = elementSymbol.clone();
			es.setGroupSymbol(old);
			params.put(es, elementSymbol);
			if (userCommand instanceof Update) {
				//default to old
				es = elementSymbol.clone();
				es.setGroupSymbol(newGroup);
				params.put(es, elementSymbol);
			}
		}
		ArrayList<Expression> selectSymbols = new ArrayList<Expression>(LanguageObject.Util.deepClone(allSymbols, ElementSymbol.class));
		QueryCommand query = new Query(new Select(selectSymbols), new From(Arrays.asList(new UnaryFromClause(ta.getView()))), ((FilteredCommand)userCommand).getCriteria(), null, null);
		return query;
	}
        
}
