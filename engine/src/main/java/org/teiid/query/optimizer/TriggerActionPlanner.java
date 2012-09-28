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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.teiid.api.exception.query.QueryMetadataException;
import org.teiid.api.exception.query.QueryResolverException;
import org.teiid.core.TeiidComponentException;
import org.teiid.core.TeiidProcessingException;
import org.teiid.core.id.IDGenerator;
import org.teiid.language.SQLConstants;
import org.teiid.query.analysis.AnalysisRecord;
import org.teiid.query.metadata.QueryMetadataInterface;
import org.teiid.query.optimizer.capabilities.CapabilitiesFinder;
import org.teiid.query.optimizer.relational.rules.RuleChooseJoinStrategy;
import org.teiid.query.processor.ProcessorPlan;
import org.teiid.query.processor.proc.ForEachRowPlan;
import org.teiid.query.processor.proc.ProcedurePlan;
import org.teiid.query.processor.relational.RelationalNode;
import org.teiid.query.resolver.QueryResolver;
import org.teiid.query.resolver.util.ResolverUtil;
import org.teiid.query.rewriter.QueryRewriter;
import org.teiid.query.sql.LanguageObject;
import org.teiid.query.sql.lang.*;
import org.teiid.query.sql.proc.CreateProcedureCommand;
import org.teiid.query.sql.proc.TriggerAction;
import org.teiid.query.sql.symbol.ElementSymbol;
import org.teiid.query.sql.symbol.Expression;
import org.teiid.query.sql.symbol.GroupSymbol;
import org.teiid.query.util.CommandContext;


public final class TriggerActionPlanner {
	
	public ProcessorPlan optimize(ProcedureContainer userCommand, TriggerAction ta, IDGenerator idGenerator, QueryMetadataInterface metadata, CapabilitiesFinder capFinder, AnalysisRecord analysisRecord, CommandContext context)
	throws QueryMetadataException, TeiidComponentException, QueryResolverException, TeiidProcessingException {
		//TODO consider caching the plans without using the changing vars
		QueryRewriter.rewrite(ta, metadata, context, QueryResolver.getVariableValues(userCommand, true, metadata));
		
		QueryCommand query = null;
		Map<ElementSymbol, Expression> params = new HashMap<ElementSymbol, Expression>();
		
		if (userCommand instanceof Insert) {
			Insert insert = (Insert)userCommand;
			if (insert.getQueryExpression() != null) {
				query = insert.getQueryExpression();
			} else {
				query = new Query();
				((Query)query).setSelect(new Select(RuleChooseJoinStrategy.createExpressionSymbols(insert.getValues())));
			}
		} else if (userCommand instanceof Delete) {
			query = createOldQuery(userCommand, ta, metadata, params);
		} else if (userCommand instanceof Update) {
			query = createOldQuery(userCommand, ta, metadata, params);
		} else {
			throw new AssertionError();
		}
		
		for (Map.Entry<ElementSymbol, Expression> entry : QueryResolver.getVariableValues(userCommand, false, metadata).entrySet()) {
			if (entry.getKey().getGroupSymbol().getShortName().equalsIgnoreCase(SQLConstants.Reserved.NEW)) {
				Expression value = entry.getValue();
				params.put(entry.getKey(), value);
				if (userCommand instanceof Update) {
					((Query)query).getSelect().addSymbol(value);
				}
			} else {
				params.put(entry.getKey(), entry.getValue()); 
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
		result.setRowProcedure(rowProcedure);
		return result;
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
		QueryCommand query = new Query(new Select(selectSymbols), new From(Arrays.asList(new UnaryFromClause(ta.getView()))), ((TranslatableProcedureContainer)userCommand).getCriteria(), null, null);
		return query;
	}
        
}
