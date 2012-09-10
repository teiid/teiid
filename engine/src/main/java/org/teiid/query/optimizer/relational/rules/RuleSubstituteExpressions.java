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

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.teiid.api.exception.query.QueryMetadataException;
import org.teiid.api.exception.query.QueryPlannerException;
import org.teiid.core.TeiidComponentException;
import org.teiid.query.analysis.AnalysisRecord;
import org.teiid.query.metadata.QueryMetadataInterface;
import org.teiid.query.optimizer.capabilities.CapabilitiesFinder;
import org.teiid.query.optimizer.relational.OptimizerRule;
import org.teiid.query.optimizer.relational.RuleStack;
import org.teiid.query.optimizer.relational.plantree.NodeConstants;
import org.teiid.query.optimizer.relational.plantree.NodeEditor;
import org.teiid.query.optimizer.relational.plantree.PlanNode;
import org.teiid.query.resolver.util.ResolverUtil;
import org.teiid.query.sql.symbol.ElementSymbol;
import org.teiid.query.sql.symbol.Expression;
import org.teiid.query.sql.symbol.GroupSymbol;
import org.teiid.query.sql.util.SymbolMap;
import org.teiid.query.sql.visitor.ExpressionMappingVisitor;
import org.teiid.query.util.CommandContext;

/**
 * Substitutes column references for expressions
 */
public class RuleSubstituteExpressions implements OptimizerRule {
	
	@Override
	public PlanNode execute(PlanNode plan, QueryMetadataInterface metadata,
			CapabilitiesFinder capabilitiesFinder, RuleStack rules,
			AnalysisRecord analysisRecord, CommandContext context)
			throws QueryPlannerException, QueryMetadataException,
			TeiidComponentException {
		boolean substitued = false;
		
        for (PlanNode accessNode : NodeEditor.findAllNodes(plan, NodeConstants.Types.ACCESS)) {
        	if (accessNode.getParent() == null) {
        		continue;
        	}
            for (GroupSymbol gs : accessNode.getGroups()) {
            	Map<Expression, Integer> fbis = metadata.getFunctionBasedExpressions(gs.getMetadataID());
            	if (fbis == null) {
            		continue;
            	}
        		substitued = true;
        		Map<Expression, ElementSymbol> replacements = new HashMap<Expression, ElementSymbol>();
        		GroupSymbol gsOrig = new GroupSymbol(gs.getNonCorrelationName());
        		gsOrig.setMetadataID(gs.getMetadataID());
        		List<ElementSymbol> curremtElems = ResolverUtil.resolveElementsInGroup(gs, metadata);
        		List<ElementSymbol> origElems = ResolverUtil.resolveElementsInGroup(gsOrig, metadata);
        		boolean nameMatches = gs.getNonCorrelationName().equals(gs.getName());
        		SymbolMap map = SymbolMap.createSymbolMap(origElems, curremtElems);
        		for (Map.Entry<Expression, Integer> entry : fbis.entrySet()) {
        			Expression ex = entry.getKey();
        			if (!nameMatches) {
        				ex = (Expression)ex.clone();
        				ExpressionMappingVisitor.mapExpressions(ex, map.asMap());
        			}
        			replacements.put(ex, curremtElems.get(entry.getValue()-1));
				}
        		FrameUtil.convertFrame(accessNode.getParent(), gs, null, replacements, metadata);
            }
        }
        if (substitued) {
        	rules.push(RuleConstants.PUSH_SELECT_CRITERIA);
        }
        return plan;
	}
	
	@Override
	public String toString() {
		return "Substitue Expressions"; //$NON-NLS-1$
	}
}
