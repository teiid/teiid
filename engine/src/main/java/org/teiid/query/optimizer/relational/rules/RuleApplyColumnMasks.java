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

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.teiid.api.exception.query.QueryMetadataException;
import org.teiid.api.exception.query.QueryPlannerException;
import org.teiid.core.TeiidComponentException;
import org.teiid.core.TeiidProcessingException;
import org.teiid.query.analysis.AnalysisRecord;
import org.teiid.query.metadata.QueryMetadataInterface;
import org.teiid.query.optimizer.capabilities.CapabilitiesFinder;
import org.teiid.query.optimizer.relational.ColumnMaskingHelper;
import org.teiid.query.optimizer.relational.OptimizerRule;
import org.teiid.query.optimizer.relational.RuleStack;
import org.teiid.query.optimizer.relational.plantree.NodeConstants;
import org.teiid.query.optimizer.relational.plantree.NodeConstants.Info;
import org.teiid.query.optimizer.relational.plantree.NodeEditor;
import org.teiid.query.optimizer.relational.plantree.PlanNode;
import org.teiid.query.resolver.util.ResolverUtil;
import org.teiid.query.sql.symbol.ElementSymbol;
import org.teiid.query.sql.symbol.Expression;
import org.teiid.query.sql.symbol.GroupSymbol;
import org.teiid.query.util.CommandContext;

public class RuleApplyColumnMasks implements OptimizerRule {

	@Override
	public PlanNode execute(PlanNode plan, QueryMetadataInterface metadata,
			CapabilitiesFinder capabilitiesFinder, RuleStack rules,
			AnalysisRecord analysisRecord, CommandContext context)
			throws QueryPlannerException, QueryMetadataException,
			TeiidComponentException {
		try {
			for (PlanNode sourceNode : NodeEditor.findAllNodes(plan, NodeConstants.Types.SOURCE)) {
				GroupSymbol group = sourceNode.getGroups().iterator().next();
				if (group.isProcedure()) {
					if (sourceNode.getProperty(Info.VIRTUAL_COMMAND) == null) {
						continue; //proc relational, will instead apply at the proc level
					}
					//for procs we can simply replace the project cols
					PlanNode project = NodeEditor.findParent(sourceNode, NodeConstants.Types.PROJECT);
					project.setProperty(NodeConstants.Info.PROJECT_COLS, ColumnMaskingHelper.maskColumns((List<ElementSymbol>) project.getProperty(Info.PROJECT_COLS), group, metadata, context));
					continue;
				}
				//TODO: ideally this should be run after ruleassignoutputelements
		        List<ElementSymbol> cols = ResolverUtil.resolveElementsInGroup(group, metadata);
		        List<? extends Expression> masked = ColumnMaskingHelper.maskColumns(cols, group, metadata, context);
		        Map<ElementSymbol, Expression> mapping = null;
		        for (int i = 0; i < masked.size(); i++) {
		        	Expression maskedCol = masked.get(i);
		        	if (maskedCol instanceof ElementSymbol) {
		        		continue;
		        	}
		        	if (mapping == null) {
		        		mapping = new HashMap<ElementSymbol, Expression>();
		        	}
		        	mapping.put(cols.get(i), maskedCol);
		        }
		        if (mapping != null) {
		        	FrameUtil.convertFrame(sourceNode, group, Collections.singleton(group), mapping, metadata);
		        }
			}
		} catch (TeiidProcessingException e) {
			throw new QueryPlannerException(e);
		}
		
		return plan;
	}

}
