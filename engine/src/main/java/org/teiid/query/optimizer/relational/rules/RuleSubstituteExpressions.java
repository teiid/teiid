/*
 * Copyright Red Hat, Inc. and/or its affiliates
 * and other contributors as indicated by the @author tags and
 * the COPYRIGHT.txt file distributed with this work.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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
