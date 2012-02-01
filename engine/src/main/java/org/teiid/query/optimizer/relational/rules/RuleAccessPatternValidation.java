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

import java.util.Collection;
import java.util.List;

import org.teiid.api.exception.query.QueryPlannerException;
import org.teiid.query.QueryPlugin;
import org.teiid.query.analysis.AnalysisRecord;
import org.teiid.query.metadata.QueryMetadataInterface;
import org.teiid.query.optimizer.capabilities.CapabilitiesFinder;
import org.teiid.query.optimizer.relational.OptimizerRule;
import org.teiid.query.optimizer.relational.RuleStack;
import org.teiid.query.optimizer.relational.plantree.NodeConstants;
import org.teiid.query.optimizer.relational.plantree.PlanNode;
import org.teiid.query.sql.lang.Criteria;
import org.teiid.query.sql.lang.Delete;
import org.teiid.query.sql.lang.Insert;
import org.teiid.query.sql.lang.Update;
import org.teiid.query.sql.symbol.ElementSymbol;
import org.teiid.query.sql.visitor.ElementCollectorVisitor;
import org.teiid.query.util.CommandContext;


/**
 * Validates that the access pattern(s) of a source are satisfied.  This means that,
 * during planning, exactly the required criteria specified by only one (if any)
 * access pattern has been pushed down to the source (in the atomic query).
 * Currently this rule just checks for a node property that was set elsewhere,
 * in the {@link RuleChooseAccessPattern} rule.
 */
public final class RuleAccessPatternValidation implements OptimizerRule {

	/**
     * @throws QueryPlannerException if an access pattern has not been satisfied
	 */
	public PlanNode execute(
		PlanNode plan,
		QueryMetadataInterface metadata,
        CapabilitiesFinder capFinder,
		RuleStack rules, AnalysisRecord analysisRecord, CommandContext context)
		throws
			QueryPlannerException {
        
        validateAccessPatterns(plan, metadata, capFinder);

		return plan;
	}

    void validateAccessPatterns(PlanNode node, QueryMetadataInterface metadata, CapabilitiesFinder capFinder)
    throws QueryPlannerException {
     
        validateAccessPatterns(node);
        
        for (PlanNode child : node.getChildren()) {
            validateAccessPatterns(child, metadata, capFinder);
		}
    }

    /** 
     * @param node
     * @throws QueryPlannerException
     */
    private void validateAccessPatterns(PlanNode node) throws QueryPlannerException {
        if (!node.hasCollectionProperty(NodeConstants.Info.ACCESS_PATTERNS)) {
            return;
        }
        Criteria criteria = null;
        if(node.hasProperty(NodeConstants.Info.ATOMIC_REQUEST) ) {
            Object req = node.getProperty(NodeConstants.Info.ATOMIC_REQUEST);
            if(req instanceof Insert) {
                return;
            }
            if(req instanceof Delete) {
                criteria = ((Delete)req).getCriteria();
            } else if (req instanceof Update) {
                criteria = ((Update)req).getCriteria();
            }
        }
        
        List accessPatterns = (List)node.getProperty(NodeConstants.Info.ACCESS_PATTERNS);
        
        if (criteria != null) {
            for(Criteria crit : Criteria.separateCriteriaByAnd(criteria)) {
                Collection<ElementSymbol> elements = ElementCollectorVisitor.getElements(crit, true);
                
                if (RulePushSelectCriteria.satisfyAccessPatterns(accessPatterns, elements)) {
                    return;
                }
            }
        }
        
        Object groups = node.getGroups();
         throw new QueryPlannerException(QueryPlugin.Event.TEIID30278, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID30278, new Object[] {groups, accessPatterns}));
    }
    
	public String toString() {
		return "AccessPatternValidation"; //$NON-NLS-1$
	}

}
