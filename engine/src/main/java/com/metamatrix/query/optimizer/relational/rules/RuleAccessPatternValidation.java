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

package com.metamatrix.query.optimizer.relational.rules;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import com.metamatrix.api.exception.query.QueryPlannerException;
import com.metamatrix.query.analysis.AnalysisRecord;
import com.metamatrix.query.execution.QueryExecPlugin;
import com.metamatrix.query.metadata.QueryMetadataInterface;
import com.metamatrix.query.optimizer.capabilities.CapabilitiesFinder;
import com.metamatrix.query.optimizer.relational.OptimizerRule;
import com.metamatrix.query.optimizer.relational.RuleStack;
import com.metamatrix.query.optimizer.relational.plantree.NodeConstants;
import com.metamatrix.query.optimizer.relational.plantree.PlanNode;
import com.metamatrix.query.sql.lang.Criteria;
import com.metamatrix.query.sql.lang.Delete;
import com.metamatrix.query.sql.lang.Insert;
import com.metamatrix.query.sql.lang.Update;
import com.metamatrix.query.sql.visitor.ElementCollectorVisitor;
import com.metamatrix.query.util.CommandContext;
import com.metamatrix.query.util.ErrorMessageKeys;

/**
 * Validates that the access pattern(s) of a source are satisfied.  This means that,
 * during planning, exactly the required criteria specified by only one (if any)
 * access pattern has been pushed down to the source (in the atomic query).
 * Currently this rule just checks for a node property that was set elsewhere,
 * in the {@link RuleChooseAccessPattern} rule.
 */
public final class RuleAccessPatternValidation implements OptimizerRule {

	/**
     * Verifies
     * @throws QueryPlannerException if an access pattern has not been satisfied
	 * @see com.metamatrix.query.optimizer.OptimizerRule#execute(PlanNode, QueryMetadataInterface, RuleStack)
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
            List crits = Criteria.separateCriteriaByAnd(criteria);
            
            for(Iterator i = crits.iterator(); i.hasNext();) {
                Criteria crit = (Criteria)i.next();
                Collection elements = ElementCollectorVisitor.getElements(crit, true);
                
                if (RulePushSelectCriteria.satisfyAccessPatterns(accessPatterns, elements)) {
                    return;
                }
            }
        }
        
        Object groups = node.getGroups();
        throw new QueryPlannerException(QueryExecPlugin.Util.getString(ErrorMessageKeys.OPTIMIZER_0012, new Object[] {groups, accessPatterns}));
    }
    
	public String toString() {
		return "AccessPatternValidation"; //$NON-NLS-1$
	}

}
