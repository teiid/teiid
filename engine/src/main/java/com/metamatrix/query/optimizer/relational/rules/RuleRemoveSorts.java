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

import com.metamatrix.query.analysis.AnalysisRecord;
import com.metamatrix.query.metadata.QueryMetadataInterface;
import com.metamatrix.query.optimizer.capabilities.CapabilitiesFinder;
import com.metamatrix.query.optimizer.relational.OptimizerRule;
import com.metamatrix.query.optimizer.relational.RuleStack;
import com.metamatrix.query.optimizer.relational.plantree.NodeConstants;
import com.metamatrix.query.optimizer.relational.plantree.NodeEditor;
import com.metamatrix.query.optimizer.relational.plantree.PlanNode;
import com.metamatrix.query.util.CommandContext;

/**
 * Removes all nested sorts that are not under a limit.  These sorts are considered unnecessary
 * since inline views and virtual layers represent unordered tuple sets. 
 */
public final class RuleRemoveSorts implements OptimizerRule {

	public RuleRemoveSorts() {
	}

	public PlanNode execute(PlanNode plan, QueryMetadataInterface metadata, CapabilitiesFinder capFinder, RuleStack rules, AnalysisRecord analysisRecord, CommandContext context)  {
		
		for (PlanNode node : NodeEditor.findAllNodes(plan, NodeConstants.Types.SORT)) {
			PlanNode parent = node.getParent();
			
			if(parent == null) { 
	            continue;
	        }
			
            if(parent.getType() != NodeConstants.Types.TUPLE_LIMIT) { 
                // Remove sort node
                NodeEditor.removeChildNode(parent, node);
            }
        }        
						
		return plan;		
	}
    
	public String toString() {
		return "RemoveSorts"; //$NON-NLS-1$
	}
	
}
