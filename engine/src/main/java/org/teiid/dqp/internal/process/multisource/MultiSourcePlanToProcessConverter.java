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

package org.teiid.dqp.internal.process.multisource;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import com.metamatrix.api.exception.MetaMatrixComponentException;
import com.metamatrix.api.exception.query.CriteriaEvaluationException;
import com.metamatrix.api.exception.query.QueryPlannerException;
import com.metamatrix.api.exception.query.QueryValidatorException;
import com.metamatrix.core.id.IDGenerator;
import com.metamatrix.dqp.service.VDBService;
import com.metamatrix.query.analysis.AnalysisRecord;
import com.metamatrix.query.metadata.QueryMetadataInterface;
import com.metamatrix.query.optimizer.capabilities.CapabilitiesFinder;
import com.metamatrix.query.optimizer.relational.PlanToProcessConverter;
import com.metamatrix.query.optimizer.relational.plantree.PlanNode;
import com.metamatrix.query.processor.relational.AccessNode;
import com.metamatrix.query.processor.relational.NullNode;
import com.metamatrix.query.processor.relational.RelationalNode;
import com.metamatrix.query.processor.relational.RelationalNodeUtil;
import com.metamatrix.query.processor.relational.UnionAllNode;
import com.metamatrix.query.rewriter.QueryRewriter;
import com.metamatrix.query.sql.lang.Command;
import com.metamatrix.query.sql.navigator.DeepPreOrderNavigator;

public class MultiSourcePlanToProcessConverter extends PlanToProcessConverter {
	
	private Set<String> multiSourceModels;
	private String vdbName;
	private String vdbVersion;
	private VDBService vdbService;
	
	public MultiSourcePlanToProcessConverter(QueryMetadataInterface metadata,
			IDGenerator idGenerator, AnalysisRecord analysisRecord,
			CapabilitiesFinder capFinder, Set<String> multiSourceModels,
			String vdbName, VDBService vdbService, String vdbVersion) {
		super(metadata, idGenerator, analysisRecord, capFinder);
		this.multiSourceModels = multiSourceModels;
		this.vdbName = vdbName;
		this.vdbService = vdbService;
		this.vdbVersion = vdbVersion;
	}

	protected RelationalNode convertNode(PlanNode planNode) throws QueryPlannerException, MetaMatrixComponentException {
		RelationalNode node = super.convertNode(planNode);
		
		if (node instanceof AccessNode) {
			try {
				return multiSourceModify((AccessNode)node);
			} catch (CriteriaEvaluationException e) {
				throw new QueryPlannerException(e, e.getMessage());
			} 
		}
		
		return node;
	}
	
	private RelationalNode multiSourceModify(AccessNode accessNode) throws MetaMatrixComponentException, CriteriaEvaluationException {
        String modelName = accessNode.getModelName();

		if(!this.multiSourceModels.contains(modelName)) {
            return accessNode;
        }
        List bindings = vdbService.getConnectorBindingNames(vdbName, vdbVersion, modelName);
        List<AccessNode> accessNodes = new ArrayList<AccessNode>(bindings.size());
        Iterator bindingIter = bindings.iterator();
        
        while(bindingIter.hasNext()) {
            String bindingUUID = (String) bindingIter.next();
            String bindingName = vdbService.getConnectorName(bindingUUID);
            
            // Create a new cloned version of the access node and set it's model name to be the bindingUUID
            AccessNode instanceNode = (AccessNode) accessNode.clone();
            instanceNode.setID(getID());
            instanceNode.setConnectorBindingId(bindingUUID);
            
            // Modify the command to pull the instance column and evaluate the criteria
            Command command = (Command)instanceNode.getCommand().clone();
            
            // Replace all multi-source elements with the source name
            DeepPreOrderNavigator.doVisit(command, new MultiSourceElementReplacementVisitor(bindingName));

            // Rewrite the command now that criteria may have been simplified
            try {
                command = QueryRewriter.rewrite(command, null, metadata, null);                    
                instanceNode.setCommand(command);
            } catch(QueryValidatorException e) {
                // ignore and use original command
            }
            
            if (!RelationalNodeUtil.shouldExecute(command, false)) {
                continue;
            }
                                
            accessNodes.add(instanceNode);
        }

        switch(accessNodes.size()) {
            case 0: 
            {
                // Replace existing access node with a NullNode
                NullNode nullNode = new NullNode(getID());
                nullNode.setElements(accessNode.getElements());
                return nullNode;         
            }
            case 1: 
            {
                // Replace existing access node with new access node (simplified command)
                AccessNode newNode = accessNodes.get(0);
                return newNode;                                                
            }
            default:
            {
                // More than 1 access node - replace with a union
                
                UnionAllNode unionNode = new UnionAllNode(getID());
                unionNode.setElements(accessNode.getElements());
                
                RelationalNode parent = unionNode;
                                
                for (AccessNode newNode : accessNodes) {
                    unionNode.addChild(newNode);
                }
                
                return parent;
            }
        }
    }

}
