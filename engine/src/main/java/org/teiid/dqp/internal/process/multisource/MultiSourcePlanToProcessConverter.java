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
import java.util.List;
import java.util.Set;

import org.teiid.adminapi.impl.ModelMetaData;
import org.teiid.adminapi.impl.VDBMetaData;
import org.teiid.api.exception.query.QueryPlannerException;
import org.teiid.api.exception.query.QueryValidatorException;
import org.teiid.core.TeiidComponentException;
import org.teiid.core.TeiidProcessingException;
import org.teiid.core.id.IDGenerator;
import org.teiid.dqp.internal.process.DQPWorkContext;
import org.teiid.query.analysis.AnalysisRecord;
import org.teiid.query.metadata.QueryMetadataInterface;
import org.teiid.query.optimizer.capabilities.CapabilitiesFinder;
import org.teiid.query.optimizer.relational.PlanToProcessConverter;
import org.teiid.query.optimizer.relational.plantree.PlanNode;
import org.teiid.query.processor.relational.AccessNode;
import org.teiid.query.processor.relational.NullNode;
import org.teiid.query.processor.relational.RelationalNode;
import org.teiid.query.processor.relational.RelationalNodeUtil;
import org.teiid.query.processor.relational.UnionAllNode;
import org.teiid.query.rewriter.QueryRewriter;
import org.teiid.query.sql.lang.Command;
import org.teiid.query.sql.navigator.DeepPreOrderNavigator;
import org.teiid.query.util.CommandContext;


public class MultiSourcePlanToProcessConverter extends PlanToProcessConverter {
	
	private Set<String> multiSourceModels;
	private DQPWorkContext workContext;
	
	public MultiSourcePlanToProcessConverter(QueryMetadataInterface metadata,
			IDGenerator idGenerator, AnalysisRecord analysisRecord,
			CapabilitiesFinder capFinder, Set<String> multiSourceModels,
			DQPWorkContext workContext, CommandContext context) {
		super(metadata, idGenerator, analysisRecord, capFinder);
		this.multiSourceModels = multiSourceModels;
		this.workContext = workContext;
	}

	protected RelationalNode convertNode(PlanNode planNode) throws QueryPlannerException, TeiidComponentException {
		RelationalNode node = super.convertNode(planNode);
		
		if (node instanceof AccessNode) {
			try {
				return multiSourceModify((AccessNode)node);
			} catch (TeiidProcessingException e) {
				throw new QueryPlannerException(e, e.getMessage());
			} 
		}
		
		return node;
	}
	
	private RelationalNode multiSourceModify(AccessNode accessNode) throws TeiidComponentException, TeiidProcessingException {
        String modelName = accessNode.getModelName();

		if(!this.multiSourceModels.contains(modelName)) {
            return accessNode;
        }
        
		VDBMetaData vdb = workContext.getVDB();
        ModelMetaData model = vdb.getModel(modelName);
        List<AccessNode> accessNodes = new ArrayList<AccessNode>();
        
        for(String sourceName:model.getSourceNames()) {
            
            // Create a new cloned version of the access node and set it's model name to be the bindingUUID
            AccessNode instanceNode = (AccessNode) accessNode.clone();
            instanceNode.setID(getID());
            instanceNode.setConnectorBindingId(sourceName);
            
            // Modify the command to pull the instance column and evaluate the criteria
            Command command = (Command)instanceNode.getCommand().clone();
            
            // Replace all multi-source elements with the source name
            DeepPreOrderNavigator.doVisit(command, new MultiSourceElementReplacementVisitor(sourceName));

            // Rewrite the command now that criteria may have been simplified
            try {
                command = QueryRewriter.rewrite(command, metadata, null);                    
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
