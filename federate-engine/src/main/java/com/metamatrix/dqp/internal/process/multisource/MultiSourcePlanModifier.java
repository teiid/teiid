/*
 * JBoss, Home of Professional Open Source.
 * Copyright (C) 2008 Red Hat, Inc.
 * Copyright (C) 2000-2007 MetaMatrix, Inc.
 * Licensed to Red Hat, Inc. under one or more contributor 
 * license agreements.  See the copyright.txt file in the
 * distribution for a full listing of individual contributors.
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

package com.metamatrix.dqp.internal.process.multisource;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import com.metamatrix.api.exception.MetaMatrixComponentException;
import com.metamatrix.api.exception.query.CriteriaEvaluationException;
import com.metamatrix.api.exception.query.QueryValidatorException;
import com.metamatrix.core.id.IDGenerator;
import com.metamatrix.core.id.IntegerID;
import com.metamatrix.core.id.IntegerIDFactory;
import com.metamatrix.dqp.service.VDBService;
import com.metamatrix.query.metadata.QueryMetadataInterface;
import com.metamatrix.query.processor.ProcessorPlan;
import com.metamatrix.query.processor.relational.AccessNode;
import com.metamatrix.query.processor.relational.NullNode;
import com.metamatrix.query.processor.relational.RelationalNode;
import com.metamatrix.query.processor.relational.RelationalNodeUtil;
import com.metamatrix.query.processor.relational.RelationalPlan;
import com.metamatrix.query.processor.relational.UnionAllNode;
import com.metamatrix.query.rewriter.QueryRewriter;
import com.metamatrix.query.sql.lang.Command;
import com.metamatrix.query.sql.navigator.DeepPreOrderNavigator;

/** 
 * @since 4.2
 */
public class MultiSourcePlanModifier implements com.metamatrix.query.execution.multisource.PlanModifier {

    private String vdbName;
    private String vdbVersion;
    private VDBService vdbService;
    private IDGenerator idGenerator;
    private Collection multiSourceModels;
    
    public void setIdGenerator(IDGenerator idGenerator) {
        this.idGenerator = idGenerator;
    }
    public void setVdbName(String vdbName) {
        this.vdbName = vdbName;
    }
    public void setVdbService(VDBService vdbService) {
        this.vdbService = vdbService;
    }
    public void setVdbVersion(String vdbVersion) {
        this.vdbVersion = vdbVersion;
    }
    public void setMultiSourceModels(Collection multiSourceModels) {
        this.multiSourceModels = multiSourceModels;
    }
        
    public void modifyPlan(ProcessorPlan plan, QueryMetadataInterface metadata) throws MetaMatrixComponentException, CriteriaEvaluationException {
        if(plan instanceof RelationalPlan) {
            RelationalPlan rplan = (RelationalPlan) plan;
            RelationalNode root = rplan.getRootNode();
            rplan.setRootNode(modifyPlan(root, metadata));
        } 
        
        Collection subPlans = plan.getChildPlans();
        if(subPlans != null && subPlans.size() > 0) {
            Iterator planIter = subPlans.iterator();
            while(planIter.hasNext()) {
                ProcessorPlan subPlan = (ProcessorPlan) planIter.next();
                modifyPlan(subPlan, metadata);
            }
        }
    }
    
    private int getID() {
        IntegerIDFactory intFactory = (IntegerIDFactory) idGenerator.getDefaultFactory();
        return ((IntegerID) intFactory.create()).getValue();
    }
    
    private RelationalNode modifyPlan(RelationalNode node, QueryMetadataInterface metadata) throws MetaMatrixComponentException, CriteriaEvaluationException {
        if(!(node instanceof AccessNode)) {
            RelationalNode[] children = node.getChildren();
            for(int i=0; i<children.length; i++) {
                if(children[i] != null) {
                    children[i] = modifyPlan(children[i], metadata);
                    children[i].setParent(node);
                } 
            }
            return node;
        }
        // terminate on AccessNode
        AccessNode accessNode = (AccessNode) node;
        String modelName = accessNode.getModelName();
        
        if(!this.multiSourceModels.contains(modelName)) {
            return node;
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
            instanceNode.setModelName(bindingUUID);
            
            // Modify the command to pull the instance column and evaluate the criteria
            Command command = instanceNode.getCommand();
            
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
