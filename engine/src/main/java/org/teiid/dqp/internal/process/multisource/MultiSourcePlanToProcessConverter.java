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
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.teiid.adminapi.impl.ModelMetaData;
import org.teiid.adminapi.impl.VDBMetaData;
import org.teiid.api.exception.query.ExpressionEvaluationException;
import org.teiid.api.exception.query.QueryPlannerException;
import org.teiid.api.exception.query.QueryValidatorException;
import org.teiid.core.TeiidComponentException;
import org.teiid.core.TeiidProcessingException;
import org.teiid.core.id.IDGenerator;
import org.teiid.core.types.DataTypeManager;
import org.teiid.dqp.internal.process.DQPWorkContext;
import org.teiid.language.SQLConstants.NonReserved;
import org.teiid.query.QueryPlugin;
import org.teiid.query.analysis.AnalysisRecord;
import org.teiid.query.metadata.QueryMetadataInterface;
import org.teiid.query.optimizer.capabilities.CapabilitiesFinder;
import org.teiid.query.optimizer.relational.PlanToProcessConverter;
import org.teiid.query.optimizer.relational.plantree.PlanNode;
import org.teiid.query.processor.relational.AccessNode;
import org.teiid.query.processor.relational.GroupingNode;
import org.teiid.query.processor.relational.NullNode;
import org.teiid.query.processor.relational.ProjectIntoNode;
import org.teiid.query.processor.relational.ProjectNode;
import org.teiid.query.processor.relational.RelationalNode;
import org.teiid.query.processor.relational.RelationalNodeUtil;
import org.teiid.query.processor.relational.RelationalPlan;
import org.teiid.query.processor.relational.UnionAllNode;
import org.teiid.query.resolver.util.ResolverUtil;
import org.teiid.query.rewriter.QueryRewriter;
import org.teiid.query.sql.lang.Command;
import org.teiid.query.sql.lang.Insert;
import org.teiid.query.sql.lang.SPParameter;
import org.teiid.query.sql.lang.StoredProcedure;
import org.teiid.query.sql.navigator.DeepPreOrderNavigator;
import org.teiid.query.sql.symbol.AggregateSymbol;
import org.teiid.query.sql.symbol.Constant;
import org.teiid.query.sql.symbol.ElementSymbol;
import org.teiid.query.sql.symbol.Expression;
import org.teiid.query.sql.symbol.ExpressionSymbol;
import org.teiid.query.sql.symbol.SingleElementSymbol;
import org.teiid.query.util.CommandContext;


public class MultiSourcePlanToProcessConverter extends PlanToProcessConverter {
	
	private Set<String> multiSourceModels;
	private DQPWorkContext workContext;
	
	private boolean multiSource;
	private boolean update;
	
	public MultiSourcePlanToProcessConverter(QueryMetadataInterface metadata,
			IDGenerator idGenerator, AnalysisRecord analysisRecord,
			CapabilitiesFinder capFinder, Set<String> multiSourceModels,
			DQPWorkContext workContext, CommandContext context) {
		super(metadata, idGenerator, analysisRecord, capFinder);
		this.multiSourceModels = multiSourceModels;
		this.workContext = workContext;
	}
	
	@Override
	public synchronized RelationalPlan convert(PlanNode planNode)
			throws QueryPlannerException, TeiidComponentException {
			RelationalPlan result = null;
		try {
			result = super.convert(planNode);
			return result;
		} finally {
			if (result != null && update && multiSource) {
				result.setMultisourceUpdate(true);
			}
			update = false;
			multiSource = false;
		}
	}

	protected RelationalNode convertNode(PlanNode planNode) throws QueryPlannerException, TeiidComponentException {
		RelationalNode node = super.convertNode(planNode);
		
		if (node instanceof AccessNode) {
			try {
				return multiSourceModify((AccessNode)node);
			} catch (TeiidProcessingException e) {
				throw new QueryPlannerException(e, e.getMessage());
			} 
		} else if (node instanceof ProjectIntoNode) {
			throw new AssertionError("Multisource insert with query expression not allowed not allowed, should have been caught in validation."); //$NON-NLS-1$
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
        
        boolean hasOutParams = false;
        if (accessNode.getCommand() instanceof StoredProcedure) {
        	StoredProcedure sp = (StoredProcedure)accessNode.getCommand();
        	hasOutParams = sp.returnParameters() && sp.getProjectedSymbols().size() > sp.getResultSetColumns().size();
        }
        
        for(String sourceName:model.getSourceNames()) {
            
            // Create a new cloned version of the access node and set it's model name to be the bindingUUID
            AccessNode instanceNode = (AccessNode) accessNode.clone();
            instanceNode.setID(getID());
            instanceNode.setConnectorBindingId(sourceName);
            
            // Modify the command to pull the instance column and evaluate the criteria
            Command command = (Command)instanceNode.getCommand().clone();
            
            command = rewriteCommand(sourceName, command);
            
            if (command == null) {
            	continue;
            }

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
        
        if (hasOutParams && accessNodes.size() != 1) {
        	throw new QueryPlannerException(QueryPlugin.Util.getString("MultiSource.out_procedure", accessNode.getCommand())); //$NON-NLS-1$
        }

        switch(accessNodes.size()) {
            case 0: 
            {
                if (RelationalNodeUtil.isUpdate(accessNode.getCommand())) {
                	//should return a 0 update count
                	ProjectNode pnode = new ProjectNode(getID());
                	pnode.setElements(accessNode.getElements());
                	pnode.setSelectSymbols(Arrays.asList(new ExpressionSymbol("x", new Constant(0)))); //$NON-NLS-1$
                	return pnode;
                }
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
            	multiSource = true;
            	UnionAllNode unionNode = new UnionAllNode(getID());
            	unionNode.setElements(accessNode.getElements());
                
                for (AccessNode newNode : accessNodes) {
                	unionNode.addChild(newNode);
                }
            	
            	RelationalNode parent = unionNode;
            	
                // More than 1 access node - replace with a union
            	if (RelationalNodeUtil.isUpdate(accessNode.getCommand())) {
            		update = true;
            		GroupingNode groupNode = new GroupingNode(getID());                    
            		AggregateSymbol sumCount = new AggregateSymbol("SumCount", NonReserved.SUM, false, (Expression)accessNode.getElements().get(0)); //$NON-NLS-1$          		
            		List<Expression> outputElements = new ArrayList<Expression>(1);            		
            		outputElements.add(sumCount); 
            		groupNode.setElements(outputElements);
            		groupNode.addChild(unionNode);
            		
            		ProjectNode projectNode = new ProjectNode(getID());
            		
            		Expression intSum = ResolverUtil.getConversion(sumCount, DataTypeManager.getDataTypeName(sumCount.getType()), DataTypeManager.DefaultDataTypes.INTEGER, false, metadata.getFunctionLibrary());
            		
            		Expression rowCount = new ExpressionSymbol("RowCount", intSum); //$NON-NLS-1$            		
            		outputElements = new ArrayList<Expression>(1);            		
            		outputElements.add(rowCount);             		
            		projectNode.setElements(outputElements);
            		projectNode.setSelectSymbols(outputElements);
            		projectNode.addChild(groupNode);
            		
            		parent = projectNode;
            	}
                return parent;
            }
        }
    }

	private Command rewriteCommand(String sourceName, Command command) throws ExpressionEvaluationException, TeiidComponentException {
		if (command instanceof StoredProcedure) {
			StoredProcedure obj = (StoredProcedure)command;
			for (Iterator<SPParameter> params = obj.getMapOfParameters().values().iterator(); params.hasNext();) {
				SPParameter param = params.next();
				if (param.getParameterType() != SPParameter.IN) {
					continue;
				}
				String shortName = SingleElementSymbol.getShortName(param.getName());        
			    if(shortName.equalsIgnoreCase(MultiSourceElement.MULTI_SOURCE_ELEMENT_NAME)) {
		        	Constant source = (Constant)param.getExpression();
		    		params.remove();
		    		if (param.isUsingDefault() && source.isNull()) {
		    			continue;
		    		}
		        	if (!source.getValue().equals(sourceName)) {
		        		return null;
		        	}
		        }
			}
		} if (command instanceof Insert) {
			Insert obj = (Insert)command;
			for (int i = 0; i < obj.getVariables().size(); i++) {
				ElementSymbol elem = obj.getVariables().get(i);
		        Object metadataID = elem.getMetadataID();            
		        if(metadataID instanceof MultiSourceElement) {
		        	Constant source = (Constant)obj.getValues().get(i);
		    		obj.getVariables().remove(i);
		    		obj.getValues().remove(i);
		        	if (!source.getValue().equals(sourceName)) {
		        		return null;
		        	}
		        }
			}
		} else {
		    // Replace all multi-source elements with the source name
		    DeepPreOrderNavigator.doVisit(command, new MultiSourceElementReplacementVisitor(sourceName));
		}
		
		if (!RelationalNodeUtil.shouldExecute(command, false)) {
            return null;
        }
		
		return command;
	}

}
