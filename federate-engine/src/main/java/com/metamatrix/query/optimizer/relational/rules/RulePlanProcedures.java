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

package com.metamatrix.query.optimizer.relational.rules;

import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import com.metamatrix.api.exception.MetaMatrixComponentException;
import com.metamatrix.api.exception.query.QueryMetadataException;
import com.metamatrix.api.exception.query.QueryPlannerException;
import com.metamatrix.common.log.LogManager;
import com.metamatrix.query.analysis.AnalysisRecord;
import com.metamatrix.query.execution.QueryExecPlugin;
import com.metamatrix.query.metadata.QueryMetadataInterface;
import com.metamatrix.query.metadata.SupportConstants;
import com.metamatrix.query.optimizer.capabilities.CapabilitiesFinder;
import com.metamatrix.query.optimizer.relational.OptimizerRule;
import com.metamatrix.query.optimizer.relational.RuleStack;
import com.metamatrix.query.optimizer.relational.plantree.NodeConstants;
import com.metamatrix.query.optimizer.relational.plantree.NodeEditor;
import com.metamatrix.query.optimizer.relational.plantree.PlanNode;
import com.metamatrix.query.sql.LanguageObject;
import com.metamatrix.query.sql.LanguageVisitor;
import com.metamatrix.query.sql.lang.CompareCriteria;
import com.metamatrix.query.sql.lang.CompoundCriteria;
import com.metamatrix.query.sql.lang.Criteria;
import com.metamatrix.query.sql.lang.DependentSetCriteria;
import com.metamatrix.query.sql.lang.IsNullCriteria;
import com.metamatrix.query.sql.lang.NotCriteria;
import com.metamatrix.query.sql.lang.SPParameter;
import com.metamatrix.query.sql.lang.SetCriteria;
import com.metamatrix.query.sql.lang.StoredProcedure;
import com.metamatrix.query.sql.navigator.PreOrderNavigator;
import com.metamatrix.query.sql.symbol.ElementSymbol;
import com.metamatrix.query.sql.symbol.Expression;
import com.metamatrix.query.sql.symbol.Reference;
import com.metamatrix.query.sql.visitor.ReferenceCollectorVisitor;
import com.metamatrix.query.util.CommandContext;
import com.metamatrix.query.util.LogConstants;

public class RulePlanProcedures implements OptimizerRule {

    /** 
     * @see com.metamatrix.query.optimizer.relational.OptimizerRule#execute(com.metamatrix.query.optimizer.relational.plantree.PlanNode, com.metamatrix.query.metadata.QueryMetadataInterface, com.metamatrix.query.optimizer.capabilities.CapabilitiesFinder, com.metamatrix.query.optimizer.relational.RuleStack, com.metamatrix.query.analysis.AnalysisRecord, com.metamatrix.query.util.CommandContext)
     */
    public PlanNode execute(PlanNode plan,
                            final QueryMetadataInterface metadata,
                            CapabilitiesFinder capabilitiesFinder,
                            RuleStack rules,
                            AnalysisRecord analysisRecord,
                            CommandContext context) throws QueryPlannerException,
                                                   QueryMetadataException,
                                                   MetaMatrixComponentException {
        
        List nodes = NodeEditor.findAllNodes(plan, NodeConstants.Types.SOURCE, NodeConstants.Types.ACCESS);
        
        for (Iterator i = nodes.iterator(); i.hasNext();) {
            PlanNode node = (PlanNode)i.next();
            
            if (!FrameUtil.isProcedure(node.getFirstChild())) {
                continue;
            }
            
            StoredProcedure proc = (StoredProcedure)node.getProperty(NodeConstants.Info.NESTED_COMMAND);
            
            HashSet inputSymbols = new HashSet();
            List inputReferences = new LinkedList();
            
            PlanNode critNode = node.getParent();
            
            List conjuncts = new LinkedList();
            HashSet coveredParams = new HashSet();
            //List preExecNodes = new LinkedList();

            // Case 6395 - maintain a list of non-nullable elements that are set IS NULL.  
            List nonNullableElems = new LinkedList();
            
            if (!proc.isProcedureRelational()) {
                continue;
            }
            
            for (Iterator params = proc.getInputParameters().iterator(); params.hasNext();) {
                SPParameter param = (SPParameter)params.next();
                ElementSymbol symbol = param.getParameterSymbol();
                Expression input = param.getExpression();
                inputReferences.add(input);
                inputSymbols.add(symbol);
            }
            
            findInputNodes(inputSymbols, critNode, conjuncts, coveredParams, nonNullableElems, metadata);
            
            // Check for non-nullable elements that are set IS NULL.  throws exception if any found.
            if(!nonNullableElems.isEmpty()) {
            	throw new QueryPlannerException(QueryExecPlugin.Util.getString("RulePlanProcedures.nonNullableParam", nonNullableElems.get(0))); //$NON-NLS-1$
            }
            
            List defaults = new LinkedList();
            
            for (Iterator params = inputReferences.iterator(); params.hasNext();) {
                Reference ref = (Reference)params.next(); 
                ElementSymbol symbol = (ElementSymbol)ref.getExpression();
                
                Expression defaultValue = null;
                
                /*try {
                    defaultValue = ResolverUtil.getDefault(symbol, metadata);
                } catch (QueryResolverException qre) {
                    //Just ignore
                }*/
                
                defaults.add(defaultValue);
                
                if (defaultValue == null && !coveredParams.contains(symbol)) {
                    throw new QueryPlannerException(QueryExecPlugin.Util.getString("RulePlanProcedures.no_values", symbol)); //$NON-NLS-1$
                }
            }
            
            if (conjuncts.isEmpty()) {
                for (int j = 0; j < inputReferences.size(); j++) {
                    Reference ref = (Reference)inputReferences.get(j);
                    ref.setValue(defaults.get(j));
                }
                continue;
            }
            
            PlanNode accessNode = NodeEditor.findNodePreOrder(node, NodeConstants.Types.ACCESS);
            
            Criteria crit = Criteria.combineCriteria(conjuncts);
            
            accessNode.setProperty(NodeConstants.Info.PROCEDURE_CRITERIA, crit);
            accessNode.setProperty(NodeConstants.Info.PROCEDURE_INPUTS, inputReferences);
            accessNode.setProperty(NodeConstants.Info.PROCEDURE_DEFAULTS, defaults);
            accessNode.setProperty(NodeConstants.Info.IS_DEPENDENT_SET, Boolean.TRUE);
        }
        
        return plan;
    }

    private void findInputNodes(final HashSet inputs,
                               PlanNode critNode,
                               final List conjuncts, final Set params, final List nonNullableElems,
                               final QueryMetadataInterface metadata) throws QueryMetadataException, MetaMatrixComponentException {
        
        while (critNode.getType() == NodeConstants.Types.SELECT) {
            final PlanNode currentNode = critNode;
            
            final Criteria crit = (Criteria)currentNode.getProperty(NodeConstants.Info.SELECT_CRITERIA);
            
            critNode = currentNode.getParent();
            
            if (FrameUtil.hasSubquery(currentNode) || !currentNode.getGroups().isEmpty()) {
                continue;
            }
            
            LanguageVisitor visitor = new LanguageVisitor() {      
                public void visit(CompareCriteria compCrit){
                    if(compCrit.getOperator() != CompareCriteria.EQ){
                        return;
                    }
                    if (checkForInput(compCrit.getLeftExpression()) && !checkForAnyInput(compCrit.getRightExpression())) {
                        addInputNode((Reference)compCrit.getLeftExpression());
                    }
                }
                
                public void visit(NotCriteria obj) {
                    setAbort(true);
                }
                
                public void visit(CompoundCriteria obj) {
                    setAbort(true);
                }

                private void addInputNode(Reference param) {
                    params.add(param.getExpression());
                    conjuncts.add(crit);
                    NodeEditor.removeChildNode(currentNode.getParent(), currentNode);
                    currentNode.setParent(null);
                    setAbort(true);
                }
                
                // method to add invalid isNull element
                private void addInvalidElem(ElementSymbol symbol) {
                	nonNullableElems.add(symbol);
                }
                
                public void visit(IsNullCriteria isNull) {
                    if (isNull.isNegated()) {
                        return;
                    }
                    // Case 6395 - check for non-nullable Elems that are IS NULL
					Expression expr = isNull.getExpression();
					if(expr instanceof Reference) {
						expr = ((Reference)expr).getExpression();
					}
					if(expr instanceof ElementSymbol &&!isNullable((ElementSymbol)expr,metadata)) {
						addInvalidElem((ElementSymbol)expr);
					}
					
                    if (checkForInput(isNull.getExpression())) {
                        addInputNode((Reference)isNull.getExpression());
                    }
                }
                
                public void visit(SetCriteria obj) {
                    if (checkForInput(obj.getExpression()) && !checkForAnyInput(obj.getValues())) {
                        addInputNode((Reference)obj.getExpression());
                    }
                }
                
                public void visit(DependentSetCriteria obj) {
                    if (checkForInput(obj.getExpression())) {
                        addInputNode((Reference)obj.getExpression());
                    }
                }
                
                boolean checkForInput(Expression expr) {
                    if (!(expr instanceof Reference)) {
                        return false;
                    }
                    //if the expr is a function containing a reference should give a warning
                    Reference ref = (Reference)expr;
                    return inputs.contains(ref.getExpression());
                }
                
                boolean checkForAnyInput(LanguageObject expr) {
                    for (Iterator refs = ReferenceCollectorVisitor.getReferences(expr).iterator(); refs.hasNext();) {
                        if (checkForInput((Expression)refs.next())) {
                            return true;
                        }
                    }
                    return false;
                }
                
                boolean checkForAnyInput(Collection expressions) {
                    for (Iterator exprs = expressions.iterator(); exprs.hasNext();) {
                        if (checkForAnyInput((Expression)exprs.next())) {
                            return true;
                        }
                    }
                    return false;
                }

                boolean isNullable(ElementSymbol element, QueryMetadataInterface metadata) {                    
                	Object elemID = element.getMetadataID();
                	try {
	                    return metadata.elementSupports(elemID, SupportConstants.Element.NULL) || 
	                        metadata.elementSupports(elemID, SupportConstants.Element.NULL_UNKNOWN);
                	} catch (Exception e){
                        LogManager.logWarning(LogConstants.CTX_QUERY_PLANNER, e , "Error getting isNullable on element: "+element.getShortName()); //$NON-NLS-1$
                        return false;
                	}
                }
                
            };

            PreOrderNavigator.doVisit(crit, visitor);
        }
    }

    /** 
     * @see java.lang.Object#toString()
     */
    public String toString() {
        return "PlanProcedures"; //$NON-NLS-1$
    }
    
}
