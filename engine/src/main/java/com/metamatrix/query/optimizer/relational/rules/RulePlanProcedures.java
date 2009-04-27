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
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import com.metamatrix.api.exception.MetaMatrixComponentException;
import com.metamatrix.api.exception.query.QueryMetadataException;
import com.metamatrix.api.exception.query.QueryPlannerException;
import com.metamatrix.query.analysis.AnalysisRecord;
import com.metamatrix.query.execution.QueryExecPlugin;
import com.metamatrix.query.metadata.QueryMetadataInterface;
import com.metamatrix.query.optimizer.capabilities.CapabilitiesFinder;
import com.metamatrix.query.optimizer.relational.OptimizerRule;
import com.metamatrix.query.optimizer.relational.RuleStack;
import com.metamatrix.query.optimizer.relational.plantree.NodeConstants;
import com.metamatrix.query.optimizer.relational.plantree.NodeEditor;
import com.metamatrix.query.optimizer.relational.plantree.PlanNode;
import com.metamatrix.query.sql.LanguageObject;
import com.metamatrix.query.sql.LanguageVisitor;
import com.metamatrix.query.sql.lang.CompareCriteria;
import com.metamatrix.query.sql.lang.Criteria;
import com.metamatrix.query.sql.lang.DependentSetCriteria;
import com.metamatrix.query.sql.lang.IsNullCriteria;
import com.metamatrix.query.sql.lang.SPParameter;
import com.metamatrix.query.sql.lang.SetCriteria;
import com.metamatrix.query.sql.lang.StoredProcedure;
import com.metamatrix.query.sql.symbol.ElementSymbol;
import com.metamatrix.query.sql.symbol.Expression;
import com.metamatrix.query.sql.symbol.Reference;
import com.metamatrix.query.sql.visitor.ReferenceCollectorVisitor;
import com.metamatrix.query.util.CommandContext;

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
        
        for (PlanNode node : NodeEditor.findAllNodes(plan, NodeConstants.Types.SOURCE, NodeConstants.Types.ACCESS)) {
            if (!FrameUtil.isProcedure(node.getFirstChild())) {
                continue;
            }
            
            StoredProcedure proc = (StoredProcedure)node.getProperty(NodeConstants.Info.NESTED_COMMAND);
            
            if (!proc.isProcedureRelational()) {
                continue;
            }

            HashSet inputSymbols = new HashSet();
            List inputReferences = new LinkedList();
            
            PlanNode critNode = node.getParent();
            
            List conjuncts = new LinkedList();
            HashSet coveredParams = new HashSet();
            //List preExecNodes = new LinkedList();
                        
            for (Iterator params = proc.getInputParameters().iterator(); params.hasNext();) {
                SPParameter param = (SPParameter)params.next();
                ElementSymbol symbol = param.getParameterSymbol();
                Expression input = param.getExpression();
                inputReferences.add(input);
                inputSymbols.add(symbol);
            }
            
            findInputNodes(inputSymbols, critNode, conjuncts, coveredParams);
            
            List defaults = new LinkedList();
            
            for (Iterator params = inputReferences.iterator(); params.hasNext();) {
                Reference ref = (Reference)params.next(); 
                ElementSymbol symbol = ref.getExpression();
                
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
            
            /*if (conjuncts.isEmpty()) {
                for (int j = 0; j < inputReferences.size(); j++) {
                    Reference ref = (Reference)inputReferences.get(j);
                    ref.setValue(defaults.get(j));
                }
                continue;
            }*/
            
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
                               final List conjuncts, final Set params) {
        
        while (critNode.getType() == NodeConstants.Types.SELECT) {
            final PlanNode currentNode = critNode;
            
            final Criteria crit = (Criteria)currentNode.getProperty(NodeConstants.Info.SELECT_CRITERIA);
            
            critNode = currentNode.getParent();
            
            if (!currentNode.getGroups().isEmpty()) {
                continue;
            }
            
            LanguageVisitor visitor = new LanguageVisitor() {      
                public void visit(CompareCriteria compCrit){
                    if (compCrit.getOperator() == CompareCriteria.EQ && checkForInput(compCrit.getLeftExpression()) && !checkForAnyInput(compCrit.getRightExpression())) {
                        addInputNode((Reference)compCrit.getLeftExpression());
                    }
                }
                
                private void addInputNode(Reference param) {
                    params.add(param.getExpression());
                    conjuncts.add(crit);
                    NodeEditor.removeChildNode(currentNode.getParent(), currentNode);
                }
                
                public void visit(IsNullCriteria isNull){
                    if (!isNull.isNegated() && checkForInput(isNull.getExpression())) {
                        addInputNode((Reference)isNull.getExpression());
                    }
                }
                
                public void visit(SetCriteria obj) {
                    if (!obj.isNegated() && checkForInput(obj.getExpression()) && !checkForAnyInput(obj.getValues())) {
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
                
            };
            crit.acceptVisitor(visitor);
        }
    }
    
    /** 
     * @see java.lang.Object#toString()
     */
    public String toString() {
        return "PlanProcedures"; //$NON-NLS-1$
    }
    
}
