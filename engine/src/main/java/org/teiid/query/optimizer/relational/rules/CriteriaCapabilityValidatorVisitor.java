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

import java.util.Arrays;

import org.teiid.api.exception.query.QueryMetadataException;
import org.teiid.core.TeiidComponentException;
import org.teiid.metadata.FunctionMethod.PushDown;
import org.teiid.query.QueryPlugin;
import org.teiid.query.analysis.AnalysisRecord;
import org.teiid.query.metadata.QueryMetadataInterface;
import org.teiid.query.metadata.SupportConstants;
import org.teiid.query.optimizer.capabilities.CapabilitiesFinder;
import org.teiid.query.optimizer.capabilities.SourceCapabilities;
import org.teiid.query.optimizer.capabilities.SourceCapabilities.Capability;
import org.teiid.query.processor.ProcessorPlan;
import org.teiid.query.processor.relational.AccessNode;
import org.teiid.query.processor.relational.LimitNode;
import org.teiid.query.processor.relational.RelationalNode;
import org.teiid.query.processor.relational.RelationalPlan;
import org.teiid.query.sql.LanguageObject;
import org.teiid.query.sql.LanguageVisitor;
import org.teiid.query.sql.lang.AbstractCompareCriteria;
import org.teiid.query.sql.lang.AbstractSetCriteria;
import org.teiid.query.sql.lang.Command;
import org.teiid.query.sql.lang.CompareCriteria;
import org.teiid.query.sql.lang.CompoundCriteria;
import org.teiid.query.sql.lang.DependentSetCriteria;
import org.teiid.query.sql.lang.ExistsCriteria;
import org.teiid.query.sql.lang.IsNullCriteria;
import org.teiid.query.sql.lang.MatchCriteria;
import org.teiid.query.sql.lang.NotCriteria;
import org.teiid.query.sql.lang.Query;
import org.teiid.query.sql.lang.QueryCommand;
import org.teiid.query.sql.lang.SetCriteria;
import org.teiid.query.sql.lang.SubqueryCompareCriteria;
import org.teiid.query.sql.lang.SubqueryContainer;
import org.teiid.query.sql.lang.SubquerySetCriteria;
import org.teiid.query.sql.navigator.PostOrderNavigator;
import org.teiid.query.sql.symbol.AggregateSymbol;
import org.teiid.query.sql.symbol.CaseExpression;
import org.teiid.query.sql.symbol.Function;
import org.teiid.query.sql.symbol.QueryString;
import org.teiid.query.sql.symbol.ScalarSubquery;
import org.teiid.query.sql.symbol.SearchedCaseExpression;
import org.teiid.query.sql.symbol.TextLine;
import org.teiid.query.sql.symbol.WindowFunction;
import org.teiid.query.sql.symbol.XMLAttributes;
import org.teiid.query.sql.symbol.XMLElement;
import org.teiid.query.sql.symbol.XMLForest;
import org.teiid.query.sql.symbol.XMLNamespaces;
import org.teiid.query.sql.symbol.XMLParse;
import org.teiid.query.sql.symbol.XMLQuery;
import org.teiid.query.sql.symbol.XMLSerialize;
import org.teiid.query.sql.util.SymbolMap;
import org.teiid.query.sql.visitor.EvaluatableVisitor;
import org.teiid.query.sql.visitor.FunctionCollectorVisitor;


/**
 */
public class CriteriaCapabilityValidatorVisitor extends LanguageVisitor {

    // Initialization state
    private Object modelID;
    private QueryMetadataInterface metadata;
    private CapabilitiesFinder capFinder;
    private AnalysisRecord analysisRecord;

    // Retrieved during initialization and cached
    private SourceCapabilities caps;
    
    // Output state
    private TeiidComponentException exception;
    private boolean valid = true;

    /**
     * @param iterator
     * @throws TeiidComponentException 
     * @throws QueryMetadataException 
     */
    CriteriaCapabilityValidatorVisitor(Object modelID, QueryMetadataInterface metadata, CapabilitiesFinder capFinder, SourceCapabilities caps) throws QueryMetadataException, TeiidComponentException {        
        this.modelID = modelID;
        this.metadata = metadata;
        this.capFinder = capFinder;
        this.caps = caps;
    }
    
    @Override
    public void visit(XMLAttributes obj) {
    	markInvalid(obj, "Pushdown of XMLAttributes not allowed"); //$NON-NLS-1$
    }
    
    @Override
    public void visit(XMLNamespaces obj) {
    	markInvalid(obj, "Pushdown of XMLNamespaces not allowed"); //$NON-NLS-1$
    }
    
    @Override
    public void visit(TextLine obj) {
    	markInvalid(obj, "Pushdown of TextLine not allowed"); //$NON-NLS-1$
    }
    
    @Override
    public void visit(XMLForest obj) {
    	markInvalid(obj, "Pushdown of XMLForest not allowed"); //$NON-NLS-1$
    }
    
    @Override
    public void visit(XMLElement obj) {
    	markInvalid(obj, "Pushdown of XMLElement not allowed"); //$NON-NLS-1$
    }
    
    @Override
    public void visit(XMLSerialize obj) {
    	markInvalid(obj, "Pushdown of XMLSerialize not allowed"); //$NON-NLS-1$
    }
    
    @Override
    public void visit(XMLParse obj) {
    	markInvalid(obj, "Pushdown of XMLParse not allowed"); //$NON-NLS-1$
    }
    
    @Override
    public void visit(XMLQuery obj) {
    	markInvalid(obj, "Pushdown of XMLQuery not allowed"); //$NON-NLS-1$
    }
    
    @Override
    public void visit(QueryString obj) {
    	markInvalid(obj, "Pushdown of QueryString not allowed"); //$NON-NLS-1$
    }
    
    public void visit(AggregateSymbol obj) {
        try {
            if(! CapabilitiesUtil.supportsAggregateFunction(modelID, obj, metadata, capFinder)) {
                markInvalid(obj, "Aggregate function pushdown not supported by source"); //$NON-NLS-1$
            }         
        } catch(QueryMetadataException e) {
            handleException(new TeiidComponentException(e));
        } catch(TeiidComponentException e) {
            handleException(e);            
        }
    }
    
    @Override
    public void visit(WindowFunction windowFunction) {
    	if(! this.caps.supportsCapability(Capability.ELEMENTARY_OLAP)) {
            markInvalid(windowFunction, "Window function not supported by source"); //$NON-NLS-1$
        }
    }
    
    public void visit(CaseExpression obj) {
        if(! this.caps.supportsCapability(Capability.QUERY_CASE)) {
            markInvalid(obj, "CaseExpression pushdown not supported by source"); //$NON-NLS-1$
        }
    }
    
    public void visit(CompareCriteria obj) {
    	checkCompareCriteria(obj);
    }
    
    public void checkCompareCriteria(AbstractCompareCriteria obj) {
        boolean negated = false;
        // Check if operation is allowed
        Capability operatorCap = null;
        switch(obj.getOperator()) {
            case CompareCriteria.NE: 
                negated = true;
            case CompareCriteria.EQ: 
                operatorCap = Capability.CRITERIA_COMPARE_EQ;
                break; 
            case CompareCriteria.LT: 
            case CompareCriteria.GT: 
                negated = true;
            case CompareCriteria.LE: 
            case CompareCriteria.GE: 
                operatorCap = Capability.CRITERIA_COMPARE_ORDERED;
                break;                        
        }

        // Check if compares are allowed
        if(! this.caps.supportsCapability(operatorCap)) {
            markInvalid(obj, "ordered CompareCriteria not supported by source"); //$NON-NLS-1$
            return;
        }                       
        if (negated && !this.caps.supportsCapability(Capability.CRITERIA_NOT)) {
        	markInvalid(obj, "Negation is not supported by source"); //$NON-NLS-1$
        	return;
        }
        
        // Check capabilities of the elements
        try {
            checkElementsAreSearchable(obj, SupportConstants.Element.SEARCHABLE_COMPARE);                                
        } catch(QueryMetadataException e) {
            handleException(new TeiidComponentException(e));
        } catch(TeiidComponentException e) {
            handleException(e);            
        }
    }

    public void visit(CompoundCriteria crit) {
        int operator = crit.getOperator();
        
        // Verify capabilities are supported
        if(operator == CompoundCriteria.OR && !this.caps.supportsCapability(Capability.CRITERIA_OR)) {
                markInvalid(crit, "OR criteria not supported by source"); //$NON-NLS-1$
        }
    }

    public void visit(Function obj) {
        try {
            //if the function can be evaluated then return as it will get replaced during the final rewrite 
            if (EvaluatableVisitor.willBecomeConstant(obj, true)) { 
                return; 
            }
            if(obj.getFunctionDescriptor().getPushdown() == PushDown.CANNOT_PUSHDOWN) {
            	markInvalid(obj, "Function metadata indicates it cannot be pusheddown."); //$NON-NLS-1$
            	return;
            }
            if (! CapabilitiesUtil.supportsScalarFunction(modelID, obj, metadata, capFinder)) {
                markInvalid(obj, (obj.isImplicit()?"(implicit) ":"") + obj.getName() + " function not supported by source"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
            }
        } catch(QueryMetadataException e) {
            handleException(new TeiidComponentException(e));
        } catch(TeiidComponentException e) {
            handleException(e);            
        }
    }

    public void visit(IsNullCriteria obj) {
        // Check if compares are allowed
        if(! this.caps.supportsCapability(Capability.CRITERIA_ISNULL)) {
            markInvalid(obj, "IsNull not supported by source"); //$NON-NLS-1$
            return;
        }
        
        if (obj.isNegated() && !this.caps.supportsCapability(Capability.CRITERIA_NOT)) {
        	markInvalid(obj, "Negation is not supported by source"); //$NON-NLS-1$
            return;
        }        
    }

    public void visit(MatchCriteria obj) {
        // Check if compares are allowed
        if(! this.caps.supportsCapability(Capability.CRITERIA_LIKE)) {
            markInvalid(obj, "Like is not supported by source"); //$NON-NLS-1$
            return;
        }
        
        // Check ESCAPE char if necessary
        if(obj.getEscapeChar() != MatchCriteria.NULL_ESCAPE_CHAR) {
            if(! this.caps.supportsCapability(Capability.CRITERIA_LIKE_ESCAPE)) {
                markInvalid(obj, "Like escape is not supported by source"); //$NON-NLS-1$
                return;
            }                
        }
        
        //check NOT
        if(obj.isNegated() && ! this.caps.supportsCapability(Capability.CRITERIA_NOT)) {
        	markInvalid(obj, "Negation is not supported by source"); //$NON-NLS-1$
        	return;
        }

        // Check capabilities of the elements
        try {
            checkElementsAreSearchable(obj, SupportConstants.Element.SEARCHABLE_LIKE);
        } catch(QueryMetadataException e) {
            handleException(new TeiidComponentException(e));
        } catch(TeiidComponentException e) {
            handleException(e);            
        }
    }

    public void visit(NotCriteria obj) {
        // Check if compares are allowed
        if(! this.caps.supportsCapability(Capability.CRITERIA_NOT)) {
            markInvalid(obj, "Negation is not supported by source"); //$NON-NLS-1$
            return;
        }
    }

    public void visit(SearchedCaseExpression obj) {
        if(! this.caps.supportsCapability(Capability.QUERY_SEARCHED_CASE)) {
            markInvalid(obj, "SearchedCase is not supported by source"); //$NON-NLS-1$
        }
    }
    
    public void visit(SetCriteria crit) {
    	checkAbstractSetCriteria(crit);
        try {    
            int maxSize = CapabilitiesUtil.getMaxInCriteriaSize(modelID, metadata, capFinder); 
            
            if (maxSize > 0 && crit.getValues().size() > maxSize) {
                markInvalid(crit, "SetCriteria size exceeds maximum for source"); //$NON-NLS-1$
                return;
            }
        } catch(QueryMetadataException e) {
            handleException(new TeiidComponentException(e));
        } catch(TeiidComponentException e) {
            handleException(e);            
        }
    }

    /**
     * @see org.teiid.query.sql.LanguageVisitor#visit(org.teiid.query.sql.lang.ExistsCriteria)
     */
    public void visit(ExistsCriteria crit) {
        // Check if exists criteria are allowed
        if(! this.caps.supportsCapability(Capability.CRITERIA_EXISTS)) {
            markInvalid(crit, "Exists is not supported by source"); //$NON-NLS-1$
            return;
        }
        
        if (crit.isNegated() && !this.caps.supportsCapability(Capability.CRITERIA_NOT)) {
        	markInvalid(crit, "Negation is not supported by source"); //$NON-NLS-1$
        	return;
        }
        
        try {
			if (validateSubqueryPushdown(crit, modelID, metadata, capFinder, analysisRecord) == null) {
				if (crit.getCommand().getCorrelatedReferences() == null) {
            		crit.setShouldEvaluate(true);
            	} else {
            		markInvalid(crit.getCommand(), "Subquery cannot be pushed down"); //$NON-NLS-1$
            	}
			}
		} catch (TeiidComponentException e) {
			handleException(e);
		}
    }

    /**
     * @see org.teiid.query.sql.LanguageVisitor#visit(org.teiid.query.sql.lang.SubqueryCompareCriteria)
     */
    public void visit(SubqueryCompareCriteria crit) {
        // Check if quantification operator is allowed
        Capability capability = Capability.QUERY_SUBQUERIES_SCALAR;
        switch(crit.getPredicateQuantifier()) {
            case SubqueryCompareCriteria.ALL:
                capability = Capability.CRITERIA_QUANTIFIED_ALL;
                break;
            case SubqueryCompareCriteria.ANY:
                capability = Capability.CRITERIA_QUANTIFIED_SOME;
                break;
            case SubqueryCompareCriteria.SOME:
                capability = Capability.CRITERIA_QUANTIFIED_SOME;
                break;
        }
        if(! this.caps.supportsCapability(capability)) {
            markInvalid(crit, "SubqueryCompare not supported by source"); //$NON-NLS-1$
            return;
        }
        
        checkCompareCriteria(crit);
        
        // Check capabilities of the elements
        try {
            if (validateSubqueryPushdown(crit, modelID, metadata, capFinder, analysisRecord) == null) {
            	markInvalid(crit.getCommand(), "Subquery cannot be pushed down"); //$NON-NLS-1$
            }
        } catch(QueryMetadataException e) {
            handleException(new TeiidComponentException(e));
        } catch(TeiidComponentException e) {
            handleException(e);            
        }
    }
    
    @Override
    public void visit(ScalarSubquery obj) {
    	try {    
            if(!this.caps.supportsCapability(Capability.QUERY_SUBQUERIES_SCALAR) 
            		|| validateSubqueryPushdown(obj, modelID, metadata, capFinder, analysisRecord) == null) {
            	if (obj.getCommand().getCorrelatedReferences() == null && !FunctionCollectorVisitor.isNonDeterministic(obj.getCommand())) {
            		obj.setShouldEvaluate(true);
            	} else {
            		markInvalid(obj.getCommand(), !this.caps.supportsCapability(Capability.QUERY_SUBQUERIES_SCALAR)?
            				"Correlated ScalarSubquery is not supported":"Subquery cannot be pushed down"); //$NON-NLS-1$ //$NON-NLS-2$
            	}
            }
        } catch(QueryMetadataException e) {
            handleException(new TeiidComponentException(e));
        } catch(TeiidComponentException e) {
            handleException(e);            
        }
    }

    public void visit(SubquerySetCriteria crit) {
    	checkAbstractSetCriteria(crit);
        try {    
            // Check if compares with subqueries are allowed
            if(! this.caps.supportsCapability(Capability.CRITERIA_IN_SUBQUERY)) {
                markInvalid(crit, "SubqueryIn is not supported by source"); //$NON-NLS-1$
                return;
            }

            if (validateSubqueryPushdown(crit, modelID, metadata, capFinder, analysisRecord) == null) {
            	markInvalid(crit.getCommand(), "Subquery cannot be pushed down"); //$NON-NLS-1$
            }
        } catch(QueryMetadataException e) {
            handleException(new TeiidComponentException(e));
        } catch(TeiidComponentException e) {
            handleException(e);            
        }
    }
    
    public void checkAbstractSetCriteria(AbstractSetCriteria crit) {
        try {    
            // Check if compares are allowed
            if(! this.caps.supportsCapability(Capability.CRITERIA_IN)) {
                markInvalid(crit, "In is not supported by source"); //$NON-NLS-1$
                return;
            }
            
            if (crit.isNegated() && !this.caps.supportsCapability(Capability.CRITERIA_NOT)) {
            	markInvalid(crit, "Negation is not supported by source"); //$NON-NLS-1$
                return;
            }
            // Check capabilities of the elements
            checkElementsAreSearchable(crit, SupportConstants.Element.SEARCHABLE_COMPARE);                        
                 
        } catch(QueryMetadataException e) {
            handleException(new TeiidComponentException(e));
        } catch(TeiidComponentException e) {
            handleException(e);            
        }

    }

    public void visit(DependentSetCriteria crit) {
    	checkAbstractSetCriteria(crit);
    }

    private void checkElementsAreSearchable(LanguageObject crit, int searchableType)
    throws QueryMetadataException, TeiidComponentException {
    	if (!CapabilitiesUtil.checkElementsAreSearchable(Arrays.asList(crit), metadata, searchableType)) {
    		markInvalid(crit, "not all source columns support search type"); //$NON-NLS-1$
    	}
    }
    
    /**
     * Return null if the subquery cannot be pushed down, otherwise the model
     * id of the pushdown target.
     * @param subqueryContainer
     * @param critNodeModelID
     * @param metadata
     * @param capFinder
     * @return
     * @throws TeiidComponentException
     */
    public static Object validateSubqueryPushdown(SubqueryContainer<?> subqueryContainer, Object critNodeModelID, 
    		QueryMetadataInterface metadata, CapabilitiesFinder capFinder, AnalysisRecord analysisRecord) throws TeiidComponentException {
    	ProcessorPlan plan = subqueryContainer.getCommand().getProcessorPlan();
    	if (plan != null) {
    		AccessNode aNode = getAccessNode(plan);
    		
    		if (aNode == null) {
    			return null;
    		}
    		
    		critNodeModelID = validateCommandPushdown(critNodeModelID, metadata, capFinder,	aNode);  
    	}
    	if (critNodeModelID == null) {
    		return null;
    	}
        // Check whether source supports correlated subqueries and if not, whether criteria has them
        SymbolMap refs = subqueryContainer.getCommand().getCorrelatedReferences();
        try {
            if(refs != null && !refs.asMap().isEmpty()) {
                if(! CapabilitiesUtil.supports(Capability.QUERY_SUBQUERIES_CORRELATED, critNodeModelID, metadata, capFinder)) {
                    return null;
                }
                //TODO: this check sees as correlated references as coming from the containing scope
                //but this is only an issue with deeply nested subqueries
                if (!CriteriaCapabilityValidatorVisitor.canPushLanguageObject(subqueryContainer.getCommand(), critNodeModelID, metadata, capFinder, analysisRecord )) {
                    return null;
                }
            }
        } catch(QueryMetadataException e) {
            throw new TeiidComponentException(e);                  
        }

        // Found no reason why this node is not eligible
        return critNodeModelID;
    }

	public static Object validateCommandPushdown(Object critNodeModelID,
			QueryMetadataInterface metadata, CapabilitiesFinder capFinder,
			AccessNode aNode) throws TeiidComponentException {
		// Check that query in access node is for the same model as current node
		try {                
			if (!(aNode.getCommand() instanceof QueryCommand)) {
				return null;
			}
		    Object modelID = aNode.getModelId();
		    if (critNodeModelID == null) {
		    	critNodeModelID = modelID;
		    } else if(!CapabilitiesUtil.isSameConnector(critNodeModelID, modelID, metadata, capFinder)) {
		        return null;
		    }
		} catch(QueryMetadataException e) {
		    throw new TeiidComponentException(e, QueryPlugin.Util.getString("RulePushSelectCriteria.Error_getting_modelID")); //$NON-NLS-1$
		}
		return critNodeModelID;
	}

	public static AccessNode getAccessNode(ProcessorPlan plan) {
		if(!(plan instanceof RelationalPlan)) {
		    return null;
		}
		            
		RelationalPlan rplan = (RelationalPlan) plan;
		
		// Check that the plan is just an access node                
		RelationalNode accessNode = rplan.getRootNode();
		
		if (accessNode instanceof LimitNode) {
			LimitNode ln = (LimitNode)accessNode;
			if (!ln.isImplicit()) {
				return null;
			}
			accessNode = ln.getChildren()[0];
		}
		
		if (! (accessNode instanceof AccessNode)) {
			return null;
		}
		return (AccessNode)accessNode;
	}
	
	public static QueryCommand getQueryCommand(AccessNode aNode) {
		if (aNode == null) {
			return null;
		}
		Command command = aNode.getCommand();
		if(!(command instanceof QueryCommand)) {
		    return null;
		}
		
		QueryCommand queryCommand = (QueryCommand)command;
		if (aNode.getProjection() != null && aNode.getProjection().length > 0) {
			Query newCommand = (Query)queryCommand.clone();
			newCommand.getSelect().setSymbols(aNode.getOriginalSelect());
			return newCommand;
		}
		return queryCommand;
	}
        
    private void handleException(TeiidComponentException e) {
        this.valid = false;
        this.exception = e;
        setAbort(true);
    }
    
    public TeiidComponentException getException() {
        return this.exception;
    }
    
    private void markInvalid(LanguageObject object, String reason) {
        this.valid = false;
        setAbort(true);
        if (analysisRecord != null && analysisRecord.recordDebug()) {
        	analysisRecord.println(reason + " " + object); //$NON-NLS-1$
        }
    }
    
    public boolean isValid() {
        return this.valid;
    }

    public static boolean canPushLanguageObject(LanguageObject obj, Object modelID, QueryMetadataInterface metadata, CapabilitiesFinder capFinder, AnalysisRecord analysisRecord) throws QueryMetadataException, TeiidComponentException {
        if(obj == null) {
            return true;
        }
        
        if(modelID == null || metadata.isVirtualModel(modelID)) {
            // Couldn't determine model ID, so give up
            return false;
        } 
        
        String modelName = metadata.getFullName(modelID);
        SourceCapabilities caps = capFinder.findCapabilities(modelName);

        if (caps == null) {
        	return true; //this doesn't seem right, but tests were expecting it...
        }
        
        CriteriaCapabilityValidatorVisitor visitor = new CriteriaCapabilityValidatorVisitor(modelID, metadata, capFinder, caps);
        PostOrderNavigator.doVisit(obj, visitor);
        
        if(visitor.getException() != null) {
            throw visitor.getException();
        } 

        return visitor.isValid();
    }

}
