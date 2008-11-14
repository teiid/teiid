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
import java.util.Iterator;

import com.metamatrix.api.exception.MetaMatrixComponentException;
import com.metamatrix.api.exception.query.QueryMetadataException;
import com.metamatrix.query.function.metadata.FunctionMethod;
import com.metamatrix.query.metadata.QueryMetadataInterface;
import com.metamatrix.query.metadata.SupportConstants;
import com.metamatrix.query.optimizer.capabilities.CapabilitiesFinder;
import com.metamatrix.query.optimizer.capabilities.SourceCapabilities;
import com.metamatrix.query.optimizer.capabilities.SourceCapabilities.Capability;
import com.metamatrix.query.sql.LanguageObject;
import com.metamatrix.query.sql.LanguageVisitor;
import com.metamatrix.query.sql.lang.BetweenCriteria;
import com.metamatrix.query.sql.lang.CompareCriteria;
import com.metamatrix.query.sql.lang.CompoundCriteria;
import com.metamatrix.query.sql.lang.DependentSetCriteria;
import com.metamatrix.query.sql.lang.ExistsCriteria;
import com.metamatrix.query.sql.lang.IsNullCriteria;
import com.metamatrix.query.sql.lang.MatchCriteria;
import com.metamatrix.query.sql.lang.NotCriteria;
import com.metamatrix.query.sql.lang.PredicateCriteria;
import com.metamatrix.query.sql.lang.SetCriteria;
import com.metamatrix.query.sql.lang.SubqueryCompareCriteria;
import com.metamatrix.query.sql.lang.SubquerySetCriteria;
import com.metamatrix.query.sql.navigator.PreOrderNavigator;
import com.metamatrix.query.sql.symbol.AggregateSymbol;
import com.metamatrix.query.sql.symbol.CaseExpression;
import com.metamatrix.query.sql.symbol.ElementSymbol;
import com.metamatrix.query.sql.symbol.Function;
import com.metamatrix.query.sql.symbol.SearchedCaseExpression;
import com.metamatrix.query.sql.visitor.ElementCollectorVisitor;
import com.metamatrix.query.sql.visitor.EvaluateExpressionVisitor;

/**
 */
public class CriteriaCapabilityValidatorVisitor extends LanguageVisitor {

    // Initialization state
    private Object modelID;
    private QueryMetadataInterface metadata;
    private CapabilitiesFinder capFinder;

    // Retrieved during initialization and cached
    private SourceCapabilities caps;
    
    // Output state
    private MetaMatrixComponentException exception;
    private boolean valid = true;

    /**
     * @param iterator
     */
    public CriteriaCapabilityValidatorVisitor(Object modelID, QueryMetadataInterface metadata, CapabilitiesFinder capFinder) {        
        this.modelID = modelID;
        this.metadata = metadata;
        this.capFinder = capFinder;
        loadCapabilities();
    }
    
    /**
     * Load the capabilities and cache locally once they are loaded.
     */
    private void loadCapabilities() {
        if(capFinder != null) {
            try {
                String modelName = metadata.getFullName(modelID);
                caps = capFinder.findCapabilities(modelName);
                
            } catch(QueryMetadataException e) {
                handleException(new MetaMatrixComponentException(e));
            } catch(MetaMatrixComponentException e) {
                handleException(e);            
            }
        }          
    }
    
    public void visit(AggregateSymbol obj) {
        if(this.caps == null) {
            return;
        }
        
        try {
            if(! CapabilitiesUtil.supportsAggregateFunction(modelID, obj, metadata, capFinder)) {
                markInvalid();
            }         
        } catch(QueryMetadataException e) {
            handleException(new MetaMatrixComponentException(e));
        } catch(MetaMatrixComponentException e) {
            handleException(e);            
        }
    }
    
    public void visit(BetweenCriteria obj) {
        if (this.caps == null) {
            return;
        }
        
        if(! this.caps.supportsCapability(Capability.QUERY_WHERE)) {
            markInvalid();
            return;
        }

        // Check if compares are allowed
        if(! this.caps.supportsCapability(Capability.QUERY_WHERE_BETWEEN)) {
            markInvalid();
            return;
        }

        // Check capabilities of the elements
        try {
            checkElementsAreSearchable(obj, SupportConstants.Element.SEARCHABLE_COMPARE);
        } catch(QueryMetadataException e) {
            handleException(new MetaMatrixComponentException(e));
        } catch(MetaMatrixComponentException e) {
            handleException(e);            
        }
    }
    
    public void visit(CaseExpression obj) {
        if (this.caps == null) {
            return;
        }
         
        if(! this.caps.supportsCapability(Capability.QUERY_CASE)) {
            markInvalid();
        }
    }
    
    public void visit(CompareCriteria obj) {
        if(this.caps != null) {
            // Check if criteria are allowed
            if(! this.caps.supportsCapability(Capability.QUERY_WHERE)) {
                markInvalid();
                return;
            }

            // Check if compares are allowed
            if(! this.caps.supportsCapability(Capability.QUERY_WHERE_COMPARE)) {
                markInvalid();
                return;
            }
             
            // Check if operation is allowed
            Capability operatorCap = null;
            switch(obj.getOperator()) {
                case CompareCriteria.EQ: 
                    operatorCap = Capability.QUERY_WHERE_COMPARE_EQ;
                    break; 
                case CompareCriteria.NE: 
                    operatorCap = Capability.QUERY_WHERE_COMPARE_NE;
                    break; 
                case CompareCriteria.LT: 
                    operatorCap = Capability.QUERY_WHERE_COMPARE_LT;
                    break; 
                case CompareCriteria.GT: 
                    operatorCap = Capability.QUERY_WHERE_COMPARE_GT;
                    break; 
                case CompareCriteria.LE: 
                    operatorCap = Capability.QUERY_WHERE_COMPARE_LE;
                    break; 
                case CompareCriteria.GE: 
                    operatorCap = Capability.QUERY_WHERE_COMPARE_GE;
                    break;                        
            }

            // Check if compares are allowed
            if(! this.caps.supportsCapability(operatorCap)) {
                markInvalid();
            }                       
        }
        
        // Check capabilities of the elements
        try {
            checkElementsAreSearchable(obj, SupportConstants.Element.SEARCHABLE_COMPARE);                                
        } catch(QueryMetadataException e) {
            handleException(new MetaMatrixComponentException(e));
        } catch(MetaMatrixComponentException e) {
            handleException(e);            
        }
    }

    public void visit(CompoundCriteria crit) {
        int operator = crit.getOperator();
        
        // Verify capabilities are supported
        if(this.caps != null) {
            if(operator == CompoundCriteria.AND) {
                // Check if AND is allowed
                if(! this.caps.supportsCapability(Capability.QUERY_WHERE_AND)) {
                    markInvalid();
                    return;
                }       
            } else {
                // Check if OR is allowed
                if(! this.caps.supportsCapability(Capability.QUERY_WHERE_OR)) {
                    markInvalid();
                    return;
                }                       
            }
        }                
    }

    public void visit(Function obj) {
        try {
            //if the function can be evaluated then return as it will get replaced during the final rewrite 
            if (EvaluateExpressionVisitor.willBecomeConstant(obj)) { 
                return; 
            }
            if(obj.getFunctionDescriptor().getPushdown() == FunctionMethod.CANNOT_PUSHDOWN || ! CapabilitiesUtil.supportsScalarFunction(modelID, obj, metadata, capFinder)) {
                markInvalid();
            }
        } catch(QueryMetadataException e) {
            handleException(new MetaMatrixComponentException(e));
        } catch(MetaMatrixComponentException e) {
            handleException(e);            
        }
    }

    public void visit(IsNullCriteria obj) {
        if(this.caps != null) {
            // Check if criteria are allowed
            if(! this.caps.supportsCapability(Capability.QUERY_WHERE)) {
                markInvalid();
                return;
            }

            // Check if compares are allowed
            if(! this.caps.supportsCapability(Capability.QUERY_WHERE_ISNULL)) {
                markInvalid();
                return;
            }
            
            // Check capabilities of the elements
            try {
                checkElementsAreSearchable(obj, SupportConstants.Element.SEARCHABLE_COMPARE);
            } catch(QueryMetadataException e) {
                handleException(new MetaMatrixComponentException(e));
            } catch(MetaMatrixComponentException e) {
                handleException(e);            
            }
        }
    }

    public void visit(MatchCriteria obj) {
        if(this.caps != null) {
            // Check if criteria are allowed
            if(! this.caps.supportsCapability(Capability.QUERY_WHERE)) {
                markInvalid();
                return;
            }

            // Check if compares are allowed
            if(! this.caps.supportsCapability(Capability.QUERY_WHERE_LIKE)) {
                markInvalid();
                return;
            }
            
            // Check ESCAPE char if necessary
            if(obj.getEscapeChar() != MatchCriteria.NULL_ESCAPE_CHAR) {
                if(! this.caps.supportsCapability(Capability.QUERY_WHERE_LIKE_ESCAPE)) {
                    markInvalid();
                    return;
                }                
            }
            
            //check NOT
            if(obj.isNegated()) {
                if(! this.caps.supportsCapability(Capability.QUERY_WHERE_NOT)) {
                	markInvalid();
                	return;
                }   
            }

            // Check capabilities of the elements
            try {
                checkElementsAreSearchable(obj, SupportConstants.Element.SEARCHABLE_LIKE);
            } catch(QueryMetadataException e) {
                handleException(new MetaMatrixComponentException(e));
            } catch(MetaMatrixComponentException e) {
                handleException(e);            
            }
             
        }
    }

    public void visit(NotCriteria obj) {
        if(this.caps != null) {
            // Check if compares are allowed
            if(! this.caps.supportsCapability(Capability.QUERY_WHERE_NOT)) {
                markInvalid();
                return;
            }
        }
    }

    public void visit(SearchedCaseExpression obj) {
        if (this.caps == null) {
            return;
        }
         
        if(! this.caps.supportsCapability(Capability.QUERY_SEARCHED_CASE)) {
            markInvalid();
        }
    }
    
    public void visit(SetCriteria crit) {
        try {    
            if(this.caps != null) {
                // Check if criteria are allowed
                if(! this.caps.supportsCapability(Capability.QUERY_WHERE)) {
                    markInvalid();
                    return;
                }

                // Check if compares are allowed
                if(! this.caps.supportsCapability(Capability.QUERY_WHERE_IN)) {
                    markInvalid();
                    return;
                }
                
                int maxSize = CapabilitiesUtil.getMaxInCriteriaSize(modelID, metadata, capFinder); 
                
                if (maxSize > 0 && crit.getValues().size() > maxSize) {
                    markInvalid();
                    return;
                }
            }

            // Check capabilities of the elements
            checkElementsAreSearchable(crit, SupportConstants.Element.SEARCHABLE_COMPARE);                        
                 
        } catch(QueryMetadataException e) {
            handleException(new MetaMatrixComponentException(e));
        } catch(MetaMatrixComponentException e) {
            handleException(e);            
        }
    }

    /**
     * @see com.metamatrix.query.sql.LanguageVisitor#visit(com.metamatrix.query.sql.lang.ExistsCriteria)
     */
    public void visit(ExistsCriteria crit) {
        if(this.caps != null) {
            // Check if criteria are allowed
            if(! this.caps.supportsCapability(Capability.QUERY_WHERE)) {
                markInvalid();
                return;
            }

            // Check if exists criteria are allowed
            if(! this.caps.supportsCapability(Capability.QUERY_WHERE_EXISTS)) {
                markInvalid();
                return;
            }
        }                           
    }

    /**
     * @see com.metamatrix.query.sql.LanguageVisitor#visit(com.metamatrix.query.sql.lang.SubqueryCompareCriteria)
     */
    public void visit(SubqueryCompareCriteria crit) {
        if(this.caps != null) {
            // Check if criteria are allowed
            if(! this.caps.supportsCapability(Capability.QUERY_WHERE)) {
                markInvalid();
                return;
            }

            // Check if compares are allowed
            if(! this.caps.supportsCapability(Capability.QUERY_WHERE_COMPARE)) {
                markInvalid();
                return;
            }

            // Check if quantified compares are allowed
            if(! this.caps.supportsCapability(Capability.QUERY_WHERE_QUANTIFIED_COMPARISON)) {
                markInvalid();
                return;
            }

            // Check if quantification operator is allowed
            Capability capability = null;
            switch(crit.getPredicateQuantifier()) {
                case SubqueryCompareCriteria.ALL:
                    capability = Capability.QUERY_WHERE_QUANTIFIED_ALL;
                    break;
                case SubqueryCompareCriteria.ANY:
                    capability = Capability.QUERY_WHERE_QUANTIFIED_SOME;
                    break;
                case SubqueryCompareCriteria.SOME:
                    capability = Capability.QUERY_WHERE_QUANTIFIED_SOME;
                    break;
            }
            if(! this.caps.supportsCapability(capability)) {
                markInvalid();
                return;
            }
             
            // Check if operation is allowed
            Capability operatorCap = null;
            switch(crit.getOperator()) {
                case CompareCriteria.EQ: 
                    operatorCap = Capability.QUERY_WHERE_COMPARE_EQ;
                    break; 
                case CompareCriteria.NE: 
                    operatorCap = Capability.QUERY_WHERE_COMPARE_NE;
                    break; 
                case CompareCriteria.LT: 
                    operatorCap = Capability.QUERY_WHERE_COMPARE_LT;
                    break; 
                case CompareCriteria.GT: 
                    operatorCap = Capability.QUERY_WHERE_COMPARE_GT;
                    break; 
                case CompareCriteria.LE: 
                    operatorCap = Capability.QUERY_WHERE_COMPARE_LE;
                    break; 
                case CompareCriteria.GE: 
                    operatorCap = Capability.QUERY_WHERE_COMPARE_GE;
                    break;                        
            }

            // Check if compares are allowed
            if(! this.caps.supportsCapability(operatorCap)) {
                markInvalid();
            }                       
        }
        
        // Check capabilities of the elements
        try {
            checkElementsAreSearchable(crit, SupportConstants.Element.SEARCHABLE_COMPARE);                                
        } catch(QueryMetadataException e) {
            handleException(new MetaMatrixComponentException(e));
        } catch(MetaMatrixComponentException e) {
            handleException(e);            
        }
    }

    public void visit(SubquerySetCriteria crit) {
        try {    
            if(this.caps != null) {
                // Check if criteria are allowed
                if(! this.caps.supportsCapability(Capability.QUERY_WHERE)) {
                    markInvalid();
                    return;
                }

                // Check if compares are allowed
                if(! this.caps.supportsCapability(Capability.QUERY_WHERE_IN)) {
                    markInvalid();
                    return;
                }

                // Check if compares with subqueries are allowed
                if(! this.caps.supportsCapability(Capability.QUERY_WHERE_IN_SUBQUERY)) {
                    markInvalid();
                    return;
                }
            }

            // Check capabilities of the elements
            checkElementsAreSearchable(crit, SupportConstants.Element.SEARCHABLE_COMPARE);                        
                 
        } catch(QueryMetadataException e) {
            handleException(new MetaMatrixComponentException(e));
        } catch(MetaMatrixComponentException e) {
            handleException(e);            
        }
    }

    public void visit(DependentSetCriteria crit) {
        try {    
            if(this.caps != null) {
                // Check if criteria are allowed
                if(! this.caps.supportsCapability(Capability.QUERY_WHERE)) {
                    markInvalid();
                    return;
                }

                // Check if compares are allowed
                if(! this.caps.supportsCapability(Capability.QUERY_WHERE_IN)) {
                    markInvalid();
                    return;
                }
            }

            // Check capabilities of the elements
            checkElementsAreSearchable(crit, SupportConstants.Element.SEARCHABLE_COMPARE);                        
                 
        } catch(QueryMetadataException e) {
            handleException(new MetaMatrixComponentException(e));
        } catch(MetaMatrixComponentException e) {
            handleException(e);            
        }
    }

    /**
     * Validate that the elements are searchable and can be used in a criteria against this source.
     */
    private void checkElementsAreSearchable(PredicateCriteria crit, int searchableType) 
    throws QueryMetadataException, MetaMatrixComponentException {
        
        Collection elements = ElementCollectorVisitor.getElements(crit, false);         
        Iterator iter = elements.iterator();
        while(iter.hasNext()) {
            ElementSymbol element = (ElementSymbol) iter.next();
            if (!metadata.elementSupports(element.getMetadataID(), searchableType)) {
                markInvalid();
                return;
            }                
        }
    }
        
    private void handleException(MetaMatrixComponentException e) {
        this.valid = false;
        this.exception = e;
        setAbort(true);
    }
    
    public MetaMatrixComponentException getException() {
        return this.exception;
    }
    
    private void markInvalid() {
        this.valid = false;
        setAbort(true);
    }
    
    public boolean isValid() {
        return this.valid;
    }

    public static boolean canPushLanguageObject(LanguageObject obj, Object modelID, QueryMetadataInterface metadata, CapabilitiesFinder capFinder) throws QueryMetadataException, MetaMatrixComponentException {
        if(obj == null) {
            return true;
        }
        
        if(modelID == null || metadata.isVirtualModel(modelID)) {
            // Couldn't determine model ID, so give up
            return false;
        } 

        CriteriaCapabilityValidatorVisitor visitor = new CriteriaCapabilityValidatorVisitor(modelID, metadata, capFinder);
        PreOrderNavigator.doVisit(obj, visitor);
        
        if(visitor.getException() != null) {
            throw visitor.getException();
        } 

        return visitor.isValid();
    }

}
