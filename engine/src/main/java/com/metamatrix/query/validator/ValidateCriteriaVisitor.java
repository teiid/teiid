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

package com.metamatrix.query.validator;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import com.metamatrix.api.exception.MetaMatrixComponentException;
import com.metamatrix.api.exception.query.QueryMetadataException;
import com.metamatrix.query.QueryPlugin;
import com.metamatrix.query.metadata.SupportConstants;
import com.metamatrix.query.sql.lang.AbstractCompareCriteria;
import com.metamatrix.query.sql.lang.BetweenCriteria;
import com.metamatrix.query.sql.lang.CompareCriteria;
import com.metamatrix.query.sql.lang.Criteria;
import com.metamatrix.query.sql.lang.DependentSetCriteria;
import com.metamatrix.query.sql.lang.IsNullCriteria;
import com.metamatrix.query.sql.lang.MatchCriteria;
import com.metamatrix.query.sql.lang.Query;
import com.metamatrix.query.sql.lang.SetCriteria;
import com.metamatrix.query.sql.lang.SubqueryCompareCriteria;
import com.metamatrix.query.sql.lang.SubquerySetCriteria;
import com.metamatrix.query.sql.symbol.ElementSymbol;
import com.metamatrix.query.sql.symbol.Expression;
import com.metamatrix.query.sql.symbol.GroupSymbol;
import com.metamatrix.query.sql.visitor.ElementCollectorVisitor;
import com.metamatrix.query.util.ErrorMessageKeys;

/**
 * Validates that the elements of various criteria are allowed (by metadata)
 * to be used in the criteria in the way that they are being used.
 */
public class ValidateCriteriaVisitor extends AbstractValidationVisitor {

    // State during validation
    private boolean isXML = false;	// only used for Query commands

    public ValidateCriteriaVisitor() {
        super();
    }

    public void reset() {
        super.reset();
        this.isXML = false;
    }

    // ############### Visitor methods for language objects ##################

    public void visit(Query obj) {
        if(isXMLCommand(obj)) {
            this.isXML = true;
        } 
    }

    public void visit(BetweenCriteria obj) {
        checkUncomparableType(obj.getExpression());
        validateCompareElements(obj);
    }

    public void visit(CompareCriteria obj) {        
        validateCompareCriteria(obj);
    }

    private void validateCompareCriteria(AbstractCompareCriteria obj) {
        checkUncomparableTypes(obj.getLeftExpression(), obj.getRightExpression());
        validateCompareElements(obj);
    }

    public void visit(IsNullCriteria obj) {
        validateCompareElements(obj);
    }

    public void visit(MatchCriteria obj) {
        validateLikeElements(obj);
    }

    public void visit(SetCriteria obj) {
        checkUncomparableType(obj.getExpression());
        validateCompareElements(obj);
    }

    public void visit(SubquerySetCriteria obj) {
        checkUncomparableType(obj.getExpression());
        validateCompareElements(obj);
    }

    public void visit(DependentSetCriteria obj) {
        checkUncomparableType(obj.getExpression());
        validateCompareElements(obj);
    }
    
    public void visit(SubqueryCompareCriteria obj) {
        validateCompareCriteria(obj);
    }

    protected void validateCompareElements(Criteria obj) {
        if(isXML) {
            return;
        }

        Collection elements = ElementCollectorVisitor.getElements(obj, true);        
        Collection badCompareVars = validateElementsSupport(filterPhysicalElements(elements), SupportConstants.Element.SEARCHABLE_COMPARE );

        if(badCompareVars != null) {
            handleValidationError(QueryPlugin.Util.getString("ValidateCriteriaVistitor.element_not_comparable", badCompareVars), badCompareVars); //$NON-NLS-1$
        }
    }

    private void checkUncomparableType(Expression expression) {
        if(ValidationVisitor.isNonComparable(expression)) {
            handleValidationError(QueryPlugin.Util.getString(ErrorMessageKeys.VALIDATOR_0027, expression), expression);            
        }
    }

    private void checkUncomparableTypes(Expression leftExpr, Expression rightExpr) {
        List uncomparableExpr = null;
        if(ValidationVisitor.isNonComparable(leftExpr)) {
            uncomparableExpr = new ArrayList();
            uncomparableExpr.add(leftExpr);
        }
        if(ValidationVisitor.isNonComparable(rightExpr)) {
            if(uncomparableExpr == null) {
                uncomparableExpr = new ArrayList();
            }
            uncomparableExpr.add(rightExpr);
        }
        
        if(uncomparableExpr != null) {
            handleValidationError(QueryPlugin.Util.getString(ErrorMessageKeys.VALIDATOR_0027, uncomparableExpr), uncomparableExpr);                        
        }
    }

    protected void validateLikeElements(MatchCriteria obj) {
    	if(isXML) {
    		return;
    	}

        Collection badLikeVars = validateElementsSupport(
            filterPhysicalElements(ElementCollectorVisitor.getElements(obj.getLeftExpression(), true)),
            SupportConstants.Element.SEARCHABLE_LIKE );

		if(badLikeVars != null) {
            handleValidationError(QueryPlugin.Util.getString(ErrorMessageKeys.VALIDATOR_0028, badLikeVars), badLikeVars);
		}
    }

    /**
     * Filter out physical elements as criteria restrictions on physical elements 
     * are stating the capabilities of the source, not access restrictions.
     * @param elements Collection of ElementSymbol
     * @return Collection of ElementSymbol with physical elements filtered out 
     */
    private Collection filterPhysicalElements(Collection allElements) {
        List filtered = new ArrayList();

        try {        
            Iterator iter = allElements.iterator();
            while(iter.hasNext()) {
                ElementSymbol elem = (ElementSymbol) iter.next();
                GroupSymbol group = elem.getGroupSymbol();                
                if(getMetadata().isVirtualGroup(group.getMetadataID())) {
                    filtered.add(elem);                    
                }
            }
        } catch(QueryMetadataException e) {
            handleException(e);
        } catch(MetaMatrixComponentException e) {
            handleException(e);
        }                  

        return filtered;
    }

}
