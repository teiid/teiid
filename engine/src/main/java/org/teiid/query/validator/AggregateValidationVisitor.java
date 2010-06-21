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

package org.teiid.query.validator;

import java.util.Collection;
import java.util.Set;

import org.teiid.core.types.DataTypeManager;
import org.teiid.language.SQLConstants.NonReserved;
import org.teiid.language.SQLConstants.Reserved;
import org.teiid.query.QueryPlugin;
import org.teiid.query.sql.LanguageObject;
import org.teiid.query.sql.navigator.PreOrderNavigator;
import org.teiid.query.sql.symbol.AggregateSymbol;
import org.teiid.query.sql.symbol.CaseExpression;
import org.teiid.query.sql.symbol.ElementSymbol;
import org.teiid.query.sql.symbol.Expression;
import org.teiid.query.sql.symbol.ExpressionSymbol;
import org.teiid.query.sql.symbol.Function;
import org.teiid.query.sql.symbol.SearchedCaseExpression;
import org.teiid.query.sql.visitor.AggregateSymbolCollectorVisitor;
import org.teiid.query.sql.visitor.ElementCollectorVisitor;
import org.teiid.query.util.ErrorMessageKeys;



/** 
 * Validate that all ElementSymbol and ExpressionSymbols used in the HAVING 
 * and SELECT clauses are based on symbols used in the GROUP BY clause.
 * @since 4.2
 */
public class AggregateValidationVisitor extends AbstractValidationVisitor {
    
    private boolean validateBelow = true;

    // Symbols from GROUP BY - may be null if no group symbols
    private Set<Expression> groupExpressions;
    
    public AggregateValidationVisitor(Set<Expression> groupExpressions) {
        this.groupExpressions = groupExpressions;
    }
    
    public void visit(AggregateSymbol obj) {
        Expression aggExp = obj.getExpression();

        // Check for any nested aggregates (which are not allowed)
        if(aggExp != null) {
            Collection<AggregateSymbol> nestedAggs = AggregateSymbolCollectorVisitor.getAggregates(aggExp, true);
            if(nestedAggs.size() > 0) {
                handleValidationError(QueryPlugin.Util.getString(ErrorMessageKeys.VALIDATOR_0039, nestedAggs), nestedAggs);
            }
        }
        
        // Verify data type of aggregate expression
        String aggregateFunction = obj.getAggregateFunction();
        if((aggregateFunction.equals(NonReserved.SUM) || aggregateFunction.equals(NonReserved.AVG)) && obj.getType() == null) {
            handleValidationError(QueryPlugin.Util.getString(ErrorMessageKeys.VALIDATOR_0041, new Object[] {aggregateFunction, obj}), obj);
        } else if (obj.getType() != DataTypeManager.DefaultDataClasses.NULL) {
        	if (aggregateFunction.equals(Reserved.XMLAGG) && aggExp.getType() != DataTypeManager.DefaultDataClasses.XML) {
        		handleValidationError(QueryPlugin.Util.getString("AggregateValidationVisitor.non_xml", new Object[] {aggregateFunction, obj}), obj); //$NON-NLS-1$
        	}
        }
        if((obj.isDistinct() || aggregateFunction.equals(NonReserved.MIN) || aggregateFunction.equals(NonReserved.MAX)) && DataTypeManager.isNonComparable(DataTypeManager.getDataTypeName(aggExp.getType()))) {
    		handleValidationError(QueryPlugin.Util.getString("AggregateValidationVisitor.non_comparable", new Object[] {aggregateFunction, obj}), obj); //$NON-NLS-1$
        }
        validateBelow = false;
    }
    
    public void visit(ElementSymbol obj) {
        validateExpression(obj);
    }
    
    public void visit(ExpressionSymbol obj) {
        validateExpression(obj);
    }
    
    public void visit(CaseExpression obj) {
        validateExpression(obj);
    }
    
    public void visit(SearchedCaseExpression obj) {
        validateExpression(obj);
    }
    
    public void visit(Function obj) {
        validateExpression(obj);
    }
    
    private void validateExpression(Expression symbol) {
        if (ElementCollectorVisitor.getElements(symbol, false).isEmpty()) {
            validateBelow = false;
            return;
        }
        
        if(groupExpressions == null) {
            if (symbol instanceof ElementSymbol) {
                handleValidationError(QueryPlugin.Util.getString(ErrorMessageKeys.VALIDATOR_0037, symbol), symbol);
            }
        } else if(! groupExpressions.contains(symbol)) {
            if (symbol instanceof ElementSymbol) {
                handleValidationError(QueryPlugin.Util.getString(ErrorMessageKeys.VALIDATOR_0038, symbol), symbol);
            }
        } else {
            validateBelow = false;
        }
    }
    
    public static void validate(LanguageObject obj, final AggregateValidationVisitor visitor) {
    	visitor.validateBelow = true;
        PreOrderNavigator nav = new PreOrderNavigator(visitor) {
            private boolean validateBelow;

            protected void visitNode(LanguageObject obj) {
                if (validateBelow) {
                    super.visitNode(obj);
                }
            }
            
            protected void preVisitVisitor(LanguageObject obj) {
                super.preVisitVisitor(obj);
                this.validateBelow = visitor.validateBelow;
            }
            
            protected void postVisitVisitor(LanguageObject obj) {
                this.validateBelow = true;
            }
        };
        obj.acceptVisitor(nav);
    }
    
}
