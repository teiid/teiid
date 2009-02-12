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

package com.metamatrix.connector.jdbc.util;

import java.util.*;

import com.metamatrix.connector.jdbc.extension.FunctionModifier;
import com.metamatrix.connector.language.*;
import com.metamatrix.connector.visitor.framework.HierarchyVisitor;

/**
 */
public class FunctionReplacementVisitor extends HierarchyVisitor {

    private Map functionModifiers;   

    /**
     * Set the functon modifiers.  
     * @param Map of function names to function modifiers.
     */
    public FunctionReplacementVisitor(Map functionModifiers){
        super();
        this.functionModifiers = functionModifiers;
    }
            
    /* 
     * @see com.metamatrix.data.visitor.LanguageObjectVisitor#visit(com.metamatrix.data.language.IAggregate)
     */
    public void visit(IAggregate obj) {
        super.visit(obj);
        obj.setExpression(replaceFunction(obj.getExpression()));
    }
    
    public void visit(IInlineView obj) {
        visitNode(obj.getQuery());
    }

    /* 
     * @see com.metamatrix.data.visitor.LanguageObjectVisitor#visit(com.metamatrix.data.language.ICaseExpression)
     */
    public void visit(ICaseExpression obj) {
        super.visit(obj);
        obj.setExpression(replaceFunction(obj.getExpression()));
        int whenCount = obj.getWhenCount();
        for(int i=0; i<whenCount; i++) {
            obj.setWhenExpression(i, replaceFunction(obj.getWhenExpression(i)));
        }
        for(int i=0; i<whenCount; i++) {
            obj.setThenExpression(i, replaceFunction(obj.getThenExpression(i)));
        }
        obj.setElseExpression(replaceFunction(obj.getElseExpression()));
    }

    /* 
     * @see com.metamatrix.data.visitor.LanguageObjectVisitor#visit(com.metamatrix.data.language.ICompareCriteria)
     */
    public void visit(ICompareCriteria obj) {
        super.visit(obj);
        obj.setLeftExpression(replaceFunction(obj.getLeftExpression()));
        obj.setRightExpression(replaceFunction(obj.getRightExpression()));
    }

    /**
     * @see com.metamatrix.data.visitor.LanguageObjectVisitor#visit(com.metamatrix.connector.language.IFunction)
     */
    public void visit(IFunction obj) {
        super.visit(obj);
        IExpression[] args = obj.getParameters();
        IExpression[] newArgs = new IExpression[args.length];
        for(int i=0; i<args.length; i++) {
            newArgs[i] = replaceFunction(args[i]);
        }
        obj.setParameters(newArgs);
    }    
    
    /** 
     * @see com.metamatrix.connector.visitor.framework.HierarchyVisitor#visit(com.metamatrix.connector.language.IGroupBy)
     * @since 4.3
     */
    public void visit(IGroupBy obj) {
        super.visit(obj);
        List expressions = obj.getElements();
        
        for (int i=0; i<expressions.size(); i++) {
            IExpression expression = (IExpression)expressions.get(i);
            expressions.set(i, replaceFunction(expression));
        }
        
        obj.setElements(expressions);
    }      

    /* 
     * @see com.metamatrix.data.visitor.LanguageObjectVisitor#visit(com.metamatrix.data.language.IInCriteria)
     */
    public void visit(IInCriteria obj) {
        super.visit(obj);
        obj.setLeftExpression(replaceFunction(obj.getLeftExpression()));
        List rightExprs = obj.getRightExpressions();
        List newRightExprs = new ArrayList(rightExprs.size());
        
        for(int i=0; i<rightExprs.size(); i++) {
            IExpression expr = (IExpression) rightExprs.get(i);
            newRightExprs.add(replaceFunction(expr));
        }
        obj.setRightExpressions(newRightExprs);
    }

    /**
     * @see com.metamatrix.data.visitor.SQLStringVisitor#visit(com.metamatrix.connector.language.IInsert)
     */
    public void visit(IInsert obj) {
        super.visit(obj);
        List values = obj.getValues();
        List newValues = new ArrayList(values.size());
        
        for(int i=0; i<values.size(); i++) {
            IExpression expr = (IExpression) values.get(i);
            newValues.add(replaceFunction(expr));
        }
        obj.setValues(newValues);        
    }  

    /* 
     * @see com.metamatrix.data.visitor.LanguageObjectVisitor#visit(com.metamatrix.data.language.IIsNullCriteria)
     */
    public void visit(IIsNullCriteria obj) {
        super.visit(obj);
        obj.setExpression(replaceFunction(obj.getExpression()));
    }

    /* 
     * @see com.metamatrix.data.visitor.LanguageObjectVisitor#visit(com.metamatrix.data.language.ILikeCriteria)
     */
    public void visit(ILikeCriteria obj) {
        super.visit(obj);
        obj.setLeftExpression(replaceFunction(obj.getLeftExpression()));
        obj.setRightExpression(replaceFunction(obj.getRightExpression()));
    }

    /* 
     * @see com.metamatrix.data.visitor.LanguageObjectVisitor#visit(com.metamatrix.data.language.ISearchedCaseExpression)
     */
    public void visit(ISearchedCaseExpression obj) {
        super.visit(obj);
        int whenCount = obj.getWhenCount();
        for(int i=0; i<whenCount; i++) {
            obj.setThenExpression(i, replaceFunction(obj.getThenExpression(i)));
        }
        obj.setElseExpression(replaceFunction(obj.getElseExpression()));
    }

    /* 
     * @see com.metamatrix.data.visitor.LanguageObjectVisitor#visit(com.metamatrix.data.language.ISelectSymbol)
     */
    public void visit(ISelectSymbol obj) {
        super.visit(obj);
        obj.setExpression(replaceFunction(obj.getExpression()));
    }

    /* 
     * @see com.metamatrix.data.visitor.LanguageObjectVisitor#visit(com.metamatrix.data.language.ISubqueryCompareCriteria)
     */
    public void visit(ISubqueryCompareCriteria obj) {
        super.visit(obj);
        obj.setLeftExpression(replaceFunction(obj.getLeftExpression()));
    }

    /* 
     * @see com.metamatrix.data.visitor.LanguageObjectVisitor#visit(com.metamatrix.data.language.ISubqueryInCriteria)
     */
    public void visit(ISubqueryInCriteria obj) {
        super.visit(obj);
        obj.setLeftExpression(replaceFunction(obj.getLeftExpression()));
    }
         
    private IExpression replaceFunction(IExpression expression) {
        if(functionModifiers != null && expression != null && expression instanceof IFunction){
            // Look for function modifier
            IFunction function = (IFunction) expression;
            String key = function.getName().toLowerCase();        
            if(functionModifiers.containsKey(key)) {
                FunctionModifier modifier = (FunctionModifier) functionModifiers.get(key);
                
                // Modify function and return it
                return modifier.modify(function);                                        
            }
        } 
        
        // Fall through and return original expression        
        return expression;
    }    
    

}
