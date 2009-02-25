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

package org.teiid.connector.jdbc.translator;

import java.util.List;
import java.util.Map;

import com.metamatrix.connector.api.ConnectorException;
import com.metamatrix.connector.api.ExecutionContext;
import com.metamatrix.connector.language.IAggregate;
import com.metamatrix.connector.language.ICompareCriteria;
import com.metamatrix.connector.language.IExpression;
import com.metamatrix.connector.language.IFunction;
import com.metamatrix.connector.language.IGroupBy;
import com.metamatrix.connector.language.IInCriteria;
import com.metamatrix.connector.language.IInlineView;
import com.metamatrix.connector.language.IInsert;
import com.metamatrix.connector.language.IIsNullCriteria;
import com.metamatrix.connector.language.ILikeCriteria;
import com.metamatrix.connector.language.IQueryCommand;
import com.metamatrix.connector.language.IScalarSubquery;
import com.metamatrix.connector.language.ISearchedCaseExpression;
import com.metamatrix.connector.language.ISelectSymbol;
import com.metamatrix.connector.language.ISubqueryCompareCriteria;
import com.metamatrix.connector.language.ISubqueryInCriteria;
import com.metamatrix.connector.visitor.framework.AbstractLanguageVisitor;
import com.metamatrix.core.MetaMatrixRuntimeException;

/**
 */
public class ReplacementVisitor extends AbstractLanguageVisitor {

    private Translator translator;  
    private Map<String, FunctionModifier> functionModifiers;
    private ExecutionContext context;

    /**
     * Set the functon modifiers.  
     * @param Map of function names to function modifiers.
     */
    public ReplacementVisitor(ExecutionContext context, Translator translator){
        this.translator = translator;
        this.functionModifiers = translator.getFunctionModifiers();
        this.context = context;
    }
            
    /* 
     * @see com.metamatrix.data.visitor.LanguageObjectVisitor#visit(com.metamatrix.data.language.IAggregate)
     */
    public void visit(IAggregate obj) {
        obj.setExpression(replaceFunction(obj.getExpression()));
    }
    
    public void visit(IInlineView obj) {
    	try {
			obj.setQuery((IQueryCommand)translator.modifyCommand(obj.getQuery(), context));
		} catch (ConnectorException e) {
			throw new MetaMatrixRuntimeException(e);
		}
    }

    /* 
     * @see com.metamatrix.data.visitor.LanguageObjectVisitor#visit(com.metamatrix.data.language.ICompareCriteria)
     */
    public void visit(ICompareCriteria obj) {
        obj.setLeftExpression(replaceFunction(obj.getLeftExpression()));
        obj.setRightExpression(replaceFunction(obj.getRightExpression()));
    }

    /**
     * @see com.metamatrix.data.visitor.LanguageObjectVisitor#visit(com.metamatrix.connector.language.IFunction)
     */
    public void visit(IFunction obj) {
        List<IExpression> args = obj.getParameters();
        for(int i=0; i<args.size(); i++) {
            args.set(i, replaceFunction(args.get(i)));
        }
    }    
    
    /** 
     * @see com.metamatrix.connector.visitor.framework.HierarchyVisitor#visit(com.metamatrix.connector.language.IGroupBy)
     * @since 4.3
     */
    public void visit(IGroupBy obj) {
        List<IExpression> expressions = obj.getElements();
        
        for (int i=0; i<expressions.size(); i++) {
            IExpression expression = (IExpression)expressions.get(i);
            expressions.set(i, replaceFunction(expression));
        }
    }      

    /* 
     * @see com.metamatrix.data.visitor.LanguageObjectVisitor#visit(com.metamatrix.data.language.IInCriteria)
     */
    public void visit(IInCriteria obj) {
        obj.setLeftExpression(replaceFunction(obj.getLeftExpression()));
        List<IExpression> rightExprs = obj.getRightExpressions();
        
        for(int i=0; i<rightExprs.size(); i++) {
            IExpression expr = (IExpression) rightExprs.get(i);
            rightExprs.set(i, replaceFunction(expr));
        }
    }

    /**
     * @see com.metamatrix.data.visitor.SQLStringVisitor#visit(com.metamatrix.connector.language.IInsert)
     */
    public void visit(IInsert obj) {
        List<IExpression> values = obj.getValues();
        
        for(int i=0; i<values.size(); i++) {
            IExpression expr = (IExpression) values.get(i);
            values.set(i, replaceFunction(expr));
        }
    }  
    
    /* 
     * @see com.metamatrix.data.visitor.LanguageObjectVisitor#visit(com.metamatrix.data.language.IIsNullCriteria)
     */
    public void visit(IIsNullCriteria obj) {
        obj.setExpression(replaceFunction(obj.getExpression()));
    }

    /* 
     * @see com.metamatrix.data.visitor.LanguageObjectVisitor#visit(com.metamatrix.data.language.ILikeCriteria)
     */
    public void visit(ILikeCriteria obj) {
        obj.setLeftExpression(replaceFunction(obj.getLeftExpression()));
        obj.setRightExpression(replaceFunction(obj.getRightExpression()));
    }

    /* 
     * @see com.metamatrix.data.visitor.LanguageObjectVisitor#visit(com.metamatrix.data.language.ISearchedCaseExpression)
     */
    public void visit(ISearchedCaseExpression obj) {
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
        obj.setExpression(replaceFunction(obj.getExpression()));
    }

    /* 
     * @see com.metamatrix.data.visitor.LanguageObjectVisitor#visit(com.metamatrix.data.language.ISubqueryCompareCriteria)
     */
    public void visit(ISubqueryCompareCriteria obj) {
        try {
			obj.setQuery((IQueryCommand)translator.modifyCommand(obj.getQuery(), context));
		} catch (ConnectorException e) {
			throw new MetaMatrixRuntimeException(e);
		}
    	obj.setLeftExpression(replaceFunction(obj.getLeftExpression()));
    }
    
    @Override
    public void visit(IScalarSubquery obj) {
    	try {
			obj.setQuery((IQueryCommand)translator.modifyCommand(obj.getQuery(), context));
		} catch (ConnectorException e) {
			throw new MetaMatrixRuntimeException(e);
		}
    }

    /* 
     * @see com.metamatrix.data.visitor.LanguageObjectVisitor#visit(com.metamatrix.data.language.ISubqueryInCriteria)
     */
    public void visit(ISubqueryInCriteria obj) {
    	try {
			obj.setQuery((IQueryCommand)translator.modifyCommand(obj.getQuery(), context));
		} catch (ConnectorException e) {
			throw new MetaMatrixRuntimeException(e);
		}
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
