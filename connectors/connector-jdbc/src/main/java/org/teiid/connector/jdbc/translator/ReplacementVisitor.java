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

import org.teiid.connector.api.ConnectorException;
import org.teiid.connector.api.ExecutionContext;
import org.teiid.connector.language.IAggregate;
import org.teiid.connector.language.ICompareCriteria;
import org.teiid.connector.language.IExpression;
import org.teiid.connector.language.IFunction;
import org.teiid.connector.language.IGroupBy;
import org.teiid.connector.language.IInCriteria;
import org.teiid.connector.language.IInlineView;
import org.teiid.connector.language.IInsertExpressionValueSource;
import org.teiid.connector.language.IIsNullCriteria;
import org.teiid.connector.language.ILikeCriteria;
import org.teiid.connector.language.IQueryCommand;
import org.teiid.connector.language.IScalarSubquery;
import org.teiid.connector.language.ISearchedCaseExpression;
import org.teiid.connector.language.ISelectSymbol;
import org.teiid.connector.language.ISubqueryCompareCriteria;
import org.teiid.connector.language.ISubqueryInCriteria;
import org.teiid.connector.visitor.framework.AbstractLanguageVisitor;

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
     * @see com.metamatrix.data.visitor.LanguageObjectVisitor#visit(org.teiid.connector.language.IFunction)
     */
    public void visit(IFunction obj) {
        List<IExpression> args = obj.getParameters();
        for(int i=0; i<args.size(); i++) {
            args.set(i, replaceFunction(args.get(i)));
        }
    }    
    
    /** 
     * @see org.teiid.connector.visitor.framework.HierarchyVisitor#visit(org.teiid.connector.language.IGroupBy)
     * @since 4.3
     */
    public void visit(IGroupBy obj) {
        List<IExpression> expressions = obj.getElements();
        
        for (int i=0; i<expressions.size(); i++) {
            IExpression expression = expressions.get(i);
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
            IExpression expr = rightExprs.get(i);
            rightExprs.set(i, replaceFunction(expr));
        }
    }
    
    @Override
    public void visit(IInsertExpressionValueSource obj) {
        List<IExpression> values = obj.getValues();
        
        for(int i=0; i<values.size(); i++) {
            IExpression expr = values.get(i);
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
                FunctionModifier modifier = functionModifiers.get(key);
                
                // Modify function and return it
                return modifier.modify(function);                                        
            }
        } 
        
        // Fall through and return original expression        
        return expression;
    }    
    
}
