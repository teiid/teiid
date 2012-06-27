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

package org.teiid.translator.yahoo;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.teiid.language.Comparison;
import org.teiid.language.Condition;
import org.teiid.language.Expression;
import org.teiid.language.In;
import org.teiid.language.Literal;
import org.teiid.language.visitor.HierarchyVisitor;
import org.teiid.translator.TranslatorException;


/**
 */
public class TickerCollectorVisitor extends HierarchyVisitor {

    private Set<String> tickers = new HashSet<String>();
    private TranslatorException exception;

    public Set<String> getTickers() {
        return this.tickers;
    }
    
    public TranslatorException getException() {
        return this.exception;
    }

    public void visit(Comparison obj) {
        Expression expr = obj.getRightExpression();
        addTickerFromExpression(expr);        
    }

    public void visit(In obj) {
        List<Expression> exprs = obj.getRightExpressions();
        for (Expression expr : exprs) {
            addTickerFromExpression(expr);
        }
    }
    
    private void addTickerFromExpression(Expression expr) {
        if(expr instanceof Literal) {
            Literal literal = (Literal) expr;
            if(literal.getType() == String.class) {
                String ticker = (String) literal.getValue();
                this.tickers.add(ticker.toUpperCase());                
            } else {
                this.exception = new TranslatorException(YahooPlugin.Util.getString("TickerCollectorVisitor.Unexpected_type", literal.getType().getName())); //$NON-NLS-1$
            }
        } else {
            this.exception = new TranslatorException(YahooPlugin.Util.getString("TickerCollectorVisitor.Unexpected_expression", expr)); //$NON-NLS-1$
        }
         
    }
    
    public static Set<String> getTickers(Condition crit) throws TranslatorException {
        TickerCollectorVisitor visitor = new TickerCollectorVisitor();
        crit.acceptVisitor(visitor);
        
        if(visitor.getException() != null) { 
            throw visitor.getException();
        }
        return visitor.getTickers();
    }

}
