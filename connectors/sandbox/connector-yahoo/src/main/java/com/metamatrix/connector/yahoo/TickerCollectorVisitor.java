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

package com.metamatrix.connector.yahoo;

import java.util.*;

import org.teiid.connector.api.ConnectorException;
import org.teiid.connector.language.*;
import org.teiid.connector.visitor.framework.HierarchyVisitor;


/**
 */
public class TickerCollectorVisitor extends HierarchyVisitor {

    private Set tickers = new HashSet();
    private ConnectorException exception;

    /**
     * 
     */
    public TickerCollectorVisitor() {
        super();        
    }

    public void reset() {
        tickers = new HashSet();
    }

    public Set getTickers() {
        return this.tickers;
    }
    
    public ConnectorException getException() {
        return this.exception;
    }

    /* 
     * @see com.metamatrix.data.visitor.LanguageObjectVisitor#visit(com.metamatrix.data.language.ICompareCriteria)
     */
    public void visit(Comparison obj) {
        Expression expr = obj.getRightExpression();
        addTickerFromExpression(expr);        
    }

    /* 
     * @see com.metamatrix.data.visitor.LanguageObjectVisitor#visit(com.metamatrix.data.language.IInCriteria)
     */
    public void visit(In obj) {
        List exprs = obj.getRightExpressions();
        Iterator iter = exprs.iterator();
        while(iter.hasNext()) {
            Expression expr = (Expression) iter.next();
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
                this.exception = new ConnectorException(YahooPlugin.Util.getString("TickerCollectorVisitor.Unexpected_type", literal.getType().getName())); //$NON-NLS-1$
            }
        } else {
            this.exception = new ConnectorException(YahooPlugin.Util.getString("TickerCollectorVisitor.Unexpected_expression", expr)); //$NON-NLS-1$
        }
         
    }
    
    
    public static Set getTickers(Condition crit) throws ConnectorException {
        TickerCollectorVisitor visitor = new TickerCollectorVisitor();
        crit.acceptVisitor(visitor);
        
        if(visitor.getException() != null) { 
            throw visitor.getException();
        }
        return visitor.getTickers();
    }

}
