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

package com.metamatrix.connector.exec;

import java.util.HashMap;
import java.util.Map;

import org.teiid.connector.api.ConnectorException;
import org.teiid.connector.language.ICompareCriteria;
import org.teiid.connector.language.ICriteria;
import org.teiid.connector.language.IElement;
import org.teiid.connector.language.IExpression;
import org.teiid.connector.language.ILiteral;
import org.teiid.connector.visitor.framework.HierarchyVisitor;



/**
 */
public class ExecVisitor extends HierarchyVisitor {

    private Map whereClause = new HashMap();
    private ConnectorException exception;

    /**
     * 
     */
    public ExecVisitor() {
        super();        
    }

    public void reset() {
        whereClause = new HashMap();
    }

    public Map getWhereClause() {
        return this.whereClause;
    }
    
    public ConnectorException getException() {
        return this.exception;
    }

    /* 
     * @see com.metamatrix.data.visitor.LanguageObjectVisitor#visit(com.metamatrix.data.language.ICompareCriteria)
     */
    public void visit(ICompareCriteria criteria) {
        IExpression lExpression = criteria.getLeftExpression();
        IExpression rExpression = criteria.getRightExpression();

        // take out the element from either left or right expression in the criteria
        IElement symbol = null;
        String whereValue = null;
        // take the left expr being element as default in CompareCriteria
        if ( lExpression instanceof IElement) {
            symbol = (IElement) lExpression;
            whereValue = getWhereValueFromExpression(rExpression);
        } else {
            symbol = (IElement) rExpression;
            whereValue = getWhereValueFromExpression(lExpression);

        }
        
        if (whereValue != null) {
            
            whereClause.put(symbol.getMetadataObject().getName(), whereValue);
//            System.out.println("SYMBOL: " + symbol.getMetadataID().getName() + " value: " + whereValue );//$NON-NLS-1$ //$NON-NLS-2$
            
        } else {
            this.exception = new ConnectorException(ExecPlugin.Util.getString("SoapVisitor.No_where_value_found", symbol.getMetadataObject().getName())); //$NON-NLS-1$
                                                 
        }

      
    }

    
    private String getWhereValueFromExpression(IExpression expr) {
        if(expr instanceof ILiteral) {
            ILiteral literal = (ILiteral) expr;
            String whereValue = literal.getValue().toString();
            return whereValue;
//            this.exception = new ConnectorException(ExecPlugin.Util.getString("SoapVisitor.Unexpected_type", literal.getType().getName())); //$NON-NLS-1$

        } 
        this.exception = new ConnectorException(ExecPlugin.Util.getString("SoapVisitor.Unexpected_expression", expr)); //$NON-NLS-1$
      
        return null;
         
    }
    

    
    public static Map getWhereClauseMap(ICriteria crit) throws ConnectorException {
        ExecVisitor visitor = new ExecVisitor();
        crit.acceptVisitor(visitor);
        
        if(visitor.getException() != null) { 
            throw visitor.getException();
        }
        
        return visitor.getWhereClause();
    }    

}
