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

/*
 */
package com.metamatrix.connector.jdbc.sqlserver;

import java.sql.Time;

import com.metamatrix.connector.jdbc.extension.SQLConversionVisitor;
import com.metamatrix.connector.language.*;

/**
 */
public class SqlServerSQLConversionVisitor extends SQLConversionVisitor {

    private final int MAX_SELECT_ALIAS_LENGTH = 30;
    
    private final int MAX_TABLE_ALIAS_LENGTH = 128;
    
    private int limit = 0;
    /**
     * Override to handle % operator.
     */
    public void visit(IFunction obj) {
        if(obj.getName().equals("%")) { //$NON-NLS-1$
            String name = obj.getName();
            IExpression[] args = obj.getParameters();

            buffer.append(LPAREN); 
            
            if(args != null) {
                for(int i=0; i<args.length; i++) {
                    append(args[i]);                  
                    if(i < (args.length-1)) {
                        buffer.append(SPACE); 
                        buffer.append(name);
                        buffer.append(SPACE); 
                    }   
                }           
            }
            buffer.append(RPAREN);             
        } else {
            super.visit(obj);
        }
    }
    
    public void visit(ILimit obj) {
        // Don't attach limit at the end
        // See appendQuery() below
    }
    
    protected void appendQuery(IQuery obj) {
        if (obj.getLimit() != null) {
            limit = obj.getLimit().getRowLimit();
        }
        super.appendQuery(obj);
    }

    protected void visitSelect(ISelect obj) {
        buffer.append(SELECT).append(SPACE);
        buffer.append(addProcessComment());
        if (limit > 0) {
            buffer.append("TOP") //$NON-NLS-1$
                  .append(SPACE)
                  .append(limit)
                  .append(SPACE);
            limit = 0;
        }
        if (obj.isDistinct()) {
            buffer.append(DISTINCT).append(SPACE);
        }
        append(obj.getSelectSymbols());
    }

    /* 
     * @see com.metamatrix.connector.jdbc.extension.SQLConversionVisitor#translateLiteralTime(java.sql.Time)
     */
    protected String translateLiteralTime(Time timeValue) {
        return "{ts'1900-01-01 " + formatDateValue(timeValue) + "'}"; //$NON-NLS-1$ //$NON-NLS-2$
    }
    
    /** 
     * @see com.metamatrix.connector.jdbc.extension.SQLConversionVisitor#getMaxSelectAliasLength()
     * @since 4.3
     */
    protected int getMaxSelectAliasLength() {
        return MAX_SELECT_ALIAS_LENGTH;
    }

    /** 
     * @see com.metamatrix.connector.jdbc.extension.SQLConversionVisitor#getMaxTableAliasLength()
     * @since 4.3
     */
    protected int getMaxTableAliasLength() {
        return MAX_TABLE_ALIAS_LENGTH;
    }    
}
