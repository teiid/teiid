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
package com.metamatrix.connector.jdbc.access;

import java.sql.Time;

import com.metamatrix.connector.jdbc.extension.SQLConversionVisitor;
import com.metamatrix.connector.language.IQuery;
import com.metamatrix.connector.language.ISelect;

public class AccessSQLConversionVisitor extends SQLConversionVisitor{
    
    private int limit = 0;
    /* 
     * @see com.metamatrix.connector.jdbc.extension.SQLConversionVisitor#translateLiteralTime(java.sql.Time)
     */
    protected String translateLiteralTime(Time timeValue) {
        return "{ts'1900-01-01 " + formatDateValue(timeValue) + "'}"; //$NON-NLS-1$ //$NON-NLS-2$
    }

    protected String translateLiteralBoolean(Boolean booleanValue) {
        if(booleanValue.booleanValue()) {
            return "-1"; //$NON-NLS-1$
        }
        return "0"; //$NON-NLS-1$
    }
    
    protected void appendQuery(IQuery obj) {
        if (obj.getLimit() != null) {
            limit = obj.getLimit().getRowLimit();
        }
        append(obj.getSelect());
        if (obj.getFrom() != null) {
            buffer.append(SPACE);
            append(obj.getFrom());
        }
        if (obj.getWhere() != null) {
            buffer.append(SPACE)
                  .append(WHERE)
                  .append(SPACE);
            append(obj.getWhere());
        }
        if (obj.getGroupBy() != null) {
            buffer.append(SPACE);
            append(obj.getGroupBy());
        }
        if (obj.getHaving() != null) {
            buffer.append(SPACE)
                  .append(HAVING)
                  .append(SPACE);
            append(obj.getHaving());
        }
        if (obj.getOrderBy() != null) {
            buffer.append(SPACE);
            append(obj.getOrderBy());
        }
    }

    protected void visitSelect(ISelect obj) {
        buffer.append(SELECT).append(SPACE);
        if (limit > 0) {
            buffer.append("TOP")  //$NON-NLS-1$
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

    protected boolean supportsComments() {
        return false;
    }       
}
