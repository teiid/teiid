/*
 * JBoss, Home of Professional Open Source.
 * Copyright (C) 2008 Red Hat, Inc.
 * Copyright (C) 2000-2007 MetaMatrix, Inc.
 * Licensed to Red Hat, Inc. under one or more contributor 
 * license agreements.  See the copyright.txt file in the
 * distribution for a full listing of individual contributors.
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
package com.metamatrix.connector.jdbc.postgresql;

import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;

import com.metamatrix.connector.jdbc.extension.SQLConversionVisitor;
import com.metamatrix.connector.language.IAggregate;
import com.metamatrix.connector.language.ILimit;

/**
 */
class PostgreSQLConversionVisitor extends SQLConversionVisitor {
    
    protected String translateLiteralBoolean(Boolean booleanValue) {
        if(booleanValue.booleanValue()) {
            return "TRUE"; //$NON-NLS-1$
        }
        return "FALSE"; //$NON-NLS-1$
    }

    protected String translateLiteralDate(Date dateValue) {
        return "DATE '" + dateValue.toString() + "'"; //$NON-NLS-1$//$NON-NLS-2$
    }

    protected String translateLiteralTime(Time timeValue) {
        return "TIME '" + timeValue.toString() + "'"; //$NON-NLS-1$//$NON-NLS-2$
    }

    protected String translateLiteralTimestamp(Timestamp timestampValue) {
        SimpleDateFormat timestampFormatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss"); //$NON-NLS-1$
        if (getDatabaseTimeZone() != null) {
            timestampFormatter.setTimeZone(getDatabaseTimeZone());
        }
        int nanos = timestampValue.getNanos();
        int micros = nanos/1000; // truncate for microseconds
        String microsStr = "" + (1000000 + micros); //$NON-NLS-1$ // Add a number at the beginning, so that we can print leading zeros
        
        return "to_timestamp('" + timestampFormatter.format(timestampValue) + "." + microsStr.substring(1) + "', 'YYYY-MM-DD HH24:MI:SS.UF')"; //$NON-NLS-1$//$NON-NLS-2$ //$NON-NLS-3$
    }
    
    /**
     * Postgres doesn't provide min/max(boolean), so this conversion writes a min(booleanval) as 
     * CASE MIN(CASE B.BooleanValue WHEN TRUE THEN 1 ELSE 0 END) WHEN 1 THEN TRUE ELSE FALSE END
     * TODO: This conversion implementation does not handle null values in the boolean column.
     * @see com.metamatrix.connector.visitor.framework.LanguageObjectVisitor#visit(com.metamatrix.connector.language.IAggregate)
     * @since 4.3
     */
    public void visit(IAggregate obj) {
        if ((obj.getName().equalsIgnoreCase("min") || obj.getName().equalsIgnoreCase("max"))  //$NON-NLS-1$//$NON-NLS-2$
                        && obj.getExpression().getType().equals(Boolean.class)) {
            buffer.append(CASE)
                  .append(SPACE).append(obj.getName())
                  // Inner case
                  .append(LPAREN).append(CASE)
                  .append(SPACE);
            append(obj.getExpression());
            buffer.append(SPACE).append(WHEN)
                  .append(SPACE).append(TRUE)
                  .append(SPACE).append(THEN)
                  .append(SPACE).append(1)
                  .append(SPACE).append(ELSE)
                  .append(SPACE).append(0)
                  .append(SPACE).append(END)
                  .append(RPAREN)
                  
                  .append(SPACE).append(WHEN)
                  .append(SPACE).append(1)
                  .append(SPACE).append(THEN)
                  .append(SPACE).append(TRUE)
                  .append(SPACE).append(ELSE)
                  .append(SPACE).append(FALSE)
                  .append(SPACE).append(END);
        } else {
            super.visit(obj);
        }
    }

    /**
     * Convert limit clause to PostgreSQL ...[LIMIT rowlimit] [OFFSET offset] syntax
     * @see com.metamatrix.connector.visitor.framework.LanguageObjectVisitor#visit(com.metamatrix.connector.language.IQuery)
     * @since 5.0 SP1
     */
    public void visit(ILimit obj) {
        buffer.append(LIMIT)
              .append(SPACE)
              .append(obj.getRowLimit());
        
        if (obj.getRowOffset() > 0) {
            if (obj.getRowLimit() > 0) {
                buffer.append(SPACE);
            }
            buffer.append("OFFSET") //$NON-NLS-1$
                  .append(SPACE)
                  .append(obj.getRowOffset());
        }
    }
}
