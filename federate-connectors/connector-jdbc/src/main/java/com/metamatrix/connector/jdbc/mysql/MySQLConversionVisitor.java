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
package com.metamatrix.connector.jdbc.mysql;

import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;

import com.metamatrix.connector.jdbc.extension.SQLConversionVisitor;
import com.metamatrix.data.language.ILimit;
import com.metamatrix.data.language.IQueryCommand;

/**
 */
class MySQLConversionVisitor extends SQLConversionVisitor {
    
    /**
     * Some very large row count. This number is from the mysql documentation for the LIMIT clause
     */
    static final String NO_LIMIT = "18446744073709551615"; //$NON-NLS-1$
    
    protected String translateLiteralDate(Date dateValue) {
        return "DATE('" + formatDateValue(dateValue) + "')";  //$NON-NLS-1$//$NON-NLS-2$
    }

    protected String translateLiteralTime(Time timeValue) {
        return "TIME('" + formatDateValue(timeValue) + "')";  //$NON-NLS-1$//$NON-NLS-2$
    }
    
    protected String translateLiteralTimestamp(Timestamp timestampValue) {
        return "TIMESTAMP('" + formatDateValue(timestampValue) + "')";  //$NON-NLS-1$//$NON-NLS-2$
    }
    
    protected String formatDateValue(Object dateObject) {
        if(dateObject instanceof Timestamp) {
            SimpleDateFormat timestampFormatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss"); //$NON-NLS-1$
            if (getDatabaseTimeZone() != null) {
                timestampFormatter.setTimeZone(getDatabaseTimeZone());
            }

            Timestamp ts = (Timestamp) dateObject;   
            int nanos = ts.getNanos();
            int micros = nanos/1000; // truncate for microseconds
            String microsStr = "" + (1000000 + micros); //$NON-NLS-1$ // Add a number at the beginning, so that we can print leading zeros
            
            return timestampFormatter.format(ts) + "." + microsStr.substring(1); //$NON-NLS-1$ // show all digits except the number we just added
        }
        return super.formatDateValue(dateObject);
    }
    
    protected void appendSetQuery(IQueryCommand obj) {
        buffer.append(LPAREN);
        append(obj);
        buffer.append(RPAREN);
    }
    
    public void visit(ILimit obj) {
        buffer.append(LIMIT)
              .append(SPACE);
        if (obj.getRowOffset() > 0) {
            buffer.append(obj.getRowOffset())
                  .append(COMMA)
                  .append(SPACE);
        }
        buffer.append(obj.getRowLimit());
    }
}
