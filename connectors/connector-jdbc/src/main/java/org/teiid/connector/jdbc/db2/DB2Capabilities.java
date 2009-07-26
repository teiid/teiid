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
package org.teiid.connector.jdbc.db2;

import java.util.ArrayList;
import java.util.List;

import org.teiid.connector.jdbc.JDBCCapabilities;


/**
 */
public class DB2Capabilities extends JDBCCapabilities {

    public DB2Capabilities() {
    }
    
    /**
     * @see com.metamatrix.data.ConnectorCapabilities#getSupportedFunctions()
     */
    public List<String> getSupportedFunctions() {
        List<String> supportedFunctions = new ArrayList<String>();
        supportedFunctions.addAll(super.getSupportedFunctions());
        supportedFunctions.add("ABS"); //$NON-NLS-1$
        supportedFunctions.add("ACOS"); //$NON-NLS-1$
        supportedFunctions.add("ASIN"); //$NON-NLS-1$
        supportedFunctions.add("ATAN"); //$NON-NLS-1$
        supportedFunctions.add("ATAN2"); //$NON-NLS-1$
        supportedFunctions.add("CEILING"); //$NON-NLS-1$
        supportedFunctions.add("COS"); //$NON-NLS-1$
        supportedFunctions.add("COT"); //$NON-NLS-1$
        supportedFunctions.add("DEGREES"); //$NON-NLS-1$
        supportedFunctions.add("EXP"); //$NON-NLS-1$
        supportedFunctions.add("FLOOR"); //$NON-NLS-1$
        supportedFunctions.add("LOG"); //$NON-NLS-1$
        supportedFunctions.add("LOG10"); //$NON-NLS-1$
        supportedFunctions.add("MOD"); //$NON-NLS-1$
        supportedFunctions.add("POWER"); //$NON-NLS-1$
        supportedFunctions.add("RADIANS"); //$NON-NLS-1$
        supportedFunctions.add("SIGN"); //$NON-NLS-1$
        supportedFunctions.add("SIN"); //$NON-NLS-1$
        supportedFunctions.add("SQRT"); //$NON-NLS-1$
        supportedFunctions.add("TAN"); //$NON-NLS-1$
        //supportedFunctions.add("ASCII"); //$NON-NLS-1$
        supportedFunctions.add("CHAR"); //$NON-NLS-1$
        supportedFunctions.add("CHR"); //$NON-NLS-1$
        supportedFunctions.add("CONCAT"); //$NON-NLS-1$
        supportedFunctions.add("||"); //$NON-NLS-1$
        //supportedFunctions.add("INITCAP"); //$NON-NLS-1$
        supportedFunctions.add("LCASE"); //$NON-NLS-1$
        supportedFunctions.add("LENGTH"); //$NON-NLS-1$
        supportedFunctions.add("LEFT"); //$NON-NLS-1$
        supportedFunctions.add("LOCATE"); //$NON-NLS-1$
        supportedFunctions.add("LOWER"); //$NON-NLS-1$
        //supportedFunctions.add("LPAD"); //$NON-NLS-1$
        supportedFunctions.add("LTRIM"); //$NON-NLS-1$
        supportedFunctions.add("RAND"); //$NON-NLS-1$
        supportedFunctions.add("REPLACE"); //$NON-NLS-1$
        //supportedFunctions.add("RPAD"); //$NON-NLS-1$
        supportedFunctions.add("RIGHT"); //$NON-NLS-1$
        supportedFunctions.add("RTRIM"); //$NON-NLS-1$
        supportedFunctions.add("SUBSTRING"); //$NON-NLS-1$
        //supportedFunctions.add("TRANSLATE"); //$NON-NLS-1$
        supportedFunctions.add("UCASE"); //$NON-NLS-1$
        supportedFunctions.add("UPPER"); //$NON-NLS-1$
        supportedFunctions.add("HOUR"); //$NON-NLS-1$
        supportedFunctions.add("MONTH"); //$NON-NLS-1$
        supportedFunctions.add("MONTHNAME"); //$NON-NLS-1$
        supportedFunctions.add("YEAR"); //$NON-NLS-1$
        supportedFunctions.add("DAY"); //$NON-NLS-1$
        supportedFunctions.add("DAYNAME"); //$NON-NLS-1$
        supportedFunctions.add("DAYOFMONTH"); //$NON-NLS-1$
        supportedFunctions.add("DAYOFWEEK"); //$NON-NLS-1$
        supportedFunctions.add("DAYOFYEAR"); //$NON-NLS-1$
        supportedFunctions.add("QUARTER"); //$NON-NLS-1$
        supportedFunctions.add("MINUTE"); //$NON-NLS-1$
        supportedFunctions.add("SECOND"); //$NON-NLS-1$
        supportedFunctions.add("QUARTER"); //$NON-NLS-1$
        supportedFunctions.add("WEEK"); //$NON-NLS-1$
        supportedFunctions.add("CAST"); //$NON-NLS-1$
        supportedFunctions.add("CONVERT"); //$NON-NLS-1$
        supportedFunctions.add("IFNULL"); //$NON-NLS-1$
        supportedFunctions.add("NVL");      //$NON-NLS-1$ 
        supportedFunctions.add("COALESCE"); //$NON-NLS-1$
        return supportedFunctions;
    }
    
    public boolean supportsInlineViews() {
        return true;
    }
    
    /** 
     * @see org.teiid.connector.api.ConnectorCapabilities#supportsFunctionsInGroupBy()
     * @since 5.0
     */
    public boolean supportsFunctionsInGroupBy() {
        return true;
    }    
    
    public boolean supportsRowLimit() {
        return true;
    }
    
    /** 
     * @see org.teiid.connector.basic.BasicConnectorCapabilities#supportsExcept()
     */
    @Override
    public boolean supportsExcept() {
        return true;
    }
    
    /** 
     * @see org.teiid.connector.basic.BasicConnectorCapabilities#supportsIntersect()
     */
    @Override
    public boolean supportsIntersect() {
        return true;
    }
}
