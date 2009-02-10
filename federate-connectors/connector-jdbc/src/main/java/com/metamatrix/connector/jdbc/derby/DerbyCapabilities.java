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

package com.metamatrix.connector.jdbc.derby;

import java.util.ArrayList;
import java.util.List;

import com.metamatrix.connector.jdbc.JDBCCapabilities;


/** 
 * @since 5.0
 */
public class DerbyCapabilities extends JDBCCapabilities {

    public List getSupportedFunctions() {
        List supportedFunctions = new ArrayList();
        supportedFunctions.addAll(super.getSupportedFunctions());

        supportedFunctions.add("ABS"); //$NON-NLS-1$
        //supportedFunctions.add("ACOS"); //$NON-NLS-1$
        //supportedFunctions.add("ASIN"); //$NON-NLS-1$
        //supportedFunctions.add("ATAN"); //$NON-NLS-1$
        //supportedFunctions.add("ATAN2"); //$NON-NLS-1$
        // These are executed within the server and never pushed down
        //supportedFunctions.add("BITAND"); //$NON-NLS-1$
        //supportedFunctions.add("BITNOT"); //$NON-NLS-1$
        //supportedFunctions.add("BITOR"); //$NON-NLS-1$
        //supportedFunctions.add("BITXOR"); //$NON-NLS-1$
        //supportedFunctions.add("CEILING"); //$NON-NLS-1$
        //supportedFunctions.add("COS"); //$NON-NLS-1$
        //supportedFunctions.add("COT"); //$NON-NLS-1$
        //supportedFunctions.add("DEGREES"); //$NON-NLS-1$
        //supportedFunctions.add("EXP"); //$NON-NLS-1$
        //supportedFunctions.add("FLOOR"); //$NON-NLS-1$
        //supportedFunctions.add("LOG"); //$NON-NLS-1$
        //supportedFunctions.add("LOG10"); //$NON-NLS-1$
        supportedFunctions.add("MOD"); //$NON-NLS-1$
        //supportedFunctions.add("PI"); //$NON-NLS-1$
        //supportedFunctions.add("POWER"); //$NON-NLS-1$
        //supportedFunctions.add("RADIANS"); //$NON-NLS-1$
        //supportedFunctions.add("ROUND"); //$NON-NLS-1$
        //supportedFunctions.add("SIGN"); //$NON-NLS-1$
        //supportedFunctions.add("SIN"); //$NON-NLS-1$
        supportedFunctions.add("SQRT"); //$NON-NLS-1$
        //supportedFunctions.add("TAN"); //$NON-NLS-1$
        
        //supportedFunctions.add("ASCII"); //$NON-NLS-1$
        //supportedFunctions.add("CHR"); //$NON-NLS-1$
        //supportedFunctions.add("CHAR"); //$NON-NLS-1$
        supportedFunctions.add("||"); //$NON-NLS-1$
        supportedFunctions.add("CONCAT"); //$NON-NLS-1$
        //supportedFunctions.add("INSERT"); //$NON-NLS-1$
        supportedFunctions.add("LCASE"); //$NON-NLS-1$
        //supportedFunctions.add("LEFT"); //$NON-NLS-1$
        supportedFunctions.add("LENGTH"); //$NON-NLS-1$
        supportedFunctions.add("LOCATE"); //$NON-NLS-1$
        supportedFunctions.add("LOWER"); //$NON-NLS-1$
        //supportedFunctions.add("LPAD"); //$NON-NLS-1$
        supportedFunctions.add("LTRIM"); //$NON-NLS-1$
        //supportedFunctions.add("REPEAT"); //$NON-NLS-1$
        //supportedFunctions.add("REPLACE"); //$NON-NLS-1$
        //supportedFunctions.add("RIGHT"); //$NON-NLS-1$
        //supportedFunctions.add("RPAD"); //$NON-NLS-1$
        supportedFunctions.add("RTRIM"); //$NON-NLS-1$
        supportedFunctions.add("SUBSTRING"); //$NON-NLS-1$
        supportedFunctions.add("UCASE"); //$NON-NLS-1$
        supportedFunctions.add("UPPER"); //$NON-NLS-1$
        
        // These are executed within the server and never pushed down
        //supportedFunctions.add("CURDATE"); //$NON-NLS-1$
        //supportedFunctions.add("CURTIME"); //$NON-NLS-1$
        //supportedFunctions.add("NOW"); //$NON-NLS-1$
        //supportedFunctions.add("DAYNAME"); //$NON-NLS-1$
        supportedFunctions.add("DAYOFMONTH"); //$NON-NLS-1$
        //supportedFunctions.add("DAYOFWEEK"); //$NON-NLS-1$
        //supportedFunctions.add("DAYOFYEAR"); //$NON-NLS-1$
        
        // These should not be pushed down since the grammar for string conversion is different
//        supportedFunctions.add("FORMATDATE"); //$NON-NLS-1$
//        supportedFunctions.add("FORMATTIME"); //$NON-NLS-1$
//        supportedFunctions.add("FORMATTIMESTAMP"); //$NON-NLS-1$
        supportedFunctions.add("HOUR"); //$NON-NLS-1$
        supportedFunctions.add("MINUTE"); //$NON-NLS-1$
        supportedFunctions.add("MONTH"); //$NON-NLS-1$
        //supportedFunctions.add("MONTHNAME"); //$NON-NLS-1$
        
        // These should not be pushed down since the grammar for string conversion is different
//        supportedFunctions.add("PARSEDATE"); //$NON-NLS-1$
//        supportedFunctions.add("PARSETIME"); //$NON-NLS-1$
//        supportedFunctions.add("PARSETIMESTAMP"); //$NON-NLS-1$
        //supportedFunctions.add("QUARTER"); //$NON-NLS-1$
        supportedFunctions.add("SECOND"); //$NON-NLS-1$
        supportedFunctions.add("TIMESTAMPADD"); //$NON-NLS-1$
        supportedFunctions.add("TIMESTAMPDIFF"); //$NON-NLS-1$
        //supportedFunctions.add("WEEK"); //$NON-NLS-1$
        supportedFunctions.add("YEAR"); //$NON-NLS-1$
        
        supportedFunctions.add("CAST"); //$NON-NLS-1$
        supportedFunctions.add("CONVERT"); //$NON-NLS-1$
        supportedFunctions.add("IFNULL"); //$NON-NLS-1$
        supportedFunctions.add("NVL"); //$NON-NLS-1$
        
        return supportedFunctions;
    }

    /**
     * Derby supports only SearchedCaseExpression, not CaseExpression. 
     * @see com.metamatrix.connector.api.ConnectorCapabilities#supportsCaseExpressions()
     * @since 5.0
     */
    public boolean supportsCaseExpressions() {
        return false;
    }
    
    /**
     * Derby supports only left and right outer joins. 
     * @see com.metamatrix.connector.api.ConnectorCapabilities#supportsFullOuterJoins()
     * @since 5.0
     */
    public boolean supportsFullOuterJoins() {
        return false;
    }
    
    /**
     * Inline views (subqueries in the FROM clause) are supported. 
     * @see com.metamatrix.connector.api.ConnectorCapabilities#supportsInlineViews()
     * @since 4.3
     */
    public boolean supportsInlineViews() {
        return true;
    }
    
    /**
     * UNION is supported, but not UNIONs with a final ORDER BY. 
     * @see com.metamatrix.connector.api.ConnectorCapabilities#supportsUnionOrderBy()
     * @since 5.0
     */
    public boolean supportsUnionOrderBy() {
        return false;
    }
    
    @Override
    public boolean supportsSetQueryOrderBy() {
    	return false;
    }
    
    /** 
     * @see com.metamatrix.connector.basic.BasicConnectorCapabilities#supportsExcept()
     */
    @Override
    public boolean supportsExcept() {
        return true;
    }
    
    /** 
     * @see com.metamatrix.connector.basic.BasicConnectorCapabilities#supportsIntersect()
     */
    @Override
    public boolean supportsIntersect() {
        return true;
    }

}
