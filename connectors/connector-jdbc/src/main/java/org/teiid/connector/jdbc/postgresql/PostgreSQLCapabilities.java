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

package org.teiid.connector.jdbc.postgresql;

import java.util.ArrayList;
import java.util.List;

import org.teiid.connector.jdbc.JDBCCapabilities;



/** 
 * @since 4.3
 */
public class PostgreSQLCapabilities extends JDBCCapabilities {
    
    
    public List<String> getSupportedFunctions() {
        List<String> supportedFunctions = new ArrayList<String>();
        supportedFunctions.addAll(super.getSupportedFunctions());
    
        supportedFunctions.add("ABS"); //$NON-NLS-1$
        supportedFunctions.add("ACOS"); //$NON-NLS-1$
        supportedFunctions.add("ASIN"); //$NON-NLS-1$
        supportedFunctions.add("ATAN"); //$NON-NLS-1$
        supportedFunctions.add("ATAN2"); //$NON-NLS-1$
        supportedFunctions.add("BITAND"); //$NON-NLS-1$
        supportedFunctions.add("BITNOT"); //$NON-NLS-1$
        supportedFunctions.add("BITOR"); //$NON-NLS-1$
        supportedFunctions.add("BITXOR"); //$NON-NLS-1$
        supportedFunctions.add("CEILING"); //$NON-NLS-1$
        supportedFunctions.add("COS"); //$NON-NLS-1$
        supportedFunctions.add("COT"); //$NON-NLS-1$
        supportedFunctions.add("DEGREES"); //$NON-NLS-1$
        supportedFunctions.add("EXP"); //$NON-NLS-1$
        supportedFunctions.add("FLOOR"); //$NON-NLS-1$
        // These should not be pushed down since the grammar for string conversion is different
//        supportedFunctions.add("FORMATBIGDECIMAL"); //$NON-NLS-1$
//        supportedFunctions.add("FORMATBIGINTEGER"); //$NON-NLS-1$
//        supportedFunctions.add("FORMATDOUBLE"); //$NON-NLS-1$
//        supportedFunctions.add("FORMATFLOAT"); //$NON-NLS-1$
//        supportedFunctions.add("FORMATINTEGER"); //$NON-NLS-1$
//        supportedFunctions.add("FORMATLONG"); //$NON-NLS-1$
        supportedFunctions.add("LOG"); //$NON-NLS-1$
        supportedFunctions.add("LOG10"); //$NON-NLS-1$
        supportedFunctions.add("MOD"); //$NON-NLS-1$
        supportedFunctions.add("PI"); //$NON-NLS-1$
        supportedFunctions.add("POWER"); //$NON-NLS-1$
        supportedFunctions.add("RADIANS"); //$NON-NLS-1$
        supportedFunctions.add("ROUND"); //$NON-NLS-1$
        supportedFunctions.add("SIGN"); //$NON-NLS-1$
        supportedFunctions.add("SIN"); //$NON-NLS-1$
        supportedFunctions.add("SQRT"); //$NON-NLS-1$
        supportedFunctions.add("TAN"); //$NON-NLS-1$
        
        supportedFunctions.add("ASCII"); //$NON-NLS-1$
        supportedFunctions.add("CHR"); //$NON-NLS-1$
        supportedFunctions.add("CHAR"); //$NON-NLS-1$
        supportedFunctions.add("||"); //$NON-NLS-1$
        supportedFunctions.add("CONCAT"); //$NON-NLS-1$
        supportedFunctions.add("INITCAP"); //$NON-NLS-1$
        supportedFunctions.add("LCASE"); //$NON-NLS-1$
        supportedFunctions.add("LEFT"); //$NON-NLS-1$
        supportedFunctions.add("LENGTH"); //$NON-NLS-1$
        // Doesn't support both forms exposed by MM
//        supportedFunctions.add("LOCATE"); //$NON-NLS-1$
        supportedFunctions.add("LOWER"); //$NON-NLS-1$
        supportedFunctions.add("LPAD"); //$NON-NLS-1$
        supportedFunctions.add("LTRIM"); //$NON-NLS-1$
        supportedFunctions.add("REPEAT"); //$NON-NLS-1$
        supportedFunctions.add("REPLACE"); //$NON-NLS-1$
        supportedFunctions.add("RIGHT"); //$NON-NLS-1$
        supportedFunctions.add("RPAD"); //$NON-NLS-1$
        supportedFunctions.add("RTRIM"); //$NON-NLS-1$
        supportedFunctions.add("SUBSTRING"); //$NON-NLS-1$
        supportedFunctions.add("UCASE"); //$NON-NLS-1$
        supportedFunctions.add("UPPER"); //$NON-NLS-1$
        
        // These are executed within the server and never pushed down
//        supportedFunctions.add("CURDATE"); //$NON-NLS-1$
//        supportedFunctions.add("CURTIME"); //$NON-NLS-1$
//        supportedFunctions.add("NOW"); //$NON-NLS-1$
        supportedFunctions.add("DAYNAME"); //$NON-NLS-1$
        supportedFunctions.add("DAYOFMONTH"); //$NON-NLS-1$
        supportedFunctions.add("DAYOFWEEK"); //$NON-NLS-1$
        supportedFunctions.add("DAYOFYEAR"); //$NON-NLS-1$
        // These should not be pushed down since the grammar for string conversion is different
//        supportedFunctions.add("FORMATDATE"); //$NON-NLS-1$
//        supportedFunctions.add("FORMATTIME"); //$NON-NLS-1$
//        supportedFunctions.add("FORMATTIMESTAMP"); //$NON-NLS-1$
        supportedFunctions.add("HOUR"); //$NON-NLS-1$
        supportedFunctions.add("MINUTE"); //$NON-NLS-1$
        supportedFunctions.add("MONTH"); //$NON-NLS-1$
        supportedFunctions.add("MONTHNAME"); //$NON-NLS-1$
        // These should not be pushed down since the grammar for string conversion is different
//        supportedFunctions.add("PARSEDATE"); //$NON-NLS-1$
//        supportedFunctions.add("PARSETIME"); //$NON-NLS-1$
//        supportedFunctions.add("PARSETIMESTAMP"); //$NON-NLS-1$
        supportedFunctions.add("QUARTER"); //$NON-NLS-1$
        supportedFunctions.add("SECOND"); //$NON-NLS-1$
//        supportedFunctions.add("TIMESTAMPADD"); //$NON-NLS-1$
//        supportedFunctions.add("TIMESTAMPDIFF"); //$NON-NLS-1$
        supportedFunctions.add("WEEK"); //$NON-NLS-1$
        supportedFunctions.add("YEAR"); //$NON-NLS-1$
        
        supportedFunctions.add("CAST"); //$NON-NLS-1$
        supportedFunctions.add("CONVERT"); //$NON-NLS-1$
        supportedFunctions.add("IFNULL"); //$NON-NLS-1$
        supportedFunctions.add("NVL"); //$NON-NLS-1$
        
        // Additional functions
//        // Math
//        supportedFunctions.add("%"); //$NON-NLS-1$
//        supportedFunctions.add("^"); //$NON-NLS-1$
//        supportedFunctions.add("|/"); //$NON-NLS-1$
//        supportedFunctions.add("||/"); //$NON-NLS-1$
//        supportedFunctions.add("!"); //$NON-NLS-1$
//        supportedFunctions.add("!!"); //$NON-NLS-1$
//        supportedFunctions.add("@"); //$NON-NLS-1$
//          // Bit manipulation
//        supportedFunctions.add("&"); //$NON-NLS-1$
//        supportedFunctions.add("|"); //$NON-NLS-1$
//        supportedFunctions.add("#"); //$NON-NLS-1$
//        supportedFunctions.add("~"); //$NON-NLS-1$
//        supportedFunctions.add("<<"); //$NON-NLS-1$
//        supportedFunctions.add(">>"); //$NON-NLS-1$
//        
//        supportedFunctions.add("CBRT"); //$NON-NLS-1$
//        supportedFunctions.add("CEIL"); //$NON-NLS-1$
//        supportedFunctions.add("LN"); //$NON-NLS-1$
//        supportedFunctions.add("MOD"); //$NON-NLS-1$
//        supportedFunctions.add("RANDOM"); //$NON-NLS-1$
//        supportedFunctions.add("SETSEED"); //$NON-NLS-1$
//        supportedFunctions.add("TRUNC"); //$NON-NLS-1$
//        supportedFunctions.add("WIDTH_BUCKET"); //$NON-NLS-1$
//        
//        // String
//        supportedFunctions.add("BIT_LENGTH"); //$NON-NLS-1$
//        supportedFunctions.add("BTRIM"); //$NON-NLS-1$
//        supportedFunctions.add("CHAR_LENGTH"); //$NON-NLS-1$
//        supportedFunctions.add("CHARACTER_LENGTH"); //$NON-NLS-1$
//        supportedFunctions.add("DECODE"); //$NON-NLS-1$
//        supportedFunctions.add("ENCODE"); //$NON-NLS-1$
//        supportedFunctions.add("MD5"); //$NON-NLS-1$
//        supportedFunctions.add("OCTET_LENGTH"); //$NON-NLS-1$
//        supportedFunctions.add("PG_CLIENT_ENCODING"); //$NON-NLS-1$
//        supportedFunctions.add("QUOTE_IDENT"); //$NON-NLS-1$
//        supportedFunctions.add("QUOTE_LITERAL"); //$NON-NLS-1$
//        supportedFunctions.add("SPLIT_PART"); //$NON-NLS-1$
//        supportedFunctions.add("STRPOS"); //$NON-NLS-1$
//        supportedFunctions.add("SUBSTR"); //$NON-NLS-1$
//        supportedFunctions.add("TO_ASCII"); //$NON-NLS-1$
//        supportedFunctions.add("TO_HEX"); //$NON-NLS-1$
//        supportedFunctions.add("TRANSLATE"); //$NON-NLS-1$
//        
//        // Bit operations
//        supportedFunctions.add("GET_BIT"); //$NON-NLS-1$
//        supportedFunctions.add("GET_BYTE"); //$NON-NLS-1$
//        supportedFunctions.add("SET_BIT"); //$NON-NLS-1$
//        supportedFunctions.add("SET_BYTE"); //$NON-NLS-1$
//        
//        // Formatting
//        supportedFunctions.add("TO_CHAR"); //$NON-NLS-1$
//        supportedFunctions.add("TO_DATE"); //$NON-NLS-1$
//        supportedFunctions.add("TO_TIMESTAMP"); //$NON-NLS-1$
//        supportedFunctions.add("TO_NUMBER"); //$NON-NLS-1$
//        
//        // Date / Time
//        supportedFunctions.add("AGE"); //$NON-NLS-1$
//        supportedFunctions.add("CURRENT_DATE"); //$NON-NLS-1$            // no ()
//        supportedFunctions.add("CURRENT_TIME"); //$NON-NLS-1$            // no ()
//        supportedFunctions.add("CURRENT_TIMESTAMP"); //$NON-NLS-1$       // no ()
//        supportedFunctions.add("DATE_PART"); //$NON-NLS-1$
//        supportedFunctions.add("DATE_TRUNC"); //$NON-NLS-1$
//        supportedFunctions.add("ISFINITE"); //$NON-NLS-1$
//        supportedFunctions.add("JUSTIFY_HOURS"); //$NON-NLS-1$
//        supportedFunctions.add("JUSTIFY_DAYS"); //$NON-NLS-1$
//        supportedFunctions.add("LOCALTIME"); //$NON-NLS-1$               // no ()
//        supportedFunctions.add("LOCALTIMESTAMP"); //$NON-NLS-1$          // no ()
//        supportedFunctions.add("TIMEOFDAY"); //$NON-NLS-1$
//        
//        // Conditional
          supportedFunctions.add("COALESCE"); //$NON-NLS-1$
//        supportedFunctions.add("NULLIF"); //$NON-NLS-1$
//        supportedFunctions.add("GREATEST"); //$NON-NLS-1$
//        supportedFunctions.add("LEAST"); //$NON-NLS-1$
//        
//        // Network Addresses
////        supportedFunctions.add("BROADCAST"); //$NON-NLS-1$
////        supportedFunctions.add("HOST"); //$NON-NLS-1$
////        supportedFunctions.add("MASKLEN"); //$NON-NLS-1$
////        supportedFunctions.add("SET_MASKLEN"); //$NON-NLS-1$
////        supportedFunctions.add("NETMASK"); //$NON-NLS-1$
////        supportedFunctions.add("HOSTMASK"); //$NON-NLS-1$
////        supportedFunctions.add("NETWORK"); //$NON-NLS-1$
////        supportedFunctions.add("TEXT"); //$NON-NLS-1$
////        supportedFunctions.add("ABBREV"); //$NON-NLS-1$
////        supportedFunctions.add("FAMILY"); //$NON-NLS-1$
////        supportedFunctions.add("TRUNC"); //$NON-NLS-1$
//        
//        // Set generator
//        supportedFunctions.add("GENERATE_SERIES"); //$NON-NLS-1$
//        
//        // Information
//        supportedFunctions.add("CURRENT_DATABASE"); //$NON-NLS-1$
//        supportedFunctions.add("CURRENT_SCHEMA"); //$NON-NLS-1$
//        supportedFunctions.add("CURRENT_SCHEMAS"); //$NON-NLS-1$
//        supportedFunctions.add("CURRENT_USER"); //$NON-NLS-1$           // no ()
//        supportedFunctions.add("INET_CLIENT_ADDR"); //$NON-NLS-1$
//        supportedFunctions.add("INET_CLIENT_PORT"); //$NON-NLS-1$
//        supportedFunctions.add("INET_SERVER_ADDR"); //$NON-NLS-1$
//        supportedFunctions.add("INET_SERVER_PORT"); //$NON-NLS-1$
//        supportedFunctions.add("SESSION_USER"); //$NON-NLS-1$           // no ()
//        supportedFunctions.add("USER"); //$NON-NLS-1$                   // no ()
//        supportedFunctions.add("VERSION"); //$NON-NLS-1$
//        
        return supportedFunctions;
    }
    
    /** 
     * This is true only after Postgre version 7.1 
     * However, since version 7 was released in 2000 we'll assume a post 7 instance.
     * 
     * @see org.teiid.connector.jdbc.JDBCCapabilities#supportsInlineViews()
     */
    public boolean supportsInlineViews() {
        return true;
    }

    public boolean supportsRowLimit() {
        return true;
    }
    public boolean supportsRowOffset() {
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
