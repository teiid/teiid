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

package com.metamatrix.connector.jdbc.mysql;

import java.util.ArrayList;
import java.util.List;

import com.metamatrix.connector.jdbc.JDBCCapabilities;


/** 
 * @since 4.3
 */
public class MySQLCapabilities extends JDBCCapabilities {

    public List getSupportedFunctions() {
        List supportedFunctions = new ArrayList();
        supportedFunctions.addAll(super.getSupportedFunctions());

        supportedFunctions.add("ABS"); //$NON-NLS-1$
        supportedFunctions.add("ACOS"); //$NON-NLS-1$
        supportedFunctions.add("ASIN"); //$NON-NLS-1$
        supportedFunctions.add("ATAN"); //$NON-NLS-1$
        supportedFunctions.add("ATAN2"); //$NON-NLS-1$
        // These are executed within the server and never pushed down
//        supportedFunctions.add("BITAND"); //$NON-NLS-1$
//        supportedFunctions.add("BITNOT"); //$NON-NLS-1$
//        supportedFunctions.add("BITOR"); //$NON-NLS-1$
//        supportedFunctions.add("BITXOR"); //$NON-NLS-1$
        supportedFunctions.add("CEILING"); //$NON-NLS-1$
        supportedFunctions.add("COS"); //$NON-NLS-1$
        supportedFunctions.add("COT"); //$NON-NLS-1$
        supportedFunctions.add("DEGREES"); //$NON-NLS-1$
        supportedFunctions.add("EXP"); //$NON-NLS-1$
        supportedFunctions.add("FLOOR"); //$NON-NLS-1$
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
        supportedFunctions.add("INSERT"); //$NON-NLS-1$
        supportedFunctions.add("LCASE"); //$NON-NLS-1$
        supportedFunctions.add("LEFT"); //$NON-NLS-1$
        supportedFunctions.add("LENGTH"); //$NON-NLS-1$
        supportedFunctions.add("LOCATE"); //$NON-NLS-1$
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
        supportedFunctions.add("TIMESTAMPADD"); //$NON-NLS-1$
//        supportedFunctions.add("TIMESTAMPDIFF"); //$NON-NLS-1$
        supportedFunctions.add("WEEK"); //$NON-NLS-1$
        supportedFunctions.add("YEAR"); //$NON-NLS-1$
        
        supportedFunctions.add("CAST"); //$NON-NLS-1$
        supportedFunctions.add("CONVERT"); //$NON-NLS-1$
        supportedFunctions.add("IFNULL"); //$NON-NLS-1$
        supportedFunctions.add("NVL"); //$NON-NLS-1$
        
        //   ADDITIONAL functions supported by MySQL
        
//        // Comparison
//        supportedFunctions.add("COALESCE"); //$NON-NLS-1$
//        supportedFunctions.add("GREATEST"); //$NON-NLS-1$
//        supportedFunctions.add("ISNULL"); //$NON-NLS-1$
//        supportedFunctions.add("LEAST"); //$NON-NLS-1$
//        supportedFunctions.add("STRCMP"); // String-specific //$NON-NLS-1$
//        
//        // String
//        supportedFunctions.add("BIN"); //$NON-NLS-1$
//        supportedFunctions.add("BIT_LENGTH"); //$NON-NLS-1$
//        supportedFunctions.add("CHAR_LENGTH"); //$NON-NLS-1$
//        supportedFunctions.add("CHARACTER_LENGTH"); //$NON-NLS-1$
//        supportedFunctions.add("COMPRESS"); //$NON-NLS-1$
//        supportedFunctions.add("CONCAT_WS"); //$NON-NLS-1$
//        supportedFunctions.add("CONV"); //$NON-NLS-1$
//        supportedFunctions.add("ELT"); //$NON-NLS-1$
//        supportedFunctions.add("EXPORT_SET"); //$NON-NLS-1$
//        supportedFunctions.add("FIELD"); //$NON-NLS-1$
//        supportedFunctions.add("FIND_IN_SET"); //$NON-NLS-1$
//        supportedFunctions.add("FORMAT"); //$NON-NLS-1$
//        supportedFunctions.add("HEX"); //$NON-NLS-1$
//        supportedFunctions.add("INSTR"); //$NON-NLS-1$
//        supportedFunctions.add("LOAD_FILE"); //$NON-NLS-1$
//        supportedFunctions.add("MAKE_SET"); //$NON-NLS-1$
//        supportedFunctions.add("MID"); //$NON-NLS-1$
//        supportedFunctions.add("OCT"); //$NON-NLS-1$
//        supportedFunctions.add("OCTET_LENGTH"); //$NON-NLS-1$
//        supportedFunctions.add("ORD"); //$NON-NLS-1$
//        supportedFunctions.add("QUOTE"); //$NON-NLS-1$
//        supportedFunctions.add("REVERSE"); //$NON-NLS-1$
//        supportedFunctions.add("SOUNDEX"); //$NON-NLS-1$
//        supportedFunctions.add("SPACE"); //$NON-NLS-1$
//        supportedFunctions.add("SUBSTR"); //$NON-NLS-1$
//        supportedFunctions.add("SUBSTRING_INDEX"); //$NON-NLS-1$
//        supportedFunctions.add("TRIM"); //$NON-NLS-1$
//        supportedFunctions.add("UNCOMPRESS"); //$NON-NLS-1$
//        supportedFunctions.add("UNHEX"); //$NON-NLS-1$
//        
//        // Math
//        supportedFunctions.add("CEIL"); //$NON-NLS-1$
//        supportedFunctions.add("CRC32"); //$NON-NLS-1$
//          // DIV is an operator equivalent to '/'
//        supportedFunctions.add("DIV"); //$NON-NLS-1$
//        supportedFunctions.add("FORMAT"); //$NON-NLS-1$
//        supportedFunctions.add("LN"); //$NON-NLS-1$
//        supportedFunctions.add("LOG2"); //$NON-NLS-1$
//        supportedFunctions.add("POW"); //$NON-NLS-1$
//        supportedFunctions.add("RAND"); //$NON-NLS-1$
//        supportedFunctions.add("TRUNCATE"); //$NON-NLS-1$
//        
//        // Date / Time
//        supportedFunctions.add("ADDDATE"); //$NON-NLS-1$
//        supportedFunctions.add("ADDTIME"); //$NON-NLS-1$
//        supportedFunctions.add("CONVERT_TZ"); //$NON-NLS-1$
//        supportedFunctions.add("CURRENT_DATE"); //$NON-NLS-1$
//        supportedFunctions.add("CURRENT_TIME"); //$NON-NLS-1$
//        supportedFunctions.add("CURRENT_TIMESTAMP"); //$NON-NLS-1$
//        supportedFunctions.add("DATE"); //$NON-NLS-1$
//        supportedFunctions.add("DATEDIFF"); //$NON-NLS-1$
////        supportedFunctions.add("DATE_ADD");
////        supportedFunctions.add("DATE_SUB");
//        supportedFunctions.add("DATE_FORMAT"); //$NON-NLS-1$
//        supportedFunctions.add("DAY"); //$NON-NLS-1$
////        supportedFunctions.add("EXTRACT");
//        supportedFunctions.add("FROM_DAYS"); //$NON-NLS-1$
//        supportedFunctions.add("FROM_UNIXTIME"); //$NON-NLS-1$
//        supportedFunctions.add("GET_FORMAT"); //$NON-NLS-1$
//        supportedFunctions.add("LAST_DAY"); //$NON-NLS-1$
//        supportedFunctions.add("LOCALTIME"); //$NON-NLS-1$
//        supportedFunctions.add("LOCALTIMESTAMP"); //$NON-NLS-1$
//        supportedFunctions.add("MAKEDATE"); //$NON-NLS-1$
//        supportedFunctions.add("MAKETIME"); //$NON-NLS-1$
//        supportedFunctions.add("MICROSECOND"); //$NON-NLS-1$
//        supportedFunctions.add("PERIOD_ADD"); //$NON-NLS-1$
//        supportedFunctions.add("PERIOD_DIFF"); //$NON-NLS-1$
//        supportedFunctions.add("SEC_TO_TIME"); //$NON-NLS-1$
//        supportedFunctions.add("STR_TO_DATE"); //$NON-NLS-1$
//        supportedFunctions.add("SUBDATE"); //$NON-NLS-1$
//        supportedFunctions.add("SUBTIME"); //$NON-NLS-1$
//        supportedFunctions.add("SYSDATE"); //$NON-NLS-1$
//        supportedFunctions.add("TIME"); //$NON-NLS-1$
//        supportedFunctions.add("TIMEDIFF"); //$NON-NLS-1$
//        supportedFunctions.add("TIMESTAMP"); //$NON-NLS-1$
//        supportedFunctions.add("TIME_FORMAT"); //$NON-NLS-1$
//        supportedFunctions.add("TIME_TO_SEC"); //$NON-NLS-1$
//        supportedFunctions.add("TO_DAYS"); //$NON-NLS-1$
//        supportedFunctions.add("UNIX_TIMESTAMP"); //$NON-NLS-1$
//        supportedFunctions.add("UTC_DATE"); //$NON-NLS-1$
//        supportedFunctions.add("UTC_TIME"); //$NON-NLS-1$
//        supportedFunctions.add("UTC_TIMESTAMP"); //$NON-NLS-1$
//        supportedFunctions.add("WEEKDAY"); //$NON-NLS-1$
//        supportedFunctions.add("WEEKOFYEAR"); //$NON-NLS-1$
//        supportedFunctions.add("YEARWEEK"); //$NON-NLS-1$
//        
//        // Bit
//        supportedFunctions.add("|"); //$NON-NLS-1$
//        supportedFunctions.add("&"); //$NON-NLS-1$
//        supportedFunctions.add("^"); //$NON-NLS-1$
//        supportedFunctions.add("<<"); //$NON-NLS-1$
//        supportedFunctions.add(">>"); //$NON-NLS-1$
//        supportedFunctions.add("~"); //$NON-NLS-1$
//        supportedFunctions.add("BIT_COUNT"); //$NON-NLS-1$
//        
//        // Encryption
//        supportedFunctions.add("AES_ENCRYPT"); //$NON-NLS-1$
//        supportedFunctions.add("AES_DECRYPT"); //$NON-NLS-1$
//        supportedFunctions.add("DECODE"); //$NON-NLS-1$
//        supportedFunctions.add("ENCODE"); //$NON-NLS-1$
//        supportedFunctions.add("DES_ENCRYPT"); //$NON-NLS-1$
//        supportedFunctions.add("DES_DECRYPT"); //$NON-NLS-1$
//        supportedFunctions.add("MD5"); //$NON-NLS-1$
//        supportedFunctions.add("OLD_PASSWORD"); //$NON-NLS-1$
//        supportedFunctions.add("PASSWORD"); //$NON-NLS-1$
//        supportedFunctions.add("SHA"); //$NON-NLS-1$
//        supportedFunctions.add("SHA1"); //$NON-NLS-1$
//        
//        // Information
//        supportedFunctions.add("BENCHMARK"); //$NON-NLS-1$
//        supportedFunctions.add("CHARSET"); //$NON-NLS-1$
//        supportedFunctions.add("COERCIBILITY"); //$NON-NLS-1$
//        supportedFunctions.add("COLLATION"); //$NON-NLS-1$
//        supportedFunctions.add("CONNECTION_ID"); //$NON-NLS-1$
//        supportedFunctions.add("CURRENT_USER"); //$NON-NLS-1$
//        supportedFunctions.add("DATABASE"); //$NON-NLS-1$
//        supportedFunctions.add("FOUND_ROWS"); //$NON-NLS-1$
//        supportedFunctions.add("LAST_INSERT_ID"); //$NON-NLS-1$
//        supportedFunctions.add("ROW_COUNT"); //$NON-NLS-1$
//        supportedFunctions.add("SCHEMA"); //$NON-NLS-1$
//        supportedFunctions.add("SESSION_USER"); //$NON-NLS-1$
//        supportedFunctions.add("SYSTEM_USER"); //$NON-NLS-1$
//        supportedFunctions.add("USER"); //$NON-NLS-1$
//        supportedFunctions.add("VERSION"); //$NON-NLS-1$
//        
//        // Misc.
//        supportedFunctions.add("DEFAULT"); //$NON-NLS-1$
//        supportedFunctions.add("FORMAT"); //$NON-NLS-1$
////        supportedFunctions.add("GET_LOCK"); //$NON-NLS-1$
//        supportedFunctions.add("INET_ATON"); //$NON-NLS-1$
//        supportedFunctions.add("INET_NTOA"); //$NON-NLS-1$
////        supportedFunctions.add("IS_FREE_LOCK"); //$NON-NLS-1$
////        supportedFunctions.add("IS_USED_LOCK"); //$NON-NLS-1$
////        supportedFunctions.add("MASTER_POS_WAIT"); //$NON-NLS-1$
////        supportedFunctions.add("NAME_CONST"); //$NON-NLS-1$
////        supportedFunctions.add("RELEASE_LOCK"); //$NON-NLS-1$
////        supportedFunctions.add("SLEEP"); //$NON-NLS-1$
//        supportedFunctions.add("UUID"); //$NON-NLS-1$
//        supportedFunctions.add("VALUES"); //$NON-NLS-1$
        return supportedFunctions;
    }

    public boolean supportsFullOuterJoins() {
        return false;
    }
    
    public boolean supportsAggregatesDistinct() {
        return false;
    }
        
    public boolean supportsRowLimit() {
        return true;
    }
    public boolean supportsRowOffset() {
        return true;
    }
}
