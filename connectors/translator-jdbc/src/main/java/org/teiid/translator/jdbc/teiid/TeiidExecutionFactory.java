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
package org.teiid.translator.jdbc.teiid;

import java.util.ArrayList;
import java.util.List;

import org.teiid.translator.SourceSystemFunctions;
import org.teiid.translator.Translator;
import org.teiid.translator.jdbc.JDBCExecutionFactory;

/** 
 * @since 4.3
 */
@Translator(name="teiid", description="A translator for Teiid 7.0 or later")
public class TeiidExecutionFactory extends JDBCExecutionFactory {
	
	public static final String SEVEN_0 = "7.0"; //$NON-NLS-1$
	public static final String SEVEN_1 = "7.1"; //$NON-NLS-1$
	public static final String SEVEN_2 = "7.2"; //$NON-NLS-1$
	public static final String SEVEN_3 = "7.3"; //$NON-NLS-1$
	public static final String SEVEN_4 = "7.4"; //$NON-NLS-1$
	public static final String SEVEN_5 = "7.5"; //$NON-NLS-1$
	
	public TeiidExecutionFactory() {
		setDatabaseVersion(SEVEN_0);
	}
    
	@Override
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
        supportedFunctions.add("FORMATBIGDECIMAL"); //$NON-NLS-1$
        supportedFunctions.add("FORMATBIGINTEGER"); //$NON-NLS-1$
        supportedFunctions.add("FORMATDOUBLE"); //$NON-NLS-1$
        supportedFunctions.add("FORMATFLOAT"); //$NON-NLS-1$
        supportedFunctions.add("FORMATINTEGER"); //$NON-NLS-1$
        supportedFunctions.add("FORMATLONG"); //$NON-NLS-1$
        supportedFunctions.add("LOG"); //$NON-NLS-1$
        supportedFunctions.add("LOG10"); //$NON-NLS-1$
        supportedFunctions.add("MOD"); //$NON-NLS-1$
        supportedFunctions.add("PARSEBIGDECIMAL"); //$NON-NLS-1$
        supportedFunctions.add("PARSEBIGINTEGER"); //$NON-NLS-1$
        supportedFunctions.add("PARSEDOUBLE"); //$NON-NLS-1$
        supportedFunctions.add("PARSEFLOAT"); //$NON-NLS-1$
        supportedFunctions.add("PARSEINTEGER"); //$NON-NLS-1$
        supportedFunctions.add("PARSELONG"); //$NON-NLS-1$
        supportedFunctions.add("PI"); //$NON-NLS-1$
        supportedFunctions.add("POWER"); //$NON-NLS-1$
        supportedFunctions.add("RADIANS"); //$NON-NLS-1$
        supportedFunctions.add("RAND"); //$NON-NLS-1$
        supportedFunctions.add("ROUND"); //$NON-NLS-1$
        supportedFunctions.add("SIGN"); //$NON-NLS-1$
        supportedFunctions.add("SIN"); //$NON-NLS-1$
        supportedFunctions.add("SQRT"); //$NON-NLS-1$
        supportedFunctions.add("TAN"); //$NON-NLS-1$
        supportedFunctions.add("ASCII"); //$NON-NLS-1$
        supportedFunctions.add("CHAR"); //$NON-NLS-1$
        supportedFunctions.add("CHR"); //$NON-NLS-1$
        supportedFunctions.add("CONCAT"); //$NON-NLS-1$
        supportedFunctions.add("CONCAT2"); //$NON-NLS-1$
        supportedFunctions.add("||"); //$NON-NLS-1$
        supportedFunctions.add("INITCAP"); //$NON-NLS-1$
        supportedFunctions.add("INSERT"); //$NON-NLS-1$
        supportedFunctions.add("LCASE"); //$NON-NLS-1$
        supportedFunctions.add("LENGTH"); //$NON-NLS-1$
        supportedFunctions.add("LEFT"); //$NON-NLS-1$
        supportedFunctions.add("LOCATE"); //$NON-NLS-1$
        supportedFunctions.add("LPAD"); //$NON-NLS-1$
        supportedFunctions.add("LTRIM"); //$NON-NLS-1$
        supportedFunctions.add("REPEAT"); //$NON-NLS-1$
        supportedFunctions.add("REPLACE"); //$NON-NLS-1$
        supportedFunctions.add("RPAD"); //$NON-NLS-1$
        supportedFunctions.add("RIGHT"); //$NON-NLS-1$
        supportedFunctions.add("RTRIM"); //$NON-NLS-1$
        supportedFunctions.add("SUBSTRING"); //$NON-NLS-1$
        supportedFunctions.add("TRANSLATE"); //$NON-NLS-1$
        supportedFunctions.add("UCASE"); //$NON-NLS-1$
        supportedFunctions.add("CURDATE"); //$NON-NLS-1$
        supportedFunctions.add("CURTIME"); //$NON-NLS-1$
        supportedFunctions.add("NOW"); //$NON-NLS-1$
        supportedFunctions.add("DAYNAME"); //$NON-NLS-1$
        supportedFunctions.add("DAYOFMONTH"); //$NON-NLS-1$
        supportedFunctions.add("DAYOFWEEK"); //$NON-NLS-1$
        supportedFunctions.add("DAYOFYEAR"); //$NON-NLS-1$
        supportedFunctions.add("FORMATDATE"); //$NON-NLS-1$
        supportedFunctions.add("FORMATTIME"); //$NON-NLS-1$
        supportedFunctions.add("FORMATTIMESTAMP"); //$NON-NLS-1$
        supportedFunctions.add("HOUR"); //$NON-NLS-1$
        supportedFunctions.add("MINUTE"); //$NON-NLS-1$
        supportedFunctions.add("MONTH"); //$NON-NLS-1$
        supportedFunctions.add("MONTHNAME"); //$NON-NLS-1$
        supportedFunctions.add("PARSEDATE"); //$NON-NLS-1$
        supportedFunctions.add("PARSETIME"); //$NON-NLS-1$
        supportedFunctions.add("PARSETIMESTAMP"); //$NON-NLS-1$
        supportedFunctions.add("SECOND"); //$NON-NLS-1$
        supportedFunctions.add("TIMESTAMPADD"); //$NON-NLS-1$
        supportedFunctions.add("TIMESTAMPDIFF"); //$NON-NLS-1$
        supportedFunctions.add("WEEK"); //$NON-NLS-1$
        supportedFunctions.add("YEAR"); //$NON-NLS-1$
        supportedFunctions.add("MODIFYTIMEZONE"); //$NON-NLS-1$
        supportedFunctions.add("DECODESTRING"); //$NON-NLS-1$
        supportedFunctions.add("DECODEINTEGER"); //$NON-NLS-1$
        supportedFunctions.add("IFNULL"); //$NON-NLS-1$
        supportedFunctions.add("NVL");      //$NON-NLS-1$ 
        supportedFunctions.add("CAST"); //$NON-NLS-1$
        supportedFunctions.add("CONVERT"); //$NON-NLS-1$
        supportedFunctions.add("USER"); //$NON-NLS-1$
        supportedFunctions.add("FROM_UNIXTIME"); //$NON-NLS-1$
        supportedFunctions.add("NULLIF"); //$NON-NLS-1$
        supportedFunctions.add("COALESCE"); //$NON-NLS-1$
        
        if (getDatabaseVersion().compareTo(SEVEN_3) >= 0) {
        	supportedFunctions.add(SourceSystemFunctions.UNESCAPE);
        }
        
        if (getDatabaseVersion().compareTo(SEVEN_4) >= 0) {
        	supportedFunctions.add(SourceSystemFunctions.UUID);
        	supportedFunctions.add(SourceSystemFunctions.ARRAY_GET);
        	supportedFunctions.add(SourceSystemFunctions.ARRAY_LENGTH);
        }
        
        if (getDatabaseVersion().compareTo(SEVEN_5) >= 0) {
        	supportedFunctions.add(SourceSystemFunctions.TRIM);
        }
        
        return supportedFunctions;
    }
    
    public boolean supportsInlineViews() {
        return true;
    }

    @Override
    public boolean supportsFunctionsInGroupBy() {
        return true;
    }    
    
    public boolean supportsRowLimit() {
        return true;
    }
    
    public boolean supportsRowOffset() {
        return true;
    }
    
    @Override
    public boolean supportsExcept() {
    	return true;
    }
    
    @Override
    public boolean supportsIntersect() {
    	return true;
    }
    
    @Override
    public boolean supportsAggregatesEnhancedNumeric() {
    	return getDatabaseVersion().compareTo(SEVEN_1) >= 0;
    }
    
    @Override
    public NullOrder getDefaultNullOrder() {
    	return NullOrder.UNKNOWN;
    }
    
    @Override
    public boolean supportsBulkUpdate() {
    	return true;
    }
    
    @Override
    public boolean supportsCommonTableExpressions() {
    	return getDatabaseVersion().compareTo(SEVEN_2) >= 0;
    }
    
    @Override
    public boolean supportsAdvancedOlapOperations() {
    	return getDatabaseVersion().compareTo(SEVEN_5) >= 0;
    }
    
    @Override
    public boolean supportsElementaryOlapOperations() {
    	return getDatabaseVersion().compareTo(SEVEN_5) >= 0;
    }
    
    @Override
    public boolean supportsArrayAgg() {
    	return getDatabaseVersion().compareTo(SEVEN_5) >= 0;
    }
}
