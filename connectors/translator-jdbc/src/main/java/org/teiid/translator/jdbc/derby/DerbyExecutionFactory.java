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

package org.teiid.translator.jdbc.derby;

import java.util.ArrayList;
import java.util.List;

import org.teiid.translator.SourceSystemFunctions;
import org.teiid.translator.Translator;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.jdbc.EscapeSyntaxModifier;
import org.teiid.translator.jdbc.db2.BaseDB2ExecutionFactory;
import org.teiid.translator.jdbc.oracle.LeftOrRightFunctionModifier;

/** 
 * @since 4.3
 */
@Translator(name="derby", description="A translator for Apache Derby Database")
public class DerbyExecutionFactory extends BaseDB2ExecutionFactory {
	
	public static final String TEN_1 = "10.1"; //$NON-NLS-1$
	public static final String TEN_2 = "10.2"; //$NON-NLS-1$
	public static final String TEN_3 = "10.3"; //$NON-NLS-1$
	public static final String TEN_4 = "10.4"; //$NON-NLS-1$
	public static final String TEN_5 = "10.5"; //$NON-NLS-1$
	
	public DerbyExecutionFactory() {
		setSupportsFullOuterJoins(false); //Derby supports only left and right outer joins.
		setDatabaseVersion(TEN_1);
	}
	
	@Override
	public void start() throws TranslatorException {
		super.start();
		//additional derby functions
        registerFunctionModifier(SourceSystemFunctions.TIMESTAMPADD, new EscapeSyntaxModifier()); 
        registerFunctionModifier(SourceSystemFunctions.TIMESTAMPDIFF, new EscapeSyntaxModifier()); 
        registerFunctionModifier(SourceSystemFunctions.LEFT, new LeftOrRightFunctionModifier(getLanguageFactory()));
        
        //overrides of db2 functions
        registerFunctionModifier(SourceSystemFunctions.CONCAT, new EscapeSyntaxModifier()); 
    }  
 
    @Override
    public boolean addSourceComment() {
        return false;
    }
    
    @Override
    public boolean supportsOrderByNullOrdering() {
    	return getDatabaseVersion().compareTo(TEN_4) >= 0;
    }
    
    @Override
    public List<String> getSupportedFunctions() {
        List<String> supportedFunctions = new ArrayList<String>();
        supportedFunctions.addAll(super.getDefaultSupportedFunctions());

        supportedFunctions.add("ABS"); //$NON-NLS-1$
        if (getDatabaseVersion().compareTo(TEN_2) >= 0) {
        	supportedFunctions.add("ACOS"); //$NON-NLS-1$
        	supportedFunctions.add("ASIN"); //$NON-NLS-1$
        	supportedFunctions.add("ATAN"); //$NON-NLS-1$
        }
        if (getDatabaseVersion().compareTo(TEN_4) >= 0) {
        	supportedFunctions.add("ATAN2"); //$NON-NLS-1$
        }
        // These are executed within the server and never pushed down
        //supportedFunctions.add("BITAND"); //$NON-NLS-1$
        //supportedFunctions.add("BITNOT"); //$NON-NLS-1$
        //supportedFunctions.add("BITOR"); //$NON-NLS-1$
        //supportedFunctions.add("BITXOR"); //$NON-NLS-1$
        if (getDatabaseVersion().compareTo(TEN_2) >= 0) {
	        supportedFunctions.add("CEILING"); //$NON-NLS-1$
	        supportedFunctions.add("COS"); //$NON-NLS-1$
	        supportedFunctions.add("COT"); //$NON-NLS-1$
	        supportedFunctions.add("DEGREES"); //$NON-NLS-1$
	        supportedFunctions.add("EXP"); //$NON-NLS-1$
	        supportedFunctions.add("FLOOR"); //$NON-NLS-1$
	        supportedFunctions.add("LOG"); //$NON-NLS-1$
	        supportedFunctions.add("LOG10"); //$NON-NLS-1$
        }
        supportedFunctions.add("MOD"); //$NON-NLS-1$
        if (getDatabaseVersion().compareTo(TEN_2) >= 0) {
        	supportedFunctions.add("PI"); //$NON-NLS-1$
        	//supportedFunctions.add("POWER"); //$NON-NLS-1$
        	supportedFunctions.add("RADIANS"); //$NON-NLS-1$
        	//supportedFunctions.add("ROUND"); //$NON-NLS-1$
        	if (getDatabaseVersion().compareTo(TEN_4) >= 0) {
        		supportedFunctions.add("SIGN"); //$NON-NLS-1$
        	}
        	supportedFunctions.add("SIN"); //$NON-NLS-1$
        }
        supportedFunctions.add("SQRT"); //$NON-NLS-1$
        //supportedFunctions.add("TAN"); //$NON-NLS-1$
        
        //supportedFunctions.add("ASCII"); //$NON-NLS-1$
        //supportedFunctions.add("CHR"); //$NON-NLS-1$
        //supportedFunctions.add("CHAR"); //$NON-NLS-1$
        supportedFunctions.add("CONCAT"); //$NON-NLS-1$
        //supportedFunctions.add("INSERT"); //$NON-NLS-1$
        supportedFunctions.add("LCASE"); //$NON-NLS-1$
        supportedFunctions.add("LEFT"); //$NON-NLS-1$
        supportedFunctions.add("LENGTH"); //$NON-NLS-1$
        supportedFunctions.add("LOCATE"); //$NON-NLS-1$
        //supportedFunctions.add("LPAD"); //$NON-NLS-1$
        supportedFunctions.add("LTRIM"); //$NON-NLS-1$
        //supportedFunctions.add("REPEAT"); //$NON-NLS-1$
        //supportedFunctions.add("REPLACE"); //$NON-NLS-1$
        //supportedFunctions.add("RIGHT"); //$NON-NLS-1$
        //supportedFunctions.add("RPAD"); //$NON-NLS-1$
        supportedFunctions.add("RTRIM"); //$NON-NLS-1$
        supportedFunctions.add("SUBSTRING"); //$NON-NLS-1$
        if (getDatabaseVersion().compareTo(TEN_3) >= 0) {
        	supportedFunctions.add(SourceSystemFunctions.TRIM);
        }
        supportedFunctions.add("UCASE"); //$NON-NLS-1$
        
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
        
        supportedFunctions.add("CONVERT"); //$NON-NLS-1$
        supportedFunctions.add("IFNULL"); //$NON-NLS-1$
        supportedFunctions.add("COALESCE"); //$NON-NLS-1$
        return supportedFunctions;
    }
    
    /**
     * Derby supports only SearchedCaseExpression, not CaseExpression. 
     * @since 5.0
     */
    @Override
    public boolean supportsCaseExpressions() {
        return false;
    }
    
    @Override
    public boolean supportsRowLimit() {
    	return this.getDatabaseVersion().compareTo(TEN_5) >= 0;
    }
    
}
