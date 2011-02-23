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

package org.teiid.translator.jdbc.h2;

import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;

import org.teiid.translator.Translator;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.SourceSystemFunctions;
import org.teiid.translator.jdbc.AliasModifier;
import org.teiid.translator.jdbc.ConvertModifier;
import org.teiid.translator.jdbc.FunctionModifier;
import org.teiid.translator.jdbc.JDBCExecutionFactory;
import org.teiid.translator.jdbc.ModFunctionModifier;
import org.teiid.translator.jdbc.hsql.AddDiffModifier;
import org.teiid.translator.jdbc.oracle.ConcatFunctionModifier;

@Translator(name="h2", description="A translator for open source H2 Database")
public class H2ExecutionFactory extends JDBCExecutionFactory {
	
	@Override
	public void start() throws TranslatorException {
		super.start();
		registerFunctionModifier(SourceSystemFunctions.PARSETIMESTAMP, new AliasModifier("parsedatetime")); //$NON-NLS-1$
		registerFunctionModifier(SourceSystemFunctions.FORMATTIMESTAMP, new AliasModifier("formatdatetime")); //$NON-NLS-1$
		registerFunctionModifier(SourceSystemFunctions.DAYOFMONTH, new AliasModifier("day_of_month")); //$NON-NLS-1$
		registerFunctionModifier(SourceSystemFunctions.DAYOFWEEK, new AliasModifier("day_of_week")); //$NON-NLS-1$
		registerFunctionModifier(SourceSystemFunctions.DAYOFYEAR, new AliasModifier("day_of_year")); //$NON-NLS-1$
		registerFunctionModifier(SourceSystemFunctions.UNESCAPE, new AliasModifier("stringdecode")); //$NON-NLS-1$
		registerFunctionModifier(SourceSystemFunctions.MOD, new ModFunctionModifier(SourceSystemFunctions.MOD, getLanguageFactory()));
		//TODO: this behavior is configurable in h2 starting with 1.1.119
		registerFunctionModifier(SourceSystemFunctions.CONCAT, new ConcatFunctionModifier(getLanguageFactory()));
		
		registerFunctionModifier(SourceSystemFunctions.TIMESTAMPADD, new AddDiffModifier(true, getLanguageFactory())); 
		registerFunctionModifier(SourceSystemFunctions.TIMESTAMPDIFF, new AddDiffModifier(false, getLanguageFactory())); 
	
		ConvertModifier convert = new ConvertModifier();
		convert.addTypeMapping("boolean", FunctionModifier.BOOLEAN); //$NON-NLS-1$
		convert.addTypeMapping("tinyint", FunctionModifier.BYTE); //$NON-NLS-1$
		convert.addTypeMapping("smallint", FunctionModifier.SHORT); //$NON-NLS-1$
		convert.addTypeMapping("int", FunctionModifier.INTEGER); //$NON-NLS-1$
		convert.addTypeMapping("bigint", FunctionModifier.LONG); //$NON-NLS-1$
		convert.addTypeMapping("real", FunctionModifier.FLOAT); //$NON-NLS-1$
		convert.addTypeMapping("double", FunctionModifier.DOUBLE); //$NON-NLS-1$
		convert.addTypeMapping("decimal", FunctionModifier.BIGDECIMAL); //$NON-NLS-1$
		convert.addTypeMapping("decimal(38,0)", FunctionModifier.BIGINTEGER); //$NON-NLS-1$
		convert.addTypeMapping("date", FunctionModifier.DATE); //$NON-NLS-1$
		convert.addTypeMapping("time", FunctionModifier.TIME); //$NON-NLS-1$
		convert.addTypeMapping("timestamp", FunctionModifier.TIMESTAMP); //$NON-NLS-1$
		convert.addTypeMapping("char(1)", FunctionModifier.CHAR); //$NON-NLS-1$
		convert.addTypeMapping("varchar", FunctionModifier.STRING); //$NON-NLS-1$
		registerFunctionModifier(SourceSystemFunctions.CONVERT, convert);		
	}
	
    @Override
    public String translateLiteralDate(Date dateValue) {
        return "DATE '" + formatDateValue(dateValue) + "'"; //$NON-NLS-1$//$NON-NLS-2$
    }

    @Override
    public String translateLiteralTime(Time timeValue) {
        return "TIME '" + formatDateValue(timeValue) + "'"; //$NON-NLS-1$//$NON-NLS-2$
    }
    
    @Override
    public String translateLiteralTimestamp(Timestamp timestampValue) {
        return "TIMESTAMP '" + formatDateValue(timestampValue) + "'"; //$NON-NLS-1$//$NON-NLS-2$ 
    }
	
    @Override
    public List<String> getSupportedFunctions() {
        List<String> supportedFunctions = new ArrayList<String>();
        supportedFunctions.addAll(super.getSupportedFunctions());

        supportedFunctions.add(SourceSystemFunctions.ABS); 
        supportedFunctions.add(SourceSystemFunctions.ACOS); 
        supportedFunctions.add(SourceSystemFunctions.ASIN);
        supportedFunctions.add(SourceSystemFunctions.ATAN);
        supportedFunctions.add(SourceSystemFunctions.ATAN2);
        supportedFunctions.add(SourceSystemFunctions.BITAND);
        //supportedFunctions.add(SourceSystemFunctions.BITNOT);
        supportedFunctions.add(SourceSystemFunctions.BITOR);
        supportedFunctions.add(SourceSystemFunctions.BITXOR);
        supportedFunctions.add(SourceSystemFunctions.CEILING);
        supportedFunctions.add(SourceSystemFunctions.COS);
        supportedFunctions.add(SourceSystemFunctions.COT);
        supportedFunctions.add(SourceSystemFunctions.DEGREES);
        supportedFunctions.add(SourceSystemFunctions.EXP);
        supportedFunctions.add(SourceSystemFunctions.FLOOR);
        supportedFunctions.add(SourceSystemFunctions.LOG);
        supportedFunctions.add(SourceSystemFunctions.LOG10);
        supportedFunctions.add(SourceSystemFunctions.MOD);
        supportedFunctions.add(SourceSystemFunctions.PI);
        supportedFunctions.add(SourceSystemFunctions.POWER);
        supportedFunctions.add(SourceSystemFunctions.RADIANS);
        supportedFunctions.add(SourceSystemFunctions.ROUND);
        supportedFunctions.add(SourceSystemFunctions.SIGN);
        supportedFunctions.add(SourceSystemFunctions.SIN);
        supportedFunctions.add(SourceSystemFunctions.SQRT);
        supportedFunctions.add(SourceSystemFunctions.TAN);

        supportedFunctions.add(SourceSystemFunctions.ASCII);
        supportedFunctions.add(SourceSystemFunctions.CHAR);
        supportedFunctions.add(SourceSystemFunctions.CONCAT);
        supportedFunctions.add(SourceSystemFunctions.INSERT);
        supportedFunctions.add(SourceSystemFunctions.LCASE);
        supportedFunctions.add(SourceSystemFunctions.LEFT);
        supportedFunctions.add(SourceSystemFunctions.LENGTH);
        supportedFunctions.add(SourceSystemFunctions.LOCATE);
        supportedFunctions.add(SourceSystemFunctions.LPAD);
        supportedFunctions.add(SourceSystemFunctions.LTRIM);
        supportedFunctions.add(SourceSystemFunctions.REPEAT);
        supportedFunctions.add(SourceSystemFunctions.REPLACE);
        supportedFunctions.add(SourceSystemFunctions.RIGHT);
        supportedFunctions.add(SourceSystemFunctions.RPAD);
        supportedFunctions.add(SourceSystemFunctions.RTRIM);
        supportedFunctions.add(SourceSystemFunctions.SUBSTRING);
        supportedFunctions.add(SourceSystemFunctions.UCASE);
        supportedFunctions.add(SourceSystemFunctions.UNESCAPE);
        
        supportedFunctions.add(SourceSystemFunctions.DAYNAME);
        supportedFunctions.add(SourceSystemFunctions.DAYOFMONTH);
        supportedFunctions.add(SourceSystemFunctions.DAYOFWEEK);
        supportedFunctions.add(SourceSystemFunctions.DAYOFYEAR);
        
        supportedFunctions.add(SourceSystemFunctions.FORMATTIMESTAMP); 
        supportedFunctions.add(SourceSystemFunctions.HOUR);
        supportedFunctions.add(SourceSystemFunctions.MINUTE);
        supportedFunctions.add(SourceSystemFunctions.MONTH);
        supportedFunctions.add(SourceSystemFunctions.MONTHNAME);
        
        supportedFunctions.add(SourceSystemFunctions.PARSETIMESTAMP);
        supportedFunctions.add(SourceSystemFunctions.QUARTER);
        supportedFunctions.add(SourceSystemFunctions.SECOND);
        supportedFunctions.add(SourceSystemFunctions.TIMESTAMPADD);
        supportedFunctions.add(SourceSystemFunctions.TIMESTAMPDIFF);
        //supportedFunctions.add(SourceSystemFunctions.TIMESTAMPCREATE);
        supportedFunctions.add(SourceSystemFunctions.WEEK);
        supportedFunctions.add(SourceSystemFunctions.YEAR);

        supportedFunctions.add(SourceSystemFunctions.CONVERT);
        supportedFunctions.add(SourceSystemFunctions.IFNULL);
        supportedFunctions.add(SourceSystemFunctions.COALESCE);
        supportedFunctions.add(SourceSystemFunctions.ARRAY_GET);
        supportedFunctions.add(SourceSystemFunctions.ARRAY_LENGTH);
        return supportedFunctions;
    }
	
    @Override
    public boolean supportsInlineViews() {
        return true;
    }
    
    @Override
    public boolean supportsRowLimit() {
        return true;
    }

    @Override
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
    	return true;
    }
}
