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
package org.teiid.translator.jdbc.intersyscache;

import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.teiid.core.types.DataTypeManager;
import org.teiid.language.Expression;
import org.teiid.language.Function;
import org.teiid.metadata.FunctionMethod;
import org.teiid.metadata.FunctionParameter;
import org.teiid.translator.SourceSystemFunctions;
import org.teiid.translator.Translator;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.jdbc.AliasModifier;
import org.teiid.translator.jdbc.ConvertModifier;
import org.teiid.translator.jdbc.FunctionModifier;
import org.teiid.translator.jdbc.JDBCExecutionFactory;

@Translator(name="intersystems-cache", description="A translator for Intersystems Cache Database")
public class InterSystemsCacheExecutionFactory extends JDBCExecutionFactory {
	
	private static final String INTER_CACHE = "intersystems-cache"; //$NON-NLS-1$
	protected ConvertModifier convert = new ConvertModifier();
	
	@Override
	public void start() throws TranslatorException {
		super.start();
		convert.addTypeMapping("bigint", FunctionModifier.LONG); //$NON-NLS-1$
		convert.addTypeMapping("character", FunctionModifier.CHAR); //$NON-NLS-1$
		convert.addTypeMapping("decimal(38,19)", FunctionModifier.BIGDECIMAL); //$NON-NLS-1$
		convert.addTypeMapping("decimal(19,0)", FunctionModifier.BIGINTEGER); //$NON-NLS-1$		
		convert.addTypeMapping("smallint", FunctionModifier.SHORT); //$NON-NLS-1$
		convert.addTypeMapping("tinyint", FunctionModifier.BYTE); //$NON-NLS-1$		
		convert.addTypeMapping("varchar(4000)", FunctionModifier.STRING); //$NON-NLS-1$
		convert.addNumericBooleanConversions();
		registerFunctionModifier(SourceSystemFunctions.CONVERT, convert);		
		
		registerFunctionModifier(SourceSystemFunctions.IFNULL, new AliasModifier("nvl")); //$NON-NLS-1$
		registerFunctionModifier(SourceSystemFunctions.CONCAT, new ModifiedFunction(SourceSystemFunctions.CONCAT));
		registerFunctionModifier(SourceSystemFunctions.ACOS, new ModifiedFunction(SourceSystemFunctions.ACOS));
		registerFunctionModifier(SourceSystemFunctions.ASIN, new ModifiedFunction(SourceSystemFunctions.ASIN));
		registerFunctionModifier(SourceSystemFunctions.ATAN, new ModifiedFunction(SourceSystemFunctions.ATAN));
		registerFunctionModifier(SourceSystemFunctions.COS, new ModifiedFunction(SourceSystemFunctions.COS));
		registerFunctionModifier(SourceSystemFunctions.COT, new ModifiedFunction(SourceSystemFunctions.COT));
		registerFunctionModifier(SourceSystemFunctions.CURDATE, new ModifiedFunction(SourceSystemFunctions.CURDATE));		
		registerFunctionModifier(SourceSystemFunctions.CURTIME, new ModifiedFunction(SourceSystemFunctions.CURTIME));   
		registerFunctionModifier(SourceSystemFunctions.DAYNAME, new ModifiedFunction(SourceSystemFunctions.DAYNAME));
        registerFunctionModifier(SourceSystemFunctions.DAYOFMONTH, new ModifiedFunction(SourceSystemFunctions.DAYOFMONTH)); 
        registerFunctionModifier(SourceSystemFunctions.DAYOFWEEK, new ModifiedFunction(SourceSystemFunctions.DAYOFWEEK));
        registerFunctionModifier(SourceSystemFunctions.DAYOFYEAR, new ModifiedFunction(SourceSystemFunctions.DAYOFYEAR));
        registerFunctionModifier(SourceSystemFunctions.EXP, new ModifiedFunction(SourceSystemFunctions.EXP));    
        registerFunctionModifier(SourceSystemFunctions.HOUR, new ModifiedFunction(SourceSystemFunctions.HOUR)); 
        registerFunctionModifier(SourceSystemFunctions.LOG,new ModifiedFunction(SourceSystemFunctions.LOG)); 
        registerFunctionModifier(SourceSystemFunctions.LOG10, new ModifiedFunction(SourceSystemFunctions.LOG10)); 
        registerFunctionModifier(SourceSystemFunctions.LEFT, new ModifiedFunction(SourceSystemFunctions.LEFT));
        registerFunctionModifier(SourceSystemFunctions.MINUTE, new ModifiedFunction(SourceSystemFunctions.MINUTE));
        registerFunctionModifier(SourceSystemFunctions.MONTH, new ModifiedFunction(SourceSystemFunctions.MONTH));
        registerFunctionModifier(SourceSystemFunctions.MONTHNAME, new ModifiedFunction(SourceSystemFunctions.MONTHNAME));
        registerFunctionModifier(SourceSystemFunctions.MOD, new ModifiedFunction(SourceSystemFunctions.MOD));
        registerFunctionModifier(SourceSystemFunctions.NOW, new ModifiedFunction(SourceSystemFunctions.NOW));
        registerFunctionModifier(SourceSystemFunctions.PI, new ModifiedFunction(SourceSystemFunctions.PI));
        registerFunctionModifier(SourceSystemFunctions.QUARTER, new ModifiedFunction(SourceSystemFunctions.QUARTER));
        registerFunctionModifier(SourceSystemFunctions.RIGHT, new ModifiedFunction(SourceSystemFunctions.RIGHT));
        registerFunctionModifier(SourceSystemFunctions.SIN, new ModifiedFunction(SourceSystemFunctions.SIN));
        registerFunctionModifier(SourceSystemFunctions.SECOND, new ModifiedFunction(SourceSystemFunctions.SECOND));
        registerFunctionModifier(SourceSystemFunctions.SQRT,new ModifiedFunction(SourceSystemFunctions.SQRT));
        registerFunctionModifier(SourceSystemFunctions.TAN,new ModifiedFunction(SourceSystemFunctions.TAN));
        registerFunctionModifier(SourceSystemFunctions.TIMESTAMPADD, new ModifiedFunction(SourceSystemFunctions.TIMESTAMPADD));   
        registerFunctionModifier(SourceSystemFunctions.TIMESTAMPDIFF, new ModifiedFunction(SourceSystemFunctions.TIMESTAMPDIFF));    
        registerFunctionModifier(SourceSystemFunctions.TRUNCATE,new ModifiedFunction(SourceSystemFunctions.TRUNCATE));   
        registerFunctionModifier(SourceSystemFunctions.WEEK,new ModifiedFunction(SourceSystemFunctions.WEEK));          
	}
	
    @Override
    public List<String> getSupportedFunctions() {
        List<String> supportedFunctions = new ArrayList<String>();
        supportedFunctions.addAll(super.getSupportedFunctions());

        supportedFunctions.add(SourceSystemFunctions.ABS);
        supportedFunctions.add(SourceSystemFunctions.ASCII);
        supportedFunctions.add(SourceSystemFunctions.CEILING);
        supportedFunctions.add(SourceSystemFunctions.CHAR);
        supportedFunctions.add(SourceSystemFunctions.COALESCE);
        supportedFunctions.add(SourceSystemFunctions.CONVERT);
        supportedFunctions.add(SourceSystemFunctions.FLOOR);
        supportedFunctions.add(SourceSystemFunctions.IFNULL);
        supportedFunctions.add(SourceSystemFunctions.LCASE);
        supportedFunctions.add(SourceSystemFunctions.LENGTH);
        supportedFunctions.add(SourceSystemFunctions.LPAD);
        supportedFunctions.add(SourceSystemFunctions.LTRIM);
        supportedFunctions.add(SourceSystemFunctions.NULLIF);
        supportedFunctions.add(SourceSystemFunctions.POWER);
        supportedFunctions.add(SourceSystemFunctions.REPEAT);
        supportedFunctions.add(SourceSystemFunctions.REPLACE);
        supportedFunctions.add(SourceSystemFunctions.ROUND);
        supportedFunctions.add(SourceSystemFunctions.RPAD);
        supportedFunctions.add(SourceSystemFunctions.RTRIM);
        supportedFunctions.add(SourceSystemFunctions.SIGN);
        supportedFunctions.add(SourceSystemFunctions.SUBSTRING);
        supportedFunctions.add(SourceSystemFunctions.UCASE);
        supportedFunctions.add(SourceSystemFunctions.XMLCONCAT);

        return supportedFunctions;
    }
    
    @Override
    public List<FunctionMethod> getPushDownFunctions(){
    	        
    	List<FunctionMethod> pushdownFunctions = new ArrayList<FunctionMethod>();
    
		pushdownFunctions.add(new FunctionMethod(INTER_CACHE + '.' + "CHARACTER_LENGTH", "CHARACTER_LENGTH", INTER_CACHE, //$NON-NLS-1$ //$NON-NLS-2$
            new FunctionParameter[] {
                new FunctionParameter("string1", DataTypeManager.DefaultDataTypes.STRING, "")}, //$NON-NLS-1$ //$NON-NLS-2$
            new FunctionParameter("result", DataTypeManager.DefaultDataTypes.INTEGER, "") ) ); //$NON-NLS-1$ //$NON-NLS-2$

		pushdownFunctions.add(new FunctionMethod(INTER_CACHE + '.' + "CHAR_LENGTH", "CHAR_LENGTH", INTER_CACHE, //$NON-NLS-1$ //$NON-NLS-2$
	            new FunctionParameter[] {
	                new FunctionParameter("string1", DataTypeManager.DefaultDataTypes.STRING, "")}, //$NON-NLS-1$ //$NON-NLS-2$
	            new FunctionParameter("result", DataTypeManager.DefaultDataTypes.INTEGER, "") ) ); //$NON-NLS-1$ //$NON-NLS-2$
		
		pushdownFunctions.add(new FunctionMethod(INTER_CACHE + '.' + "CHARINDEX", "CHARINDEX", INTER_CACHE, //$NON-NLS-1$ //$NON-NLS-2$
	            new FunctionParameter[] {
                new FunctionParameter("string1", DataTypeManager.DefaultDataTypes.STRING, ""), //$NON-NLS-1$ //$NON-NLS-2$
                new FunctionParameter("string2", DataTypeManager.DefaultDataTypes.STRING, "")}, //$NON-NLS-1$ //$NON-NLS-2$
	            new FunctionParameter("result", DataTypeManager.DefaultDataTypes.INTEGER, "") ) ); //$NON-NLS-1$ //$NON-NLS-2$
		
		pushdownFunctions.add(new FunctionMethod(INTER_CACHE + '.' + "CHARINDEX", "CHARINDEX", INTER_CACHE, //$NON-NLS-1$ //$NON-NLS-2$
	            new FunctionParameter[] {
                new FunctionParameter("string1", DataTypeManager.DefaultDataTypes.STRING, ""), //$NON-NLS-1$ //$NON-NLS-2$
                new FunctionParameter("string2", DataTypeManager.DefaultDataTypes.STRING, ""), //$NON-NLS-1$ //$NON-NLS-2$
                new FunctionParameter("integer1", DataTypeManager.DefaultDataTypes.INTEGER, "")}, //$NON-NLS-1$ //$NON-NLS-2$
	            new FunctionParameter("result", DataTypeManager.DefaultDataTypes.INTEGER, "") ) ); //$NON-NLS-1$ //$NON-NLS-2$    		
		
		pushdownFunctions.add(new FunctionMethod(INTER_CACHE + '.' + "INSTR", "INSTR", INTER_CACHE, //$NON-NLS-1$ //$NON-NLS-2$
	            new FunctionParameter[] {
                new FunctionParameter("string1", DataTypeManager.DefaultDataTypes.STRING, ""), //$NON-NLS-1$ //$NON-NLS-2$
                new FunctionParameter("string2", DataTypeManager.DefaultDataTypes.STRING, "")}, //$NON-NLS-1$ //$NON-NLS-2$
	            new FunctionParameter("result", DataTypeManager.DefaultDataTypes.INTEGER, "") ) ); //$NON-NLS-1$ //$NON-NLS-2$
		
		pushdownFunctions.add(new FunctionMethod(INTER_CACHE + '.' + "INSTR", "INSTR", INTER_CACHE, //$NON-NLS-1$ //$NON-NLS-2$
	            new FunctionParameter[] {
                new FunctionParameter("string1", DataTypeManager.DefaultDataTypes.STRING, ""), //$NON-NLS-1$ //$NON-NLS-2$
                new FunctionParameter("string2", DataTypeManager.DefaultDataTypes.STRING, ""), //$NON-NLS-1$ //$NON-NLS-2$
                new FunctionParameter("integer1", DataTypeManager.DefaultDataTypes.INTEGER, "")}, //$NON-NLS-1$ //$NON-NLS-2$
	            new FunctionParameter("result", DataTypeManager.DefaultDataTypes.INTEGER, "") ) ); //$NON-NLS-1$ //$NON-NLS-2$    		
		
		pushdownFunctions.add(new FunctionMethod(INTER_CACHE + '.' + "IS_NUMERIC", "IS_NUMERIC", INTER_CACHE, //$NON-NLS-1$ //$NON-NLS-2$
	            new FunctionParameter[] {
	                new FunctionParameter("string1", DataTypeManager.DefaultDataTypes.STRING, "")}, //$NON-NLS-1$ //$NON-NLS-2$
	            new FunctionParameter("result", DataTypeManager.DefaultDataTypes.INTEGER, "") ) ); //$NON-NLS-1$ //$NON-NLS-2$
				
		pushdownFunctions.add(new FunctionMethod(INTER_CACHE + '.' + "REPLICATE", "REPLICATE", INTER_CACHE, //$NON-NLS-1$ //$NON-NLS-2$
	            new FunctionParameter[] {
                new FunctionParameter("string1", DataTypeManager.DefaultDataTypes.STRING, ""), //$NON-NLS-1$ //$NON-NLS-2$
                new FunctionParameter("integer1", DataTypeManager.DefaultDataTypes.INTEGER, "")}, //$NON-NLS-1$ //$NON-NLS-2$
	            new FunctionParameter("result", DataTypeManager.DefaultDataTypes.STRING, "") ) ); //$NON-NLS-1$ //$NON-NLS-2$    		
		
		pushdownFunctions.add(new FunctionMethod(INTER_CACHE + '.' + "REVERSE", "REVERSE", INTER_CACHE, //$NON-NLS-1$ //$NON-NLS-2$
	            new FunctionParameter[] {
	                new FunctionParameter("string1", DataTypeManager.DefaultDataTypes.STRING, "")}, //$NON-NLS-1$ //$NON-NLS-2$
	            new FunctionParameter("result", DataTypeManager.DefaultDataTypes.STRING, "") ) ); //$NON-NLS-1$ //$NON-NLS-2$
		
		pushdownFunctions.add(new FunctionMethod(INTER_CACHE + '.' + "STUFF", "STUFF", INTER_CACHE, //$NON-NLS-1$ //$NON-NLS-2$
	            new FunctionParameter[] {
                new FunctionParameter("string1", DataTypeManager.DefaultDataTypes.STRING, ""), //$NON-NLS-1$ //$NON-NLS-2$
                new FunctionParameter("integer1", DataTypeManager.DefaultDataTypes.STRING, ""), //$NON-NLS-1$ //$NON-NLS-2$
                new FunctionParameter("integer2", DataTypeManager.DefaultDataTypes.INTEGER, ""), //$NON-NLS-1$ //$NON-NLS-2$
                new FunctionParameter("string2", DataTypeManager.DefaultDataTypes.STRING, "")}, //$NON-NLS-1$ //$NON-NLS-2$
	            new FunctionParameter("result", DataTypeManager.DefaultDataTypes.STRING, "") ) ); //$NON-NLS-1$ //$NON-NLS-2$  		
		
		pushdownFunctions.add(new FunctionMethod(INTER_CACHE + '.' + "TRIM", "TRIM", INTER_CACHE, //$NON-NLS-1$ //$NON-NLS-2$
	            new FunctionParameter[] {
	                new FunctionParameter("string1", DataTypeManager.DefaultDataTypes.STRING, "")}, //$NON-NLS-1$ //$NON-NLS-2$
	            new FunctionParameter("result", DataTypeManager.DefaultDataTypes.STRING, "") ) ); //$NON-NLS-1$ //$NON-NLS-2$
		
    	return pushdownFunctions;
    }

    
    @Override
    public String translateLiteralDate(Date dateValue) {
        return "to_date('" + formatDateValue(dateValue) + "', 'yyyy-mm-dd')"; //$NON-NLS-1$//$NON-NLS-2$
    }

    @Override
    public String translateLiteralTime(Time timeValue) {
        return "to_date('" + formatDateValue(timeValue) + "', 'hh:mi:ss')"; //$NON-NLS-1$//$NON-NLS-2$
    }
    
    @Override
    public String translateLiteralTimestamp(Timestamp timestampValue) {
        return "to_timestamp('" + formatDateValue(timestampValue) + "', 'yyyy-mm-dd hh:mi:ss.fffffffff')"; //$NON-NLS-1$//$NON-NLS-2$ 
    }	
    
    @Override
    public NullOrder getDefaultNullOrder() {
    	return NullOrder.LAST;
    }    
    
    @Override
    public boolean supportsInlineViews() {
        return true;
    } 
    
    static class ModifiedFunction extends FunctionModifier{
    	String name;
    	ModifiedFunction(String name){
    		this.name = name;
    	}
    	
		@Override
		public List<?> translate(Function function) {
			StringBuilder sb = new StringBuilder();
			sb.append("{fn ").append(this.name).append('(');//$NON-NLS-1$ 
			List<Expression> params = function.getParameters();
			if (params != null && !params.isEmpty()) {
				for (int i = 0; i < params.size(); i++) {
					sb.append(params.get(0));
					if (i < (params.size()-1)) {
						sb.append(',');
					}
				}
			}
			sb.append(")}");//$NON-NLS-1$
			
			return Arrays.asList(sb); 
		}
    }
}
