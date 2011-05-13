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
package org.teiid.translator.jdbc.ingres;

import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.teiid.core.types.DataTypeManager;
import org.teiid.language.Limit;
import org.teiid.metadata.FunctionMethod;
import org.teiid.metadata.FunctionParameter;
import org.teiid.translator.ExecutionContext;
import org.teiid.translator.SourceSystemFunctions;
import org.teiid.translator.Translator;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.jdbc.AliasModifier;
import org.teiid.translator.jdbc.ConvertModifier;
import org.teiid.translator.jdbc.FunctionModifier;
import org.teiid.translator.jdbc.JDBCExecutionFactory;

@Translator(name="ingres", description="A translator for Ingres Databases")
public class IngresExecutionFactory extends JDBCExecutionFactory {
	
	private static final String INGRES = "ingres"; //$NON-NLS-1$
	protected ConvertModifier convert = new ConvertModifier();
	
	@Override
	public void start() throws TranslatorException {
		super.start();
		convert.addTypeMapping("tinyint", FunctionModifier.BOOLEAN, FunctionModifier.BYTE); //$NON-NLS-1$
		convert.addTypeMapping("smallint", FunctionModifier.SHORT); //$NON-NLS-1$
		convert.addTypeMapping("integer", FunctionModifier.INTEGER); //$NON-NLS-1$
		convert.addTypeMapping("bigint", FunctionModifier.LONG); //$NON-NLS-1$
		convert.addTypeMapping("real", FunctionModifier.FLOAT); //$NON-NLS-1$
		convert.addTypeMapping("float", FunctionModifier.DOUBLE); //$NON-NLS-1$
		convert.addTypeMapping("decimal(38,19)", FunctionModifier.BIGDECIMAL); //$NON-NLS-1$
		convert.addTypeMapping("decimal(15,0)", FunctionModifier.BIGINTEGER); //$NON-NLS-1$
		convert.addTypeMapping("date", FunctionModifier.DATE); //$NON-NLS-1$
		convert.addTypeMapping("time with time zone", FunctionModifier.TIME); //$NON-NLS-1$
		convert.addTypeMapping("timestamp with time zone", FunctionModifier.TIMESTAMP); //$NON-NLS-1$
		convert.addTypeMapping("char(1)", FunctionModifier.CHAR); //$NON-NLS-1$
		convert.addTypeMapping("varchar(4000)", FunctionModifier.STRING); //$NON-NLS-1$
		convert.addTypeMapping("blob", FunctionModifier.BLOB); //$NON-NLS-1$
		convert.addTypeMapping("clob", FunctionModifier.CLOB); //$NON-NLS-1$
		convert.addNumericBooleanConversions();
		registerFunctionModifier(SourceSystemFunctions.CONVERT, convert);		
		
        registerFunctionModifier(SourceSystemFunctions.BITAND, new AliasModifier("bit_and")); //$NON-NLS-1$
        registerFunctionModifier(SourceSystemFunctions.BITNOT, new AliasModifier("bit_not")); //$NON-NLS-1$
        registerFunctionModifier(SourceSystemFunctions.BITOR, new AliasModifier("bit_or")); //$NON-NLS-1$
        registerFunctionModifier(SourceSystemFunctions.BITXOR, new AliasModifier("bit_xor")); //$NON-NLS-1$
        registerFunctionModifier(SourceSystemFunctions.CURTIME, new AliasModifier("current_time")); //$NON-NLS-1$
        registerFunctionModifier(SourceSystemFunctions.CURDATE, new AliasModifier("current_date")); //$NON-NLS-1$
        registerFunctionModifier(SourceSystemFunctions.LCASE, new AliasModifier("lowercase")); //$NON-NLS-1$
        registerFunctionModifier(SourceSystemFunctions.RAND, new AliasModifier("random")); //$NON-NLS-1$
        registerFunctionModifier(SourceSystemFunctions.UCASE, new AliasModifier("uppercase")); //$NON-NLS-1$
        registerFunctionModifier(SourceSystemFunctions.DAYOFMONTH, new AliasModifier("day")); //$NON-NLS-1$
        registerFunctionModifier(SourceSystemFunctions.LOCATE, new LocateFunctionModifier(getLanguageFactory())); 		
	}
	
    @Override
    public List<String> getSupportedFunctions() {
        List<String> supportedFunctions = new ArrayList<String>();
        supportedFunctions.addAll(super.getSupportedFunctions());

		supportedFunctions.add(SourceSystemFunctions.ABS);
		supportedFunctions.add(SourceSystemFunctions.ATAN);
		supportedFunctions.add(SourceSystemFunctions.BITAND);
		supportedFunctions.add(SourceSystemFunctions.BITNOT);
		supportedFunctions.add(SourceSystemFunctions.BITOR);
		supportedFunctions.add(SourceSystemFunctions.BITXOR);		
		supportedFunctions.add(SourceSystemFunctions.CONCAT);
		supportedFunctions.add(SourceSystemFunctions.COS);
		supportedFunctions.add(SourceSystemFunctions.CONVERT);
		supportedFunctions.add(SourceSystemFunctions.CURTIME);
		supportedFunctions.add(SourceSystemFunctions.CURDATE);		
		supportedFunctions.add(SourceSystemFunctions.DAYOFMONTH);
		supportedFunctions.add(SourceSystemFunctions.EXP);
		supportedFunctions.add(SourceSystemFunctions.HOUR);
		supportedFunctions.add(SourceSystemFunctions.LCASE);
		supportedFunctions.add(SourceSystemFunctions.LEFT);
		supportedFunctions.add(SourceSystemFunctions.LPAD);
		supportedFunctions.add(SourceSystemFunctions.LOCATE);
		supportedFunctions.add(SourceSystemFunctions.LENGTH);		
		supportedFunctions.add(SourceSystemFunctions.LOG);		
		supportedFunctions.add(SourceSystemFunctions.MINUTE);
		supportedFunctions.add(SourceSystemFunctions.MONTH);
		supportedFunctions.add(SourceSystemFunctions.POWER);
		supportedFunctions.add(SourceSystemFunctions.RAND);
		supportedFunctions.add(SourceSystemFunctions.RIGHT);
		supportedFunctions.add(SourceSystemFunctions.RPAD);
		supportedFunctions.add(SourceSystemFunctions.SECOND);
		supportedFunctions.add(SourceSystemFunctions.SIN);
		supportedFunctions.add(SourceSystemFunctions.SQRT);
		supportedFunctions.add(SourceSystemFunctions.SUBSTRING);
		supportedFunctions.add(SourceSystemFunctions.YEAR);
		supportedFunctions.add(SourceSystemFunctions.UCASE);

        return supportedFunctions;
    }
    
    @Override
    public List<FunctionMethod> getPushDownFunctions(){
    	List<FunctionMethod> pushdownFunctions = new ArrayList<FunctionMethod>();
    
		pushdownFunctions.add(new FunctionMethod(INGRES + '.' + "bit_add", "bit_add", INGRES, //$NON-NLS-1$ //$NON-NLS-2$
            new FunctionParameter[] {
                new FunctionParameter("integer1", DataTypeManager.DefaultDataTypes.INTEGER, ""), //$NON-NLS-1$ //$NON-NLS-2$
                new FunctionParameter("integer2", DataTypeManager.DefaultDataTypes.INTEGER, "")}, //$NON-NLS-1$ //$NON-NLS-2$
            new FunctionParameter("result", DataTypeManager.DefaultDataTypes.INTEGER, "") ) ); //$NON-NLS-1$ //$NON-NLS-2$
		
		pushdownFunctions.add(new FunctionMethod(INGRES + '.' + "bit_length", "bit_length", INGRES, //$NON-NLS-1$ //$NON-NLS-2$
	            new FunctionParameter[] {
	                new FunctionParameter("integer1", DataTypeManager.DefaultDataTypes.INTEGER, "")}, //$NON-NLS-1$ //$NON-NLS-2$
	            new FunctionParameter("result", DataTypeManager.DefaultDataTypes.INTEGER, "") ) ); //$NON-NLS-1$ //$NON-NLS-2$    		
		
		pushdownFunctions.add(new FunctionMethod(INGRES + '.' + "character_length", "character_length", INGRES, //$NON-NLS-1$ //$NON-NLS-2$
	            new FunctionParameter[] {
	                new FunctionParameter("string1", DataTypeManager.DefaultDataTypes.STRING, "")}, //$NON-NLS-1$ //$NON-NLS-2$
	            new FunctionParameter("result", DataTypeManager.DefaultDataTypes.INTEGER, "") ) ); //$NON-NLS-1$ //$NON-NLS-2$  

		pushdownFunctions.add(new FunctionMethod(INGRES + '.' + "charextract", "charextract", INGRES, //$NON-NLS-1$ //$NON-NLS-2$
	            new FunctionParameter[] {
	                new FunctionParameter("string1", DataTypeManager.DefaultDataTypes.STRING, ""), //$NON-NLS-1$ //$NON-NLS-2$
	                new FunctionParameter("integer1", DataTypeManager.DefaultDataTypes.INTEGER, "")}, //$NON-NLS-1$ //$NON-NLS-2$
	            new FunctionParameter("result", DataTypeManager.DefaultDataTypes.CHAR, "") ) ); //$NON-NLS-1$ //$NON-NLS-2$
		
		// uses ingres date??
		//supportedFunctions.add("date_trunc");		
		//supportedFunctions.add("dow");		
		//supportedFunctions.add("extract");

		pushdownFunctions.add(new FunctionMethod(INGRES + '.' + "gmt_timestamp", "gmt_timestamp", INGRES, //$NON-NLS-1$ //$NON-NLS-2$
	            new FunctionParameter[] {
	                new FunctionParameter("string1", DataTypeManager.DefaultDataTypes.INTEGER, "")}, //$NON-NLS-1$ //$NON-NLS-2$
	            new FunctionParameter("result", DataTypeManager.DefaultDataTypes.STRING, "") ) ); //$NON-NLS-1$ //$NON-NLS-2$  

		pushdownFunctions.add(new FunctionMethod(INGRES + '.' + "hash", "hash", INGRES,//$NON-NLS-1$ //$NON-NLS-2$
	            new FunctionParameter[] {
                new FunctionParameter("string1", DataTypeManager.DefaultDataTypes.STRING, "")}, //$NON-NLS-1$ //$NON-NLS-2$				
	            new FunctionParameter("result", DataTypeManager.DefaultDataTypes.INTEGER, "") ) ); //$NON-NLS-1$ //$NON-NLS-2$  
		
		pushdownFunctions.add(new FunctionMethod(INGRES + '.' + "hex", "hex", INGRES, //$NON-NLS-1$ //$NON-NLS-2$
	            new FunctionParameter[] {
	                new FunctionParameter("string1", DataTypeManager.DefaultDataTypes.STRING, "")}, //$NON-NLS-1$ //$NON-NLS-2$
	            new FunctionParameter("result", DataTypeManager.DefaultDataTypes.STRING, "") ) ); //$NON-NLS-1$ //$NON-NLS-2$    		

		// do not have byte[] type
		//supportedFunctions.add("intextract");
		
		pushdownFunctions.add(new FunctionMethod(INGRES + '.' + "ln", "ln", INGRES, //$NON-NLS-1$ //$NON-NLS-2$
	            new FunctionParameter[] {
	                new FunctionParameter("float1", DataTypeManager.DefaultDataTypes.DOUBLE, "")}, //$NON-NLS-1$ //$NON-NLS-2$
	            new FunctionParameter("result", DataTypeManager.DefaultDataTypes.DOUBLE, "") ) ); //$NON-NLS-1$ //$NON-NLS-2$
		
		// see lowercase
		// supportedFunctions.add("lower");
		// supportedFunctions.add("upper");
		
		pushdownFunctions.add(new FunctionMethod(INGRES + '.' + "octet_length", "octet_length", INGRES, //$NON-NLS-1$ //$NON-NLS-2$
	            new FunctionParameter[] {
	                new FunctionParameter("string1", DataTypeManager.DefaultDataTypes.STRING, "")}, //$NON-NLS-1$ //$NON-NLS-2$
	            new FunctionParameter("result", DataTypeManager.DefaultDataTypes.INTEGER, "") ) ); //$NON-NLS-1$ //$NON-NLS-2$
		
		pushdownFunctions.add(new FunctionMethod(INGRES + '.' + "pad", "pad", INGRES, //$NON-NLS-1$ //$NON-NLS-2$
	            new FunctionParameter[] {
	                new FunctionParameter("string1", DataTypeManager.DefaultDataTypes.STRING, "")}, //$NON-NLS-1$ //$NON-NLS-2$
	            new FunctionParameter("result", DataTypeManager.DefaultDataTypes.STRING, "") ) ); //$NON-NLS-1$ //$NON-NLS-2$
		
		pushdownFunctions.add(new FunctionMethod(INGRES + '.' + "pad", "pad", INGRES, //$NON-NLS-1$ //$NON-NLS-2$
	            new FunctionParameter[] {
	                new FunctionParameter("string1", DataTypeManager.DefaultDataTypes.STRING, ""), //$NON-NLS-1$ //$NON-NLS-2$
	                new FunctionParameter("string2", DataTypeManager.DefaultDataTypes.STRING, ""), //$NON-NLS-1$ //$NON-NLS-2$
	                new FunctionParameter("integer1", DataTypeManager.DefaultDataTypes.INTEGER, "")}, //$NON-NLS-1$ //$NON-NLS-2$
	            new FunctionParameter("result", DataTypeManager.DefaultDataTypes.INTEGER, "") ) ); //$NON-NLS-1$ //$NON-NLS-2$
		
		pushdownFunctions.add(new FunctionMethod(INGRES + '.' + "randomf", "randomf", INGRES, null,//$NON-NLS-1$ //$NON-NLS-2$
	            new FunctionParameter("result", DataTypeManager.DefaultDataTypes.FLOAT, "") ) ); //$NON-NLS-1$ //$NON-NLS-2$  

		pushdownFunctions.add(new FunctionMethod(INGRES + '.' + "session_user", "session_user", INGRES, null,//$NON-NLS-1$ //$NON-NLS-2$
	            new FunctionParameter("result", DataTypeManager.DefaultDataTypes.STRING, "") ) ); //$NON-NLS-1$ //$NON-NLS-2$  

		pushdownFunctions.add(new FunctionMethod(INGRES + '.' + "size", "size", INGRES, //$NON-NLS-1$ //$NON-NLS-2$
	            new FunctionParameter[] {
	                new FunctionParameter("string1", DataTypeManager.DefaultDataTypes.STRING, "")}, //$NON-NLS-1$ //$NON-NLS-2$
	            new FunctionParameter("result", DataTypeManager.DefaultDataTypes.INTEGER, "") ) ); //$NON-NLS-1$ //$NON-NLS-2$

		pushdownFunctions.add(new FunctionMethod(INGRES + '.' + "squeeze", "squeeze", INGRES, //$NON-NLS-1$ //$NON-NLS-2$
	            new FunctionParameter[] {
	                new FunctionParameter("string1", DataTypeManager.DefaultDataTypes.STRING, "")}, //$NON-NLS-1$ //$NON-NLS-2$
	            new FunctionParameter("result", DataTypeManager.DefaultDataTypes.STRING, "") ) ); //$NON-NLS-1$ //$NON-NLS-2$

		pushdownFunctions.add(new FunctionMethod(INGRES + '.' + "soundex", "soundex", INGRES, //$NON-NLS-1$ //$NON-NLS-2$
	            new FunctionParameter[] {
	                new FunctionParameter("string1", DataTypeManager.DefaultDataTypes.STRING, "")}, //$NON-NLS-1$ //$NON-NLS-2$
	            new FunctionParameter("result", DataTypeManager.DefaultDataTypes.STRING, "") ) ); //$NON-NLS-1$ //$NON-NLS-2$

		pushdownFunctions.add(new FunctionMethod(INGRES + '.' + "unhex", "unhex", INGRES, //$NON-NLS-1$ //$NON-NLS-2$
	            new FunctionParameter[] {
	                new FunctionParameter("string1", DataTypeManager.DefaultDataTypes.STRING, "")}, //$NON-NLS-1$ //$NON-NLS-2$
	            new FunctionParameter("result", DataTypeManager.DefaultDataTypes.STRING, "") ) ); //$NON-NLS-1$ //$NON-NLS-2$    		

		pushdownFunctions.add(new FunctionMethod(INGRES + '.' + "usercode", "usercode", INGRES, null,//$NON-NLS-1$ //$NON-NLS-2$
	            new FunctionParameter("result", DataTypeManager.DefaultDataTypes.STRING, "") ) ); //$NON-NLS-1$ //$NON-NLS-2$  

		pushdownFunctions.add(new FunctionMethod(INGRES + '.' + "username", "username", INGRES, null,//$NON-NLS-1$ //$NON-NLS-2$
	            new FunctionParameter("result", DataTypeManager.DefaultDataTypes.STRING, "") ) ); //$NON-NLS-1$ //$NON-NLS-2$  
		
		// ignore
		// supportedFunctions.add("uuid_create");
		// supportedFunctions.add("uuid_compare");
		// supportedFunctions.add("uuid_from_char");
		// supportedFunctions.add("uuid_to_char");
		
    	return pushdownFunctions;
    }

    @Override
    public boolean supportsRowLimit() {
    	return true;
    }
    
	@SuppressWarnings("unchecked")
	@Override
	public List<?> translateLimit(Limit limit, ExecutionContext context) {
		return Arrays.asList("FETCH FIRST ", limit.getRowLimit(), " ROWS ONLY"); //$NON-NLS-1$ //$NON-NLS-2$ 
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
    public NullOrder getDefaultNullOrder() {
    	return NullOrder.LAST;
    }    
    
    @Override
    public boolean supportsInlineViews() {
        return true;
    } 
    
}
