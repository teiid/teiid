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

package org.teiid.translator.jdbc.netezza;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.teiid.language.Expression;
import org.teiid.language.Function;
import org.teiid.language.Limit;
import org.teiid.translator.ExecutionContext;
import org.teiid.translator.SourceSystemFunctions;
import org.teiid.translator.Translator;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.jdbc.AliasModifier;
import org.teiid.translator.jdbc.ConvertModifier;
import org.teiid.translator.jdbc.ExtractFunctionModifier;
import org.teiid.translator.jdbc.FunctionModifier;
import org.teiid.translator.jdbc.JDBCExecutionFactory;
import org.teiid.translator.jdbc.LocateFunctionModifier;


@Translator(name = "netezza", description = "A translator for Netezza Database")
public class NetezzaExecutionFactory extends JDBCExecutionFactory {

	private static final String TIME_FORMAT = "HH24:MI:SS"; 
	private static final String DATE_FORMAT = "YYYY-MM-DD"; 
	private static final String DATETIME_FORMAT = DATE_FORMAT + " " + TIME_FORMAT; 
	private static final String TIMESTAMP_FORMAT = DATETIME_FORMAT + ".MS";  

	public NetezzaExecutionFactory() {
		setSupportsFullOuterJoins(true);
		setSupportsOrderBy(true);
		setSupportsOuterJoins(true);
		setSupportsSelectDistinct(true);
		setSupportsInnerJoins(true);
	}

	public void start() throws TranslatorException {
		super.start();

		//STRING FUNCTION MODIFIERS
		////////////////////////////////////
		registerFunctionModifier(SourceSystemFunctions.CHAR, new AliasModifier("chr"));  
		registerFunctionModifier(SourceSystemFunctions.LCASE,new AliasModifier("lower"));  
		registerFunctionModifier(SourceSystemFunctions.UCASE,new AliasModifier("upper"));  
		registerFunctionModifier(SourceSystemFunctions.LOCATE, new  LocateFunctionModifier(getLanguageFactory(), "INSTR", true));	  
       	registerFunctionModifier(SourceSystemFunctions.CONCAT, new AliasModifier("||")); 
		///NUMERIC FUNCTION MODIFIERS
        ////////////////////////////////////
		registerFunctionModifier(SourceSystemFunctions.CEILING,	new AliasModifier("ceil"));  
		registerFunctionModifier(SourceSystemFunctions.POWER,	new AliasModifier("pow"));  
		registerFunctionModifier(SourceSystemFunctions.LOG,	new AliasModifier("LN"));
		///BIT FUNCTION MODIFIERS
		////////////////////////////////////
        registerFunctionModifier(SourceSystemFunctions.BITAND, new AliasModifier("intNand")); 
        registerFunctionModifier(SourceSystemFunctions.BITNOT, new AliasModifier("intNnot")); 
        registerFunctionModifier(SourceSystemFunctions.BITOR, new AliasModifier("intNor")); 
        registerFunctionModifier(SourceSystemFunctions.BITXOR, new AliasModifier("intNxor")); 
		//DATE FUNCTION MODIFIERS
        //////////////////////////////////////////
		 registerFunctionModifier(SourceSystemFunctions.YEAR, new ExtractFunctionModifier());
		 registerFunctionModifier(SourceSystemFunctions.DAYOFYEAR, new	ExtractModifier("DOY"));
		 registerFunctionModifier(SourceSystemFunctions.QUARTER, new  ExtractFunctionModifier());
		 registerFunctionModifier(SourceSystemFunctions.MONTH, new ExtractFunctionModifier());
		 registerFunctionModifier(SourceSystemFunctions.DAYOFMONTH, new  ExtractModifier("DAY"));
		 registerFunctionModifier(SourceSystemFunctions.WEEK, new ExtractFunctionModifier());
		 registerFunctionModifier(SourceSystemFunctions.DAYOFWEEK, new  ExtractModifier("DOW"));
         registerFunctionModifier(SourceSystemFunctions.HOUR, new	ExtractFunctionModifier());
		 registerFunctionModifier(SourceSystemFunctions.MINUTE, new  ExtractFunctionModifier());
		 registerFunctionModifier(SourceSystemFunctions.SECOND, new ExtractFunctionModifier());
		 registerFunctionModifier(SourceSystemFunctions.CURDATE, new AliasModifier("CURRENT_DATE")); 
		 registerFunctionModifier(SourceSystemFunctions.CURTIME, new AliasModifier("CURRENT_TIME")); 
       //SYSTEM FUNCTIONS
       ////////////////////////////////////
		registerFunctionModifier(SourceSystemFunctions.IFNULL,new AliasModifier("NVL"));  
        

		// DATA TYPE CONVERSION
		///////////////////////////////////////////
		ConvertModifier convertModifier = new ConvertModifier();
		convertModifier.addTypeMapping("char(1)", FunctionModifier.CHAR); 
		convertModifier.addTypeMapping("byteint", FunctionModifier.BYTE); 
		convertModifier.addTypeMapping("smallint", FunctionModifier.SHORT); 
		convertModifier.addTypeMapping("bigint", FunctionModifier.LONG); 
    	convertModifier.addTypeMapping("numeric(38)", FunctionModifier.BIGINTEGER); 
    	convertModifier.addTypeMapping("numeric(38,18)", FunctionModifier.BIGDECIMAL); 
		convertModifier.addTypeMapping("varchar(4000)", FunctionModifier.STRING); 
		//convertModifier.addTypeMapping("nvarchar(5)", FunctionModifier.BOOLEAN);

 		///NO BOOLEAN, INTEGER, FLOAT, DATE, TIME, TIMESTAMP, as they are directly available in netezza
		///NO NULL, CLOB, BLOB, OBJECT, XML
		
		
		///BOOLEAN--BYTE, SHORT, INTEGER, LONG, FLOAT, DOUBLE, BIGINTEGER, BIGDECIMAL--AS IT DOESN'T WORK IMPLICITLY IN NETEZZA

		convertModifier.addConvert(FunctionModifier.BOOLEAN, FunctionModifier.INTEGER, new BooleanToNumericConversionModifier());
		convertModifier.addConvert(FunctionModifier.BOOLEAN, FunctionModifier.BYTE, new BooleanToNumericConversionModifier());
		convertModifier.addConvert(FunctionModifier.BOOLEAN, FunctionModifier.SHORT, new BooleanToNumericConversionModifier());
		convertModifier.addConvert(FunctionModifier.BOOLEAN, FunctionModifier.LONG, new BooleanToNumericConversionModifier());
		convertModifier.addConvert(FunctionModifier.BOOLEAN, FunctionModifier.FLOAT, new BooleanToNumericConversionModifier());
		convertModifier.addConvert(FunctionModifier.BOOLEAN, FunctionModifier.DOUBLE, new BooleanToNumericConversionModifier());
		convertModifier.addConvert(FunctionModifier.BOOLEAN, FunctionModifier.BIGINTEGER, new BooleanToNumericConversionModifier());
		convertModifier.addConvert(FunctionModifier.BOOLEAN, FunctionModifier.BIGDECIMAL, new BooleanToNumericConversionModifier());
		convertModifier.addConvert(FunctionModifier.BOOLEAN, FunctionModifier.STRING, new BooleanToStringConversionModifier());
    	convertModifier.addConvert(FunctionModifier.STRING, FunctionModifier.BOOLEAN, new FunctionModifier() {
			@Override
			public List<?> translate(Function function) {
				Expression stringValue = function.getParameters().get(0);
				return Arrays.asList("CASE WHEN ", stringValue, " IN ('false', '0') THEN '0' WHEN ", stringValue, " IS NOT NULL THEN '1' END"); 
			}
		});
		convertModifier.addTypeConversion(new FunctionModifier() {
			@Override
			public List<?> translate(Function function) {
				Expression stringValue = function.getParameters().get(0);
				return Arrays.asList("CASE WHEN ", stringValue, " = 0 THEN '0' WHEN ", stringValue, " IS NOT NULL THEN '1' END"); 
			}
		}, FunctionModifier.BOOLEAN);
		
		
		

		////////STRING TO DATATYPE CONVERSION OTHER THAN DATE/TIME
		convertModifier.addConvert(FunctionModifier.STRING, FunctionModifier.INTEGER, new CastModifier("integer")); 
		convertModifier.addConvert(FunctionModifier.STRING, FunctionModifier.FLOAT, new CastModifier("float"));
    	convertModifier.addConvert(FunctionModifier.STRING, FunctionModifier.DOUBLE, new CastModifier("double"));
    	///// STRING --> CHAR, BYTE, SHORT, LONG, BIGI, BIGD, BOOLEAN is taken care by Type Mapping
    	///// NO conversion support for NULL, CLOB, BLOB, OBJECT, XML
		////STRING TO DATE/TIME CONVERSION////
		//////////////////////////////////////
    	convertModifier.addConvert(FunctionModifier.STRING, FunctionModifier.DATE, new ConvertModifier.FormatModifier("to_date", DATE_FORMAT));  
    	convertModifier.addConvert(FunctionModifier.STRING, FunctionModifier.TIME, new ConvertModifier.FormatModifier("to_timestamp", TIME_FORMAT));  
    	convertModifier.addConvert(FunctionModifier.STRING, FunctionModifier.TIMESTAMP, new ConvertModifier.FormatModifier("to_timestamp", TIMESTAMP_FORMAT));  
		//////DATE/TIME INTERNAL CONVERSION/////////
		convertModifier.addConvert(FunctionModifier.TIMESTAMP, FunctionModifier.TIME, new CastModifier("TIME")); 
		convertModifier.addConvert(FunctionModifier.TIMESTAMP, FunctionModifier.DATE,  new CastModifier("DATE"));  
		convertModifier.addConvert(FunctionModifier.DATE, FunctionModifier.TIMESTAMP,  new CastModifier("TIMESTAMP")); 
		//convertModifier.addConvert(FunctionModifier.TIME, FunctionModifier.TIMESTAMP, new CastModifier("TIMESTAMP")); //TIME --> TIMESTAMP --DOESN't WORK IN NETEZZA-NO FUNCTION SUPPORT
				
		////DATE/TIME to STRING CONVERION////
		/////////////////////////////////////
    	convertModifier.addConvert(FunctionModifier.TIMESTAMP, FunctionModifier.STRING, new ConvertModifier.FormatModifier("to_char", TIMESTAMP_FORMAT));
    	///NO NETEZAA FUNCTION for DATE, TIME to STRING
		

		convertModifier.setWideningNumericImplicit(true);
		registerFunctionModifier(SourceSystemFunctions.CONVERT, convertModifier);
	}

	@Override
	public List<String> getSupportedFunctions() {
		List<String> supportedFunctions = new ArrayList<String>();
		supportedFunctions.addAll(super.getSupportedFunctions());

		////////////////////////////////////////////////////////////
		//STRING FUNCTIONS
		//////////////////////////////////////////////////////////
		supportedFunctions.add(SourceSystemFunctions.ASCII);// taken care with alias function modifier
		supportedFunctions.add(SourceSystemFunctions.CHAR);//ALIAS to use 'chr'
		supportedFunctions.add(SourceSystemFunctions.CONCAT); // ALIAS ||
		supportedFunctions.add(SourceSystemFunctions.INITCAP);
		supportedFunctions.add(SourceSystemFunctions.LCASE);//ALIAS 'lower'
		supportedFunctions.add(SourceSystemFunctions.LPAD);
		supportedFunctions.add(SourceSystemFunctions.LENGTH);
		supportedFunctions.add(SourceSystemFunctions.LOCATE); //LOCATE FUNCTIO MODIFIER
		supportedFunctions.add(SourceSystemFunctions.LTRIM);
		//supportedFunctions.add(SourceSystemFunctions.REPEAT);
		supportedFunctions.add(SourceSystemFunctions.RPAD);
		supportedFunctions.add(SourceSystemFunctions.RTRIM);
		supportedFunctions.add(SourceSystemFunctions.SUBSTRING); //No Need of ALIAS as both substring and substr work in netezza
		supportedFunctions.add(SourceSystemFunctions.UCASE); //ALIAS UPPER
		// FUNCTION DIFFERENCE = "difference";  ///NO FUNCTION FOUND--DIFFERENCE 
		//	FUNCTION INSERT = "insert"; 
		// supportedFunctions.add(SourceSystemFunctions.LEFT); //is this available or is it simply for LEFT OUTER JOIN?
		// FUNCTION REPLACE = "replace"; // NO REPLACE Function
		// supportedFunctions.add(SourceSystemFunctions.RIGHT);--is this available or is it simply for RIGHT OUTER JOIN?
		// FUNCTION SOUNDEX = "soundex";
		//	FUNCTION TO_BYTES = "to_bytes"; 
		//	FUNCTION TO_CHARS = "to_chars"; 
		//////////	////////////////////////////////////////////////////////////////////
		//NUMERIC FUNCTIONS////////////////////////////////////////////////////////////
		//////////////////////////////////////////////////////////////////////////////
		//supportedFunctions.add(SourceSystemFunctions.ABS);
		supportedFunctions.add(SourceSystemFunctions.ACOS);
		supportedFunctions.add(SourceSystemFunctions.ASIN);
		supportedFunctions.add(SourceSystemFunctions.ATAN);
		supportedFunctions.add(SourceSystemFunctions.ATAN2);
		supportedFunctions.add(SourceSystemFunctions.CEILING);  ///ALIAS-ceil
		supportedFunctions.add(SourceSystemFunctions.COS);
		supportedFunctions.add(SourceSystemFunctions.COT);
		supportedFunctions.add(SourceSystemFunctions.DEGREES);
		//supportedFunctions.add(SourceSystemFunctions.EXP);
		supportedFunctions.add(SourceSystemFunctions.FLOOR);
		supportedFunctions.add(SourceSystemFunctions.LOG);
		supportedFunctions.add(SourceSystemFunctions.MOD);
		supportedFunctions.add(SourceSystemFunctions.PI);
		supportedFunctions.add(SourceSystemFunctions.POWER);//	ALIAS-POW 
		supportedFunctions.add(SourceSystemFunctions.RADIANS);
		supportedFunctions.add(SourceSystemFunctions.ROUND);
		supportedFunctions.add(SourceSystemFunctions.SIGN);	
		supportedFunctions.add(SourceSystemFunctions.SIN);
		supportedFunctions.add(SourceSystemFunctions.SQRT);
		supportedFunctions.add(SourceSystemFunctions.TAN);
		//		FUNCTION TRANSLATE = "translate"; 
		//		FUNCTION TRUNCATE = "truncate"; 
		//		FUNCTION FORMATINTEGER = "formatinteger"; 
		//		FUNCTION FORMATLONG = "formatlong"; 
		//		FUNCTION FORMATDOUBLE = "formatdouble"; 
		//		FUNCTION FORMATFLOAT = "formatfloat"; 
		//		FUNCTION FORMATBIGINTEGER = "formatbiginteger"; 
		//		FUNCTION FORMATBIGDECIMAL = "formatbigdecimal"; 
		//		FUNCTION LOG10 = "log10"; 
		//		FUNCTION PARSEINTEGER = "parseinteger"; 
		//		FUNCTION PARSELONG = "parselong"; 
		//		FUNCTION PARSEDOUBLE = "parsedouble"; 
		//		FUNCTION PARSEFLOAT = "parsefloat"; 
		//		FUNCTION PARSEBIGINTEGER = "parsebiginteger"; 
		//		FUNCTION PARSEBIGDECIMAL = "parsebigdecimal"; 
		// supportedFunctions.add(SourceSystemFunctions.RAND); --Needs Alias--But, is it required to even have an alias???
		/////////////////////////////////////////////////////////////////////
		//BIT FUNCTIONS//////////////////////////////////////////////////////
		//ALIAS FUNCTION MODIFIER IS APPLIED//////////////////////////////
		supportedFunctions.add(SourceSystemFunctions.BITAND);
		supportedFunctions.add(SourceSystemFunctions.BITOR);
		supportedFunctions.add(SourceSystemFunctions.BITNOT);
		supportedFunctions.add(SourceSystemFunctions.BITXOR);
		// DATE FUNCTIONS
		supportedFunctions.add(SourceSystemFunctions.CURDATE); 
		supportedFunctions.add(SourceSystemFunctions.CURTIME); 
		supportedFunctions.add(SourceSystemFunctions.DAYOFMONTH); 
		supportedFunctions.add(SourceSystemFunctions.DAYOFYEAR);
		supportedFunctions.add(SourceSystemFunctions.DAYOFWEEK);
		supportedFunctions.add(SourceSystemFunctions.HOUR); 
		supportedFunctions.add(SourceSystemFunctions.MINUTE); 
		supportedFunctions.add(SourceSystemFunctions.MONTH);
		supportedFunctions.add(SourceSystemFunctions.QUARTER);
		supportedFunctions.add(SourceSystemFunctions.SECOND);
		supportedFunctions.add(SourceSystemFunctions.WEEK);
		supportedFunctions.add(SourceSystemFunctions.YEAR);
		//		FUNCTION DAYNAME = "dayname"; 
		//		FUNCTION FORMATTIMESTAMP = "formattimestamp"; 
		//		FUNCTION MODIFYTIMEZONE = "modifytimezone"; 
		//		FUNCTION MONTHNAME = "monthname"; 
		//		FUNCTION NOW = "now"; 
		//		FUNCTION PARSETIMESTAMP = "parsetimestamp"; 
		//		FUNCTION TIMESTAMPADD = "timestampadd"; 
		//		FUNCTION TIMESTAMPCREATE = "timestampcreate"; 
		//		FUNCTION TIMESTAMPDIFF = "timestampdiff"; 

		
		//SYSTEM FUNCTIONS
		supportedFunctions.add(SourceSystemFunctions.IFNULL); //ALIAS-NVL
		supportedFunctions.add(SourceSystemFunctions.COALESCE);
		supportedFunctions.add(SourceSystemFunctions.NULLIF);
		
		
		//CONVERSION functions
		supportedFunctions.add(SourceSystemFunctions.CONVERT);
		
		
		return supportedFunctions;
	}
	
	public static class ExtractModifier extends FunctionModifier {
    	private String type;
    	public ExtractModifier(String type) {
    		this.type = type;
    	}
		@Override
		public List<?> translate(Function function) {
			return Arrays.asList("extract(",this.type," from ",function.getParameters().get(0) ,")");    				
		}
	}

	public static class BooleanToNumericConversionModifier extends FunctionModifier {
		@Override
		public List<?> translate(Function function) {
			Expression booleanValue = function.getParameters().get(0);
			if (booleanValue instanceof Function) {
				Function nested = (Function)booleanValue;
				if (nested.getName().equalsIgnoreCase("convert") && Number.class.isAssignableFrom(nested.getParameters().get(0).getType())) { 
					booleanValue = nested.getParameters().get(0);
				}
			}
			return Arrays.asList("(CASE WHEN ", booleanValue, " IN ( '0', 'FALSE') THEN 0 WHEN ", booleanValue, " IS NOT NULL THEN 1 END)");
		}

	}
	public static class BooleanToStringConversionModifier extends FunctionModifier {
		@Override
		public List<?> translate(Function function) {
			Expression booleanValue = function.getParameters().get(0);
			if (booleanValue instanceof Function) {
				Function nested = (Function)booleanValue;
				if (nested.getName().equalsIgnoreCase("convert") && Number.class.isAssignableFrom(nested.getParameters().get(0).getType())) { 
					booleanValue = nested.getParameters().get(0);
				}
			}
			return Arrays.asList("CASE WHEN ", booleanValue, " = '0' THEN 'false' WHEN ", booleanValue, " IS NOT NULL THEN 'true' END"); 
		}

	}
	
	
    public static class CastModifier extends FunctionModifier {
    	private String target;
    	public CastModifier(String target) {
    		this.target = target;
    	}
		@Override
		public List<?> translate(Function function) {
			return Arrays.asList("cast(", function.getParameters().get(0), " AS "+this.target+")");   
		}
	}
    
    
	@Override
    public List<?> translateLimit(Limit limit, ExecutionContext context) {
    	if (limit.getRowOffset() > 0) {
    		return Arrays.asList("LIMIT ", limit.getRowLimit(), " OFFSET ", limit.getRowOffset());   
    	}
        return null;
    }

    @Override
    public boolean supportsCorrelatedSubqueries() {
        return false;
    }

	@Override
    public boolean supportsIntersect() {
    	return true;
    }

	@Override
    public boolean supportsExcept() {
    	return true;
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
	public boolean supportsAggregatesEnhancedNumeric() {
		return true;
	}
	
}
