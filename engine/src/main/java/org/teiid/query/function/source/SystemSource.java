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

package org.teiid.query.function.source;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import org.teiid.core.types.DataTypeManager;
import org.teiid.metadata.FunctionParameter;
import org.teiid.metadata.FunctionMethod.PushDown;
import org.teiid.metadata.FunctionMethod.Determinism;
import org.teiid.query.QueryPlugin;
import org.teiid.query.function.FunctionLibrary;
import org.teiid.query.function.FunctionMetadataSource;
import org.teiid.query.function.FunctionMethods;
import org.teiid.query.function.metadata.FunctionCategoryConstants;
import org.teiid.query.function.metadata.FunctionMethod;
import org.teiid.translator.SourceSystemFunctions;


/**
 * This metadata source has metadata for the hard-coded system functions.  All
 * system functions are described by this metadata.
 */
public class SystemSource implements FunctionMetadataSource, FunctionCategoryConstants {

    /** The name of the invocation class for all of the system functions. */
    private static final String FUNCTION_CLASS = FunctionMethods.class.getName(); 
    private static final String XML_FUNCTION_CLASS = XMLSystemFunctions.class.getName(); 
    private static final String SECURITY_FUNCTION_CLASS = SecuritySystemFunctions.class.getName(); 
    
    /** Cached list of system function metadata, created in constructor */
    private List<org.teiid.metadata.FunctionMethod> functions = new ArrayList<org.teiid.metadata.FunctionMethod>();
    
    /**
     * Construct a source of system metadata.
     */
    public SystemSource(boolean allowEnvFunction) {
		// +, -, *, /
        addArithmeticFunction(SourceSystemFunctions.ADD_OP, QueryPlugin.Util.getString("SystemSource.Add_desc"), "plus", QueryPlugin.Util.getString("SystemSource.Add_result_desc")); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ 
        addArithmeticFunction(SourceSystemFunctions.SUBTRACT_OP, QueryPlugin.Util.getString("SystemSource.Subtract_desc"), "minus", QueryPlugin.Util.getString("SystemSource.Subtract_result_desc")); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ 
        addArithmeticFunction(SourceSystemFunctions.MULTIPLY_OP, QueryPlugin.Util.getString("SystemSource.Multiply_desc"), "multiply", QueryPlugin.Util.getString("SystemSource.Multiply_result_desc")); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ 
        addArithmeticFunction(SourceSystemFunctions.DIVIDE_OP, QueryPlugin.Util.getString("SystemSource.Divide_desc"), "divide", QueryPlugin.Util.getString("SystemSource.Divide_result_desc")); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ 
        addArithmeticFunction(SourceSystemFunctions.MOD, QueryPlugin.Util.getString("SystemSource.Mod_desc"), "mod", QueryPlugin.Util.getString("SystemSource.Mod_result_desc")); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ 
        
        // numeric
        addAbsFunction();
        addRandFunction();
        addPowerFunction();
        addRoundFunction();
        addSignFunction();
        addSqrtFunction();        
		addDoubleFunction(SourceSystemFunctions.ACOS, QueryPlugin.Util.getString("SystemSource.Acos_desc")); //$NON-NLS-1$ 
		addDoubleFunction(SourceSystemFunctions.ASIN, QueryPlugin.Util.getString("SystemSource.Asin_desc")); //$NON-NLS-1$ 
		addDoubleFunction(SourceSystemFunctions.ATAN, QueryPlugin.Util.getString("SystemSource.Atan_desc")); //$NON-NLS-1$ 
		addAtan2Function(SourceSystemFunctions.ATAN2, QueryPlugin.Util.getString("SystemSource.Atan2_desc")); //$NON-NLS-1$ 
		addDoubleFunction(SourceSystemFunctions.COS, QueryPlugin.Util.getString("SystemSource.Cos_desc")); //$NON-NLS-1$ 
		addDoubleFunction(SourceSystemFunctions.COT, QueryPlugin.Util.getString("SystemSource.Cot_desc")); //$NON-NLS-1$ 
		addDoubleFunction(SourceSystemFunctions.DEGREES, QueryPlugin.Util.getString("SystemSource.Degrees_desc")); //$NON-NLS-1$ 
		addPiFunction(SourceSystemFunctions.PI, QueryPlugin.Util.getString("SystemSource.Pi_desc")); //$NON-NLS-1$ 
		addDoubleFunction(SourceSystemFunctions.RADIANS, QueryPlugin.Util.getString("SystemSource.Radians_desc")); //$NON-NLS-1$ 
		addDoubleFunction(SourceSystemFunctions.SIN, QueryPlugin.Util.getString("SystemSource.Sin_desc")); //$NON-NLS-1$ 
		addDoubleFunction(SourceSystemFunctions.TAN, QueryPlugin.Util.getString("SystemSource.Tan_desc")); //$NON-NLS-1$ 
        addDoubleFunction(SourceSystemFunctions.LOG, QueryPlugin.Util.getString("SystemSource.Log_desc")); //$NON-NLS-1$ 
        addDoubleFunction(SourceSystemFunctions.LOG10, QueryPlugin.Util.getString("SystemSource.Log10_desc")); //$NON-NLS-1$ 
        addDoubleFunction(SourceSystemFunctions.CEILING, QueryPlugin.Util.getString("SystemSource.Ceiling_desc")); //$NON-NLS-1$ 
        addDoubleFunction(SourceSystemFunctions.EXP, QueryPlugin.Util.getString("SystemSource.Exp_desc")); //$NON-NLS-1$ 
        addDoubleFunction(SourceSystemFunctions.FLOOR, QueryPlugin.Util.getString("SystemSource.Floor_desc")); //$NON-NLS-1$ 
        
        // bit
        addBitFunction(SourceSystemFunctions.BITAND, QueryPlugin.Util.getString("SystemSource.Bitand_desc"), "bitand", 2, QueryPlugin.Util.getString("SystemSource.Bitand_result_desc")); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ 
        addBitFunction(SourceSystemFunctions.BITOR, QueryPlugin.Util.getString("SystemSource.Bitor_desc"), "bitor", 2, QueryPlugin.Util.getString("SystemSource.Bitor_result_desc")); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ 
        addBitFunction(SourceSystemFunctions.BITXOR, QueryPlugin.Util.getString("SystemSource.Bitxor_desc"), "bitxor", 2, QueryPlugin.Util.getString("SystemSource.Bitxor_result_desc")); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ 
        addBitFunction(SourceSystemFunctions.BITNOT, QueryPlugin.Util.getString("SystemSource.Bitnot_desc"), "bitnot", 1, QueryPlugin.Util.getString("SystemSource.Bitnot_result_desc")); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ 

        // date
        addConstantDateFunction(SourceSystemFunctions.CURDATE, QueryPlugin.Util.getString("SystemSource.Curdate_desc"), "currentDate", DataTypeManager.DefaultDataTypes.DATE); //$NON-NLS-1$ //$NON-NLS-2$ 
        addConstantDateFunction(SourceSystemFunctions.CURTIME, QueryPlugin.Util.getString("SystemSource.Curtime_desc"), "currentTime", DataTypeManager.DefaultDataTypes.TIME); //$NON-NLS-1$ //$NON-NLS-2$ 
        addConstantDateFunction(SourceSystemFunctions.NOW, QueryPlugin.Util.getString("SystemSource.Now_desc"), "currentTimestamp", DataTypeManager.DefaultDataTypes.TIMESTAMP); //$NON-NLS-1$ //$NON-NLS-2$ 
        addDateFunction(SourceSystemFunctions.DAYNAME, "dayName", QueryPlugin.Util.getString("SystemSource.Dayname_result_d_desc"), QueryPlugin.Util.getString("SystemSource.Dayname_result_ts_desc"), DataTypeManager.DefaultDataTypes.STRING); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ 
        addDateFunction(SourceSystemFunctions.DAYOFMONTH, "dayOfMonth", QueryPlugin.Util.getString("SystemSource.Dayofmonth_result_d_desc"), QueryPlugin.Util.getString("SystemSource.Dayofmonth_result_ts_desc"), DataTypeManager.DefaultDataTypes.INTEGER); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ 
        addDateFunction(SourceSystemFunctions.DAYOFWEEK, "dayOfWeek", QueryPlugin.Util.getString("SystemSource.Dayofweek_result_d_desc"), QueryPlugin.Util.getString("SystemSource.Dayofweek_result_ts_desc"), DataTypeManager.DefaultDataTypes.INTEGER); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ 
        addDateFunction(SourceSystemFunctions.DAYOFYEAR, "dayOfYear", QueryPlugin.Util.getString("SystemSource.Dayofyear_result_d_desc"), QueryPlugin.Util.getString("SystemSource.Dayofyear_result_ts_desc"), DataTypeManager.DefaultDataTypes.INTEGER); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ 
        addDateFunction(SourceSystemFunctions.MONTH, "month", QueryPlugin.Util.getString("SystemSource.Month_result_d_desc"), QueryPlugin.Util.getString("SystemSource.Month_result_ts_desc"), DataTypeManager.DefaultDataTypes.INTEGER); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ 
        addDateFunction(SourceSystemFunctions.MONTHNAME, "monthName", QueryPlugin.Util.getString("SystemSource.Monthname_result_d_desc"), QueryPlugin.Util.getString("SystemSource.Monthname_result_ts_desc"), DataTypeManager.DefaultDataTypes.STRING); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ 
        addDateFunction(SourceSystemFunctions.WEEK, "week", QueryPlugin.Util.getString("SystemSource.Week_result_d_desc"), QueryPlugin.Util.getString("SystemSource.Week_result_ts_desc"), DataTypeManager.DefaultDataTypes.INTEGER); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ 
        addDateFunction(SourceSystemFunctions.YEAR, "year", QueryPlugin.Util.getString("SystemSource.Year_result_d_desc"), QueryPlugin.Util.getString("SystemSource.Year_result_ts_desc"), DataTypeManager.DefaultDataTypes.INTEGER); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ 
        addTimeFunction(SourceSystemFunctions.HOUR, "hour", QueryPlugin.Util.getString("SystemSource.Hour_result_t_desc"), QueryPlugin.Util.getString("SystemSource.Hour_result_ts_desc"), DataTypeManager.DefaultDataTypes.INTEGER); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ 
        addTimeFunction(SourceSystemFunctions.MINUTE, "minute", QueryPlugin.Util.getString("SystemSource.Minute_result_t_desc"), QueryPlugin.Util.getString("SystemSource.Minute_result_ts_desc"), DataTypeManager.DefaultDataTypes.INTEGER); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ 
        addTimeFunction(SourceSystemFunctions.SECOND, "second", QueryPlugin.Util.getString("SystemSource.Second_result_t_desc"), QueryPlugin.Util.getString("SystemSource.Second_result_ts_desc"), DataTypeManager.DefaultDataTypes.INTEGER); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ 
		addQuarterFunction(SourceSystemFunctions.QUARTER, "quarter", QueryPlugin.Util.getString("SystemSource.Quarter_result_d_desc"), QueryPlugin.Util.getString("SystemSource.Quarter_result_ts_desc"), DataTypeManager.DefaultDataTypes.INTEGER); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ 
		addTimestampAddFunction();
        addTimestampDiffFunction();
        addTimeZoneFunctions();
        addTimestampCreateFunction();
        addUnixTimeFunctions();
		                  
        // string
        addStringFunction(SourceSystemFunctions.LENGTH, QueryPlugin.Util.getString("SystemSource.Length_result"), "length", DataTypeManager.DefaultDataTypes.INTEGER); //$NON-NLS-1$ //$NON-NLS-2$ 
        addStringFunction(SourceSystemFunctions.UCASE, QueryPlugin.Util.getString("SystemSource.Ucase_result"), "upperCase", DataTypeManager.DefaultDataTypes.STRING); //$NON-NLS-1$ //$NON-NLS-2$ 
        addStringFunction(SourceSystemFunctions.LCASE, QueryPlugin.Util.getString("SystemSource.Lcase_result"), "lowerCase", DataTypeManager.DefaultDataTypes.STRING); //$NON-NLS-1$ //$NON-NLS-2$ 
		addStringFunction("lower", QueryPlugin.Util.getString("SystemSource.Lower_result"), "lowerCase", DataTypeManager.DefaultDataTypes.STRING); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
		addStringFunction("upper", QueryPlugin.Util.getString("SystemSource.Upper_result"), "upperCase", DataTypeManager.DefaultDataTypes.STRING); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
        addStringFunction(SourceSystemFunctions.LTRIM, QueryPlugin.Util.getString("SystemSource.Left_result"), "leftTrim", DataTypeManager.DefaultDataTypes.STRING); //$NON-NLS-1$ //$NON-NLS-2$ 
        addStringFunction(SourceSystemFunctions.RTRIM, QueryPlugin.Util.getString("SystemSource.Right_result"), "rightTrim", DataTypeManager.DefaultDataTypes.STRING); //$NON-NLS-1$ //$NON-NLS-2$ 
        addConcatFunction();    
        addSubstringFunction(); 
        addLeftRightFunctions();
        addLocateFunction();
        addReplaceFunction();
        addAsciiFunction();
        addCharFunction();
        addInitCapFunction();
        addLpadFunction();
        addRpadFunction();
        addTranslateFunction();
        addRepeatFunction();
		addSpaceFunction();
		addInsertFunction();
		
        // clob
        addClobFunction(SourceSystemFunctions.UCASE, QueryPlugin.Util.getString("SystemSource.UcaseClob_result"), "upperCase", DataTypeManager.DefaultDataTypes.CLOB); //$NON-NLS-1$ //$NON-NLS-2$ 
        addClobFunction(SourceSystemFunctions.LCASE, QueryPlugin.Util.getString("SystemSource.LcaseClob_result"), "lowerCase", DataTypeManager.DefaultDataTypes.CLOB); //$NON-NLS-1$ //$NON-NLS-2$ 
        addClobFunction("lower", QueryPlugin.Util.getString("SystemSource.LowerClob_result"), "lowerCase", DataTypeManager.DefaultDataTypes.CLOB); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
        addClobFunction("upper", QueryPlugin.Util.getString("SystemSource.UpperClob_result"), "upperCase", DataTypeManager.DefaultDataTypes.CLOB); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
        
        addToCharsFunction();
        addToBytesFunction();
        
        // conversion
        addConversionFunctions();   
        
        // miscellaneous functions
        addContextFunctions(); 
        addRowLimitFunctions();                        
        addRowLimitExceptionFunctions();                        
        addDecodeFunctions();
        addLookupFunctions();
        addUserFunction();
        addCurrentDatabaseFunction();
        if (allowEnvFunction) {
        	addEnvFunction();
        }
        addSessionIdFunction();
        addCommandPayloadFunctions();
		addIfNullFunctions();
        
		// format 
		addFormatTimestampFunction();  
		addFormatNumberFunctions();
		
		// parse
		addParseTimestampFunction();
		addParseNumberFunctions();
        
        // xml functions
        addXpathValueFunction();
        addXslTransformFunction();
        addXmlConcat();
        addXmlComment();
        addXmlPi();
        addJsonToXml();
        
        addSecurityFunctions();
        
        for (String type : DataTypeManager.getAllDataTypeNames()) {
        	if (!DataTypeManager.isNonComparable(type)) {
        		addTypedNullIfFunction(type);
        	}
        	addTypedCoalesceFunction(type);
        }
        
        addUnescape();
        addUuidFunction();
        addArrayGet();
        addArrayLength();
    }

    private void addArrayLength() {
    	functions.add(new FunctionMethod(SourceSystemFunctions.ARRAY_LENGTH, QueryPlugin.Util.getString("SystemSource.array_length_desc"), MISCELLANEOUS, PushDown.CAN_PUSHDOWN, FUNCTION_CLASS, SourceSystemFunctions.ARRAY_LENGTH, //$NON-NLS-1$ 
                new FunctionParameter[] { 
                    new FunctionParameter("array", DataTypeManager.DefaultDataTypes.OBJECT, QueryPlugin.Util.getString("SystemSource.array_param1"))}, //$NON-NLS-1$ //$NON-NLS-2$
                new FunctionParameter("result", DataTypeManager.DefaultDataTypes.INTEGER, QueryPlugin.Util.getString("SystemSource.array_length_result")), false, Determinism.DETERMINISTIC ) );       //$NON-NLS-1$ //$NON-NLS-2$
	}

	private void addArrayGet() {
    	functions.add(new FunctionMethod(SourceSystemFunctions.ARRAY_GET, QueryPlugin.Util.getString("SystemSource.array_get_desc"), MISCELLANEOUS, PushDown.CAN_PUSHDOWN, FUNCTION_CLASS, SourceSystemFunctions.ARRAY_GET, //$NON-NLS-1$ 
                new FunctionParameter[] { 
                    new FunctionParameter("array", DataTypeManager.DefaultDataTypes.OBJECT, QueryPlugin.Util.getString("SystemSource.array_param1")), //$NON-NLS-1$ //$NON-NLS-2$
                    new FunctionParameter("index", DataTypeManager.DefaultDataTypes.INTEGER, QueryPlugin.Util.getString("SystemSource.array_get_param2"))}, //$NON-NLS-1$ //$NON-NLS-2$
                new FunctionParameter("result", DataTypeManager.DefaultDataTypes.OBJECT, QueryPlugin.Util.getString("SystemSource.array_get_result")), false, Determinism.DETERMINISTIC ) );       //$NON-NLS-1$ //$NON-NLS-2$
	}

	private void addUnescape() {
    	functions.add(new FunctionMethod(SourceSystemFunctions.UNESCAPE, QueryPlugin.Util.getString("SystemSource.unescape_desc"), STRING, PushDown.CANNOT_PUSHDOWN, FUNCTION_CLASS, SourceSystemFunctions.UNESCAPE, //$NON-NLS-1$ 
                new FunctionParameter[] { 
                    new FunctionParameter("string", DataTypeManager.DefaultDataTypes.STRING, QueryPlugin.Util.getString("SystemSource.unescape_param1"))}, //$NON-NLS-1$ //$NON-NLS-2$
                new FunctionParameter("result", DataTypeManager.DefaultDataTypes.STRING, QueryPlugin.Util.getString("SystemSource.unescape_result")), false, Determinism.DETERMINISTIC ) );       //$NON-NLS-1$ //$NON-NLS-2$
	}

	private void addSecurityFunctions() {
        functions.add(new FunctionMethod("hasRole", QueryPlugin.Util.getString("SystemSource.hasRole_description"), SECURITY, PushDown.CANNOT_PUSHDOWN, SECURITY_FUNCTION_CLASS, "hasRole", //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
                                        new FunctionParameter[] { 
                                            new FunctionParameter("roleType", DataTypeManager.DefaultDataTypes.STRING, QueryPlugin.Util.getString("SystemSource.hasRole_param1")), //$NON-NLS-1$ //$NON-NLS-2$
                                            new FunctionParameter("roleName", DataTypeManager.DefaultDataTypes.STRING, QueryPlugin.Util.getString("SystemSource.hasRole_param2"))}, //$NON-NLS-1$ //$NON-NLS-2$ 
                                        new FunctionParameter("result", DataTypeManager.DefaultDataTypes.BOOLEAN, QueryPlugin.Util.getString("SystemSource.hasRole_result")), false, Determinism.USER_DETERMINISTIC ) );       //$NON-NLS-1$ //$NON-NLS-2$
        
        functions.add(new FunctionMethod("hasRole", QueryPlugin.Util.getString("SystemSource.hasRole_description"), SECURITY, PushDown.CANNOT_PUSHDOWN, SECURITY_FUNCTION_CLASS, "hasRole", //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
                new FunctionParameter[] { 
                    new FunctionParameter("roleName", DataTypeManager.DefaultDataTypes.STRING, QueryPlugin.Util.getString("SystemSource.hasRole_param2"))}, //$NON-NLS-1$ //$NON-NLS-2$ 
                new FunctionParameter("result", DataTypeManager.DefaultDataTypes.BOOLEAN, QueryPlugin.Util.getString("SystemSource.hasRole_result")), false, Determinism.USER_DETERMINISTIC ) );       //$NON-NLS-1$ //$NON-NLS-2$
    }

    private void addFormatNumberFunctions() {
		addFormatNumberFunction(SourceSystemFunctions.FORMATINTEGER, QueryPlugin.Util.getString("SystemSource.Formatinteger_desc"), "format", "integer", DataTypeManager.DefaultDataTypes.INTEGER, QueryPlugin.Util.getString("SystemSource.Formatinteger_result_desc")); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$ 
		addFormatNumberFunction(SourceSystemFunctions.FORMATLONG, QueryPlugin.Util.getString("SystemSource.Formatlong_desc"), "format", "long", DataTypeManager.DefaultDataTypes.LONG, QueryPlugin.Util.getString("SystemSource.Formatlong_result_desc")); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$ 
		addFormatNumberFunction(SourceSystemFunctions.FORMATDOUBLE, QueryPlugin.Util.getString("SystemSource.Formatdouble_desc"), "format", "double", DataTypeManager.DefaultDataTypes.DOUBLE, QueryPlugin.Util.getString("SystemSource.Formatdouble_result_desc")); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$ 
		addFormatNumberFunction(SourceSystemFunctions.FORMATFLOAT, QueryPlugin.Util.getString("SystemSource.Formatfloat_desc"), "format", "float", DataTypeManager.DefaultDataTypes.FLOAT, QueryPlugin.Util.getString("SystemSource.Formatfloat_result_desc")); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$ 
		addFormatNumberFunction(SourceSystemFunctions.FORMATBIGINTEGER, QueryPlugin.Util.getString("SystemSource.Formatbiginteger_desc"), "format", "biginteger", DataTypeManager.DefaultDataTypes.BIG_INTEGER, QueryPlugin.Util.getString("SystemSource.Formatbiginteger_result_desc")); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$ 
		addFormatNumberFunction(SourceSystemFunctions.FORMATBIGDECIMAL, QueryPlugin.Util.getString("SystemSource.Formatbigdecimal_desc"), "format", "bigdecimal", DataTypeManager.DefaultDataTypes.BIG_DECIMAL, QueryPlugin.Util.getString("SystemSource.Formatbigdecimal_result_desc")); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$ 
	}
	
	private void addParseNumberFunctions() {
		addParseNumberFunction(SourceSystemFunctions.PARSEINTEGER, QueryPlugin.Util.getString("SystemSource.Parseinteger_desc"), "parseInteger", "integer", DataTypeManager.DefaultDataTypes.INTEGER, QueryPlugin.Util.getString("SystemSource.Parseinteger_result_desc")); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$ 
		addParseNumberFunction(SourceSystemFunctions.PARSELONG, QueryPlugin.Util.getString("SystemSource.Parselong_desc"), "parseLong", "long", DataTypeManager.DefaultDataTypes.LONG, QueryPlugin.Util.getString("SystemSource.Parselong_result_desc")); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$ 
		addParseNumberFunction(SourceSystemFunctions.PARSEDOUBLE, QueryPlugin.Util.getString("SystemSource.Parsedouble_desc"), "parseDouble", "double", DataTypeManager.DefaultDataTypes.DOUBLE, QueryPlugin.Util.getString("SystemSource.Parsedouble_result_desc")); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$ 
		addParseNumberFunction(SourceSystemFunctions.PARSEFLOAT, QueryPlugin.Util.getString("SystemSource.Parsefloat_desc"), "parseFloat", "float", DataTypeManager.DefaultDataTypes.FLOAT, QueryPlugin.Util.getString("SystemSource.Parsefloat_result_desc")); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$ 
		addParseNumberFunction(SourceSystemFunctions.PARSEBIGINTEGER, QueryPlugin.Util.getString("SystemSource.Parsebiginteger_desc"), "parseBigInteger", "biginteger", DataTypeManager.DefaultDataTypes.BIG_INTEGER, QueryPlugin.Util.getString("SystemSource.Parsebiginteger_result_desc")); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$ 
		addParseNumberFunction(SourceSystemFunctions.PARSEBIGDECIMAL, QueryPlugin.Util.getString("SystemSource.Parsebigdecimal_desc"), "parseBigDecimal", "bigdecimal", DataTypeManager.DefaultDataTypes.BIG_DECIMAL, QueryPlugin.Util.getString("SystemSource.Parsebigdecimal_result_desc"));	 //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$ 
	}
	
    private void addArithmeticFunction(String functionName, String description, String methodName, String resultsDescription) {
        addTypedArithmeticFunction(functionName, description, methodName, resultsDescription, DataTypeManager.DefaultDataTypes.INTEGER);
        addTypedArithmeticFunction(functionName, description, methodName, resultsDescription, DataTypeManager.DefaultDataTypes.LONG);
        addTypedArithmeticFunction(functionName, description, methodName, resultsDescription, DataTypeManager.DefaultDataTypes.FLOAT);
        addTypedArithmeticFunction(functionName, description, methodName, resultsDescription, DataTypeManager.DefaultDataTypes.DOUBLE);
        addTypedArithmeticFunction(functionName, description, methodName, resultsDescription, DataTypeManager.DefaultDataTypes.BIG_INTEGER);
        addTypedArithmeticFunction(functionName, description, methodName, resultsDescription, DataTypeManager.DefaultDataTypes.BIG_DECIMAL);
    }
    
    private void addTypedArithmeticFunction(String functionName, String description, String methodName, String resultsDescription, String type) {
        functions.add(
            new FunctionMethod(functionName, description, NUMERIC, FUNCTION_CLASS, methodName,
                new FunctionParameter[] { 
                    new FunctionParameter("op1", type, QueryPlugin.Util.getString("SystemSource.Arith_left_op")), //$NON-NLS-1$ //$NON-NLS-2$
                    new FunctionParameter("op2", type, QueryPlugin.Util.getString("SystemSource.Arith_right_op")) }, //$NON-NLS-1$ //$NON-NLS-2$
                new FunctionParameter("result", type, resultsDescription) ) );                 //$NON-NLS-1$
    }
    
    private void addAbsFunction() {
        addTypedAbsFunction(DataTypeManager.DefaultDataTypes.INTEGER);
        addTypedAbsFunction(DataTypeManager.DefaultDataTypes.LONG);
        addTypedAbsFunction(DataTypeManager.DefaultDataTypes.FLOAT);
        addTypedAbsFunction(DataTypeManager.DefaultDataTypes.DOUBLE);
        addTypedAbsFunction(DataTypeManager.DefaultDataTypes.BIG_INTEGER);
        addTypedAbsFunction(DataTypeManager.DefaultDataTypes.BIG_DECIMAL);
    }

    private void addTypedAbsFunction(String type) {
        functions.add(
            new FunctionMethod(SourceSystemFunctions.ABS, QueryPlugin.Util.getString("SystemSource.Abs_desc"), NUMERIC, FUNCTION_CLASS, "abs", //$NON-NLS-1$ //$NON-NLS-2$ 
                new FunctionParameter[] { 
                    new FunctionParameter("number", type, QueryPlugin.Util.getString("SystemSource.Abs_arg")) }, //$NON-NLS-1$ //$NON-NLS-2$
                new FunctionParameter("result", type, QueryPlugin.Util.getString("SystemSource.Abs_result_desc")) ) );                 //$NON-NLS-1$ //$NON-NLS-2$
    }
    
    private void addRandFunction() {
        // With Seed
        FunctionMethod rand = new FunctionMethod(SourceSystemFunctions.RAND, QueryPlugin.Util.getString("SystemSource.Rand_desc"), NUMERIC, FUNCTION_CLASS, "rand", //$NON-NLS-1$ //$NON-NLS-2$ 
                                          new FunctionParameter[] {new FunctionParameter("seed", DataTypeManager.DefaultDataTypes.INTEGER, QueryPlugin.Util.getString("SystemSource.Rand_arg")) }, //$NON-NLS-1$ //$NON-NLS-2$
                                          new FunctionParameter("result", DataTypeManager.DefaultDataTypes.DOUBLE, QueryPlugin.Util.getString("SystemSource.Rand_result_desc")) );                 //$NON-NLS-1$ //$NON-NLS-2$
        rand.setNullOnNull(false);
        rand.setDeterminism(Determinism.NONDETERMINISTIC);
        functions.add(rand);
        // Without Seed
        rand = new FunctionMethod(SourceSystemFunctions.RAND, QueryPlugin.Util.getString("SystemSource.Rand_desc"), NUMERIC, FUNCTION_CLASS, "rand", //$NON-NLS-1$ //$NON-NLS-2$ 
                                          new FunctionParameter[] {}, 
                                          new FunctionParameter("result", DataTypeManager.DefaultDataTypes.DOUBLE, QueryPlugin.Util.getString("SystemSource.Rand_result_desc")) ); //$NON-NLS-1$ //$NON-NLS-2$
        rand.setDeterminism(Determinism.NONDETERMINISTIC);
        functions.add(rand);
    }
    
    private void addUuidFunction() {
        FunctionMethod rand = new FunctionMethod(SourceSystemFunctions.UUID, QueryPlugin.Util.getString("SystemSource.uuid_desc"), MISCELLANEOUS, FUNCTION_CLASS, "uuid", //$NON-NLS-1$ //$NON-NLS-2$ 
                                          new FunctionParameter[] {},
                                          new FunctionParameter("result", DataTypeManager.DefaultDataTypes.STRING, QueryPlugin.Util.getString("SystemSource.uuid_result_desc")) );                 //$NON-NLS-1$ //$NON-NLS-2$
        rand.setDeterminism(Determinism.NONDETERMINISTIC);
        functions.add(rand);
    }

	private void addDoubleFunction(String name, String description) {
		functions.add(
			new FunctionMethod(name, description, NUMERIC, FUNCTION_CLASS, name,
				new FunctionParameter[] { 
					new FunctionParameter("number", DataTypeManager.DefaultDataTypes.DOUBLE, QueryPlugin.Util.getString("SystemSource.Double_arg2")) }, //$NON-NLS-1$ //$NON-NLS-2$
				new FunctionParameter("result", DataTypeManager.DefaultDataTypes.DOUBLE, description) ) );                 //$NON-NLS-1$
		functions.add(
				new FunctionMethod(name, description, NUMERIC, FUNCTION_CLASS, name,
					new FunctionParameter[] { 
						new FunctionParameter("number", DataTypeManager.DefaultDataTypes.BIG_DECIMAL, QueryPlugin.Util.getString("SystemSource.Double_arg2")) }, //$NON-NLS-1$ //$NON-NLS-2$
					new FunctionParameter("result", DataTypeManager.DefaultDataTypes.DOUBLE, description) ) );                 //$NON-NLS-1$
	}

	private void addAtan2Function(String name, String description) {
		functions.add(
			new FunctionMethod(name, description, NUMERIC, FUNCTION_CLASS, name,
				new FunctionParameter[] { 
					new FunctionParameter("number1", DataTypeManager.DefaultDataTypes.DOUBLE, QueryPlugin.Util.getString("SystemSource.Atan_arg1")), //$NON-NLS-1$ //$NON-NLS-2$
					new FunctionParameter("number2", DataTypeManager.DefaultDataTypes.DOUBLE, QueryPlugin.Util.getString("SystemSource.Atan_arg2")) }, //$NON-NLS-1$ //$NON-NLS-2$
				new FunctionParameter("result", DataTypeManager.DefaultDataTypes.DOUBLE, description) ) );                 //$NON-NLS-1$
		functions.add(
				new FunctionMethod(name, description, NUMERIC, FUNCTION_CLASS, name,
					new FunctionParameter[] { 
						new FunctionParameter("number1", DataTypeManager.DefaultDataTypes.BIG_DECIMAL, QueryPlugin.Util.getString("SystemSource.Atan_arg1")), //$NON-NLS-1$ //$NON-NLS-2$
						new FunctionParameter("number2", DataTypeManager.DefaultDataTypes.BIG_DECIMAL, QueryPlugin.Util.getString("SystemSource.Atan_arg2")) }, //$NON-NLS-1$ //$NON-NLS-2$
					new FunctionParameter("result", DataTypeManager.DefaultDataTypes.DOUBLE, description) ) );                 //$NON-NLS-1$
	}

	private void addPiFunction(String name, String description) {
		functions.add(
			new FunctionMethod(name, description, NUMERIC, FUNCTION_CLASS, name,
				new FunctionParameter[] { },
				new FunctionParameter("result", DataTypeManager.DefaultDataTypes.DOUBLE, description) ) );                 //$NON-NLS-1$
	}
			
    private void addPowerFunction() {
        addTypedPowerFunction(DataTypeManager.DefaultDataTypes.DOUBLE, DataTypeManager.DefaultDataTypes.DOUBLE);
        addTypedPowerFunction(DataTypeManager.DefaultDataTypes.BIG_INTEGER, DataTypeManager.DefaultDataTypes.INTEGER);        
        addTypedPowerFunction(DataTypeManager.DefaultDataTypes.BIG_DECIMAL, DataTypeManager.DefaultDataTypes.INTEGER);
    }

    private void addTypedPowerFunction(String baseType, String powerType) { 
        functions.add(
            new FunctionMethod(SourceSystemFunctions.POWER, QueryPlugin.Util.getString("SystemSource.Power_desc"), NUMERIC, FUNCTION_CLASS, "power", //$NON-NLS-1$ //$NON-NLS-2$ 
                new FunctionParameter[] { 
                    new FunctionParameter("base", baseType, QueryPlugin.Util.getString("SystemSource.Power_arg1")), //$NON-NLS-1$ //$NON-NLS-2$
                    new FunctionParameter("power", powerType, QueryPlugin.Util.getString("SystemSource.Power_arg2")) }, //$NON-NLS-1$ //$NON-NLS-2$
                new FunctionParameter("result", baseType, QueryPlugin.Util.getString("SystemSource.Power_result_desc")) ) );                 //$NON-NLS-1$ //$NON-NLS-2$
    } 

    private void addRoundFunction() {
        addTypedRoundFunction(DataTypeManager.DefaultDataTypes.INTEGER);
        addTypedRoundFunction(DataTypeManager.DefaultDataTypes.FLOAT);
        addTypedRoundFunction(DataTypeManager.DefaultDataTypes.DOUBLE);
        addTypedRoundFunction(DataTypeManager.DefaultDataTypes.BIG_DECIMAL);        
    }

    private void addTypedRoundFunction(String roundType) { 
        functions.add(
            new FunctionMethod(SourceSystemFunctions.ROUND, QueryPlugin.Util.getString("SystemSource.Round_desc"), NUMERIC, FUNCTION_CLASS, "round", //$NON-NLS-1$ //$NON-NLS-2$ 
                new FunctionParameter[] { 
                    new FunctionParameter("number", roundType, QueryPlugin.Util.getString("SystemSource.Round_arg1")), //$NON-NLS-1$ //$NON-NLS-2$
                    new FunctionParameter("places", DataTypeManager.DefaultDataTypes.INTEGER, QueryPlugin.Util.getString("SystemSource.Round_arg2")) }, //$NON-NLS-1$ //$NON-NLS-2$
                new FunctionParameter("result", roundType, QueryPlugin.Util.getString("SystemSource.Round_result_desc")) ) );                 //$NON-NLS-1$ //$NON-NLS-2$
    } 

    private void addSignFunction() {
        addTypedSignFunction(DataTypeManager.DefaultDataTypes.INTEGER);
        addTypedSignFunction(DataTypeManager.DefaultDataTypes.LONG);
        addTypedSignFunction(DataTypeManager.DefaultDataTypes.FLOAT);
        addTypedSignFunction(DataTypeManager.DefaultDataTypes.DOUBLE);
        addTypedSignFunction(DataTypeManager.DefaultDataTypes.BIG_INTEGER);
        addTypedSignFunction(DataTypeManager.DefaultDataTypes.BIG_DECIMAL);
    }
    
    private void addTypedSignFunction(String type) {        
        functions.add(
            new FunctionMethod(SourceSystemFunctions.SIGN, QueryPlugin.Util.getString("SystemSource.Sign_desc"), NUMERIC, FUNCTION_CLASS, "sign", //$NON-NLS-1$ //$NON-NLS-2$ 
                new FunctionParameter[] { 
                    new FunctionParameter("number", type, QueryPlugin.Util.getString("SystemSource.Sign_arg1")) }, //$NON-NLS-1$ //$NON-NLS-2$
                new FunctionParameter("result", DataTypeManager.DefaultDataTypes.INTEGER, QueryPlugin.Util.getString("SystemSource.Sign_result_desc")) ) );                 //$NON-NLS-1$ //$NON-NLS-2$
    } 
    
    private void addSqrtFunction() {
        addTypedSqrtFunction(DataTypeManager.DefaultDataTypes.LONG);
        addTypedSqrtFunction(DataTypeManager.DefaultDataTypes.DOUBLE);
        addTypedSqrtFunction(DataTypeManager.DefaultDataTypes.BIG_DECIMAL);
    }
    
    private void addTypedSqrtFunction(String type) {        
        functions.add(
            new FunctionMethod(SourceSystemFunctions.SQRT, QueryPlugin.Util.getString("SystemSource.Sqrt_desc"), NUMERIC, FUNCTION_CLASS, "sqrt", //$NON-NLS-1$ //$NON-NLS-2$ 
                new FunctionParameter[] { 
                    new FunctionParameter("number", type, QueryPlugin.Util.getString("SystemSource.Sqrt_arg1")) }, //$NON-NLS-1$ //$NON-NLS-2$
                new FunctionParameter("result", DataTypeManager.DefaultDataTypes.DOUBLE, QueryPlugin.Util.getString("SystemSource.Sqrt_result_desc")) ) );                 //$NON-NLS-1$ //$NON-NLS-2$
    } 
    
    /**
     * Date functions a marked as command deterministic, since we prefer pre-evaluation rather than row-by-row
     * evaluation.
     */
    private void addConstantDateFunction(String name, String description, String methodName, String returnType) {
        FunctionMethod method = new FunctionMethod(name, description, DATETIME, FUNCTION_CLASS, methodName,
                new FunctionParameter[] {},
                new FunctionParameter("result", returnType, description));                 //$NON-NLS-1$
        method.setDeterminism(Determinism.COMMAND_DETERMINISTIC);
        functions.add(method);
    }

    private void addDateFunction(String name, String methodName, String dateDesc, String timestampDesc, String returnType) {
        functions.add(
            new FunctionMethod(name, dateDesc, DATETIME, FUNCTION_CLASS, methodName,
                new FunctionParameter[] {
                    new FunctionParameter("date", DataTypeManager.DefaultDataTypes.DATE, dateDesc) }, //$NON-NLS-1$
                new FunctionParameter("result", returnType, dateDesc) ) );                 //$NON-NLS-1$
        functions.add(
            new FunctionMethod(name, timestampDesc, DATETIME, FUNCTION_CLASS, methodName,
                new FunctionParameter[] {
                    new FunctionParameter("timestamp", DataTypeManager.DefaultDataTypes.TIMESTAMP, timestampDesc) }, //$NON-NLS-1$
                new FunctionParameter("result", returnType, timestampDesc) ) );                 //$NON-NLS-1$
    }

	private void addQuarterFunction(String name, String methodName, String dateDesc, String timestampDesc, String returnType) {
		functions.add(
			new FunctionMethod(name, dateDesc, DATETIME, FUNCTION_CLASS, methodName,
				new FunctionParameter[] {
					new FunctionParameter("date", DataTypeManager.DefaultDataTypes.DATE, dateDesc) }, //$NON-NLS-1$
				new FunctionParameter("result", returnType, dateDesc) ) );                 //$NON-NLS-1$
		functions.add(
			new FunctionMethod(name, timestampDesc, DATETIME, FUNCTION_CLASS, methodName,
				new FunctionParameter[] {
					new FunctionParameter("timestamp", DataTypeManager.DefaultDataTypes.TIMESTAMP, timestampDesc) }, //$NON-NLS-1$
				new FunctionParameter("result", returnType, timestampDesc) ) );                 //$NON-NLS-1$
	}

	private void addTimestampAddFunction() {
		functions.add(
			new FunctionMethod(SourceSystemFunctions.TIMESTAMPADD, QueryPlugin.Util.getString("SystemSource.Timestampadd_d_desc"), DATETIME, PushDown.SYNTHETIC, null, null, //$NON-NLS-1$ 
				new FunctionParameter[] {
					new FunctionParameter("interval", DataTypeManager.DefaultDataTypes.STRING, QueryPlugin.Util.getString("SystemSource.Timestampadd_d_arg1")),  //$NON-NLS-1$ //$NON-NLS-2$
					new FunctionParameter("count", DataTypeManager.DefaultDataTypes.INTEGER, QueryPlugin.Util.getString("SystemSource.Timestampadd_d_arg2")),  //$NON-NLS-1$ //$NON-NLS-2$
					new FunctionParameter("timestamp", DataTypeManager.DefaultDataTypes.DATE, QueryPlugin.Util.getString("SystemSource.Timestampadd_d_arg3"))}, //$NON-NLS-1$ //$NON-NLS-2$
				new FunctionParameter("result", DataTypeManager.DefaultDataTypes.DATE, QueryPlugin.Util.getString("SystemSource.Timestampadd_d_result_desc")) ) );                 //$NON-NLS-1$ //$NON-NLS-2$
		functions.add(
			new FunctionMethod(SourceSystemFunctions.TIMESTAMPADD, QueryPlugin.Util.getString("SystemSource.Timestampadd_t_desc"), DATETIME, PushDown.SYNTHETIC, null, null, //$NON-NLS-1$  
				new FunctionParameter[] {
					new FunctionParameter("interval", DataTypeManager.DefaultDataTypes.STRING, QueryPlugin.Util.getString("SystemSource.Timestampadd_t_arg1")),  //$NON-NLS-1$ //$NON-NLS-2$
					new FunctionParameter("count", DataTypeManager.DefaultDataTypes.INTEGER, QueryPlugin.Util.getString("SystemSource.Timestampadd_t_arg2")),  //$NON-NLS-1$ //$NON-NLS-2$
					new FunctionParameter("timestamp", DataTypeManager.DefaultDataTypes.TIME, QueryPlugin.Util.getString("SystemSource.Timestampadd_t_arg3"))}, //$NON-NLS-1$ //$NON-NLS-2$
				new FunctionParameter("result", DataTypeManager.DefaultDataTypes.TIME, QueryPlugin.Util.getString("SystemSource.Timestampadd_t_result_desc")) ) );                 //$NON-NLS-1$ //$NON-NLS-2$
		functions.add(
			new FunctionMethod(SourceSystemFunctions.TIMESTAMPADD, QueryPlugin.Util.getString("SystemSource.Timestampadd_ts_desc"), DATETIME, FUNCTION_CLASS, "timestampAdd", //$NON-NLS-1$ //$NON-NLS-2$ 
				new FunctionParameter[] {
					new FunctionParameter("interval", DataTypeManager.DefaultDataTypes.STRING, QueryPlugin.Util.getString("SystemSource.Timestampadd_ts_arg1")),  //$NON-NLS-1$ //$NON-NLS-2$
					new FunctionParameter("count", DataTypeManager.DefaultDataTypes.INTEGER, QueryPlugin.Util.getString("SystemSource.Timestampadd_ts_arg2")),  //$NON-NLS-1$ //$NON-NLS-2$
					new FunctionParameter("timestamp", DataTypeManager.DefaultDataTypes.TIMESTAMP, QueryPlugin.Util.getString("SystemSource.Timestampadd_ts_arg3"))}, //$NON-NLS-1$ //$NON-NLS-2$
				new FunctionParameter("result", DataTypeManager.DefaultDataTypes.TIMESTAMP, QueryPlugin.Util.getString("SystemSource.Timestampadd_ts_result")) ) );			                //$NON-NLS-1$ //$NON-NLS-2$
	}

    private void addTimestampDiffFunction() {
        functions.add(
            new FunctionMethod(SourceSystemFunctions.TIMESTAMPDIFF, QueryPlugin.Util.getString("SystemSource.Timestampdiff_ts_desc"), DATETIME, FUNCTION_CLASS, "timestampDiff", //$NON-NLS-1$ //$NON-NLS-2$ 
                new FunctionParameter[] {
                    new FunctionParameter("interval", DataTypeManager.DefaultDataTypes.STRING, QueryPlugin.Util.getString("SystemSource.Timestampdiff_ts_arg1")),  //$NON-NLS-1$ //$NON-NLS-2$
                    new FunctionParameter("timestamp1", DataTypeManager.DefaultDataTypes.TIMESTAMP, QueryPlugin.Util.getString("SystemSource.Timestampdiff_ts_arg2")), //$NON-NLS-1$ //$NON-NLS-2$
                    new FunctionParameter("timestamp2", DataTypeManager.DefaultDataTypes.TIMESTAMP, QueryPlugin.Util.getString("SystemSource.Timestampdiff_ts_arg3"))}, //$NON-NLS-1$ //$NON-NLS-2$
                new FunctionParameter("result", DataTypeManager.DefaultDataTypes.LONG, QueryPlugin.Util.getString("SystemSource.Timestampdiff_ts_result_desc")) ) );                 //$NON-NLS-1$ //$NON-NLS-2$
    }
    
    private void addTimestampCreateFunction() {
        functions.add(
              new FunctionMethod(SourceSystemFunctions.TIMESTAMPCREATE, QueryPlugin.Util.getString("SystemSource.TimestampCreate_desc"), DATETIME, FUNCTION_CLASS, "timestampCreate", //$NON-NLS-1$ //$NON-NLS-2$ 
                  new FunctionParameter[] {
                      new FunctionParameter("date", DataTypeManager.DefaultDataTypes.DATE, QueryPlugin.Util.getString("SystemSource.TimestampCreate_arg1")),  //$NON-NLS-1$ //$NON-NLS-2$
                      new FunctionParameter("time", DataTypeManager.DefaultDataTypes.TIME, QueryPlugin.Util.getString("SystemSource.TimestampCreate_arg2"))}, //$NON-NLS-1$ //$NON-NLS-2$
                  new FunctionParameter("result", DataTypeManager.DefaultDataTypes.TIMESTAMP, QueryPlugin.Util.getString("SystemSource.TimestampCreate_result_desc")) ) );                 //$NON-NLS-1$ //$NON-NLS-2$
    }
   
    private void addTimeFunction(String name, String methodName, String timeDesc, String timestampDesc, String returnType) {
        functions.add(
            new FunctionMethod(name, timeDesc, DATETIME, FUNCTION_CLASS, methodName,
                new FunctionParameter[] {
                    new FunctionParameter("time", DataTypeManager.DefaultDataTypes.TIME, timeDesc) }, //$NON-NLS-1$
                new FunctionParameter("result", returnType, timeDesc) ) );                 //$NON-NLS-1$
        functions.add(
            new FunctionMethod(name, timestampDesc, DATETIME, FUNCTION_CLASS, methodName,
                new FunctionParameter[] {
                    new FunctionParameter("timestamp", DataTypeManager.DefaultDataTypes.TIMESTAMP, timestampDesc) }, //$NON-NLS-1$
                new FunctionParameter("result", returnType, timestampDesc) ) );                 //$NON-NLS-1$
    }
    
    private void addStringFunction(String name, String description, String methodName, String returnType) {
        functions.add(
            new FunctionMethod(name, description, STRING, FUNCTION_CLASS, methodName,
                new FunctionParameter[] {
                    new FunctionParameter("string", DataTypeManager.DefaultDataTypes.STRING, QueryPlugin.Util.getString("SystemSource.Stringfunc_arg1")) }, //$NON-NLS-1$ //$NON-NLS-2$
                new FunctionParameter("result", returnType, description) ) );                 //$NON-NLS-1$
    }
    
    private void addClobFunction(String name, String description, String methodName, String returnType) {
        functions.add(
            new FunctionMethod(name, description, STRING, PushDown.MUST_PUSHDOWN, FUNCTION_CLASS, methodName,
                new FunctionParameter[] {
                    new FunctionParameter("clob", DataTypeManager.DefaultDataTypes.CLOB, QueryPlugin.Util.getString("SystemSource.Clobfunc_arg1")) }, //$NON-NLS-1$ //$NON-NLS-2$
                new FunctionParameter("result", returnType, description), false, Determinism.DETERMINISTIC ) );                 //$NON-NLS-1$
    }

    private void addConcatFunction() {
        functions.add(
            new FunctionMethod(SourceSystemFunctions.CONCAT, QueryPlugin.Util.getString("SystemSource.Concat_desc"), STRING, FUNCTION_CLASS, "concat", //$NON-NLS-1$ //$NON-NLS-2$ 
                new FunctionParameter[] {
                    new FunctionParameter("string1", DataTypeManager.DefaultDataTypes.STRING, QueryPlugin.Util.getString("SystemSource.Concat_arg1")), //$NON-NLS-1$ //$NON-NLS-2$
                    new FunctionParameter("string2", DataTypeManager.DefaultDataTypes.STRING, QueryPlugin.Util.getString("SystemSource.Concat_arg2")) }, //$NON-NLS-1$ //$NON-NLS-2$
                new FunctionParameter("result", DataTypeManager.DefaultDataTypes.STRING, QueryPlugin.Util.getString("SystemSource.Concat_result_desc")) ) );                 //$NON-NLS-1$ //$NON-NLS-2$
        functions.add(
            new FunctionMethod("||", QueryPlugin.Util.getString("SystemSource.Concatop_desc"), STRING, FUNCTION_CLASS, "concat", //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
                new FunctionParameter[] {
                    new FunctionParameter("string1", DataTypeManager.DefaultDataTypes.STRING, QueryPlugin.Util.getString("SystemSource.Concatop_arg1")), //$NON-NLS-1$ //$NON-NLS-2$
                    new FunctionParameter("string2", DataTypeManager.DefaultDataTypes.STRING, QueryPlugin.Util.getString("SystemSource.Concatop_arg2")) }, //$NON-NLS-1$ //$NON-NLS-2$
                new FunctionParameter("result", DataTypeManager.DefaultDataTypes.STRING, QueryPlugin.Util.getString("SystemSource.Concatop_result_desc")) ) );                 //$NON-NLS-1$ //$NON-NLS-2$
        
        FunctionMethod concat2 = new FunctionMethod("concat2", QueryPlugin.Util.getString("SystemSource.Concat_desc"), STRING, PushDown.SYNTHETIC, null, null, //$NON-NLS-1$ //$NON-NLS-2$ 
                           new FunctionParameter[] {
                               new FunctionParameter("string1", DataTypeManager.DefaultDataTypes.STRING, QueryPlugin.Util.getString("SystemSource.Concat_arg1")), //$NON-NLS-1$ //$NON-NLS-2$
                               new FunctionParameter("string2", DataTypeManager.DefaultDataTypes.STRING, QueryPlugin.Util.getString("SystemSource.Concat_arg2")) }, //$NON-NLS-1$ //$NON-NLS-2$
                           new FunctionParameter("result", DataTypeManager.DefaultDataTypes.STRING, QueryPlugin.Util.getString("SystemSource.Concat_result_desc")), true, Determinism.DETERMINISTIC ); //$NON-NLS-1$ //$NON-NLS-2$
        functions.add(concat2);                         
    }

    private void addSubstringFunction() {
        functions.add(
            new FunctionMethod(SourceSystemFunctions.SUBSTRING, QueryPlugin.Util.getString("SystemSource.Substring_desc"), STRING, FUNCTION_CLASS, "substring", //$NON-NLS-1$ //$NON-NLS-2$ 
                new FunctionParameter[] {
                    new FunctionParameter("string", DataTypeManager.DefaultDataTypes.STRING, QueryPlugin.Util.getString("SystemSource.Substring_arg1")), //$NON-NLS-1$ //$NON-NLS-2$
                    new FunctionParameter("index", DataTypeManager.DefaultDataTypes.INTEGER, QueryPlugin.Util.getString("SystemSource.Substring_arg2")), //$NON-NLS-1$ //$NON-NLS-2$
                    new FunctionParameter("length", DataTypeManager.DefaultDataTypes.INTEGER, QueryPlugin.Util.getString("SystemSource.Substring_arg3")) }, //$NON-NLS-1$ //$NON-NLS-2$
                new FunctionParameter("result", DataTypeManager.DefaultDataTypes.STRING, QueryPlugin.Util.getString("SystemSource.Substring_result")) ) );                 //$NON-NLS-1$ //$NON-NLS-2$
        functions.add(
            new FunctionMethod(SourceSystemFunctions.SUBSTRING, QueryPlugin.Util.getString("SystemSource.Susbstring2_desc"), STRING, FUNCTION_CLASS, "substring", //$NON-NLS-1$ //$NON-NLS-2$ 
                new FunctionParameter[] {
                    new FunctionParameter("string", DataTypeManager.DefaultDataTypes.STRING, QueryPlugin.Util.getString("SystemSource.Substring2_arg1")), //$NON-NLS-1$ //$NON-NLS-2$
                    new FunctionParameter("index", DataTypeManager.DefaultDataTypes.INTEGER, QueryPlugin.Util.getString("SystemSource.Substring2_arg2")) }, //$NON-NLS-1$ //$NON-NLS-2$
                new FunctionParameter("result", DataTypeManager.DefaultDataTypes.STRING, QueryPlugin.Util.getString("SystemSource.Substring2_result")) ) );                 //$NON-NLS-1$ //$NON-NLS-2$
    }

    private void addLeftRightFunctions() {
        functions.add(
            new FunctionMethod(SourceSystemFunctions.LEFT, QueryPlugin.Util.getString("SystemSource.Left_desc"), STRING, FUNCTION_CLASS, "left", //$NON-NLS-1$ //$NON-NLS-2$ 
                new FunctionParameter[] {
                    new FunctionParameter("string", DataTypeManager.DefaultDataTypes.STRING, QueryPlugin.Util.getString("SystemSource.Left_arg1")), //$NON-NLS-1$ //$NON-NLS-2$
                    new FunctionParameter("length", DataTypeManager.DefaultDataTypes.INTEGER, QueryPlugin.Util.getString("SystemSource.Left_arg2")) }, //$NON-NLS-1$ //$NON-NLS-2$
                new FunctionParameter("result", DataTypeManager.DefaultDataTypes.STRING, QueryPlugin.Util.getString("SystemSource.Left2_result")) ) );                 //$NON-NLS-1$ //$NON-NLS-2$
        functions.add(
            new FunctionMethod(SourceSystemFunctions.RIGHT, QueryPlugin.Util.getString("SystemSource.Right_desc"), STRING, FUNCTION_CLASS, "right", //$NON-NLS-1$ //$NON-NLS-2$ 
                new FunctionParameter[] {
                    new FunctionParameter("string", DataTypeManager.DefaultDataTypes.STRING, QueryPlugin.Util.getString("SystemSource.Right_arg1")), //$NON-NLS-1$ //$NON-NLS-2$
                    new FunctionParameter("length", DataTypeManager.DefaultDataTypes.INTEGER, QueryPlugin.Util.getString("SystemSource.Right_arg2")) }, //$NON-NLS-1$ //$NON-NLS-2$
                new FunctionParameter("result", DataTypeManager.DefaultDataTypes.STRING, QueryPlugin.Util.getString("SystemSource.Right2_result")) ) );                 //$NON-NLS-1$ //$NON-NLS-2$
    }
           
    private void addLocateFunction() {
        FunctionMethod func =
            new FunctionMethod(SourceSystemFunctions.LOCATE, QueryPlugin.Util.getString("SystemSource.Locate_desc"), STRING, FUNCTION_CLASS, "locate", //$NON-NLS-1$ //$NON-NLS-2$ 
                new FunctionParameter[] {
                    new FunctionParameter("substring", DataTypeManager.DefaultDataTypes.STRING, QueryPlugin.Util.getString("SystemSource.Locate_arg1")), //$NON-NLS-1$ //$NON-NLS-2$
                    new FunctionParameter("string", DataTypeManager.DefaultDataTypes.STRING, QueryPlugin.Util.getString("SystemSource.Locate_arg2")), //$NON-NLS-1$ //$NON-NLS-2$
                    new FunctionParameter("index", DataTypeManager.DefaultDataTypes.INTEGER, QueryPlugin.Util.getString("SystemSource.Locate_arg3")) }, //$NON-NLS-1$ //$NON-NLS-2$
                new FunctionParameter("result", DataTypeManager.DefaultDataTypes.INTEGER, QueryPlugin.Util.getString("SystemSource.Locate_result")) );                 //$NON-NLS-1$ //$NON-NLS-2$
        func.setNullOnNull(false);
        functions.add(func);
        functions.add(
            new FunctionMethod(SourceSystemFunctions.LOCATE, QueryPlugin.Util.getString("SystemSource.Locate2_desc"), STRING, FUNCTION_CLASS, "locate", //$NON-NLS-1$ //$NON-NLS-2$ 
                new FunctionParameter[] {
                    new FunctionParameter("substring", DataTypeManager.DefaultDataTypes.STRING, QueryPlugin.Util.getString("SystemSource.Locate2_arg1")), //$NON-NLS-1$ //$NON-NLS-2$
                    new FunctionParameter("string", DataTypeManager.DefaultDataTypes.STRING, QueryPlugin.Util.getString("SystemSource.Locate2_arg2")) }, //$NON-NLS-1$ //$NON-NLS-2$
                new FunctionParameter("result", DataTypeManager.DefaultDataTypes.INTEGER, QueryPlugin.Util.getString("SystemSource.Locate2_result")) ) );                 //$NON-NLS-1$ //$NON-NLS-2$
    }

    private void addReplaceFunction() {
        functions.add(
            new FunctionMethod(SourceSystemFunctions.REPLACE, QueryPlugin.Util.getString("SystemSource.Replace_desc"), STRING, FUNCTION_CLASS, "replace", //$NON-NLS-1$ //$NON-NLS-2$ 
                new FunctionParameter[] {
                    new FunctionParameter("string", DataTypeManager.DefaultDataTypes.STRING, QueryPlugin.Util.getString("SystemSource.Replace_arg1")), //$NON-NLS-1$ //$NON-NLS-2$
                    new FunctionParameter("substring", DataTypeManager.DefaultDataTypes.STRING, QueryPlugin.Util.getString("SystemSource.Replace_arg2")), //$NON-NLS-1$ //$NON-NLS-2$
                    new FunctionParameter("replacement", DataTypeManager.DefaultDataTypes.STRING, QueryPlugin.Util.getString("SystemSource.Replace_arg3")) }, //$NON-NLS-1$ //$NON-NLS-2$
                new FunctionParameter("result", DataTypeManager.DefaultDataTypes.STRING, QueryPlugin.Util.getString("SystemSource.Replace_result")) ) );                 //$NON-NLS-1$ //$NON-NLS-2$
    }

	private void addRepeatFunction() {
		functions.add(
			new FunctionMethod(SourceSystemFunctions.REPEAT, QueryPlugin.Util.getString("SystemSource.Repeat_desc"), STRING, FUNCTION_CLASS, "repeat", //$NON-NLS-1$ //$NON-NLS-2$ 
				new FunctionParameter[] {
					new FunctionParameter("string", DataTypeManager.DefaultDataTypes.STRING, QueryPlugin.Util.getString("SystemSource.Repeat_arg1")), //$NON-NLS-1$ //$NON-NLS-2$
					new FunctionParameter("count", DataTypeManager.DefaultDataTypes.INTEGER, QueryPlugin.Util.getString("SystemSource.Repeat_arg2"))}, //$NON-NLS-1$ //$NON-NLS-2$
				new FunctionParameter("result", DataTypeManager.DefaultDataTypes.STRING, QueryPlugin.Util.getString("SystemSource.Repeat_result")) ) );                 //$NON-NLS-1$ //$NON-NLS-2$
	}

	private void addSpaceFunction() {
		functions.add(
			new FunctionMethod(FunctionLibrary.SPACE, QueryPlugin.Util.getString("SystemSource.Space_desc"), STRING, PushDown.SYNTHETIC, null, null, //$NON-NLS-1$ 
				new FunctionParameter[] {
					new FunctionParameter("count", DataTypeManager.DefaultDataTypes.INTEGER, QueryPlugin.Util.getString("SystemSource.Space_arg1"))}, //$NON-NLS-1$ //$NON-NLS-2$
				new FunctionParameter("result", DataTypeManager.DefaultDataTypes.STRING, QueryPlugin.Util.getString("SystemSource.Space_result")) ) );                 //$NON-NLS-1$ //$NON-NLS-2$
	}

	private void addInsertFunction() {
		functions.add(
			new FunctionMethod(SourceSystemFunctions.INSERT, QueryPlugin.Util.getString("SystemSource.Insert_desc"), STRING, FUNCTION_CLASS, "insert", //$NON-NLS-1$ //$NON-NLS-2$ 
				new FunctionParameter[] {
					new FunctionParameter("str1", DataTypeManager.DefaultDataTypes.STRING, QueryPlugin.Util.getString("SystemSource.Insert_arg1")), //$NON-NLS-1$ //$NON-NLS-2$
					new FunctionParameter("start", DataTypeManager.DefaultDataTypes.INTEGER, QueryPlugin.Util.getString("SystemSource.Insert_arg2")), //$NON-NLS-1$ //$NON-NLS-2$
					new FunctionParameter("length", DataTypeManager.DefaultDataTypes.INTEGER, QueryPlugin.Util.getString("SystemSource.Insert_arg3")), //$NON-NLS-1$ //$NON-NLS-2$
					new FunctionParameter("str2", DataTypeManager.DefaultDataTypes.STRING, QueryPlugin.Util.getString("SystemSource.Insert_arg4")) }, //$NON-NLS-1$ //$NON-NLS-2$
				new FunctionParameter("result", DataTypeManager.DefaultDataTypes.STRING, QueryPlugin.Util.getString("SystemSource.Insert_result")) ) );                 //$NON-NLS-1$ //$NON-NLS-2$
	}
	
	private void addToCharsFunction() {
		functions.add(
			new FunctionMethod("to_chars", QueryPlugin.Util.getString("SystemSource.encode_desc"), CONVERSION, FUNCTION_CLASS, "toChars", //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$  
				new FunctionParameter[] {
					new FunctionParameter("value", DataTypeManager.DefaultDataTypes.BLOB, QueryPlugin.Util.getString("SystemSource.encode_arg1")), //$NON-NLS-1$ //$NON-NLS-2$
					new FunctionParameter("encoding", DataTypeManager.DefaultDataTypes.STRING, QueryPlugin.Util.getString("SystemSource.encode_arg2"))}, //$NON-NLS-1$ //$NON-NLS-2$
				new FunctionParameter("result", DataTypeManager.DefaultDataTypes.CLOB, QueryPlugin.Util.getString("SystemSource.encode_result")) ) );                 //$NON-NLS-1$ //$NON-NLS-2$
	}
	
	private void addToBytesFunction() {
		functions.add(
			new FunctionMethod("to_bytes", QueryPlugin.Util.getString("SystemSource.decode_desc"), CONVERSION, FUNCTION_CLASS, "toBytes", //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$  
				new FunctionParameter[] {
					new FunctionParameter("value", DataTypeManager.DefaultDataTypes.CLOB, QueryPlugin.Util.getString("SystemSource.decode_arg1")), //$NON-NLS-1$ //$NON-NLS-2$
					new FunctionParameter("encoding", DataTypeManager.DefaultDataTypes.STRING, QueryPlugin.Util.getString("SystemSource.decode_arg2"))}, //$NON-NLS-1$ //$NON-NLS-2$
				new FunctionParameter("result", DataTypeManager.DefaultDataTypes.BLOB, QueryPlugin.Util.getString("SystemSource.decode_result")) ) );                 //$NON-NLS-1$ //$NON-NLS-2$
	}
	
    private void addAsciiFunction() {
        functions.add(
            new FunctionMethod(SourceSystemFunctions.ASCII, QueryPlugin.Util.getString("SystemSource.Ascii_desc"), STRING, FUNCTION_CLASS, "ascii", //$NON-NLS-1$ //$NON-NLS-2$ 
                new FunctionParameter[] {
                    new FunctionParameter("string", DataTypeManager.DefaultDataTypes.STRING, QueryPlugin.Util.getString("SystemSource.Ascii_arg1")) }, //$NON-NLS-1$ //$NON-NLS-2$
                new FunctionParameter("result", DataTypeManager.DefaultDataTypes.INTEGER, QueryPlugin.Util.getString("SystemSource.Ascii_result")) ) ); //$NON-NLS-1$ //$NON-NLS-2$
        functions.add(
            new FunctionMethod(SourceSystemFunctions.ASCII, QueryPlugin.Util.getString("SystemSource.Ascii2_desc"), STRING, FUNCTION_CLASS, "ascii", //$NON-NLS-1$ //$NON-NLS-2$ 
                new FunctionParameter[] {
                    new FunctionParameter("char", DataTypeManager.DefaultDataTypes.CHAR, QueryPlugin.Util.getString("SystemSource.Ascii2_arg1")) }, //$NON-NLS-1$ //$NON-NLS-2$
                new FunctionParameter("result", DataTypeManager.DefaultDataTypes.INTEGER, QueryPlugin.Util.getString("SystemSource.Ascii2_result")) ) ); //$NON-NLS-1$ //$NON-NLS-2$
    }

	private void addCharFunction() {
		functions.add(
			new FunctionMethod(SourceSystemFunctions.CHAR, QueryPlugin.Util.getString("SystemSource.Char_desc"), STRING, FUNCTION_CLASS, "chr", //$NON-NLS-1$ //$NON-NLS-2$ 
				new FunctionParameter[] {
					new FunctionParameter("code", DataTypeManager.DefaultDataTypes.INTEGER, QueryPlugin.Util.getString("SystemSource.Char_arg1")) }, //$NON-NLS-1$ //$NON-NLS-2$
				new FunctionParameter("result", DataTypeManager.DefaultDataTypes.CHAR, QueryPlugin.Util.getString("SystemSource.Char_result")) ) ); //$NON-NLS-1$ //$NON-NLS-2$
        functions.add(
                new FunctionMethod("chr", QueryPlugin.Util.getString("SystemSource.Chr_desc"), STRING, FUNCTION_CLASS, "chr", //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
                    new FunctionParameter[] {
                        new FunctionParameter("code", DataTypeManager.DefaultDataTypes.INTEGER, QueryPlugin.Util.getString("SystemSource.Chr_arg1")) }, //$NON-NLS-1$ //$NON-NLS-2$
                    new FunctionParameter("result", DataTypeManager.DefaultDataTypes.CHAR, QueryPlugin.Util.getString("SystemSource.Chr_result")) ) ); //$NON-NLS-1$ //$NON-NLS-2$
	}
	
    private void addInitCapFunction() {
        functions.add(
            new FunctionMethod(SourceSystemFunctions.INITCAP, QueryPlugin.Util.getString("SystemSource.Initcap_desc"), STRING, FUNCTION_CLASS, "initCap", //$NON-NLS-1$ //$NON-NLS-2$ 
                new FunctionParameter[] {
                    new FunctionParameter("string", DataTypeManager.DefaultDataTypes.STRING, QueryPlugin.Util.getString("SystemSource.Initcap_arg1")) }, //$NON-NLS-1$ //$NON-NLS-2$
                new FunctionParameter("result", DataTypeManager.DefaultDataTypes.STRING, QueryPlugin.Util.getString("SystemSource.Initcap_result")) ) ); //$NON-NLS-1$ //$NON-NLS-2$
    }

    private void addLpadFunction() {
        functions.add(
            new FunctionMethod(SourceSystemFunctions.LPAD, QueryPlugin.Util.getString("SystemSource.Lpad_desc"), STRING, FUNCTION_CLASS, "lpad", //$NON-NLS-1$ //$NON-NLS-2$ 
                new FunctionParameter[] {
                    new FunctionParameter("string", DataTypeManager.DefaultDataTypes.STRING, QueryPlugin.Util.getString("SystemSource.Lpad_arg1")), //$NON-NLS-1$ //$NON-NLS-2$
                    new FunctionParameter("length", DataTypeManager.DefaultDataTypes.INTEGER, QueryPlugin.Util.getString("SystemSource.Lpad_arg2")) }, //$NON-NLS-1$ //$NON-NLS-2$
                new FunctionParameter("result", DataTypeManager.DefaultDataTypes.STRING, QueryPlugin.Util.getString("SystemSource.Lpad_result")) ) ); //$NON-NLS-1$ //$NON-NLS-2$
        functions.add(
            new FunctionMethod(SourceSystemFunctions.LPAD, QueryPlugin.Util.getString("SystemSource.Lpad3_desc"), STRING, FUNCTION_CLASS, "lpad", //$NON-NLS-1$ //$NON-NLS-2$ 
                new FunctionParameter[] {
                    new FunctionParameter("string", DataTypeManager.DefaultDataTypes.STRING, QueryPlugin.Util.getString("SystemSource.Lpad3_arg1")), //$NON-NLS-1$ //$NON-NLS-2$
                    new FunctionParameter("length", DataTypeManager.DefaultDataTypes.INTEGER, QueryPlugin.Util.getString("SystemSource.Lpad3_arg2")), //$NON-NLS-1$ //$NON-NLS-2$
                    new FunctionParameter("char", DataTypeManager.DefaultDataTypes.STRING, QueryPlugin.Util.getString("SystemSource.Lpad3_arg3")) }, //$NON-NLS-1$ //$NON-NLS-2$
                new FunctionParameter("result", DataTypeManager.DefaultDataTypes.STRING, QueryPlugin.Util.getString("SystemSource.Lpad3_result")) ) ); //$NON-NLS-1$ //$NON-NLS-2$
    }

    private void addRpadFunction() {
        functions.add(
            new FunctionMethod(SourceSystemFunctions.RPAD, QueryPlugin.Util.getString("SystemSource.Rpad1_desc"), STRING, FUNCTION_CLASS, "rpad", //$NON-NLS-1$ //$NON-NLS-2$ 
                new FunctionParameter[] {
                    new FunctionParameter("string", DataTypeManager.DefaultDataTypes.STRING, QueryPlugin.Util.getString("SystemSource.Rpad1_arg1")), //$NON-NLS-1$ //$NON-NLS-2$
                    new FunctionParameter("length", DataTypeManager.DefaultDataTypes.INTEGER, QueryPlugin.Util.getString("SystemSource.Rpad1_arg2")) }, //$NON-NLS-1$ //$NON-NLS-2$
                new FunctionParameter("result", DataTypeManager.DefaultDataTypes.STRING, QueryPlugin.Util.getString("SystemSource.Rpad1_result")) ) ); //$NON-NLS-1$ //$NON-NLS-2$
        functions.add(
            new FunctionMethod(SourceSystemFunctions.RPAD, QueryPlugin.Util.getString("SystemSource.Rpad3_desc"), STRING, FUNCTION_CLASS, "rpad", //$NON-NLS-1$ //$NON-NLS-2$ 
                new FunctionParameter[] {
                    new FunctionParameter("string", DataTypeManager.DefaultDataTypes.STRING, QueryPlugin.Util.getString("SystemSource.Rpad3_arg1")), //$NON-NLS-1$ //$NON-NLS-2$
                    new FunctionParameter("length", DataTypeManager.DefaultDataTypes.INTEGER, QueryPlugin.Util.getString("SystemSource.Rpad3_arg2")), //$NON-NLS-1$ //$NON-NLS-2$
                    new FunctionParameter("char", DataTypeManager.DefaultDataTypes.STRING, QueryPlugin.Util.getString("SystemSource.Rpad3_arg3")) }, //$NON-NLS-1$ //$NON-NLS-2$
                new FunctionParameter("result", DataTypeManager.DefaultDataTypes.STRING, QueryPlugin.Util.getString("SystemSource.Rpad3_result")) ) ); //$NON-NLS-1$ //$NON-NLS-2$
    }

    private void addTranslateFunction() {
        functions.add(
            new FunctionMethod(SourceSystemFunctions.TRANSLATE, QueryPlugin.Util.getString("SystemSource.Translate_desc"), STRING, FUNCTION_CLASS, "translate", //$NON-NLS-1$ //$NON-NLS-2$ 
                new FunctionParameter[] {
                    new FunctionParameter("string", DataTypeManager.DefaultDataTypes.STRING, QueryPlugin.Util.getString("SystemSource.Translate_arg1")), //$NON-NLS-1$ //$NON-NLS-2$
                    new FunctionParameter("source", DataTypeManager.DefaultDataTypes.STRING, QueryPlugin.Util.getString("SystemSource.Translate_arg2")), //$NON-NLS-1$ //$NON-NLS-2$
                    new FunctionParameter("destination", DataTypeManager.DefaultDataTypes.STRING, QueryPlugin.Util.getString("SystemSource.Translate_arg3")) }, //$NON-NLS-1$ //$NON-NLS-2$
                new FunctionParameter("result", DataTypeManager.DefaultDataTypes.STRING, QueryPlugin.Util.getString("SystemSource.Translate_result")) ) ); //$NON-NLS-1$ //$NON-NLS-2$
    }
    
    private void addConversionFunctions() {
    	for (String type : DataTypeManager.getAllDataTypeNames()) {
            addTypedConversionFunction(SourceSystemFunctions.CONVERT, type); 
            addTypedConversionFunction("cast", type); //$NON-NLS-1$
    	}
    }
    
    private void addTypedConversionFunction(String name, String sourceType) {
        functions.add(
            new FunctionMethod(name, QueryPlugin.Util.getString("SystemSource.Convert_desc", sourceType), CONVERSION, FUNCTION_CLASS, "convert", //$NON-NLS-1$ //$NON-NLS-2$ 
                new FunctionParameter[] {
                    new FunctionParameter("value", sourceType, QueryPlugin.Util.getString("SystemSource.Convert_arg1")), //$NON-NLS-1$ //$NON-NLS-2$
                    new FunctionParameter("target", DataTypeManager.DefaultDataTypes.STRING, QueryPlugin.Util.getString("SystemSource.Convert_arg2")) }, //$NON-NLS-1$ //$NON-NLS-2$
                new FunctionParameter("result", DataTypeManager.DefaultDataTypes.OBJECT, QueryPlugin.Util.getString("SystemSource.Convert_result")) ) );                 //$NON-NLS-1$ //$NON-NLS-2$
    
    }    

    private void addContextFunctions() {
    	for (String contextType : DataTypeManager.getAllDataTypeNames()) {
    		for (String exprType : DataTypeManager.getAllDataTypeNames()) {
                addTypedContextFunction(contextType, exprType);
        	}
    	}
    }
    
    private void addTypedContextFunction(String contextType, String exprType) {
        functions.add(
            new FunctionMethod("context", QueryPlugin.Util.getString("SystemSource.Context_desc"), MISCELLANEOUS, FUNCTION_CLASS, "context", //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
                new FunctionParameter[] {
                    new FunctionParameter("context", contextType, QueryPlugin.Util.getString("SystemSource.Context_arg1")), //$NON-NLS-1$ //$NON-NLS-2$
                    new FunctionParameter("element", exprType, QueryPlugin.Util.getString("SystemSource.Context_arg2")) }, //$NON-NLS-1$ //$NON-NLS-2$
                new FunctionParameter("result", exprType, QueryPlugin.Util.getString("SystemSource.Context_result")) ) );                     //$NON-NLS-1$ //$NON-NLS-2$
    } 
    
    private void addRowLimitFunctions() {
    	for (String exprType : DataTypeManager.getAllDataTypeNames()) {
            functions.add(
                    new FunctionMethod("rowlimit", QueryPlugin.Util.getString("SystemSource.Rowlimit_desc"), MISCELLANEOUS, FUNCTION_CLASS, "rowlimit", //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
                        new FunctionParameter[] {
                            new FunctionParameter("element", exprType, QueryPlugin.Util.getString("SystemSource.Rowlimit_arg1")) }, //$NON-NLS-1$ //$NON-NLS-2$
                        new FunctionParameter("result", DataTypeManager.DefaultDataTypes.INTEGER, QueryPlugin.Util.getString("SystemSource.Rowlimit_result")) ) );                     //$NON-NLS-1$ //$NON-NLS-2$
    	}
    }
    
    private void addRowLimitExceptionFunctions() {
    	for (String exprType : DataTypeManager.getAllDataTypeNames()) {
            functions.add(
                    new FunctionMethod("rowlimitexception", QueryPlugin.Util.getString("SystemSource.RowlimitException_desc"), MISCELLANEOUS, FUNCTION_CLASS, "rowlimitexception", //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
                        new FunctionParameter[] {
                            new FunctionParameter("element", exprType, QueryPlugin.Util.getString("SystemSource.Rowlimit_arg1")) }, //$NON-NLS-1$ //$NON-NLS-2$
                        new FunctionParameter("result", DataTypeManager.DefaultDataTypes.INTEGER, QueryPlugin.Util.getString("SystemSource.Rowlimit_result")) ) );                     //$NON-NLS-1$ //$NON-NLS-2$
    	}
    }    
    
    private void addDecodeFunctions(){
        addDecodeFunction("decodeInteger", DataTypeManager.DefaultDataTypes.INTEGER); //$NON-NLS-1$
        addDecodeFunction("decodeString", DataTypeManager.DefaultDataTypes.STRING); //$NON-NLS-1$
    }
        
    private void addDecodeFunction(String functionName, String resultType) {
        functions.add(
            new FunctionMethod(functionName, QueryPlugin.Util.getString("SystemSource.Decode1_desc"), MISCELLANEOUS, PushDown.SYNTHETIC, null, null, //$NON-NLS-1$
                new FunctionParameter[] { 
                    new FunctionParameter("input", DataTypeManager.DefaultDataTypes.STRING, QueryPlugin.Util.getString("SystemSource.Decode1_arg1")), //$NON-NLS-1$ //$NON-NLS-2$
                    new FunctionParameter("decodeString", DataTypeManager.DefaultDataTypes.STRING, QueryPlugin.Util.getString("SystemSource.Decode1_arg2")) }, //$NON-NLS-1$ //$NON-NLS-2$
                new FunctionParameter("result", resultType, QueryPlugin.Util.getString("SystemSource.Decode1_result") ) ) );    //$NON-NLS-1$ //$NON-NLS-2$
                     
        functions.add(
            new FunctionMethod(functionName, QueryPlugin.Util.getString("SystemSource.Decode2_desc"), MISCELLANEOUS, PushDown.SYNTHETIC, null, null, //$NON-NLS-1$
                new FunctionParameter[] { 
                    new FunctionParameter("input", DataTypeManager.DefaultDataTypes.STRING, QueryPlugin.Util.getString("SystemSource.Decode2_arg1")), //$NON-NLS-1$ //$NON-NLS-2$
                    new FunctionParameter("decodeString", DataTypeManager.DefaultDataTypes.STRING, QueryPlugin.Util.getString("SystemSource.Decode2_arg2")), //$NON-NLS-1$ //$NON-NLS-2$
                    new FunctionParameter("delimiter", DataTypeManager.DefaultDataTypes.STRING, QueryPlugin.Util.getString("SystemSource.Decode2_arg3")) }, //$NON-NLS-1$ //$NON-NLS-2$
                new FunctionParameter("result", resultType, QueryPlugin.Util.getString("SystemSource.Decode2_result") ) ) );    //$NON-NLS-1$ //$NON-NLS-2$

    }

    private void addLookupFunctions() {
    	for (String keyValueType : DataTypeManager.getAllDataTypeNames()) {
            functions.add(
                    new FunctionMethod("lookup", QueryPlugin.Util.getString("SystemSource.Lookup_desc"), MISCELLANEOUS, PushDown.CANNOT_PUSHDOWN, FUNCTION_CLASS, "lookup", //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
                        new FunctionParameter[] {
                            new FunctionParameter("codetable", DataTypeManager.DefaultDataTypes.STRING, QueryPlugin.Util.getString("SystemSource.Lookup_arg1")), //$NON-NLS-1$ //$NON-NLS-2$
                            new FunctionParameter("returnelement", DataTypeManager.DefaultDataTypes.STRING, QueryPlugin.Util.getString("SystemSource.Lookup_arg2")), //$NON-NLS-1$ //$NON-NLS-2$
                            new FunctionParameter("keyelement", DataTypeManager.DefaultDataTypes.STRING, QueryPlugin.Util.getString("SystemSource.Lookup_arg3")), //$NON-NLS-1$ //$NON-NLS-2$
                            new FunctionParameter("keyvalue", keyValueType, QueryPlugin.Util.getString("SystemSource.Lookup_arg4")), //$NON-NLS-1$ //$NON-NLS-2$
                             },
                        new FunctionParameter("result", DataTypeManager.DefaultDataTypes.OBJECT, QueryPlugin.Util.getString("SystemSource.Lookup_result")), true, Determinism.VDB_DETERMINISTIC ) );                     //$NON-NLS-1$ //$NON-NLS-2$
    	}
    }

    private void addUserFunction() {
        functions.add(
            new FunctionMethod("user", QueryPlugin.Util.getString("SystemSource.User_desc"), MISCELLANEOUS, PushDown.CANNOT_PUSHDOWN, FUNCTION_CLASS, "user", null, //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
                new FunctionParameter("result", DataTypeManager.DefaultDataTypes.STRING, QueryPlugin.Util.getString("SystemSource.User_result")), false, Determinism.USER_DETERMINISTIC) );                     //$NON-NLS-1$ //$NON-NLS-2$
    }
    
    private void addCurrentDatabaseFunction() {
        functions.add(
            new FunctionMethod("current_database", QueryPlugin.Util.getString("SystemSource.current_database_desc"), MISCELLANEOUS, PushDown.CANNOT_PUSHDOWN, FUNCTION_CLASS, "current_database", null, //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
                new FunctionParameter("result", DataTypeManager.DefaultDataTypes.STRING, QueryPlugin.Util.getString("current_database_result")), false, Determinism.VDB_DETERMINISTIC) );                     //$NON-NLS-1$ //$NON-NLS-2$
    }    
    
    private void addEnvFunction() {
        functions.add(
            new FunctionMethod("env", QueryPlugin.Util.getString("SystemSource.Env_desc"), MISCELLANEOUS, PushDown.CANNOT_PUSHDOWN, FUNCTION_CLASS, "env", //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
                new FunctionParameter[] {
                    new FunctionParameter("variablename", DataTypeManager.DefaultDataTypes.STRING, QueryPlugin.Util.getString("SystemSource.Env_varname")) //$NON-NLS-1$ //$NON-NLS-2$
                     },
                new FunctionParameter("result", DataTypeManager.DefaultDataTypes.STRING, QueryPlugin.Util.getString("SystemSource.Env_result")), false, Determinism.SESSION_DETERMINISTIC ) );                     //$NON-NLS-1$ //$NON-NLS-2$
    }
    
    private void addSessionIdFunction() {
        functions.add(
            new FunctionMethod(FunctionLibrary.SESSION_ID, QueryPlugin.Util.getString("SystemSource.session_id_desc"), MISCELLANEOUS, PushDown.CANNOT_PUSHDOWN, FUNCTION_CLASS, "session_id", null, //$NON-NLS-1$ //$NON-NLS-2$ 
                new FunctionParameter("result", DataTypeManager.DefaultDataTypes.STRING, QueryPlugin.Util.getString("SystemSource.session_id_result")), false, Determinism.SESSION_DETERMINISTIC) );                     //$NON-NLS-1$ //$NON-NLS-2$
    }    
    
    private void addCommandPayloadFunctions() {
        functions.add(
            new FunctionMethod("commandpayload", QueryPlugin.Util.getString("SystemSource.CommandPayload_desc0"), MISCELLANEOUS, PushDown.CANNOT_PUSHDOWN, FUNCTION_CLASS, "commandPayload", //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
                 new FunctionParameter[] {},
                 new FunctionParameter("result", DataTypeManager.DefaultDataTypes.STRING, QueryPlugin.Util.getString("SystemSource.CommandPayload_result")), false, Determinism.COMMAND_DETERMINISTIC ) );                     //$NON-NLS-1$ //$NON-NLS-2$
        functions.add(
            new FunctionMethod("commandpayload", QueryPlugin.Util.getString("SystemSource.CommandPayload_desc1"), MISCELLANEOUS, PushDown.CANNOT_PUSHDOWN, FUNCTION_CLASS, "commandPayload", //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
                 new FunctionParameter[] {
                     new FunctionParameter("property", DataTypeManager.DefaultDataTypes.STRING, QueryPlugin.Util.getString("SystemSource.CommandPayload_property")) //$NON-NLS-1$ //$NON-NLS-2$
                      },
                 new FunctionParameter("result", DataTypeManager.DefaultDataTypes.STRING, QueryPlugin.Util.getString("SystemSource.CommandPayload_result")), false, Determinism.COMMAND_DETERMINISTIC ) );                     //$NON-NLS-1$ //$NON-NLS-2$
    }
    
    private void addIfNullFunctions() {
    	for (String type : DataTypeManager.getAllDataTypeNames()) {
            addNvlFunction(type);
            addIfNullFunction(type);
    	}
    }
	
    private void addNvlFunction(String valueType) {
        FunctionMethod nvl = 
            new FunctionMethod("nvl", QueryPlugin.Util.getString("SystemSource.Nvl_desc"), MISCELLANEOUS, FUNCTION_CLASS, "ifnull", //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
                new FunctionParameter[] {
                    new FunctionParameter("value", valueType, QueryPlugin.Util.getString("SystemSource.Nvl_arg1")), //$NON-NLS-1$ //$NON-NLS-2$
                    new FunctionParameter("valueIfNull", valueType, QueryPlugin.Util.getString("SystemSource.Nvl_arg2")) }, //$NON-NLS-1$ //$NON-NLS-2$
                new FunctionParameter("result", valueType, QueryPlugin.Util.getString("SystemSource.Nvl_result")) ); //$NON-NLS-1$ //$NON-NLS-2$
        nvl.setNullOnNull(false);
        functions.add(nvl); 
    }

	private void addIfNullFunction(String valueType) {
		FunctionMethod nvl = 
			new FunctionMethod("ifnull", QueryPlugin.Util.getString("SystemSource.Ifnull_desc"), MISCELLANEOUS, FUNCTION_CLASS, "ifnull", //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
				new FunctionParameter[] {
					new FunctionParameter("value", valueType, QueryPlugin.Util.getString("SystemSource.Ifnull_arg1")), //$NON-NLS-1$ //$NON-NLS-2$
					new FunctionParameter("valueIfNull", valueType, QueryPlugin.Util.getString("SystemSource.Ifnull_arg2")) }, //$NON-NLS-1$ //$NON-NLS-2$
				new FunctionParameter("result", valueType, QueryPlugin.Util.getString("SystemSource.Ifnull_result")) ); //$NON-NLS-1$ //$NON-NLS-2$
        nvl.setNullOnNull(false);
        functions.add(nvl); 
	}
			
	private void addFormatTimestampFunction() {
		functions.add(
			new FunctionMethod(SourceSystemFunctions.FORMATTIMESTAMP, QueryPlugin.Util.getString("SystemSource.Formattimestamp_desc"),CONVERSION, FUNCTION_CLASS, "format", //$NON-NLS-1$ //$NON-NLS-2$
				new FunctionParameter[] { 
					new FunctionParameter("timestamp", DataTypeManager.DefaultDataTypes.TIMESTAMP, QueryPlugin.Util.getString("SystemSource.Formattimestamp_arg1")), //$NON-NLS-1$ //$NON-NLS-2$
					new FunctionParameter("format", DataTypeManager.DefaultDataTypes.STRING, QueryPlugin.Util.getString("SystemSource.Formattimestamp_arg2")) }, //$NON-NLS-1$ //$NON-NLS-2$
				new FunctionParameter("result", DataTypeManager.DefaultDataTypes.STRING, QueryPlugin.Util.getString("SystemSource.Formattimestamp_result_desc")) ) );       //$NON-NLS-1$ //$NON-NLS-2$
		functions.add(
				new FunctionMethod(FunctionLibrary.FORMATDATE, QueryPlugin.Util.getString("SystemSource.Formatdate_desc"),CONVERSION, PushDown.SYNTHETIC, null, null, //$NON-NLS-1$
					new FunctionParameter[] { 
						new FunctionParameter("date", DataTypeManager.DefaultDataTypes.DATE, QueryPlugin.Util.getString("SystemSource.Formatdate_arg1")), //$NON-NLS-1$ //$NON-NLS-2$
						new FunctionParameter("format", DataTypeManager.DefaultDataTypes.STRING, QueryPlugin.Util.getString("SystemSource.Formatdate_arg2")) }, //$NON-NLS-1$ //$NON-NLS-2$
					new FunctionParameter("result", DataTypeManager.DefaultDataTypes.STRING, QueryPlugin.Util.getString("SystemSource.Formatdate_result_desc")) ) );       //$NON-NLS-1$ //$NON-NLS-2$
		functions.add(
				new FunctionMethod(FunctionLibrary.FORMATTIME, QueryPlugin.Util.getString("SystemSource.Formattime_desc"),CONVERSION, PushDown.SYNTHETIC, null, null, //$NON-NLS-1$
					new FunctionParameter[] { 
						new FunctionParameter("time", DataTypeManager.DefaultDataTypes.TIME, QueryPlugin.Util.getString("SystemSource.Formattime_arg1")), //$NON-NLS-1$ //$NON-NLS-2$
						new FunctionParameter("format", DataTypeManager.DefaultDataTypes.STRING, QueryPlugin.Util.getString("SystemSource.Formattime_arg2")) }, //$NON-NLS-1$ //$NON-NLS-2$
					new FunctionParameter("result", DataTypeManager.DefaultDataTypes.STRING, QueryPlugin.Util.getString("SystemSource.Formattime_result_desc")) ) );       //$NON-NLS-1$ //$NON-NLS-2$
	}
					
	private void addParseTimestampFunction() {
		functions.add(
			new FunctionMethod(SourceSystemFunctions.PARSETIMESTAMP, QueryPlugin.Util.getString("SystemSource.Parsetimestamp_desc"),CONVERSION, FUNCTION_CLASS, "parseTimestamp", //$NON-NLS-1$ //$NON-NLS-2$
				new FunctionParameter[] { 
					new FunctionParameter("timestamp", DataTypeManager.DefaultDataTypes.STRING, QueryPlugin.Util.getString("SystemSource.Parsetimestamp_arg1")), //$NON-NLS-1$ //$NON-NLS-2$
					new FunctionParameter("format", DataTypeManager.DefaultDataTypes.STRING, QueryPlugin.Util.getString("SystemSource.Parsetimestamp_arg2")) }, //$NON-NLS-1$ //$NON-NLS-2$
				new FunctionParameter("result", DataTypeManager.DefaultDataTypes.TIMESTAMP, QueryPlugin.Util.getString("SystemSource.Parsetimestamp_result_desc")) ) );       //$NON-NLS-1$ //$NON-NLS-2$
		functions.add(
				new FunctionMethod(FunctionLibrary.PARSETIME, QueryPlugin.Util.getString("SystemSource.Parsetime_desc"),CONVERSION, PushDown.SYNTHETIC, null, null, //$NON-NLS-1$
					new FunctionParameter[] { 
						new FunctionParameter("time", DataTypeManager.DefaultDataTypes.STRING, QueryPlugin.Util.getString("SystemSource.Parsetime_arg1")), //$NON-NLS-1$ //$NON-NLS-2$
						new FunctionParameter("format", DataTypeManager.DefaultDataTypes.STRING, QueryPlugin.Util.getString("SystemSource.Parsetime_arg2")) }, //$NON-NLS-1$ //$NON-NLS-2$
					new FunctionParameter("result", DataTypeManager.DefaultDataTypes.TIME, QueryPlugin.Util.getString("SystemSource.Parsetime_result_desc")) ) );       //$NON-NLS-1$ //$NON-NLS-2$
		functions.add(
				new FunctionMethod(FunctionLibrary.PARSEDATE, QueryPlugin.Util.getString("SystemSource.Parsedate_desc"),CONVERSION, PushDown.SYNTHETIC, null, null, //$NON-NLS-1$
					new FunctionParameter[] { 
						new FunctionParameter("date", DataTypeManager.DefaultDataTypes.STRING, QueryPlugin.Util.getString("SystemSource.Parsedate_arg1")), //$NON-NLS-1$ //$NON-NLS-2$
						new FunctionParameter("format", DataTypeManager.DefaultDataTypes.STRING, QueryPlugin.Util.getString("SystemSource.Parsedate_arg2")) }, //$NON-NLS-1$ //$NON-NLS-2$
					new FunctionParameter("result", DataTypeManager.DefaultDataTypes.DATE, QueryPlugin.Util.getString("SystemSource.Parsedate_result_desc")) ) );       //$NON-NLS-1$ //$NON-NLS-2$
	}

	private void addFormatNumberFunction(String functionName, String description, String methodName, String inputParam, String dataType,  String resultDesc) {
		functions.add(
			new FunctionMethod(functionName, description, CONVERSION, FUNCTION_CLASS, methodName,
				new FunctionParameter[] { 
					new FunctionParameter(inputParam, dataType, QueryPlugin.Util.getString("SystemSource.Formatnumber_arg1")), //$NON-NLS-1$
					new FunctionParameter("format", DataTypeManager.DefaultDataTypes.STRING, QueryPlugin.Util.getString("SystemSource.Formatnumber_arg2"))}, //$NON-NLS-1$ //$NON-NLS-2$
				new FunctionParameter("result", DataTypeManager.DefaultDataTypes.STRING, resultDesc) ) );       //$NON-NLS-1$
	}
		
	private void addParseNumberFunction(String functionName, String description, String methodName, String inputParam, String dataType,  String resultDesc) {
			functions.add(
				new FunctionMethod(functionName, description, CONVERSION, FUNCTION_CLASS, methodName,
					new FunctionParameter[] { 
						new FunctionParameter(inputParam, DataTypeManager.DefaultDataTypes.STRING, QueryPlugin.Util.getString("SystemSource.Parsenumber_arg1")), //$NON-NLS-1$
						new FunctionParameter("format", DataTypeManager.DefaultDataTypes.STRING, QueryPlugin.Util.getString("SystemSource.Parsenumber_arg2"))}, //$NON-NLS-1$ //$NON-NLS-2$
					new FunctionParameter("result", dataType, resultDesc) ) );       //$NON-NLS-1$
	}
    
    private void addBitFunction(String functionName, String description, String methodName, int parameters, String resultDescription) {
        FunctionParameter[] paramArray = null;
        if (parameters == 1) {
            paramArray = new FunctionParameter[] {
                new FunctionParameter("integer", DataTypeManager.DefaultDataTypes.INTEGER, QueryPlugin.Util.getString("SystemSource.Bitfunc_arg1")) //$NON-NLS-1$ //$NON-NLS-2$
            };
        } else if (parameters == 2) {
            paramArray = new FunctionParameter[] {
                new FunctionParameter("integer1", DataTypeManager.DefaultDataTypes.INTEGER, QueryPlugin.Util.getString("SystemSource.Bitfunc2_arg1")), //$NON-NLS-1$ //$NON-NLS-2$
                new FunctionParameter("integer2", DataTypeManager.DefaultDataTypes.INTEGER, QueryPlugin.Util.getString("SystemSource.Bitfunc2_arg2")) //$NON-NLS-1$ //$NON-NLS-2$
            };
        }
        functions.add(
            new FunctionMethod(functionName, description, NUMERIC, FUNCTION_CLASS, methodName,
                paramArray,
                new FunctionParameter("result", DataTypeManager.DefaultDataTypes.INTEGER, resultDescription) //$NON-NLS-1$
            )
        );
    }
    
    private void addXpathValueFunction() {
        functions.add(new FunctionMethod(SourceSystemFunctions.XPATHVALUE, QueryPlugin.Util.getString("SystemSource.xpathvalue_description"), XML, XML_FUNCTION_CLASS, "xpathValue", //$NON-NLS-1$ //$NON-NLS-2$ 
                            new FunctionParameter[] { 
                                new FunctionParameter("document", DataTypeManager.DefaultDataTypes.STRING, QueryPlugin.Util.getString("SystemSource.xpath_param1")), //$NON-NLS-1$ //$NON-NLS-2$
                                new FunctionParameter("xpath", DataTypeManager.DefaultDataTypes.STRING, QueryPlugin.Util.getString("SystemSource.xpath_param2"))}, //$NON-NLS-1$ //$NON-NLS-2$ 
                            new FunctionParameter("result", DataTypeManager.DefaultDataTypes.STRING, QueryPlugin.Util.getString("SystemSource.xpathvalue_result")) ) );       //$NON-NLS-1$ //$NON-NLS-2$

        functions.add(new FunctionMethod(SourceSystemFunctions.XPATHVALUE, QueryPlugin.Util.getString("SystemSource.xpathvalue_description"), XML, XML_FUNCTION_CLASS, "xpathValue", //$NON-NLS-1$ //$NON-NLS-2$ 
                new FunctionParameter[] { 
                    new FunctionParameter("document", DataTypeManager.DefaultDataTypes.CLOB, QueryPlugin.Util.getString("SystemSource.xpath_param1")), //$NON-NLS-1$ //$NON-NLS-2$
                    new FunctionParameter("xpath", DataTypeManager.DefaultDataTypes.STRING, QueryPlugin.Util.getString("SystemSource.xpath_param2"))}, //$NON-NLS-1$ //$NON-NLS-2$ 
                new FunctionParameter("result", DataTypeManager.DefaultDataTypes.STRING, QueryPlugin.Util.getString("SystemSource.xpathvalue_result")) ) );       //$NON-NLS-1$ //$NON-NLS-2$
        
        functions.add(new FunctionMethod(SourceSystemFunctions.XPATHVALUE, QueryPlugin.Util.getString("SystemSource.xpathvalue_description"), XML, XML_FUNCTION_CLASS, "xpathValue", //$NON-NLS-1$ //$NON-NLS-2$ 
                                         new FunctionParameter[] { 
                                             new FunctionParameter("document", DataTypeManager.DefaultDataTypes.XML, QueryPlugin.Util.getString("SystemSource.xpath_param1")), //$NON-NLS-1$ //$NON-NLS-2$
                                             new FunctionParameter("xpath", DataTypeManager.DefaultDataTypes.STRING, QueryPlugin.Util.getString("SystemSource.xpath_param2"))}, //$NON-NLS-1$ //$NON-NLS-2$ 
                                         new FunctionParameter("result", DataTypeManager.DefaultDataTypes.STRING, QueryPlugin.Util.getString("SystemSource.xpathvalue_result")) ) );       //$NON-NLS-1$ //$NON-NLS-2$
    }
    
    private void addXslTransformFunction() {
    	for (String type1 : Arrays.asList(DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.XML, DataTypeManager.DefaultDataTypes.CLOB)) {
    		for (String type2 : Arrays.asList(DataTypeManager.DefaultDataTypes.STRING, DataTypeManager.DefaultDataTypes.XML, DataTypeManager.DefaultDataTypes.CLOB)) {
    	        functions.add(new FunctionMethod(SourceSystemFunctions.XSLTRANSFORM, QueryPlugin.Util.getString("SystemSource.xsltransform_description"), XML, XML_FUNCTION_CLASS, "xslTransform", //$NON-NLS-1$ //$NON-NLS-2$  
                        new FunctionParameter[] { 
                            new FunctionParameter("document", type1, QueryPlugin.Util.getString("SystemSource.xsltransform_param1")), //$NON-NLS-1$ //$NON-NLS-2$
                            new FunctionParameter("xsl", type2, QueryPlugin.Util.getString("SystemSource.xsltransform_param2"))}, //$NON-NLS-1$ //$NON-NLS-2$ 
                        new FunctionParameter("result", DataTypeManager.DefaultDataTypes.CLOB, QueryPlugin.Util.getString("SystemSource.xsltransform_result")) ) );       //$NON-NLS-1$ //$NON-NLS-2$
    		}
    	}
    }
    
    private void addXmlComment() {
        functions.add(new FunctionMethod(SourceSystemFunctions.XMLCOMMENT, QueryPlugin.Util.getString("SystemSource.xmlcomment_description"), XML, XML_FUNCTION_CLASS, "xmlComment", //$NON-NLS-1$ //$NON-NLS-2$  
                            new FunctionParameter[] { 
                                new FunctionParameter("value", DataTypeManager.DefaultDataTypes.STRING, QueryPlugin.Util.getString("SystemSource.xmlcomment_param2"))}, //$NON-NLS-1$ //$NON-NLS-2$ 
                            new FunctionParameter("result", DataTypeManager.DefaultDataTypes.XML, QueryPlugin.Util.getString("SystemSource.xmlcomment_result")) ) );       //$NON-NLS-1$ //$NON-NLS-2$
    }

    private void addXmlPi() {
        functions.add(new FunctionMethod(SourceSystemFunctions.XMLPI, QueryPlugin.Util.getString("SystemSource.xmlpi_description"), XML, XML_FUNCTION_CLASS, "xmlPi", //$NON-NLS-1$ //$NON-NLS-2$  
                            new FunctionParameter[] { 
					            new FunctionParameter("name", DataTypeManager.DefaultDataTypes.STRING, QueryPlugin.Util.getString("SystemSource.xmlpi_param1"))}, //$NON-NLS-1$ //$NON-NLS-2$
                            new FunctionParameter("result", DataTypeManager.DefaultDataTypes.XML, QueryPlugin.Util.getString("SystemSource.xmlpi_result")) ) );       //$NON-NLS-1$ //$NON-NLS-2$
        
        functions.add(new FunctionMethod(SourceSystemFunctions.XMLPI, QueryPlugin.Util.getString("SystemSource.xmlpi_description"), XML, XML_FUNCTION_CLASS, "xmlPi", //$NON-NLS-1$ //$NON-NLS-2$  
                new FunctionParameter[] { 
		            new FunctionParameter("name", DataTypeManager.DefaultDataTypes.STRING, QueryPlugin.Util.getString("SystemSource.xmlpi_param1")), //$NON-NLS-1$ //$NON-NLS-2$
		            new FunctionParameter("value", DataTypeManager.DefaultDataTypes.STRING, QueryPlugin.Util.getString("SystemSource.xmlpi_param2"))}, //$NON-NLS-1$ //$NON-NLS-2$ 
                new FunctionParameter("result", DataTypeManager.DefaultDataTypes.XML, QueryPlugin.Util.getString("SystemSource.xmlpi_result")) ) );       //$NON-NLS-1$ //$NON-NLS-2$
    }
    
    private void addJsonToXml() {
        functions.add(new FunctionMethod(SourceSystemFunctions.JSONTOXML, QueryPlugin.Util.getString("SystemSource.jsonToXml_description"), XML, XML_FUNCTION_CLASS, "jsonToXml", //$NON-NLS-1$ //$NON-NLS-2$  
                new FunctionParameter[] { 
        	new FunctionParameter("rootElementName", DataTypeManager.DefaultDataTypes.STRING, QueryPlugin.Util.getString("SystemSource.jsonToXml_param1")), //$NON-NLS-1$ //$NON-NLS-2$
        	new FunctionParameter("json", DataTypeManager.DefaultDataTypes.CLOB, QueryPlugin.Util.getString("SystemSource.jsonToXml_param2"))}, //$NON-NLS-1$ //$NON-NLS-2$ 
                new FunctionParameter("result", DataTypeManager.DefaultDataTypes.XML, QueryPlugin.Util.getString("SystemSource.jsonToXml_result")) ) );       //$NON-NLS-1$ //$NON-NLS-2$
        functions.add(new FunctionMethod(SourceSystemFunctions.JSONTOXML, QueryPlugin.Util.getString("SystemSource.jsonToXml_description"), XML, XML_FUNCTION_CLASS, "jsonToXml", //$NON-NLS-1$ //$NON-NLS-2$  
                new FunctionParameter[] { 
        	new FunctionParameter("rootElementName", DataTypeManager.DefaultDataTypes.STRING, QueryPlugin.Util.getString("SystemSource.jsonToXml_param1")), //$NON-NLS-1$ //$NON-NLS-2$
        	new FunctionParameter("json", DataTypeManager.DefaultDataTypes.BLOB, QueryPlugin.Util.getString("SystemSource.jsonToXml_param2"))}, //$NON-NLS-1$ //$NON-NLS-2$ 
                new FunctionParameter("result", DataTypeManager.DefaultDataTypes.XML, QueryPlugin.Util.getString("SystemSource.jsonToXml_result")) ) );       //$NON-NLS-1$ //$NON-NLS-2$
    }
    
    private void addXmlConcat() {
        functions.add(new FunctionMethod(SourceSystemFunctions.XMLCONCAT, QueryPlugin.Util.getString("SystemSource.xmlconcat_description"), XML, PushDown.CAN_PUSHDOWN, XML_FUNCTION_CLASS, "xmlConcat", //$NON-NLS-1$ //$NON-NLS-2$  
                            new FunctionParameter[] { 
                                new FunctionParameter("param1", DataTypeManager.DefaultDataTypes.XML, QueryPlugin.Util.getString("SystemSource.xmlconcat_param1")), //$NON-NLS-1$ //$NON-NLS-2$
                                new FunctionParameter("param2", DataTypeManager.DefaultDataTypes.XML, QueryPlugin.Util.getString("SystemSource.xmlconcat_param2"), true)}, //$NON-NLS-1$ //$NON-NLS-2$ 
                            new FunctionParameter("result", DataTypeManager.DefaultDataTypes.XML, QueryPlugin.Util.getString("SystemSource.xmlconcat_result")), true, Determinism.DETERMINISTIC ) );       //$NON-NLS-1$ //$NON-NLS-2$
    }
    
    private void addTimeZoneFunctions() {
        functions.add(new FunctionMethod(SourceSystemFunctions.MODIFYTIMEZONE, QueryPlugin.Util.getString("SystemSource.modifyTimeZone_description"), DATETIME, FUNCTION_CLASS, "modifyTimeZone", //$NON-NLS-1$ //$NON-NLS-2$ 
                            new FunctionParameter[] { 
                                new FunctionParameter("timestamp", DataTypeManager.DefaultDataTypes.TIMESTAMP, QueryPlugin.Util.getString("SystemSource.modifyTimeZone_param1")), //$NON-NLS-1$ //$NON-NLS-2$
                                new FunctionParameter("startTimeZone", DataTypeManager.DefaultDataTypes.STRING, QueryPlugin.Util.getString("SystemSource.modifyTimeZone_param2")), //$NON-NLS-1$ //$NON-NLS-2$
                                new FunctionParameter("endTimeZone", DataTypeManager.DefaultDataTypes.STRING, QueryPlugin.Util.getString("SystemSource.modifyTimeZone_param3"))}, //$NON-NLS-1$ //$NON-NLS-2$ 
                            new FunctionParameter("result", DataTypeManager.DefaultDataTypes.TIMESTAMP, QueryPlugin.Util.getString("SystemSource.modifyTimeZone_result")) ) );       //$NON-NLS-1$ //$NON-NLS-2$

        functions.add(new FunctionMethod(SourceSystemFunctions.MODIFYTIMEZONE, QueryPlugin.Util.getString("SystemSource.modifyTimeZone_description"), DATETIME, FUNCTION_CLASS, "modifyTimeZone", //$NON-NLS-1$ //$NON-NLS-2$ 
                                         new FunctionParameter[] { 
                                             new FunctionParameter("timestamp", DataTypeManager.DefaultDataTypes.TIMESTAMP, QueryPlugin.Util.getString("SystemSource.modifyTimeZone_param1")), //$NON-NLS-1$ //$NON-NLS-2$
                                             new FunctionParameter("endTimeZone", DataTypeManager.DefaultDataTypes.STRING, QueryPlugin.Util.getString("SystemSource.modifyTimeZone_param3"))}, //$NON-NLS-1$ //$NON-NLS-2$ 
                                         new FunctionParameter("result", DataTypeManager.DefaultDataTypes.TIMESTAMP, QueryPlugin.Util.getString("SystemSource.modifyTimeZone_result")) ) );       //$NON-NLS-1$ //$NON-NLS-2$

    }
    
    private void addUnixTimeFunctions() {
    	functions.add(new FunctionMethod(FunctionLibrary.FROM_UNIXTIME, QueryPlugin.Util.getString("SystemSource.from_unixtime_description"), DATETIME, PushDown.SYNTHETIC, null, null, //$NON-NLS-1$ 
    			new FunctionParameter[] {
    				new FunctionParameter("unix_timestamp", DataTypeManager.DefaultDataTypes.INTEGER, QueryPlugin.Util.getString("SystemSource.from_unixtime_param1")) //$NON-NLS-1$ //$NON-NLS-2$
    			},
                new FunctionParameter("result", DataTypeManager.DefaultDataTypes.TIMESTAMP, QueryPlugin.Util.getString("SystemSource.from_unixtime_result")), false, Determinism.DETERMINISTIC )); //$NON-NLS-1$ //$NON-NLS-2$
    }
    
    private void addTypedNullIfFunction(String type) {
        functions.add(
            new FunctionMethod(FunctionLibrary.NULLIF, QueryPlugin.Util.getString("SystemSource.nullif_description"), MISCELLANEOUS, PushDown.SYNTHETIC, null, null, //$NON-NLS-1$ 
                new FunctionParameter[] { 
                    new FunctionParameter("op1", type, QueryPlugin.Util.getString("SystemSource.nullif_param1")), //$NON-NLS-1$ //$NON-NLS-2$
                    new FunctionParameter("op2", type, QueryPlugin.Util.getString("SystemSource.nullif_param1")) }, //$NON-NLS-1$ //$NON-NLS-2$
                new FunctionParameter("result", type, QueryPlugin.Util.getString("SystemSource.nullif_result")), true, Determinism.DETERMINISTIC)); //$NON-NLS-1$ //$NON-NLS-2$
    }
    
    private void addTypedCoalesceFunction(String type) {
        functions.add(
            new FunctionMethod(FunctionLibrary.COALESCE, QueryPlugin.Util.getString("SystemSource.coalesce_description"), MISCELLANEOUS, PushDown.CAN_PUSHDOWN, FUNCTION_CLASS, "coalesce", //$NON-NLS-1$ //$NON-NLS-2$
                new FunctionParameter[] { 
                    new FunctionParameter("op1", type, QueryPlugin.Util.getString("SystemSource.coalesce_param1")), //$NON-NLS-1$ //$NON-NLS-2$
                    new FunctionParameter("op2", type, QueryPlugin.Util.getString("SystemSource.coalesce_param1")), //$NON-NLS-1$ //$NON-NLS-2$
                    new FunctionParameter("op3", type, QueryPlugin.Util.getString("SystemSource.coalesce_param1"), true) }, //$NON-NLS-1$ //$NON-NLS-2$
                new FunctionParameter("result", type, QueryPlugin.Util.getString("SystemSource.coalesce_result")), true, Determinism.DETERMINISTIC)); //$NON-NLS-1$ //$NON-NLS-2$
    }
		
    /**
     * Get all function signatures for this metadata source.
     * @return Unordered collection of {@link FunctionMethod}s
     */
    public Collection<org.teiid.metadata.FunctionMethod> getFunctionMethods() {
        return this.functions;
	}
    
    /**
     * Sources are returned from the system classpath.
     * @param className Name of class
     * @return Class reference  
     * @throws ClassNotFoundException If class could not be found
     */
    public Class<?> getInvocationClass(String className) throws ClassNotFoundException {
        return Class.forName(className);    
    }
}
