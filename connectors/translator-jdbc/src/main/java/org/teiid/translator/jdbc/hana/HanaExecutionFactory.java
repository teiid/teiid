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

package org.teiid.translator.jdbc.hana;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.teiid.language.ColumnReference;
import org.teiid.language.Expression;
import org.teiid.language.Function;
import org.teiid.language.Literal;
import org.teiid.metadata.MetadataFactory;
import org.teiid.translator.MetadataProcessor;
import org.teiid.translator.SourceSystemFunctions;
import org.teiid.translator.Translator;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.TypeFacility;
import org.teiid.translator.jdbc.AliasModifier;
import org.teiid.translator.jdbc.ConvertModifier;
import org.teiid.translator.jdbc.ExtractFunctionModifier;
import org.teiid.translator.jdbc.FunctionModifier;
import org.teiid.translator.jdbc.JDBCExecutionFactory;
import org.teiid.translator.jdbc.LocateFunctionModifier;
import org.teiid.translator.jdbc.oracle.ConcatFunctionModifier;
import org.teiid.translator.jdbc.oracle.DayWeekQuarterFunctionModifier;
import org.teiid.translator.jdbc.oracle.LeftOrRightFunctionModifier;
import org.teiid.translator.jdbc.oracle.Log10FunctionModifier;
import org.teiid.translator.jdbc.oracle.MonthOrDayNameFunctionModifier;

@Translator(name = "hana", description = "SAP HANA translator")
public class HanaExecutionFactory extends JDBCExecutionFactory {
	
	private static final String TIME_FORMAT = "HH24:MI:SS"; //$NON-NLS-1$
	private static final String DATE_FORMAT = "YYYY-MM-DD"; //$NON-NLS-1$
	private static final String DATETIME_FORMAT = DATE_FORMAT + " " + TIME_FORMAT; //$NON-NLS-1$
	private static final String TIMESTAMP_FORMAT = DATETIME_FORMAT + ".FF7";  //$NON-NLS-1$
	
	private final class DateAwareExtract extends ExtractFunctionModifier {
		@Override
		public List<?> translate(Function function) {
			Expression ex = function.getParameters().get(0);
			if ((ex instanceof ColumnReference && "date".equalsIgnoreCase(((ColumnReference)ex).getMetadataObject().getNativeType())) //$NON-NLS-1$ 
					|| (!(ex instanceof ColumnReference) && !(ex instanceof Literal) && !(ex instanceof Function))) {
				ex = ConvertModifier.createConvertFunction(getLanguageFactory(), function.getParameters().get(0), TypeFacility.RUNTIME_NAMES.TIMESTAMP);
				function.getParameters().set(0, ex);
			}
			return super.translate(function);
		}
	}
	
	/*
	 * Handling for cursor return values
	 */
	static final class RefCursorType {}
	static int CURSOR_TYPE = -10;
	static final String REF_CURSOR = "REF CURSOR"; //$NON-NLS-1$
	
	/*
	 * handling for char bindings
	 */
	static final class FixedCharType {}
	static int FIXED_CHAR_TYPE = 5000;

	public HanaExecutionFactory() {
	}

	@Override
	public void start() throws TranslatorException {
		super.start();
		
		registerFunctionModifier(SourceSystemFunctions.LCASE, new AliasModifier("lower")); //$NON-NLS-1$ 
		registerFunctionModifier(SourceSystemFunctions.CEILING, new AliasModifier("ceil")); //$NON-NLS-1$ 
        registerFunctionModifier(SourceSystemFunctions.LCASE, new AliasModifier("lower")); //$NON-NLS-1$ 
        registerFunctionModifier(SourceSystemFunctions.UCASE, new AliasModifier("upper")); //$NON-NLS-1$ 
        registerFunctionModifier(SourceSystemFunctions.LOG, new AliasModifier("ln")); //$NON-NLS-1$ 
        registerFunctionModifier(SourceSystemFunctions.CEILING, new AliasModifier("ceil")); //$NON-NLS-1$ 
        registerFunctionModifier(SourceSystemFunctions.LOG10, new Log10FunctionModifier(getLanguageFactory())); 
        registerFunctionModifier(SourceSystemFunctions.HOUR, new DateAwareExtract());
        registerFunctionModifier(SourceSystemFunctions.YEAR, new ExtractFunctionModifier()); 
        registerFunctionModifier(SourceSystemFunctions.MINUTE, new DateAwareExtract()); 
        registerFunctionModifier(SourceSystemFunctions.SECOND, new DateAwareExtract()); 
        registerFunctionModifier(SourceSystemFunctions.MONTH, new ExtractFunctionModifier()); 
        registerFunctionModifier(SourceSystemFunctions.DAYOFMONTH, new ExtractFunctionModifier()); 
        registerFunctionModifier(SourceSystemFunctions.MONTHNAME, new MonthOrDayNameFunctionModifier(getLanguageFactory(), "Month"));//$NON-NLS-1$ 
        registerFunctionModifier(SourceSystemFunctions.DAYNAME, new MonthOrDayNameFunctionModifier(getLanguageFactory(), "Day"));//$NON-NLS-1$ 
        registerFunctionModifier(SourceSystemFunctions.WEEK, new DayWeekQuarterFunctionModifier("IW"));//$NON-NLS-1$ 
        registerFunctionModifier(SourceSystemFunctions.QUARTER, new DayWeekQuarterFunctionModifier("Q"));//$NON-NLS-1$ 
        registerFunctionModifier(SourceSystemFunctions.DAYOFWEEK, new DayWeekQuarterFunctionModifier("D"));//$NON-NLS-1$ 
        registerFunctionModifier(SourceSystemFunctions.DAYOFYEAR, new DayWeekQuarterFunctionModifier("DDD"));//$NON-NLS-1$ 
        registerFunctionModifier(SourceSystemFunctions.SUBSTRING, new AliasModifier("substr"));//$NON-NLS-1$ 
        registerFunctionModifier(SourceSystemFunctions.LEFT, new LeftOrRightFunctionModifier(getLanguageFactory()));
        registerFunctionModifier(SourceSystemFunctions.RIGHT, new LeftOrRightFunctionModifier(getLanguageFactory()));
        registerFunctionModifier(SourceSystemFunctions.CONCAT, new ConcatFunctionModifier(getLanguageFactory())); 
        registerFunctionModifier(SourceSystemFunctions.COT, new FunctionModifier() {
			@Override
			public List<?> translate(Function function) {
				function.setName(SourceSystemFunctions.TAN);
				return Arrays.asList(getLanguageFactory().createFunction(SourceSystemFunctions.DIVIDE_OP, new Expression[] {new Literal(1, TypeFacility.RUNTIME_TYPES.INTEGER), function}, TypeFacility.RUNTIME_TYPES.DOUBLE));
			}
		});
	      
		//////////////////////////////////////////////////////////
		//TYPE CONVERION MODIFIERS////////////////////////////////
		//////////////////////////////////////////////////////////
        ConvertModifier convertModifier = new ConvertModifier();
        //TODO Add type mappings
    	//convertModifier.addTypeMapping(nativeType, targetType);
    	convertModifier.addTypeMapping("nvarchar(1)", FunctionModifier.CHAR); //$NON-NLS-1$
    	convertModifier.addTypeMapping("date", FunctionModifier.DATE, FunctionModifier.TIME); //$NON-NLS-1$
    	convertModifier.addTypeMapping("timestamp", FunctionModifier.TIMESTAMP); //$NON-NLS-1$
    	convertModifier.addConvert(FunctionModifier.DATE, FunctionModifier.STRING, new ConvertModifier.FormatModifier("to_varchar", DATE_FORMAT)); //$NON-NLS-1$ 
    	convertModifier.addConvert(FunctionModifier.TIME, FunctionModifier.STRING, new ConvertModifier.FormatModifier("to_varchar", TIME_FORMAT)); //$NON-NLS-1$
    	convertModifier.addConvert(FunctionModifier.TIMESTAMP, FunctionModifier.STRING, new FunctionModifier() {
			@Override
			public List<?> translate(Function function) {
				//if column and type is date, just use date format
				Expression ex = function.getParameters().get(0);
				String format = TIMESTAMP_FORMAT; 
				if (ex instanceof ColumnReference && "date".equalsIgnoreCase(((ColumnReference)ex).getMetadataObject().getNativeType())) { //$NON-NLS-1$
					format = DATETIME_FORMAT; 
				} else if (!(ex instanceof Literal) && !(ex instanceof Function)) {
					//this isn't needed in every case, but it's simpler than inspecting the expression more
					ex = ConvertModifier.createConvertFunction(getLanguageFactory(), function.getParameters().get(0), TypeFacility.RUNTIME_NAMES.TIMESTAMP);
				}
				return Arrays.asList("to_char(", ex, ", '", format, "')"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
			}
		});
    	convertModifier.addConvert(FunctionModifier.STRING, FunctionModifier.DATE, new ConvertModifier.FormatModifier("to_date", DATE_FORMAT)); //$NON-NLS-1$ 
    	convertModifier.addConvert(FunctionModifier.STRING, FunctionModifier.TIME, new ConvertModifier.FormatModifier("to_date", TIME_FORMAT)); //$NON-NLS-1$ 
    	convertModifier.addConvert(FunctionModifier.STRING, FunctionModifier.TIMESTAMP, new ConvertModifier.FormatModifier("to_timestamp", TIMESTAMP_FORMAT)); //$NON-NLS-1$ 
    	convertModifier.addTypeConversion(new ConvertModifier.FormatModifier("to_varchar"), FunctionModifier.STRING); //$NON-NLS-1$
    	convertModifier.setWideningNumericImplicit(true);
    	registerFunctionModifier(SourceSystemFunctions.CONVERT, convertModifier);
		
	}
	
	@Override
	public List<String> getSupportedFunctions() {
		List<String> supportedFunctions = new ArrayList<String>();
		supportedFunctions.addAll(super.getSupportedFunctions());

		//////////////////////////////////////////////////////////
		//STRING FUNCTIONS////////////////////////////////////////
		//////////////////////////////////////////////////////////
		supportedFunctions.add(SourceSystemFunctions.ASCII);// taken care with alias function modifier
		supportedFunctions.add(SourceSystemFunctions.CHAR);
		supportedFunctions.add(SourceSystemFunctions.CONCAT); 
		supportedFunctions.add(SourceSystemFunctions.LCASE);//ALIAS 'lower'
		supportedFunctions.add(SourceSystemFunctions.LPAD);
		supportedFunctions.add(SourceSystemFunctions.LENGTH);
		supportedFunctions.add(SourceSystemFunctions.LOCATE); 
		supportedFunctions.add(SourceSystemFunctions.LTRIM);
		supportedFunctions.add(SourceSystemFunctions.REPLACE);
		supportedFunctions.add(SourceSystemFunctions.RIGHT);
		supportedFunctions.add(SourceSystemFunctions.RPAD);
		supportedFunctions.add(SourceSystemFunctions.RTRIM);
		supportedFunctions.add(SourceSystemFunctions.SUBSTRING);
		supportedFunctions.add(SourceSystemFunctions.UCASE); //No Need of ALIAS as both ucase and upper work in HANA
		supportedFunctions.add(SourceSystemFunctions.RTRIM);
		
		///////////////////////////////////////////////////////////
		//NUMERIC FUNCTIONS////////////////////////////////////////
		///////////////////////////////////////////////////////////
		supportedFunctions.add(SourceSystemFunctions.ABS);
		supportedFunctions.add(SourceSystemFunctions.ACOS);
		supportedFunctions.add(SourceSystemFunctions.ASIN);
		supportedFunctions.add(SourceSystemFunctions.ATAN);
		supportedFunctions.add(SourceSystemFunctions.ATAN2);
		supportedFunctions.add(SourceSystemFunctions.CEILING);  ///ALIAS-ceil
		supportedFunctions.add(SourceSystemFunctions.COS);
		supportedFunctions.add(SourceSystemFunctions.COT);
		supportedFunctions.add(SourceSystemFunctions.EXP);
		supportedFunctions.add(SourceSystemFunctions.FLOOR);
		supportedFunctions.add(SourceSystemFunctions.LOG);
		supportedFunctions.add(SourceSystemFunctions.MOD);
		supportedFunctions.add(SourceSystemFunctions.POWER);
		supportedFunctions.add(SourceSystemFunctions.ROUND);
		supportedFunctions.add(SourceSystemFunctions.SIGN);	
		supportedFunctions.add(SourceSystemFunctions.SIN);
		supportedFunctions.add(SourceSystemFunctions.SQRT);
		supportedFunctions.add(SourceSystemFunctions.TAN);
		supportedFunctions.add(SourceSystemFunctions.RAND); 
		
		/////////////////////////////////////////////////////////////////////
		//BIT FUNCTIONS//////////////////////////////////////////////////////
		/////////////////////////////////////////////////////////////////////
		supportedFunctions.add(SourceSystemFunctions.BITAND);
		supportedFunctions.add(SourceSystemFunctions.BITOR);
		supportedFunctions.add(SourceSystemFunctions.BITNOT);
		supportedFunctions.add(SourceSystemFunctions.BITXOR);
		
		/////////////////////////////////////////////////////////////////////
		//DATE FUNCTIONS/////////////////////////////////////////////////////
		/////////////////////////////////////////////////////////////////////
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

        /////////////////////////////////////////////////////////////////////
		//SYSTEM FUNCTIONS///////////////////////////////////////////////////
        /////////////////////////////////////////////////////////////////////
		supportedFunctions.add(SourceSystemFunctions.IFNULL); //ALIAS-NVL
		supportedFunctions.add(SourceSystemFunctions.COALESCE);
		supportedFunctions.add(SourceSystemFunctions.NULLIF);
		
		/////////////////////////////////////////////////////////////////////
		//CONVERSION functions///////////////////////////////////////////////
		/////////////////////////////////////////////////////////////////////
		supportedFunctions.add(SourceSystemFunctions.CONVERT);
		
		return supportedFunctions;
	}

	public boolean supportsCompareCriteriaEquals() {
		return true;
	}

	public boolean supportsInCriteria() {
		return true;
	}

	@Override
	public boolean isSourceRequired() {
		return false;
	}

	public void getMetadata(MetadataFactory metadataFactory,
			Connection connection) throws TranslatorException {
		super.getMetadata(metadataFactory, connection);
	}

	@Override
	public MetadataProcessor<Connection> getMetadataProcessor() {
		return new HanaMetadataProcessor();
	}

	@Override
	public boolean supportsOnlyLiteralComparison() {
		return true;
	}
	
	

}
