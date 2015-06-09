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

import org.teiid.language.Expression;
import org.teiid.language.Function;
import org.teiid.language.Limit;
import org.teiid.metadata.MetadataFactory;
import org.teiid.translator.ExecutionContext;
import org.teiid.translator.MetadataProcessor;
import org.teiid.translator.SourceSystemFunctions;
import org.teiid.translator.Translator;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.jdbc.AliasModifier;
import org.teiid.translator.jdbc.ConvertModifier;
import org.teiid.translator.jdbc.FunctionModifier;
import org.teiid.translator.jdbc.JDBCExecutionFactory;
import org.teiid.translator.jdbc.LocateFunctionModifier;

@Translator(name = "hana", description = "SAP HANA translator")
public class HanaExecutionFactory extends JDBCExecutionFactory {
	
	private static final String TIME_FORMAT = "HH24:MI:SS"; //$NON-NLS-1$
	private static final String DATE_FORMAT = "YYYY-MM-DD"; //$NON-NLS-1$
	private static final String DATETIME_FORMAT = DATE_FORMAT + " " + TIME_FORMAT; //$NON-NLS-1$
//	private static final String TIMESTAMP_FORMAT = DATETIME_FORMAT + ".FF7";  //$NON-NLS-1$
	
	public HanaExecutionFactory() {
	}

	@Override
	public void start() throws TranslatorException {
		super.start();
		
		registerFunctionModifier(SourceSystemFunctions.LCASE, new AliasModifier("lower")); //$NON-NLS-1$ 
		registerFunctionModifier(SourceSystemFunctions.CEILING, new AliasModifier("ceil")); //$NON-NLS-1$ 
        registerFunctionModifier(SourceSystemFunctions.LCASE, new AliasModifier("lower")); //$NON-NLS-1$ 
        registerFunctionModifier(SourceSystemFunctions.CHAR, new AliasModifier("to_nvarchar")); //$NON-NLS-1$ 
        registerFunctionModifier(SourceSystemFunctions.UCASE, new AliasModifier("upper")); //$NON-NLS-1$ 
        registerFunctionModifier(SourceSystemFunctions.LOG, new Log10FunctionModifier(getLanguageFactory()));
        registerFunctionModifier(SourceSystemFunctions.CEILING, new AliasModifier("ceil")); //$NON-NLS-1$ 
        registerFunctionModifier(SourceSystemFunctions.LOG10, new Log10FunctionModifier(getLanguageFactory())); 
        registerFunctionModifier(SourceSystemFunctions.LOCATE, new LocateFunctionModifier(getLanguageFactory(), "locate", true) {

			@Override
			public void modify(Function function) {
				super.modify(function);
				//If a start index was passed in, we convert to a substring on the search string since
				//HANA does not support the start index parameter in LOCATE().
				List<Expression> args = function.getParameters();
				if (args.size() > 2) {
		        	List<Expression> substringArgs = new ArrayList<Expression>();
		            substringArgs.add(args.get(0));
		            substringArgs.add(args.get(2));
		            args.set(0, getLanguageFactory().createFunction(SourceSystemFunctions.SUBSTRING, substringArgs, null));
		            args.remove(2);
		        }
				
			}
        	
    	});
        registerFunctionModifier(SourceSystemFunctions.CURDATE, new AliasModifier("current_date")); //$NON-NLS-1$ 
        registerFunctionModifier(SourceSystemFunctions.CURTIME, new AliasModifier("current_time")); //$NON-NLS-1$ 
        registerFunctionModifier(SourceSystemFunctions.DAYOFWEEK, new AliasModifier("dayname")); //$NON-NLS-1$ 
        registerFunctionModifier(SourceSystemFunctions.NOW, new AliasModifier("current_timestamp")); //$NON-NLS-1$ 
          
		//////////////////////////////////////////////////////////
		//TYPE CONVERION MODIFIERS////////////////////////////////
		//////////////////////////////////////////////////////////
        ConvertModifier convertModifier = new ConvertModifier();
        convertModifier.addTypeMapping("tinyint", FunctionModifier.BOOLEAN, FunctionModifier.BYTE); //$NON-NLS-1$
        convertModifier.addTypeMapping("smallint", FunctionModifier.SHORT); //$NON-NLS-1$
        convertModifier.addTypeMapping("integer", FunctionModifier.INTEGER); //$NON-NLS-1$
        convertModifier.addTypeMapping("bigint", FunctionModifier.LONG, FunctionModifier.BIGINTEGER); //$NON-NLS-1$
        //convertModifier.addTypeMapping("smalldecimal", FunctionModifier.LONG, FunctionModifier.BIGINTEGER); //$NON-NLS-1$
        convertModifier.addTypeMapping("decimal", FunctionModifier.BIGDECIMAL); //$NON-NLS-1$
        convertModifier.addTypeMapping("float", FunctionModifier.FLOAT); //$NON-NLS-1$
        //convertModifier.addTypeMapping("real", FunctionModifier.FLOAT); //$NON-NLS-1$
        convertModifier.addTypeMapping("date", FunctionModifier.DATE); //$NON-NLS-1$
    	convertModifier.addTypeMapping("double", FunctionModifier.DOUBLE); //$NON-NLS-1$
    	convertModifier.addTypeMapping("time", FunctionModifier.TIME); //$NON-NLS-1$
    	convertModifier.addTypeMapping("timestamp", FunctionModifier.TIMESTAMP); //$NON-NLS-1$
    	//convertModifier.addTypeMapping("seconddate", FunctionModifier.TIMESTAMP); //$NON-NLS-1$
    	//convertModifier.addTypeMapping("varchar", FunctionModifier.CHAR, FunctionModifier.STRING); //$NON-NLS-1$
    	convertModifier.addTypeMapping("nvarchar", FunctionModifier.CHAR, FunctionModifier.STRING); //$NON-NLS-1$
    	//convertModifier.addTypeMapping("alphanum", FunctionModifier.CHAR, FunctionModifier.STRING); //$NON-NLS-1$
    	convertModifier.addTypeMapping("varbinary", FunctionModifier.VARBINARY); //$NON-NLS-1$
    	convertModifier.addTypeMapping("blob", FunctionModifier.BLOB, FunctionModifier.OBJECT); //$NON-NLS-1$
    	convertModifier.addTypeMapping("clob", FunctionModifier.CLOB); //$NON-NLS-1$

    	convertModifier.addConvert(FunctionModifier.STRING, FunctionModifier.DATE, new ConvertModifier.FormatModifier("to_date", DATE_FORMAT)); //$NON-NLS-1$ 
    	convertModifier.addConvert(FunctionModifier.STRING, FunctionModifier.TIME, new ConvertModifier.FormatModifier("to_time", TIME_FORMAT)); //$NON-NLS-1$ 
        convertModifier.addConvert(FunctionModifier.STRING, FunctionModifier.TIMESTAMP, new ConvertModifier.FormatModifier("to_timestamp", DATETIME_FORMAT));  //$NON-NLS-1$ 
        
    	convertModifier.setWideningNumericImplicit(true);
    	registerFunctionModifier(SourceSystemFunctions.CONVERT, convertModifier);
		
	}
	
	@Override
	public String getHibernateDialectClassName() {
		return "org.hibernate.dialect.HANARowStoreDialect";
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
		supportedFunctions.add(SourceSystemFunctions.LEFT);
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
		supportedFunctions.add(SourceSystemFunctions.DAYOFWEEK);
		supportedFunctions.add(SourceSystemFunctions.DAYOFMONTH); 
		supportedFunctions.add(SourceSystemFunctions.DAYOFYEAR);
		supportedFunctions.add(SourceSystemFunctions.DAYOFWEEK);
		supportedFunctions.add(SourceSystemFunctions.HOUR); 
		supportedFunctions.add(SourceSystemFunctions.MINUTE); 
		supportedFunctions.add(SourceSystemFunctions.MONTH);
		supportedFunctions.add(SourceSystemFunctions.MONTHNAME);
		supportedFunctions.add(SourceSystemFunctions.QUARTER);
		supportedFunctions.add(SourceSystemFunctions.SECOND);
		supportedFunctions.add(SourceSystemFunctions.WEEK);
		supportedFunctions.add(SourceSystemFunctions.YEAR);

        /////////////////////////////////////////////////////////////////////
		//SYSTEM FUNCTIONS///////////////////////////////////////////////////
        /////////////////////////////////////////////////////////////////////
		supportedFunctions.add(SourceSystemFunctions.IFNULL); 
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
	
	@Override
    public List<?> translateLimit(Limit limit, ExecutionContext context) {
    	if (limit.getRowOffset() > 0) {
    		return Arrays.asList("LIMIT ", limit.getRowLimit(), " OFFSET ", limit.getRowOffset()); //$NON-NLS-1$ //$NON-NLS-2$ 
    	}
        return null;
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