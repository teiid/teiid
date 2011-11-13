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
package org.teiid.translator.hive;

import static org.teiid.translator.TypeFacility.RUNTIME_NAMES.BIG_INTEGER;
import static org.teiid.translator.TypeFacility.RUNTIME_NAMES.DATE;
import static org.teiid.translator.TypeFacility.RUNTIME_NAMES.DOUBLE;
import static org.teiid.translator.TypeFacility.RUNTIME_NAMES.INTEGER;
import static org.teiid.translator.TypeFacility.RUNTIME_NAMES.OBJECT;
import static org.teiid.translator.TypeFacility.RUNTIME_NAMES.STRING;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.teiid.core.util.PropertiesUtils;
import org.teiid.language.Command;
import org.teiid.language.Function;
import org.teiid.language.Limit;
import org.teiid.metadata.MetadataFactory;
import org.teiid.translator.ExecutionContext;
import org.teiid.translator.SourceSystemFunctions;
import org.teiid.translator.Translator;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.TypeFacility;
import org.teiid.translator.jdbc.AliasModifier;
import org.teiid.translator.jdbc.ConvertModifier;
import org.teiid.translator.jdbc.FunctionModifier;
import org.teiid.translator.jdbc.JDBCExecutionFactory;
import org.teiid.translator.jdbc.ModFunctionModifier;
import org.teiid.translator.jdbc.SQLConversionVisitor;

@Translator(name="hive", description="A translator for hive based database on HDFS")
public class HiveExecutionFactory extends JDBCExecutionFactory {
	
	public static String HIVE = "hive"; //$NON-NLS-1$
	protected ConvertModifier convert = new ConvertModifier();
	
	
	public HiveExecutionFactory() {
		setSupportedJoinCriteria(SupportedJoinCriteria.EQUI);
	}

	@Override
	public void start() throws TranslatorException {
		super.start();
		convert.addTypeMapping("tinyint", FunctionModifier.BYTE); //$NON-NLS-1$
		convert.addTypeMapping("smallint", FunctionModifier.SHORT); //$NON-NLS-1$
		convert.addTypeMapping("int", FunctionModifier.INTEGER); //$NON-NLS-1$
		convert.addTypeMapping("bigint", FunctionModifier.BIGINTEGER, FunctionModifier.LONG); //$NON-NLS-1$
		convert.addTypeMapping("boolean", FunctionModifier.BOOLEAN); //$NON-NLS-1$
		convert.addTypeMapping("double", FunctionModifier.DOUBLE, FunctionModifier.BIGDECIMAL); //$NON-NLS-1$
		convert.addTypeMapping("float", FunctionModifier.FLOAT); //$NON-NLS-1$
		convert.addTypeMapping("string", FunctionModifier.STRING); //$NON-NLS-1$

		// unsupported types
		//FunctionModifier.DATE, 
		//FunctionModifier.TIMESTAMP
		//FunctionModifier.TIME, 
		//FunctionModifier.CHAR, 
		//FunctionModifier.BLOB, 
		//FunctionModifier.CLOB, 
		//FunctionModifier.XML
		
		registerFunctionModifier(SourceSystemFunctions.CONVERT, convert);
		
		registerFunctionModifier(SourceSystemFunctions.BITAND, new AliasModifier("&")); //$NON-NLS-1$
		registerFunctionModifier(SourceSystemFunctions.BITNOT, new AliasModifier("~")); //$NON-NLS-1$
		registerFunctionModifier(SourceSystemFunctions.BITOR, new AliasModifier("&")); //$NON-NLS-1$
		registerFunctionModifier(SourceSystemFunctions.BITXOR, new AliasModifier("|")); //$NON-NLS-1$
		registerFunctionModifier(SourceSystemFunctions.CURDATE, new AliasModifier("unix_timestamp")); //$NON-NLS-1$
		registerFunctionModifier(SourceSystemFunctions.IFNULL, new AliasModifier("coalesce")); //$NON-NLS-1$
		registerFunctionModifier(SourceSystemFunctions.MOD, new ModFunctionModifier("%", getLanguageFactory(), Arrays.asList(TypeFacility.RUNTIME_TYPES.BIG_INTEGER, TypeFacility.RUNTIME_TYPES.BIG_DECIMAL))); //$NON-NLS-1$		
        registerFunctionModifier(SourceSystemFunctions.ARRAY_GET, new FunctionModifier() {
			
			@Override
			public List<?> translate(Function function) {
				return Arrays.asList(function.getParameters().get(0), '[', function.getParameters().get(1), ']');
			}
		});        
		
		
		addPushDownFunction(HIVE, "lower", STRING, STRING); //$NON-NLS-1$
		addPushDownFunction(HIVE, "upper", STRING, STRING); //$NON-NLS-1$
		addPushDownFunction(HIVE, "positive", INTEGER, DOUBLE); //$NON-NLS-1$
		addPushDownFunction(HIVE, "positive", DOUBLE, DOUBLE); //$NON-NLS-1$
		addPushDownFunction(HIVE, "negitive", INTEGER, DOUBLE); //$NON-NLS-1$
		addPushDownFunction(HIVE, "negitive", DOUBLE, DOUBLE); //$NON-NLS-1$	
		addPushDownFunction(HIVE, "ln", DOUBLE, DOUBLE); //$NON-NLS-1$
		addPushDownFunction(HIVE, "reverse", STRING, STRING); //$NON-NLS-1$
		addPushDownFunction(HIVE, "space", INTEGER, STRING); //$NON-NLS-1$
		addPushDownFunction(HIVE, "split", OBJECT, STRING, STRING); //$NON-NLS-1$
		addPushDownFunction(HIVE, "hex", STRING, STRING); //$NON-NLS-1$
		addPushDownFunction(HIVE, "unhex", STRING, STRING); //$NON-NLS-1$
		addPushDownFunction(HIVE, "bin", STRING, BIG_INTEGER); //$NON-NLS-1$
		addPushDownFunction(HIVE, "day", INTEGER, DATE); //$NON-NLS-1$
		addPushDownFunction(HIVE, "datediff", INTEGER, DATE, DATE); //$NON-NLS-1$
		addPushDownFunction(HIVE, "date_add", INTEGER, DATE, INTEGER); //$NON-NLS-1$
		addPushDownFunction(HIVE, "date_sub", INTEGER, DATE, INTEGER); //$NON-NLS-1$
	}
	
	@Override
    public SQLConversionVisitor getSQLConversionVisitor() {
    	return new HiveSQLConversionVisitor(this);
    }	
	
	@Override
    public boolean useAnsiJoin() {
    	return true;
    }	
	
    @Override
    public boolean supportsBetweenCriteria() {
        return false;
    }

    @Override
    public boolean supportsCaseExpressions() {
    	//https://issues.apache.org/jira/browse/HIVE-164
        return true;
    }

    @Override
    public boolean supportsCorrelatedSubqueries() {
    	//https://issues.apache.org/jira/browse/HIVE-784
        return false;
    }

    @Override
    public boolean supportsExistsCriteria() {
        return false;
    }

    @Override
    public boolean supportsInCriteriaSubquery() {
    	// the website documents a way to semi-join to re-write this but did not handle NOT IN case.
        return false;
    }

    @Override
    public boolean supportsLikeCriteriaEscapeCharacter() {
        return false;
    }

    @Override
    public boolean supportsQuantifiedCompareCriteriaAll() {
        return false;
    }

    @Override
    public boolean supportsQuantifiedCompareCriteriaSome() {
        return false;
    }
    
    @Override
    public boolean supportsBulkUpdate() {
    	return false;
    }
    
    @Override
    public boolean supportsBatchedUpdates() {
    	return false;
    }
    
    @Override
    public List<?> translateCommand(Command command, ExecutionContext context) {
    	return null;
    }

    @Override
    public List<?> translateLimit(Limit limit, ExecutionContext context) {
    	return null;
    }
       
    @Override
    public boolean addSourceComment() {
        return false;
    }   
    
    @Override
    public boolean useAsInGroupAlias(){
        return false;
    }
    
    public boolean hasTimeType() {
    	return false;
    }

	public String getLikeRegexString() {
		return "REGEXP"; //$NON-NLS-1$
	}
	
    @Override
    public boolean supportsScalarSubqueries() {
    	// Supported only in FROM clause
        return false;
    }    
    
    @Override
    public boolean supportsInlineViews() {
    	// must be aliased.
        return true;
    }      
    
    @Override
    public boolean supportsUnions() {
        return true;
        // only union all in subquery
    }    
    
    @Override
    public boolean supportsInsertWithQueryExpression() {
    	return false; // insert seems to be only with overwrite always
    }    
    
    @Override
    public boolean supportsIntersect() {
    	return false;
    }

    @Override
    public boolean supportsExcept() {
    	return false;
    }      

    @Override
    public boolean supportsAggregatesEnhancedNumeric() {
    	return true;
    }
    
    @Override
    public boolean supportsCommonTableExpressions() {
    	return false;
    } 
    
    @Override
    public String translateLiteralBoolean(Boolean booleanValue) {
        if(booleanValue.booleanValue()) {
            return "true"; //$NON-NLS-1$
        }
        return "false"; //$NON-NLS-1$
    }    
    
    @Override
    public String translateLiteralDate(java.sql.Date dateValue) {
        return formatDateValue(dateValue);
    }    
    
    @Override
    public String translateLiteralTime(Time timeValue) {
    	if (!hasTimeType()) {
    		return translateLiteralTimestamp(new Timestamp(timeValue.getTime())); 
    	}
        return formatDateValue(timeValue);
    } 
    
    @Override
    public String translateLiteralTimestamp(Timestamp timestampValue) {
        return formatDateValue(timestampValue);
    }
    
    @Override
    public List<String> getSupportedFunctions() {
        List<String> supportedFunctions = new ArrayList<String>();
        supportedFunctions.addAll(super.getSupportedFunctions());

        supportedFunctions.add(SourceSystemFunctions.ABS);
        supportedFunctions.add(SourceSystemFunctions.ACOS);
        supportedFunctions.add(SourceSystemFunctions.ARRAY_GET);
        supportedFunctions.add(SourceSystemFunctions.ASIN);
        supportedFunctions.add(SourceSystemFunctions.ASCII);
        supportedFunctions.add(SourceSystemFunctions.ATAN);
        supportedFunctions.add(SourceSystemFunctions.BITAND);
        supportedFunctions.add(SourceSystemFunctions.BITNOT);
        supportedFunctions.add(SourceSystemFunctions.BITOR);
        supportedFunctions.add(SourceSystemFunctions.BITXOR);
        supportedFunctions.add(SourceSystemFunctions.CEILING);
        supportedFunctions.add(SourceSystemFunctions.COALESCE);
        supportedFunctions.add(SourceSystemFunctions.CONCAT);
        supportedFunctions.add(SourceSystemFunctions.COS);
        supportedFunctions.add(SourceSystemFunctions.CONVERT);
		supportedFunctions.add(SourceSystemFunctions.CURDATE); 
		supportedFunctions.add(SourceSystemFunctions.CURTIME);
		supportedFunctions.add(SourceSystemFunctions.DEGREES);
		supportedFunctions.add(SourceSystemFunctions.DAYOFMONTH);
        supportedFunctions.add(SourceSystemFunctions.EXP);
        supportedFunctions.add(SourceSystemFunctions.FLOOR);
        supportedFunctions.add(SourceSystemFunctions.HOUR);
        supportedFunctions.add(SourceSystemFunctions.IFNULL);
        supportedFunctions.add(SourceSystemFunctions.LCASE);
        supportedFunctions.add(SourceSystemFunctions.LOCATE);
        supportedFunctions.add(SourceSystemFunctions.LPAD);
        supportedFunctions.add(SourceSystemFunctions.LENGTH);
        supportedFunctions.add(SourceSystemFunctions.LTRIM);
        supportedFunctions.add(SourceSystemFunctions.LOG);
        supportedFunctions.add(SourceSystemFunctions.LOG10);
        supportedFunctions.add(SourceSystemFunctions.MINUTE);
        supportedFunctions.add(SourceSystemFunctions.MOD);
        supportedFunctions.add(SourceSystemFunctions.POWER);
        supportedFunctions.add(SourceSystemFunctions.SECOND);
        supportedFunctions.add(SourceSystemFunctions.SQRT);
        supportedFunctions.add(SourceSystemFunctions.RAND);
        supportedFunctions.add(SourceSystemFunctions.ROUND);
        supportedFunctions.add(SourceSystemFunctions.RTRIM);
        supportedFunctions.add(SourceSystemFunctions.RPAD);
        supportedFunctions.add(SourceSystemFunctions.MONTH);
        supportedFunctions.add(SourceSystemFunctions.PI);
        supportedFunctions.add(SourceSystemFunctions.SIN);
        supportedFunctions.add(SourceSystemFunctions.SUBSTRING);
        supportedFunctions.add(SourceSystemFunctions.TRIM);
        supportedFunctions.add(SourceSystemFunctions.UCASE);
        supportedFunctions.add(SourceSystemFunctions.YEAR);
        return supportedFunctions;
    }    
    
	@Override
	public void getMetadata(MetadataFactory metadataFactory, Connection conn) throws TranslatorException {
		try {
			HiveMetadataProcessor metadataProcessor = new HiveMetadataProcessor();
			PropertiesUtils.setBeanProperties(metadataProcessor, metadataFactory.getImportProperties(), "importer"); //$NON-NLS-1$
			metadataProcessor.getConnectorMetadata(conn, metadataFactory);
		} catch (SQLException e) {
			throw new TranslatorException(e);
		}
	}    
    
}
