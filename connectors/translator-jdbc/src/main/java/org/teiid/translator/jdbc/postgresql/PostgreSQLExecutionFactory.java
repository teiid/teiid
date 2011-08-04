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

package org.teiid.translator.jdbc.postgresql;

import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.teiid.language.AggregateFunction;
import org.teiid.language.Expression;
import org.teiid.language.Function;
import org.teiid.language.LanguageObject;
import org.teiid.language.Limit;
import org.teiid.language.Literal;
import org.teiid.language.SQLConstants.NonReserved;
import org.teiid.translator.ExecutionContext;
import org.teiid.translator.SourceSystemFunctions;
import org.teiid.translator.Translator;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.TypeFacility;
import org.teiid.translator.jdbc.AliasModifier;
import org.teiid.translator.jdbc.ConvertModifier;
import org.teiid.translator.jdbc.EscapeSyntaxModifier;
import org.teiid.translator.jdbc.ExtractFunctionModifier;
import org.teiid.translator.jdbc.FunctionModifier;
import org.teiid.translator.jdbc.JDBCExecutionFactory;
import org.teiid.translator.jdbc.ModFunctionModifier;
import org.teiid.translator.jdbc.oracle.LeftOrRightFunctionModifier;
import org.teiid.translator.jdbc.oracle.MonthOrDayNameFunctionModifier;



/** 
 * Translator class for PostgreSQL.  Updated to expect a 8.0+ jdbc client
 * @since 4.3
 */
@Translator(name="postgresql", description="A translator for postgreSQL Database")
public class PostgreSQLExecutionFactory extends JDBCExecutionFactory {
	
	public static final String EIGHT_0 = "8.0"; //$NON-NLS-1$
	public static final String EIGHT_1 = "8.1"; //$NON-NLS-1$
	public static final String EIGHT_2 = "8.2"; //$NON-NLS-1$
	public static final String EIGHT_3 = "8.3"; //$NON-NLS-1$
	public static final String EIGHT_4 = "8.4"; //$NON-NLS-1$
    
	public PostgreSQLExecutionFactory() {
		setDatabaseVersion(EIGHT_0);
	}
	
    public void start() throws TranslatorException {
        //TODO: all of the functions (except for convert) can be handled through just the escape syntax
        super.start();
        
        registerFunctionModifier(SourceSystemFunctions.LOG, new AliasModifier("ln")); //$NON-NLS-1$ 
        registerFunctionModifier(SourceSystemFunctions.LOG10, new AliasModifier("log")); //$NON-NLS-1$ 
        
        registerFunctionModifier(SourceSystemFunctions.BITAND, new AliasModifier("&")); //$NON-NLS-1$ 
        registerFunctionModifier(SourceSystemFunctions.BITNOT, new AliasModifier("~")); //$NON-NLS-1$ 
        registerFunctionModifier(SourceSystemFunctions.BITOR, new AliasModifier("|")); //$NON-NLS-1$ 
        registerFunctionModifier(SourceSystemFunctions.BITXOR, new AliasModifier("#")); //$NON-NLS-1$ 
        
        registerFunctionModifier(SourceSystemFunctions.CHAR, new AliasModifier("chr")); //$NON-NLS-1$ 
        registerFunctionModifier(SourceSystemFunctions.CONCAT, new AliasModifier("||")); //$NON-NLS-1$ 
        registerFunctionModifier(SourceSystemFunctions.LCASE, new AliasModifier("lower")); //$NON-NLS-1$ 
        registerFunctionModifier(SourceSystemFunctions.LEFT, new LeftOrRightFunctionModifier(getLanguageFactory()));
        registerFunctionModifier(SourceSystemFunctions.RIGHT, new LeftOrRightFunctionModifier(getLanguageFactory()));
        registerFunctionModifier(SourceSystemFunctions.SUBSTRING, new AliasModifier("substr")); //$NON-NLS-1$ 
        registerFunctionModifier(SourceSystemFunctions.UCASE, new AliasModifier("upper")); //$NON-NLS-1$ 
        
        registerFunctionModifier(SourceSystemFunctions.DAYNAME, new MonthOrDayNameFunctionModifier(getLanguageFactory(), "Day"));//$NON-NLS-1$ 
        registerFunctionModifier(SourceSystemFunctions.DAYOFWEEK, new ExtractFunctionModifier()); 
        registerFunctionModifier(SourceSystemFunctions.DAYOFMONTH, new ExtractFunctionModifier()); 
        registerFunctionModifier(SourceSystemFunctions.DAYOFYEAR, new ExtractFunctionModifier()); 
        registerFunctionModifier(SourceSystemFunctions.HOUR, new ExtractFunctionModifier()); 
        registerFunctionModifier(SourceSystemFunctions.MINUTE, new ExtractFunctionModifier()); 
        registerFunctionModifier(SourceSystemFunctions.MONTH, new ExtractFunctionModifier()); 
        registerFunctionModifier(SourceSystemFunctions.MONTHNAME, new MonthOrDayNameFunctionModifier(getLanguageFactory(), "Month"));//$NON-NLS-1$ 
        registerFunctionModifier(SourceSystemFunctions.QUARTER, new ExtractFunctionModifier()); 
        registerFunctionModifier(SourceSystemFunctions.SECOND, new ExtractFunctionModifier()); 
        registerFunctionModifier(SourceSystemFunctions.WEEK, new ExtractFunctionModifier()); 
        registerFunctionModifier(SourceSystemFunctions.YEAR, new ExtractFunctionModifier()); 
        registerFunctionModifier(SourceSystemFunctions.LOCATE, new LocateFunctionModifier(getLanguageFactory()));
        registerFunctionModifier(SourceSystemFunctions.IFNULL, new AliasModifier("coalesce")); //$NON-NLS-1$
        
        registerFunctionModifier(SourceSystemFunctions.MOD, new ModFunctionModifier("%", getLanguageFactory(), Arrays.asList(TypeFacility.RUNTIME_TYPES.BIG_INTEGER, TypeFacility.RUNTIME_TYPES.BIG_DECIMAL))); //$NON-NLS-1$ 

        //specific to 8.2 client or later
        registerFunctionModifier(SourceSystemFunctions.TIMESTAMPADD, new EscapeSyntaxModifier());
        registerFunctionModifier(SourceSystemFunctions.TIMESTAMPDIFF, new EscapeSyntaxModifier());
        
        registerFunctionModifier(SourceSystemFunctions.ARRAY_GET, new FunctionModifier() {
			
			@Override
			public List<?> translate(Function function) {
				return Arrays.asList(function.getParameters().get(0), '[', function.getParameters().get(1), ']');
			}
		});
        registerFunctionModifier(SourceSystemFunctions.ARRAY_LENGTH, new FunctionModifier() {
			
			@Override
			public List<?> translate(Function function) {
				if (function.getParameters().size() == 1) {
					function.getParameters().add(new Literal(1, TypeFacility.RUNTIME_TYPES.INTEGER));
				}
				return null;
			}
		});
                
        //add in type conversion
        ConvertModifier convertModifier = new ConvertModifier();
        convertModifier.addTypeMapping("boolean", FunctionModifier.BOOLEAN); //$NON-NLS-1$
    	convertModifier.addTypeMapping("smallint", FunctionModifier.BYTE, FunctionModifier.SHORT); //$NON-NLS-1$
    	convertModifier.addTypeMapping("integer", FunctionModifier.INTEGER); //$NON-NLS-1$
    	convertModifier.addTypeMapping("bigint", FunctionModifier.LONG); //$NON-NLS-1$
    	convertModifier.addTypeMapping("real", FunctionModifier.FLOAT); //$NON-NLS-1$
    	convertModifier.addTypeMapping("float8", FunctionModifier.DOUBLE); //$NON-NLS-1$
    	convertModifier.addTypeMapping("numeric(38)", FunctionModifier.BIGINTEGER); //$NON-NLS-1$
    	convertModifier.addTypeMapping("decimal", FunctionModifier.BIGDECIMAL); //$NON-NLS-1$
    	convertModifier.addTypeMapping("char(1)", FunctionModifier.CHAR); //$NON-NLS-1$
    	convertModifier.addTypeMapping("varchar(4000)", FunctionModifier.STRING); //$NON-NLS-1$
    	convertModifier.addTypeMapping("date", FunctionModifier.DATE); //$NON-NLS-1$
    	convertModifier.addTypeMapping("time", FunctionModifier.TIME); //$NON-NLS-1$
    	convertModifier.addTypeMapping("timestamp", FunctionModifier.TIMESTAMP); //$NON-NLS-1$
    	convertModifier.addConvert(FunctionModifier.TIME, FunctionModifier.TIMESTAMP, new FunctionModifier() {
			@Override
			public List<?> translate(Function function) {
				return Arrays.asList(function.getParameters().get(0), " + TIMESTAMP '1970-01-01'"); //$NON-NLS-1$
			}
		});
    	convertModifier.addConvert(FunctionModifier.TIMESTAMP, FunctionModifier.TIME, new FunctionModifier() {
			@Override
			public List<?> translate(Function function) {
				return Arrays.asList("cast(date_trunc('second', ", function.getParameters().get(0), ") AS time)"); //$NON-NLS-1$ //$NON-NLS-2$
			}
		});
    	convertModifier.addConvert(FunctionModifier.DATE, FunctionModifier.STRING, new ConvertModifier.FormatModifier("to_char", "YYYY-MM-DD")); //$NON-NLS-1$ //$NON-NLS-2$
    	convertModifier.addConvert(FunctionModifier.TIME, FunctionModifier.STRING, new ConvertModifier.FormatModifier("to_char", "HH24:MI:SS")); //$NON-NLS-1$ //$NON-NLS-2$
    	convertModifier.addConvert(FunctionModifier.TIMESTAMP, FunctionModifier.STRING, new ConvertModifier.FormatModifier("to_char", "YYYY-MM-DD HH24:MI:SS.US")); //$NON-NLS-1$ //$NON-NLS-2$
    	convertModifier.addConvert(FunctionModifier.BOOLEAN, FunctionModifier.STRING, new FunctionModifier() {
			@Override
			public List<?> translate(Function function) {
				Expression stringValue = function.getParameters().get(0);
				return Arrays.asList("CASE WHEN ", stringValue, " THEN 'true' WHEN not(", stringValue, ") THEN 'false' END"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
			}
		});
    	convertModifier.addSourceConversion(new FunctionModifier() {
			@Override
			public List<?> translate(Function function) {
				((Literal)function.getParameters().get(1)).setValue("integer"); //$NON-NLS-1$
				return null;
			}
		}, FunctionModifier.BOOLEAN);
    	registerFunctionModifier(SourceSystemFunctions.CONVERT, convertModifier); 
    }    
    
    @Override
    public String translateLiteralBoolean(Boolean booleanValue) {
        if(booleanValue.booleanValue()) {
            return "TRUE"; //$NON-NLS-1$
        }
        return "FALSE"; //$NON-NLS-1$
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
    public int getTimestampNanoPrecision() {
    	return 6;
    }
    
    @SuppressWarnings("unchecked")
	@Override
    public List<?> translateLimit(Limit limit, ExecutionContext context) {
    	if (limit.getRowOffset() > 0) {
    		return Arrays.asList("LIMIT ", limit.getRowLimit(), " OFFSET ", limit.getRowOffset()); //$NON-NLS-1$ //$NON-NLS-2$ 
    	}
        return null;
    }

    /**
     * Postgres doesn't provide min/max(boolean), so this conversion writes a min(BooleanValue) as 
     * bool_and(BooleanValue)
     * @see org.teiid.language.visitor.LanguageObjectVisitor#visit(org.teiid.language.AggregateFunction)
     * @since 4.3
     */
    @Override
    public List<?> translate(LanguageObject obj, ExecutionContext context) {
    	if (obj instanceof AggregateFunction) {
    		AggregateFunction agg = (AggregateFunction)obj;
    		if (agg.getExpression() != null && TypeFacility.RUNTIME_TYPES.BOOLEAN.equals(agg.getExpression().getType())) {
            	if (agg.getName().equalsIgnoreCase(NonReserved.MIN)) {
            		agg.setName("bool_and"); //$NON-NLS-1$
            	} else if (agg.getName().equalsIgnoreCase(NonReserved.MAX)) {
            		agg.setName("bool_or"); //$NON-NLS-1$
            	}
            }
    	}
    	return super.translate(obj, context);
    }
    
    @Override
    public NullOrder getDefaultNullOrder() {
    	return NullOrder.HIGH;
    }
    
    @Override
    public boolean supportsOrderByNullOrdering() {
    	return getDatabaseVersion().compareTo(EIGHT_4) >= 0;
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
        supportedFunctions.add("BITAND"); //$NON-NLS-1$
        supportedFunctions.add("BITNOT"); //$NON-NLS-1$
        supportedFunctions.add("BITOR"); //$NON-NLS-1$
        supportedFunctions.add("BITXOR"); //$NON-NLS-1$
        supportedFunctions.add("CEILING"); //$NON-NLS-1$
        supportedFunctions.add("COS"); //$NON-NLS-1$
        supportedFunctions.add("COT"); //$NON-NLS-1$
        supportedFunctions.add("DEGREES"); //$NON-NLS-1$
        supportedFunctions.add("EXP"); //$NON-NLS-1$
        supportedFunctions.add("FLOOR"); //$NON-NLS-1$
        // These should not be pushed down since the grammar for string conversion is different
//        supportedFunctions.add("FORMATBIGDECIMAL"); //$NON-NLS-1$
//        supportedFunctions.add("FORMATBIGINTEGER"); //$NON-NLS-1$
//        supportedFunctions.add("FORMATDOUBLE"); //$NON-NLS-1$
//        supportedFunctions.add("FORMATFLOAT"); //$NON-NLS-1$
//        supportedFunctions.add("FORMATINTEGER"); //$NON-NLS-1$
//        supportedFunctions.add("FORMATLONG"); //$NON-NLS-1$
        supportedFunctions.add("LOG"); //$NON-NLS-1$
        supportedFunctions.add("LOG10"); //$NON-NLS-1$
        supportedFunctions.add("MOD"); //$NON-NLS-1$
        supportedFunctions.add("PI"); //$NON-NLS-1$
        supportedFunctions.add("POWER"); //$NON-NLS-1$
        supportedFunctions.add("RADIANS"); //$NON-NLS-1$
        supportedFunctions.add("ROUND"); //$NON-NLS-1$
        supportedFunctions.add("SIGN"); //$NON-NLS-1$
        supportedFunctions.add("SIN"); //$NON-NLS-1$
        supportedFunctions.add("SQRT"); //$NON-NLS-1$
        supportedFunctions.add("TAN"); //$NON-NLS-1$
        
        supportedFunctions.add("ASCII"); //$NON-NLS-1$
        supportedFunctions.add("CHR"); //$NON-NLS-1$
        supportedFunctions.add("CHAR"); //$NON-NLS-1$
        supportedFunctions.add("||"); //$NON-NLS-1$
        supportedFunctions.add("CONCAT"); //$NON-NLS-1$
        supportedFunctions.add("INITCAP"); //$NON-NLS-1$
        supportedFunctions.add("LCASE"); //$NON-NLS-1$
        supportedFunctions.add("LEFT"); //$NON-NLS-1$
        supportedFunctions.add("LENGTH"); //$NON-NLS-1$
        supportedFunctions.add("LOCATE"); //$NON-NLS-1$
        supportedFunctions.add("LOWER"); //$NON-NLS-1$
        supportedFunctions.add("LPAD"); //$NON-NLS-1$
        supportedFunctions.add("LTRIM"); //$NON-NLS-1$
        supportedFunctions.add("REPEAT"); //$NON-NLS-1$
        supportedFunctions.add("REPLACE"); //$NON-NLS-1$
        supportedFunctions.add("RIGHT"); //$NON-NLS-1$
        supportedFunctions.add("RPAD"); //$NON-NLS-1$
        supportedFunctions.add("RTRIM"); //$NON-NLS-1$
        supportedFunctions.add("SUBSTRING"); //$NON-NLS-1$
        supportedFunctions.add(SourceSystemFunctions.TRIM);
        supportedFunctions.add("UCASE"); //$NON-NLS-1$
        supportedFunctions.add("UPPER"); //$NON-NLS-1$
        
        // These are executed within the server and never pushed down
//        supportedFunctions.add("CURDATE"); //$NON-NLS-1$
//        supportedFunctions.add("CURTIME"); //$NON-NLS-1$
//        supportedFunctions.add("NOW"); //$NON-NLS-1$
        supportedFunctions.add("DAYNAME"); //$NON-NLS-1$
        supportedFunctions.add("DAYOFMONTH"); //$NON-NLS-1$
        supportedFunctions.add("DAYOFWEEK"); //$NON-NLS-1$
        supportedFunctions.add("DAYOFYEAR"); //$NON-NLS-1$
        // These should not be pushed down since the grammar for string conversion is different
//        supportedFunctions.add("FORMATDATE"); //$NON-NLS-1$
//        supportedFunctions.add("FORMATTIME"); //$NON-NLS-1$
//        supportedFunctions.add("FORMATTIMESTAMP"); //$NON-NLS-1$
        supportedFunctions.add("HOUR"); //$NON-NLS-1$
        supportedFunctions.add("MINUTE"); //$NON-NLS-1$
        supportedFunctions.add("MONTH"); //$NON-NLS-1$
        supportedFunctions.add("MONTHNAME"); //$NON-NLS-1$
        // These should not be pushed down since the grammar for string conversion is different
//        supportedFunctions.add("PARSEDATE"); //$NON-NLS-1$
//        supportedFunctions.add("PARSETIME"); //$NON-NLS-1$
//        supportedFunctions.add("PARSETIMESTAMP"); //$NON-NLS-1$
        supportedFunctions.add("QUARTER"); //$NON-NLS-1$
        supportedFunctions.add("SECOND"); //$NON-NLS-1$
        if (this.getDatabaseVersion().compareTo(EIGHT_2) >= 0) {
        	supportedFunctions.add("TIMESTAMPADD"); //$NON-NLS-1$
        	supportedFunctions.add("TIMESTAMPDIFF"); //$NON-NLS-1$
        }
        supportedFunctions.add("WEEK"); //$NON-NLS-1$
        supportedFunctions.add("YEAR"); //$NON-NLS-1$
        
        supportedFunctions.add("CAST"); //$NON-NLS-1$
        supportedFunctions.add("CONVERT"); //$NON-NLS-1$
        supportedFunctions.add("IFNULL"); //$NON-NLS-1$
        supportedFunctions.add("NVL"); //$NON-NLS-1$
        
        // Additional functions
//        // Math
//        supportedFunctions.add("%"); //$NON-NLS-1$
//        supportedFunctions.add("^"); //$NON-NLS-1$
//        supportedFunctions.add("|/"); //$NON-NLS-1$
//        supportedFunctions.add("||/"); //$NON-NLS-1$
//        supportedFunctions.add("!"); //$NON-NLS-1$
//        supportedFunctions.add("!!"); //$NON-NLS-1$
//        supportedFunctions.add("@"); //$NON-NLS-1$
//          // Bit manipulation
//        supportedFunctions.add("&"); //$NON-NLS-1$
//        supportedFunctions.add("|"); //$NON-NLS-1$
//        supportedFunctions.add("#"); //$NON-NLS-1$
//        supportedFunctions.add("~"); //$NON-NLS-1$
//        supportedFunctions.add("<<"); //$NON-NLS-1$
//        supportedFunctions.add(">>"); //$NON-NLS-1$
//        
//        supportedFunctions.add("CBRT"); //$NON-NLS-1$
//        supportedFunctions.add("CEIL"); //$NON-NLS-1$
//        supportedFunctions.add("LN"); //$NON-NLS-1$
//        supportedFunctions.add("MOD"); //$NON-NLS-1$
//        supportedFunctions.add("RANDOM"); //$NON-NLS-1$
//        supportedFunctions.add("SETSEED"); //$NON-NLS-1$
//        supportedFunctions.add("TRUNC"); //$NON-NLS-1$
//        supportedFunctions.add("WIDTH_BUCKET"); //$NON-NLS-1$
//        
//        // String
//        supportedFunctions.add("BIT_LENGTH"); //$NON-NLS-1$
//        supportedFunctions.add("BTRIM"); //$NON-NLS-1$
//        supportedFunctions.add("CHAR_LENGTH"); //$NON-NLS-1$
//        supportedFunctions.add("CHARACTER_LENGTH"); //$NON-NLS-1$
//        supportedFunctions.add("DECODE"); //$NON-NLS-1$
//        supportedFunctions.add("ENCODE"); //$NON-NLS-1$
//        supportedFunctions.add("MD5"); //$NON-NLS-1$
//        supportedFunctions.add("OCTET_LENGTH"); //$NON-NLS-1$
//        supportedFunctions.add("PG_CLIENT_ENCODING"); //$NON-NLS-1$
//        supportedFunctions.add("QUOTE_IDENT"); //$NON-NLS-1$
//        supportedFunctions.add("QUOTE_LITERAL"); //$NON-NLS-1$
//        supportedFunctions.add("SPLIT_PART"); //$NON-NLS-1$
//        supportedFunctions.add("STRPOS"); //$NON-NLS-1$
//        supportedFunctions.add("SUBSTR"); //$NON-NLS-1$
//        supportedFunctions.add("TO_ASCII"); //$NON-NLS-1$
//        supportedFunctions.add("TO_HEX"); //$NON-NLS-1$
//        supportedFunctions.add("TRANSLATE"); //$NON-NLS-1$
//        
//        // Bit operations
//        supportedFunctions.add("GET_BIT"); //$NON-NLS-1$
//        supportedFunctions.add("GET_BYTE"); //$NON-NLS-1$
//        supportedFunctions.add("SET_BIT"); //$NON-NLS-1$
//        supportedFunctions.add("SET_BYTE"); //$NON-NLS-1$
//        
//        // Formatting
//        supportedFunctions.add("TO_CHAR"); //$NON-NLS-1$
//        supportedFunctions.add("TO_DATE"); //$NON-NLS-1$
//        supportedFunctions.add("TO_TIMESTAMP"); //$NON-NLS-1$
//        supportedFunctions.add("TO_NUMBER"); //$NON-NLS-1$
//        
//        // Date / Time
//        supportedFunctions.add("AGE"); //$NON-NLS-1$
//        supportedFunctions.add("CURRENT_DATE"); //$NON-NLS-1$            // no ()
//        supportedFunctions.add("CURRENT_TIME"); //$NON-NLS-1$            // no ()
//        supportedFunctions.add("CURRENT_TIMESTAMP"); //$NON-NLS-1$       // no ()
//        supportedFunctions.add("DATE_PART"); //$NON-NLS-1$
//        supportedFunctions.add("DATE_TRUNC"); //$NON-NLS-1$
//        supportedFunctions.add("ISFINITE"); //$NON-NLS-1$
//        supportedFunctions.add("JUSTIFY_HOURS"); //$NON-NLS-1$
//        supportedFunctions.add("JUSTIFY_DAYS"); //$NON-NLS-1$
//        supportedFunctions.add("LOCALTIME"); //$NON-NLS-1$               // no ()
//        supportedFunctions.add("LOCALTIMESTAMP"); //$NON-NLS-1$          // no ()
//        supportedFunctions.add("TIMEOFDAY"); //$NON-NLS-1$
//        
//        // Conditional
          supportedFunctions.add("COALESCE"); //$NON-NLS-1$
//        supportedFunctions.add("NULLIF"); //$NON-NLS-1$
//        supportedFunctions.add("GREATEST"); //$NON-NLS-1$
//        supportedFunctions.add("LEAST"); //$NON-NLS-1$
//        
//        // Network Addresses
////        supportedFunctions.add("BROADCAST"); //$NON-NLS-1$
////        supportedFunctions.add("HOST"); //$NON-NLS-1$
////        supportedFunctions.add("MASKLEN"); //$NON-NLS-1$
////        supportedFunctions.add("SET_MASKLEN"); //$NON-NLS-1$
////        supportedFunctions.add("NETMASK"); //$NON-NLS-1$
////        supportedFunctions.add("HOSTMASK"); //$NON-NLS-1$
////        supportedFunctions.add("NETWORK"); //$NON-NLS-1$
////        supportedFunctions.add("TEXT"); //$NON-NLS-1$
////        supportedFunctions.add("ABBREV"); //$NON-NLS-1$
////        supportedFunctions.add("FAMILY"); //$NON-NLS-1$
////        supportedFunctions.add("TRUNC"); //$NON-NLS-1$
//        
//        // Set generator
//        supportedFunctions.add("GENERATE_SERIES"); //$NON-NLS-1$
//        
//        // Information
//        supportedFunctions.add("CURRENT_DATABASE"); //$NON-NLS-1$
//        supportedFunctions.add("CURRENT_SCHEMA"); //$NON-NLS-1$
//        supportedFunctions.add("CURRENT_SCHEMAS"); //$NON-NLS-1$
//        supportedFunctions.add("CURRENT_USER"); //$NON-NLS-1$           // no ()
//        supportedFunctions.add("INET_CLIENT_ADDR"); //$NON-NLS-1$
//        supportedFunctions.add("INET_CLIENT_PORT"); //$NON-NLS-1$
//        supportedFunctions.add("INET_SERVER_ADDR"); //$NON-NLS-1$
//        supportedFunctions.add("INET_SERVER_PORT"); //$NON-NLS-1$
//        supportedFunctions.add("SESSION_USER"); //$NON-NLS-1$           // no ()
//        supportedFunctions.add("USER"); //$NON-NLS-1$                   // no ()
//        supportedFunctions.add("VERSION"); //$NON-NLS-1$
//        
        supportedFunctions.add(SourceSystemFunctions.ARRAY_GET);
        supportedFunctions.add(SourceSystemFunctions.ARRAY_LENGTH);
        return supportedFunctions;
    }
    
    /** 
     * This is true only after Postgre version 7.1 
     * However, since version 7 was released in 2000 we'll assume a post 7 instance.
     */
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
    	return getDatabaseVersion().compareTo(EIGHT_2) >= 0;
    }
    
    @Override
    public boolean supportsCommonTableExpressions() {
    	return getDatabaseVersion().compareTo(EIGHT_4) >= 0;
    }
    
    @Override
    public boolean supportsArrayAgg() {
    	return getDatabaseVersion().compareTo(EIGHT_4) >= 0;
    }
    
    @Override
    public boolean supportsElementaryOlapOperations() {
    	return getDatabaseVersion().compareTo(EIGHT_4) >= 0;
    }
    
}
