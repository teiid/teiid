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

package org.teiid.connector.jdbc.postgresql;

import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.List;

import org.teiid.connector.api.ConnectorCapabilities;
import org.teiid.connector.api.ConnectorEnvironment;
import org.teiid.connector.api.ConnectorException;
import org.teiid.connector.api.ExecutionContext;
import org.teiid.connector.api.SourceSystemFunctions;
import org.teiid.connector.api.TypeFacility;
import org.teiid.connector.jdbc.oracle.LeftOrRightFunctionModifier;
import org.teiid.connector.jdbc.oracle.MonthOrDayNameFunctionModifier;
import org.teiid.connector.jdbc.translator.AliasModifier;
import org.teiid.connector.jdbc.translator.ConvertModifier;
import org.teiid.connector.jdbc.translator.EscapeSyntaxModifier;
import org.teiid.connector.jdbc.translator.ExtractFunctionModifier;
import org.teiid.connector.jdbc.translator.FunctionModifier;
import org.teiid.connector.jdbc.translator.ModFunctionModifier;
import org.teiid.connector.jdbc.translator.Translator;
import org.teiid.connector.language.IAggregate;
import org.teiid.connector.language.IExpression;
import org.teiid.connector.language.IFunction;
import org.teiid.connector.language.ILanguageObject;
import org.teiid.connector.language.ILimit;
import org.teiid.connector.language.ILiteral;
import org.teiid.connector.visitor.util.SQLReservedWords;



/** 
 * Translator class for PostgreSQL.  Updated to expect a 8.0+ jdbc client
 * @since 4.3
 */
public class PostgreSQLTranslator extends Translator {
	
	private String version = PostgreSQLCapabilities.EIGHT_0;

    public void initialize(ConnectorEnvironment env) throws ConnectorException {
        //TODO: all of the functions (except for convert) can be handled through just the escape syntax
        super.initialize(env);
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
			public List<?> translate(IFunction function) {
				return Arrays.asList(function.getParameters().get(0), " + TIMESTAMP '1970-01-01'"); //$NON-NLS-1$
			}
		});
    	convertModifier.addConvert(FunctionModifier.TIMESTAMP, FunctionModifier.TIME, new FunctionModifier() {
			@Override
			public List<?> translate(IFunction function) {
				return Arrays.asList("cast(date_trunc('second', ", function.getParameters().get(0), ") AS time)"); //$NON-NLS-1$ //$NON-NLS-2$
			}
		});
    	convertModifier.addConvert(FunctionModifier.DATE, FunctionModifier.STRING, new ConvertModifier.FormatModifier("to_char", "YYYY-MM-DD")); //$NON-NLS-1$ //$NON-NLS-2$
    	convertModifier.addConvert(FunctionModifier.TIME, FunctionModifier.STRING, new ConvertModifier.FormatModifier("to_char", "HH24:MI:SS")); //$NON-NLS-1$ //$NON-NLS-2$
    	convertModifier.addConvert(FunctionModifier.TIMESTAMP, FunctionModifier.STRING, new ConvertModifier.FormatModifier("to_char", "YYYY-MM-DD HH24:MI:SS.UF")); //$NON-NLS-1$ //$NON-NLS-2$
    	convertModifier.addConvert(FunctionModifier.BOOLEAN, FunctionModifier.STRING, new FunctionModifier() {
			@Override
			public List<?> translate(IFunction function) {
				IExpression stringValue = function.getParameters().get(0);
				return Arrays.asList("CASE WHEN ", stringValue, " THEN 'true' WHEN not(", stringValue, ") THEN 'false' END"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
			}
		});
    	convertModifier.addSourceConversion(new FunctionModifier() {
			@Override
			public List<?> translate(IFunction function) {
				((ILiteral)function.getParameters().get(1)).setValue("integer"); //$NON-NLS-1$
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
    public List<?> translateLimit(ILimit limit, ExecutionContext context) {
    	if (limit.getRowOffset() > 0) {
    		return Arrays.asList("LIMIT ", limit.getRowLimit(), " OFFSET ", limit.getRowOffset()); //$NON-NLS-1$ //$NON-NLS-2$ 
    	}
        return null;
    }

    /**
     * Postgres doesn't provide min/max(boolean), so this conversion writes a min(BooleanValue) as 
     * bool_and(BooleanValue)
     * @see org.teiid.connector.visitor.framework.LanguageObjectVisitor#visit(org.teiid.connector.language.IAggregate)
     * @since 4.3
     */
    @Override
    public List<?> translate(ILanguageObject obj, ExecutionContext context) {
    	if (obj instanceof IAggregate) {
    		IAggregate agg = (IAggregate)obj;
    		if (TypeFacility.RUNTIME_TYPES.BOOLEAN.equals(agg.getExpression().getType())) {
            	if (agg.getName().equalsIgnoreCase(SQLReservedWords.MIN)) {
            		agg.setName("bool_and"); //$NON-NLS-1$
            	} else if (agg.getName().equalsIgnoreCase(SQLReservedWords.MAX)) {
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
    public boolean supportsExplicitNullOrdering() {
    	return version.compareTo(PostgreSQLCapabilities.EIGHT_4) >= 0;
    }
    
    @Override
    public Class<? extends ConnectorCapabilities> getDefaultCapabilities() {
    	return PostgreSQLCapabilities.class;
    }

}
