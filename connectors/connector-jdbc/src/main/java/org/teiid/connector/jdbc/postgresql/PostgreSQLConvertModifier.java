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

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Arrays;
import java.util.List;

import org.teiid.connector.jdbc.translator.BasicFunctionModifier;
import org.teiid.connector.jdbc.translator.DropFunctionModifier;
import org.teiid.connector.jdbc.translator.FunctionModifier;
import org.teiid.connector.language.IExpression;
import org.teiid.connector.language.IFunction;
import org.teiid.connector.language.ILanguageFactory;
import org.teiid.connector.language.ILiteral;
import org.teiid.connector.language.ICompareCriteria.Operator;


/**
 */
class PostgreSQLConvertModifier extends BasicFunctionModifier implements FunctionModifier {     
    private static DropFunctionModifier DROP_MODIFIER = new DropFunctionModifier();
    private ILanguageFactory langFactory;
    
    PostgreSQLConvertModifier(ILanguageFactory langFactory) {
        this.langFactory = langFactory;
    }

    public IExpression modify(IFunction function) {
        List<IExpression> args = function.getParameters();

        if (args.get(0) != null && args.get(0) instanceof ILiteral && ((ILiteral)args.get(0)).getValue() == null ) {
            if (args.get(1) != null && args.get(1) instanceof ILiteral) {
                // This is a convert(null, ...) or cast(null as ...)
                return DROP_MODIFIER.modify(function);
            }
        } 
        
        if (args.get(1) != null && args.get(1) instanceof ILiteral) {
            String target = ((String)((ILiteral)args.get(1)).getValue()).toLowerCase();
            if (target.equals("string")) {  //$NON-NLS-1$ 
                return convertToString(function);
            } else if (target.equals("short")) {  //$NON-NLS-1$ 
                return createCastFunction(args.get(0), "smallint", Short.class); //$NON-NLS-1$
            } else if (target.equals("integer")) { //$NON-NLS-1$ 
                return createCastFunction(args.get(0), "integer", Integer.class); //$NON-NLS-1$
            } else if (target.equals("long")) { //$NON-NLS-1$ 
                return createCastFunction(args.get(0), "bigint", Long.class); //$NON-NLS-1$
            } else if (target.equals("biginteger")) { //$NON-NLS-1$ 
                return createCastFunction(args.get(0), "numeric", BigInteger.class); //$NON-NLS-1$
            } else if (target.equals("float")) { //$NON-NLS-1$ 
                return createCastFunction(args.get(0), "real", Float.class); //$NON-NLS-1$
            } else if (target.equals("double")) { //$NON-NLS-1$ 
                return createCastFunction(args.get(0), "float8", Double.class); //$NON-NLS-1$
            } else if (target.equals("bigdecimal")) { //$NON-NLS-1$ 
                return createCastFunction(args.get(0), "decimal", BigDecimal.class); //$NON-NLS-1$
            } else if (target.equals("date")) { //$NON-NLS-1$ 
                return convertToDate(function);
            } else if (target.equals("time")) { //$NON-NLS-1$ 
                return convertToTime(function);
            } else if (target.equals("timestamp")) { //$NON-NLS-1$ 
                return convertToTimestamp(function);
            } else if (target.equals("char")) { //$NON-NLS-1$ 
                return createCastFunction(args.get(0), "varchar", String.class); //$NON-NLS-1$
            } else if (target.equals("boolean")) {  //$NON-NLS-1$ 
                return createCastFunction(args.get(0), "boolean", Boolean.class); //$NON-NLS-1$
            } else if (target.equals("byte")) {  //$NON-NLS-1$ 
                return createCastFunction(args.get(0), "smallint", Byte.class); //$NON-NLS-1$
            }
        }
        return DROP_MODIFIER.modify(function); 
    }
    
    private IExpression convertToDate(IFunction function) {
        List<IExpression> args = function.getParameters();
        int srcCode = getSrcCode(function);

        switch(srcCode) {
            case STRING:
                return createConversionFunction("to_date", args.get(0), "YYYY-MM-DD", java.sql.Date.class); //$NON-NLS-1$//$NON-NLS-2$
            case TIMESTAMP:
                return createCastFunction(args.get(0), "date", java.sql.Date.class); //$NON-NLS-1$
            default:
                return DROP_MODIFIER.modify(function);
        }
    }

    private IExpression convertToTime(IFunction function) {
        List<IExpression> args = function.getParameters();
        
        int srcCode = getSrcCode(function);
        switch(srcCode) {
            case STRING:
                //convert(STRING, time) --> to_timestamp('1970-01-01 ' || timevalue, 'YYYY-MM-DD HH24:MI:SS')
                IExpression prependedPart0 = langFactory.createFunction("||",  //$NON-NLS-1$
                                                                          Arrays.asList(langFactory.createLiteral("1970-01-01 ", String.class), args.get(0)),  //$NON-NLS-1$
                                                                          String.class);    
                    
                return createConversionFunction("to_timestamp", prependedPart0, "YYYY-MM-DD HH24:MI:SS", java.sql.Time.class); //$NON-NLS-1$ //$NON-NLS-2$
            case TIMESTAMP:
                return createCastFunction(args.get(0), "time", java.sql.Time.class); //$NON-NLS-1$
            default:
                return DROP_MODIFIER.modify(function);
        }
    }    
    
    /**
     * This works only for Oracle 9i.
     * @param src
     * @return IFunction
     */
    private IExpression convertToTimestamp(IFunction function) {
        List<IExpression> args = function.getParameters();
        int srcCode = getSrcCode(function);
        switch(srcCode) {
            case STRING:
                // convert(STRING, timestamp) --> to_date(timestampvalue, 'YYYY-MM-DD HH24:MI:SS'))) from smalla 
                return createConversionFunction("to_timestamp", args.get(0), "YYYY-MM-DD HH24:MI:SS.UF", java.sql.Timestamp.class); //$NON-NLS-1$ //$NON-NLS-2$
            case TIME:
            case DATE:
                // convert(DATE, timestamp) --> to_date(to_char(DATE, 'YYYY-MM-DD HH24:MI:SS'), 'YYYY-MM-DD HH24:MI:SS')
                IFunction inner = createStringFunction(args.get(0), "YYYY-MM-DD HH24:MI:SS");  //$NON-NLS-1$
                        
                return createConversionFunction("to_timestamp", inner, "YYYY-MM-DD HH24:MI:SS", java.sql.Timestamp.class); //$NON-NLS-1$ //$NON-NLS-2$
            default:
                return DROP_MODIFIER.modify(function);
        }
    }
    
    private IExpression convertToString(IFunction function) {
        List<IExpression> args = function.getParameters();

        int srcCode = getSrcCode(function);
        switch(srcCode) { 
            case BOOLEAN:
                // convert(booleanSrc, string) --> CASE WHEN booleanSrc THEN '1' ELSE '0' END
                List when = Arrays.asList(langFactory.createCompareCriteria(Operator.EQ, function.getParameters().get(0), langFactory.createLiteral(Boolean.TRUE, Boolean.class)));
                List then = Arrays.asList(langFactory.createLiteral("1", String.class)); //$NON-NLS-1$
                IExpression elseExpr = langFactory.createLiteral("0", String.class); //$NON-NLS-1$
                return langFactory.createSearchedCaseExpression(when, then, elseExpr, String.class);
            case BYTE:
            case SHORT:
            case INTEGER:
            case LONG:
            case BIGINTEGER:
            case FLOAT:
            case DOUBLE:
            case BIGDECIMAL:
                // convert(src, string) --> cast (src AS varchar)
                return createCastFunction(args.get(0), "varchar", String.class); //$NON-NLS-1$
            // convert(input, string) --> to_char(input, format)
            case DATE:
                return createStringFunction(args.get(0), "YYYY-MM-DD"); //$NON-NLS-1$
            case TIME:
                return createStringFunction(args.get(0), "HH24:MI:SS"); //$NON-NLS-1$
            case TIMESTAMP:
                return createStringFunction(args.get(0), "YYYY-MM-DD HH24:MI:SS.US"); //$NON-NLS-1$
            default:
                return DROP_MODIFIER.modify(function);
        }
    }

    private IFunction createStringFunction(IExpression args0, String format) {
        return createConversionFunction("to_char", args0, format, String.class); //$NON-NLS-1$           
    }
    
    private IFunction createCastFunction(IExpression value, String typeName, Class targetClass) {
        return createConversionFunction("cast", value, typeName, targetClass); //$NON-NLS-1$
    }

    private IFunction createConversionFunction(String functionName, IExpression value, String target, Class targetClass) {
        return langFactory.createFunction(functionName, Arrays.asList(value, langFactory.createLiteral(target, String.class)), targetClass);
    }
    
    private int getSrcCode(IFunction function) {
        List<IExpression> args = function.getParameters();
        Class srcType = args.get(0).getType();
        return ((Integer) typeMap.get(srcType)).intValue();
    }
}
