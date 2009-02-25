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

package org.teiid.connector.jdbc.oracle;

import java.util.Arrays;
import java.util.List;

import org.teiid.connector.api.ConnectorLogger;
import org.teiid.connector.jdbc.translator.BasicFunctionModifier;
import org.teiid.connector.jdbc.translator.DropFunctionModifier;
import org.teiid.connector.jdbc.translator.FunctionModifier;
import org.teiid.connector.language.IExpression;
import org.teiid.connector.language.IFunction;
import org.teiid.connector.language.ILanguageFactory;
import org.teiid.connector.language.ILiteral;


/**
 */
public class OracleConvertModifier extends BasicFunctionModifier implements FunctionModifier {     
    private static DropFunctionModifier DROP_MODIFIER = new DropFunctionModifier();
    private ILanguageFactory langFactory;
    
    public OracleConvertModifier(ILanguageFactory langFactory, ConnectorLogger logger) {
        this.langFactory = langFactory;
    }

    public IExpression modify(IFunction function) {
        List<IExpression> args = function.getParameters();
        IExpression modified = null;

        String target = ((String)((ILiteral)args.get(1)).getValue()).toLowerCase();
        if (target.equals("string")) {  //$NON-NLS-1$ 
            modified = convertToString(function);
        } else if (target.equals("short")) {  //$NON-NLS-1$ 
            modified = convertToShort(function);
        } else if (target.equals("integer")) { //$NON-NLS-1$ 
            modified = convertToInteger(function);
        } else if (target.equals("long")) { //$NON-NLS-1$ 
            modified = convertToLong(function);
        } else if (target.equals("biginteger")) { //$NON-NLS-1$ 
            modified = convertToBigInteger(function);
        } else if (target.equals("float")) { //$NON-NLS-1$ 
            modified = convertToFloat(function);
        } else if (target.equals("double")) { //$NON-NLS-1$ 
            modified = convertToDouble(function);
        } else if (target.equals("bigdecimal")) { //$NON-NLS-1$ 
            modified = convertToBigDecimal(function);
        } else if (target.equals("date")) { //$NON-NLS-1$ 
            modified = convertToDate(function);
        } else if (target.equals("time")) { //$NON-NLS-1$ 
            modified = convertToTime(function);
        } else if (target.equals("timestamp")) { //$NON-NLS-1$ 
            modified = convertToTimestamp(function);
        } else if (target.equals("char")) { //$NON-NLS-1$ 
            modified = convertToChar(function);
        } else if (target.equals("boolean")) {  //$NON-NLS-1$ 
            modified = convertToBoolean(function);
        } else if (target.equals("byte")) {  //$NON-NLS-1$ 
            modified = convertToByte(function);
        } else {
            modified = DROP_MODIFIER.modify(function);          
        }
        return modified;
    }
    
    private IExpression convertToDate(IFunction function) {
        IExpression convert = null;
        List<IExpression> args = function.getParameters();
        Class srcType = args.get(0).getType();
        int srcCode = getSrcCode(srcType);

        switch(srcCode) {
            case STRING:
                // convert(STRING, date) --> to_date(STRING, format)
                String format = "YYYY-MM-DD";  //$NON-NLS-1$ 
                convert = dateTypeHelper("to_date", Arrays.asList(args.get(0),  //$NON-NLS-1$ 
                    langFactory.createLiteral(format, String.class)), java.sql.Date.class);
                break;
            case TIMESTAMP:
                // convert(TSELEMENT, date) --> trunc(TSELEMENT) 
                convert = dateTypeHelper("trunc", Arrays.asList(args.get(0)), java.sql.Date.class);  //$NON-NLS-1$ 
                break;
            default:
                convert = DROP_MODIFIER.modify(function); 
                break;
        }
        
        return convert;
    }

    /**
     * TODO: need to remove the prepend 1970-01-01 and the {ts''}
     * @param function
     * @return IExpression
     */
    private IExpression convertToTime(IFunction function) {
        IExpression convert = null;
        List<IExpression> args = function.getParameters();
        Class srcType = args.get(0).getType();
        String format = "YYYY-MM-DD HH24:MI:SS";  //$NON-NLS-1$ 
        
        int srcCode = getSrcCode(srcType);
        switch(srcCode) {
            case STRING:
                //convert(STRING, time) --> to_date('1970-01-01 ' || to_char(timevalue, 'HH24:MI:SS'), 'YYYY-MM-DD HH24:MI:SS')
                IFunction inner0 = langFactory.createFunction("to_char",  //$NON-NLS-1$
                    Arrays.asList( 
                        args.get(0),
                        langFactory.createLiteral("HH24:MI:SS", String.class)),  //$NON-NLS-1$
                        String.class); 
                        
                IExpression prependedPart0 = langFactory.createFunction("||",  //$NON-NLS-1$
                Arrays.asList(
                    langFactory.createLiteral("1970-01-01 ", String.class),  //$NON-NLS-1$
                    inner0),
                    String.class);    
                    
                convert = langFactory.createFunction("to_date",  //$NON-NLS-1$
                    Arrays.asList(prependedPart0,
                        langFactory.createLiteral(format, String.class)), 
                        java.sql.Time.class);   
                break;                                                                 
            case TIMESTAMP:
                // convert(timestamp, time) 
                // --> to_date(('1970-01-01 ' || to_char(timestampvalue, 'HH24:MI:SS'))),  
                //         'YYYY-MM-DD HH24:MI:SS') 
                IFunction inner = langFactory.createFunction("to_char",  //$NON-NLS-1$
                    Arrays.asList( 
                        args.get(0),
                        langFactory.createLiteral("HH24:MI:SS", String.class)),  //$NON-NLS-1$
                        String.class); 
                
                IExpression prependedPart =  langFactory.createFunction("||",  //$NON-NLS-1$
                    Arrays.asList(
                        langFactory.createLiteral("1970-01-01 ", String.class),  //$NON-NLS-1$
                        inner),
                        String.class);
                                          
                convert = langFactory.createFunction("to_date",  //$NON-NLS-1$
                    Arrays.asList(prependedPart,
                        langFactory.createLiteral(format, String.class)), 
                        java.sql.Time.class);                                     
                break;
            default:
                convert = DROP_MODIFIER.modify(function); 
                break;
        }
        
        return convert;
    }    
    
    private IExpression convertToTimestamp(IFunction function) {
        IExpression convert = null;
        List<IExpression> args = function.getParameters();
        Class srcType = args.get(0).getType();
        int srcCode = getSrcCode(srcType);
        switch(srcCode) {
            case STRING:
                // convert(STRING, timestamp) --> to_date(timestampvalue, 'YYYY-MM-DD HH24:MI:SS.FF')))  
                String format = "YYYY-MM-DD HH24:MI:SS.FF";  //$NON-NLS-1$
                convert = dateTypeHelper("to_timestamp", Arrays.asList(args.get(0),  //$NON-NLS-1$ 
                    langFactory.createLiteral(format, String.class)), java.sql.Timestamp.class);
                break;
            case TIME:
            case DATE:
            	convert = dateTypeHelper("cast", Arrays.asList(args.get(0),  //$NON-NLS-1$ 
                        langFactory.createLiteral("timestamp", String.class)), java.sql.Timestamp.class); //$NON-NLS-1$
                break; 
            default:
                convert = DROP_MODIFIER.modify(function); 
                break;
        }
        
        return convert;
    }
    
    private IExpression convertToChar(IFunction function) {
        // two cases: 
        //          1) 2-byte: convert(string, char) --> cast(stringkey AS char(2))
        //          2) single bit: just drop
        // TODO: case 1)
        return  DROP_MODIFIER.modify(function);         
    }

    private IExpression convertToString(IFunction function) {
        IExpression convert = null;
        List<IExpression> args = function.getParameters();
        String format = null;

        int srcCode = getSrcCode(function);
        switch(srcCode) { // convert(input, string) --> to_char(input)
            case BOOLEAN:
                convert = langFactory.createFunction("decode", Arrays.asList( //$NON-NLS-1$
                        args.get(0),
                        langFactory.createLiteral(new Integer(0), Integer.class),
                        langFactory.createLiteral("false", String.class), //$NON-NLS-1$
                        langFactory.createLiteral(new Integer(1), Integer.class),
                        langFactory.createLiteral("true", String.class) ),  //$NON-NLS-1$
                    String.class);
                
                break;
            case BYTE:
            case SHORT:
            case INTEGER:
            case LONG:
            case BIGINTEGER:
            case FLOAT:
            case DOUBLE:
            case BIGDECIMAL:
                convert = createStringFunction(args.get(0));
                break;
            // convert(input, string) --> to_char(input, format)
            case DATE:
                format = "YYYY-MM-DD"; //$NON-NLS-1$
                convert = createStringFunction(args.get(0), format); 
                break;
            case TIME:
                format = "HH24:MI:SS"; //$NON-NLS-1$
                convert = createStringFunction(args.get(0), format); 
                break;
            case TIMESTAMP:
                convert = createStringFunction(args.get(0), "YYYY-MM-DD HH24:MI:SS.FF"); //$NON-NLS-1$ 
                break;
            default:
                convert = DROP_MODIFIER.modify(function);
                break;
        }
        
        return convert;
    }
    
    private IExpression convertToBoolean(IFunction function) {
        IExpression convert = null;
        int srcCode = getSrcCode(function);
        switch(srcCode) {
            case STRING:
                // convert(src, boolean) --> decode(string, 'true', 1, 'false', 0)
                convert = booleanHelper(function);  
                break;  
            case BYTE:
            case SHORT:
            case INTEGER:
            case LONG:
            case BIGINTEGER:
            case FLOAT:
            case DOUBLE:
            case BIGDECIMAL:
            default:
                convert = DROP_MODIFIER.modify(function);
                break;
        }
        
        return convert;
    }
    
    private IExpression convertToByte(IFunction function) {
        IExpression convert = null;
        int srcCode = getSrcCode(function);

        switch(srcCode) {
            case STRING:
                convert = stringSrcHelper(function);
                break;  
            case BOOLEAN:
            case SHORT: 
            case INTEGER:
            case LONG:
            case BIGINTEGER:                     
            case FLOAT:
            case DOUBLE:
            case BIGDECIMAL:
            default:
                convert = DROP_MODIFIER.modify(function); 
                break;
        }
        
        return convert;
    }
    
    private IExpression convertToShort(IFunction function) {
        IExpression convert = null;
        int srcCode = getSrcCode(function);

        switch(srcCode) {
            case STRING:
                convert = stringSrcHelper(function);
                break;  
            case BOOLEAN:
            case BYTE:
            case INTEGER:
            case LONG:
            case BIGINTEGER:
            case FLOAT:
            case DOUBLE:
            case BIGDECIMAL:
            default:
                convert = DROP_MODIFIER.modify(function); 
                break;
        }
        
        return convert;
    }
    
    private IExpression convertToInteger(IFunction function) {
        IExpression convert = null;
        int srcCode = getSrcCode(function);

        switch(srcCode) {
            case STRING:
                convert = stringSrcHelper(function);
                break;  
            case BOOLEAN:
            case LONG:
            case BIGINTEGER:
            case FLOAT:
            case DOUBLE:
            case BIGDECIMAL:
            default:
                convert = DROP_MODIFIER.modify(function); 
                break;
        }
        
        return convert;
    }

    private IExpression convertToLong(IFunction function) {
        IExpression convert = null;
        int srcCode = getSrcCode(function);

        switch(srcCode) {
            case STRING:
                convert = stringSrcHelper(function);
                break;  
            case BOOLEAN:
            case BIGINTEGER:
            case FLOAT:
            case DOUBLE:
            case BIGDECIMAL:
            default:
                convert = DROP_MODIFIER.modify(function); 
                break;
        }
        
        return convert;
    }
    
    private IExpression convertToBigInteger(IFunction function) {
        IExpression convert = null;
        int srcCode = getSrcCode(function);

        switch(srcCode) {
            case STRING:
                convert = stringSrcHelper(function);
                break;  
            case BOOLEAN:
            case FLOAT:
            case DOUBLE:
            case BIGDECIMAL:
            default:
                convert = DROP_MODIFIER.modify(function); 
                break;
        }
        
        return convert;
    }
    
    private IExpression convertToFloat(IFunction function) {
        IExpression convert = null;
        int srcCode = getSrcCode(function);

        switch(srcCode) {
            case STRING:
                convert = stringSrcHelper(function);
                break;  
            case BOOLEAN:
            case DOUBLE: 
            case BIGDECIMAL:
            default:
                convert = DROP_MODIFIER.modify(function); 
                break;
        }
        
        return convert;
    }   
       
    private IExpression convertToDouble(IFunction function) {
        IExpression convert = null;
        int srcCode = getSrcCode(function);

        switch(srcCode) {
            case STRING:
                convert = stringSrcHelper(function);
                break;  
            case BOOLEAN:
            case BIGDECIMAL:   
            default:
                convert = DROP_MODIFIER.modify(function); 
                break;
        }
        
        return convert;
    }
    
    private IExpression convertToBigDecimal(IFunction function) {
        IExpression convert = null;
        int srcCode = getSrcCode(function);

        switch(srcCode) {
            case STRING:
                convert = stringSrcHelper(function);
                break;  
            case BOOLEAN:
            default:
                convert = DROP_MODIFIER.modify(function); 
                break;
        }
        
        return convert;
    }  

    private IFunction dateTypeHelper(String functionName, List<IExpression> args, Class target) {
        IFunction convert = langFactory.createFunction(functionName,  
            args, target);
        return convert;          
    }
       
    private IFunction booleanHelper(IFunction function) {
        // using decode(value, 'true', 1, 'false', 0)
        List<IExpression> args = function.getParameters();
       
        return langFactory.createFunction("decode", //$NON-NLS-1$
        		Arrays.asList(
        	            args.get(0),
        	            langFactory.createLiteral("true", String.class), //$NON-NLS-1$ 
        	            langFactory.createLiteral(new Byte((byte)1), Byte.class),
        	            langFactory.createLiteral("false", String.class), //$NON-NLS-1$ 
        	            langFactory.createLiteral(new Byte((byte)0), Byte.class)                        
        	        ), java.lang.Boolean.class);  
    }
            
    private IExpression stringSrcHelper(IFunction function) {
        IExpression convert = null;
        List<IExpression> args = function.getParameters();
        // switch the target type
        String functionName = "to_number"; //$NON-NLS-1$
        int targetCode = getTargetCode(function.getType());
        switch(targetCode) {
            case BYTE:
                convert = createFunction(functionName, args.get(0), Byte.class);
                break;
            case SHORT:
                convert = createFunction(functionName, args.get(0), Short.class);
                break;                    
            case INTEGER:
                convert = createFunction(functionName, args.get(0), Integer.class);
                break;
            case LONG:
                convert = createFunction(functionName, args.get(0), Long.class);
                break;           
            case BIGINTEGER:
                convert = createFunction(functionName, args.get(0), java.math.BigInteger.class);
                break;    
            case FLOAT:
                convert = createFunction(functionName, args.get(0), Float.class);
                break;
            case DOUBLE:
                convert = createFunction(functionName, args.get(0), Double.class);
                break;
            case BIGDECIMAL:
                convert = createFunction(functionName, args.get(0), java.math.BigDecimal.class);
                break;   
            default:
                convert = DROP_MODIFIER.modify(function);
                break;               
        }             
        return convert;
    } 
          
    private IFunction createFunction(String functionName, IExpression args0, Class targetClass) {
        IFunction created = langFactory.createFunction(functionName,
            Arrays.asList(args0), targetClass);
        return created;            
    }

    private IFunction createStringFunction(IExpression args0, String format) {
        IFunction created = langFactory.createFunction("to_char", //$NON-NLS-1$ 
            Arrays.asList(args0, langFactory.createLiteral(format, String.class)), 
            String.class);
        return created;            
    }
    
    private IFunction createStringFunction(IExpression args) {
        IFunction created = langFactory.createFunction("to_char", //$NON-NLS-1$ 
            Arrays.asList( args ), String.class); 
        return created;
    }
    
    private int getSrcCode(IFunction function) {
        List<IExpression> args = function.getParameters();
        Class srcType = args.get(0).getType();
        return ((Integer) typeMap.get(srcType)).intValue();
    }
    
    private int getSrcCode(Class source) {
        return ((Integer) typeMap.get(source)).intValue();
    }
    
    private int getTargetCode(Class target) {
        return ((Integer) typeMap.get(target)).intValue();
    }           
}
