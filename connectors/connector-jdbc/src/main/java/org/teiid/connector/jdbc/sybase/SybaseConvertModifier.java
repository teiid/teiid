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

package org.teiid.connector.jdbc.sybase;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.teiid.connector.jdbc.translator.BasicFunctionModifier;
import org.teiid.connector.jdbc.translator.DropFunctionModifier;
import org.teiid.connector.language.IExpression;
import org.teiid.connector.language.IFunction;
import org.teiid.connector.language.ILanguageFactory;
import org.teiid.connector.language.ILiteral;


/**
 */
public class SybaseConvertModifier extends BasicFunctionModifier {

    private static DropFunctionModifier DROP_MODIFIER = new DropFunctionModifier();
    static {
        // index of expressions in convert functions in Sybase is one, not zero
        DROP_MODIFIER.setReplaceIndex(1);
    }

    private ILanguageFactory langFactory;
    
    public SybaseConvertModifier(ILanguageFactory langFactory) {
        this.langFactory = langFactory;
    }

    public IExpression modify(IFunction function) {
        List<IExpression> args = function.getParameters();
        IExpression modified = null;
        
        if (args.get(1) != null && args.get(1) instanceof ILiteral) {
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
        return DROP_MODIFIER.modify(function); 
    }
    
    /* 
     * @see com.metamatrix.connector.jdbc.extension.FunctionModifier#translate(com.metamatrix.data.language.IFunction)
     */
    public List translate(IFunction function) {        
        List parts = new ArrayList();
        parts.add("convert("); //$NON-NLS-1$
            
        List<IExpression> args = function.getParameters();
        
        if(args != null && args.size() > 0) {
            ILiteral type = (ILiteral) args.get(0);
            String typeStr = type.getValue().toString();
            
            parts.add(typeStr);
            
            for(int i=1; i<args.size(); i++) {
                parts.add(", "); //$NON-NLS-1$
                parts.add(args.get(i));
            }
        }
        parts.add(")"); //$NON-NLS-1$
        return parts;
    }
    
    private IExpression convertToBoolean(IFunction function) {
        IExpression convert = null;
        List<IExpression> args = function.getParameters();
        Class src = args.get(0).getType();
        int srcCode = getSrcCode(src);
        
        switch(srcCode) {
            case STRING:
                //TODO: how to map the 'true' to '1' before it is translated to bit
            case BYTE:
            case SHORT:
            case INTEGER:
            case LONG:
            case BIGINTEGER:
            case FLOAT:
            case DOUBLE:
            case BIGDECIMAL:
                convert = createFunction(args.get(0), "bit", Boolean.class); //$NON-NLS-1$      
                break;                
            default:
                convert = DROP_MODIFIER.modify(function);
                break;
        }
        
        return convert;
    }
    
    private IExpression convertToByte(IFunction function) {
        IExpression convert = null;
        List<IExpression> args = function.getParameters();
        Class src = args.get(0).getType();
        int srcCode = getSrcCode(src);
        
        switch(srcCode) {
            case STRING:
            case BOOLEAN:
            case SHORT: 
            case INTEGER:
            case LONG:
            case BIGINTEGER:                     
            case FLOAT:
            case DOUBLE:
            case BIGDECIMAL:
                convert = createFunction(args.get(0), "tinyint", String.class);  //$NON-NLS-1$ 
                break;  
            default:
                convert = DROP_MODIFIER.modify(function); 
                break;
        }
        
        return convert;
    }
    
    private IExpression convertToString(IFunction function) {
        IExpression convert = null;
        List<IExpression> args = function.getParameters();
        Class src = args.get(0).getType();
        int srcCode = getSrcCode(src);
        
        switch(srcCode) { 
            case CHAR:
            case BOOLEAN:
            case BYTE: 
            case SHORT:
            case INTEGER:
            case LONG:
            case BIGINTEGER:
            case FLOAT:
            case DOUBLE:
            case BIGDECIMAL:       
                convert = createFunction(args.get(0), "varchar", String.class); //$NON-NLS-1$                                            
                break;                        
            case DATE: // convert(date, string) --> convert(varchar, date, 112) 
                //TODO: what is the best format 111/110/101?
                convert = createFunction(args.get(0), 101, String.class);
                break;
            case TIME: // convert(time, string) --> convert(varchar, time, 108)
                convert = createFunction(args.get(0), 108, String.class);                        
                break;
            case TIMESTAMP:  // convert(time, string) --> convert(varchar, timestamp, 109)          
                convert = createFunction(args.get(0), 109, String.class);                          
                break;
            default:
                convert = DROP_MODIFIER.modify(function);
                break;
        }
        
        return convert;
    }

    private IExpression convertToShort(IFunction function) {
        IExpression convert = null;
        List<IExpression> args = function.getParameters();
        Class src = args.get(0).getType();
        int srcCode = getSrcCode(src);

        switch(srcCode) {
            case STRING:
            case BOOLEAN: 
            case BYTE:    
            case INTEGER:
            case LONG:
            case BIGINTEGER:
            case FLOAT:
            case DOUBLE:
            case BIGDECIMAL:
                convert = createFunction(args.get(0), "smallint", Short.class); //$NON-NLS-1$
                break;
            default:
                convert = DROP_MODIFIER.modify(function); 
                break;
        }
        
        return convert;
    }
        
    private IExpression convertToInteger(IFunction function) {
        IExpression convert = null;
        List<IExpression> args = function.getParameters();
        Class src = args.get(0).getType();
        int srcCode = getSrcCode(src);

        switch(srcCode) {
            case STRING: 
            case BOOLEAN:       
            case BYTE:
            case SHORT:
            case LONG:
            case BIGINTEGER:
            case FLOAT:
            case DOUBLE:
            case BIGDECIMAL:
                convert = createFunction(args.get(0), "int", Integer.class); //$NON-NLS-1$
                break;
            default:
                convert = DROP_MODIFIER.modify(function); 
                break;
        }
        
        return convert;
    }
      
    private IExpression convertToLong(IFunction function) {
        IExpression convert = null;
        List<IExpression> args = function.getParameters();
        Class src = args.get(0).getType();
        int srcCode = getSrcCode(src);

        switch(srcCode) {
            case STRING:
            case BOOLEAN:
            case BYTE:
            case SHORT:
            case INTEGER:       
            case BIGINTEGER:
            case FLOAT:
            case DOUBLE:
            case BIGDECIMAL:
                convert = createFunction(args.get(0), "numeric", Long.class); //$NON-NLS-1$
                break;
            default:
                convert = DROP_MODIFIER.modify(function); 
                break;
        }
        
        return convert;
    }
    
    private IExpression convertToBigInteger(IFunction function) {
        IExpression convert = null;
        List<IExpression> args = function.getParameters();
        Class src = args.get(0).getType();
        int srcCode = getSrcCode(src);

        switch(srcCode) {
            case STRING:
            case BOOLEAN:
            case BYTE:
            case SHORT:
            case INTEGER:
            case LONG:   
            case FLOAT:
            case DOUBLE:
            case BIGDECIMAL:
                convert = createFunction(args.get(0), "numeric", java.math.BigInteger.class); //$NON-NLS-1$
                break;
            default:
                convert = DROP_MODIFIER.modify(function); 
                break;
        }
        
        return convert;
    }

    private IExpression convertToFloat(IFunction function) {
        IExpression convert = null;
        List<IExpression> args = function.getParameters();
        Class src = args.get(0).getType();
        int srcCode = getSrcCode(src);

        switch(srcCode) {
            case STRING:
            case BOOLEAN:        
            case BYTE:
            case SHORT:
            case INTEGER:
            case LONG:   
            case BIGINTEGER:                                 
            case DOUBLE: 
            case BIGDECIMAL:
                convert = createFunction(args.get(0), "real", Float.class); //$NON-NLS-1$
                break;
            default:
                convert = DROP_MODIFIER.modify(function); 
                break;
        }
        
        return convert;
    }   
       
    private IExpression convertToDouble(IFunction function) {
        IExpression convert = null;
        List<IExpression> args = function.getParameters();
        Class src = args.get(0).getType();
        int srcCode = getSrcCode(src);

        switch(srcCode) {
            case STRING:
            case BOOLEAN:      
            case BYTE:
            case SHORT:
            case INTEGER:
            case LONG:   
            case BIGINTEGER:                                 
            case FLOAT:       
            case BIGDECIMAL:   
                convert = createFunction(args.get(0), "float", Double.class); //$NON-NLS-1$
                break;
            default:
                convert = DROP_MODIFIER.modify(function); 
                break;
        }
        
        return convert;
    }
    
    private IExpression convertToBigDecimal(IFunction function) {
        IExpression convert = null;
        List<IExpression> args = function.getParameters();
        Class src = args.get(0).getType();
        int srcCode = getSrcCode(src);

        switch(srcCode) {
            case STRING:
            case BOOLEAN:         
            case BYTE:
            case SHORT:
            case INTEGER:
            case LONG:   
            case BIGINTEGER:                                 
            case FLOAT:       
            case DOUBLE:
                convert = createFunction(args.get(0), "float", java.math.BigDecimal.class); //$NON-NLS-1$
                break;            
            default:
                convert = DROP_MODIFIER.modify(function); 
                break;
        }
        
        return convert;
    }
        
    private IExpression convertToChar(IFunction function) {
        List<IExpression> args = function.getParameters();
        return createFunction(args.get(0), "char", Character.class); //$NON-NLS-1$
    } 
             
    private IExpression convertToDate(IFunction function) {
        IExpression convert = null;
        List<IExpression> args = function.getParameters();
        Class srcType = args.get(0).getType();
        int srcCode = getSrcCode(srcType);

        switch(srcCode) {
            case STRING:
                // convert(STRING, date) --> convert(datetime, STRING)
                convert = createFunction(args.get(0), "datetime", java.sql.Date.class); //$NON-NLS-1$
                break;
            case TIMESTAMP:
                // convert(TIMESTAMP, date) --> convert(datetime, convert(varchar, TIMESTAMP, 1/101))
                // Build inner convert
                IFunction innerConvert = langFactory.createFunction("convert",  //$NON-NLS-1$
                    Arrays.asList( 
                        langFactory.createLiteral("varchar", String.class),  //$NON-NLS-1$
                        args.get(0),
                        langFactory.createLiteral(new Integer(109), Integer.class) ),
                    String.class);
                
                // Build outer convert
                convert = langFactory.createFunction("convert",  //$NON-NLS-1$
                    Arrays.asList( 
                        langFactory.createLiteral("datetime", String.class),  //$NON-NLS-1$
                        innerConvert ),
                    java.sql.Timestamp.class);
            
                break;
            default:
                convert = DROP_MODIFIER.modify(function); 
                break;
        }
        
        return convert;
    }

    private IExpression convertToTime(IFunction function) {
        IExpression convert = null;
        List<IExpression> args = function.getParameters();
        Class srcType = args.get(0).getType();
        
        int srcCode = getSrcCode(srcType);
        switch(srcCode) {
            case STRING:
                //convert(STRING, time) --> convert(datetime, STRING)
                convert = createFunction(args.get(0), "datetime", java.sql.Time.class); //$NON-NLS-1$
                break;                                                                 
            case TIMESTAMP:
                // convert(TIMESTAMP, time) --> convert(datetime, convert(varchar, TIMESTAMP, 108/8) 
                // Build inner convert
                IFunction innerConvert = langFactory.createFunction("convert",  //$NON-NLS-1$
                    Arrays.asList( 
                        langFactory.createLiteral("varchar", String.class),  //$NON-NLS-1$
                        args.get(0),
                        langFactory.createLiteral(new Integer(108), Integer.class) ),
                    String.class);
                    
                // Build outer convert
                convert = langFactory.createFunction("convert",  //$NON-NLS-1$
                    Arrays.asList( 
                        langFactory.createLiteral("datetime", String.class),  //$NON-NLS-1$
                        innerConvert ),
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
            case TIME:                
            case DATE:
                // convert(DATE/TIME/STRING, timestamp) --> convert(datetime, DATE)
                convert = createFunction(args.get(0), "datetime", java.sql.Timestamp.class); //$NON-NLS-1$ 
                    break;              
            default:
                convert = DROP_MODIFIER.modify(function); 
                break;
        }
        
        return convert;
    }
    
    private IFunction createFunction(IExpression args0, String targetType, Class targetClass) {
        IFunction created = langFactory.createFunction("convert", //$NON-NLS-1$
            Arrays.asList(
                langFactory.createLiteral(targetType, String.class),
                args0), 
                targetClass);
        return created;            
    }
                
    private IFunction createFunction(IExpression args0, int formatNumber, Class targetClass) {
        IFunction created = langFactory.createFunction("convert",  //$NON-NLS-1$
            Arrays.asList( langFactory.createLiteral("varchar", String.class), //$NON-NLS-1$ 
                args0,
                langFactory.createLiteral(new Integer(formatNumber), Integer.class) ), 
                targetClass);
        return created;            
    }
        
    private int getSrcCode(Class source) {
        return ((Integer) typeMap.get(source)).intValue();
    }

}
