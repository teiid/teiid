/*
 * JBoss, Home of Professional Open Source.
 * Copyright (C) 2008 Red Hat, Inc.
 * Copyright (C) 2000-2007 MetaMatrix, Inc.
 * Licensed to Red Hat, Inc. under one or more contributor 
 * license agreements.  See the copyright.txt file in the
 * distribution for a full listing of individual contributors.
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

package com.metamatrix.connector.jdbc.oracle;

import java.util.Iterator;
import java.util.List;

import com.metamatrix.connector.api.ConnectorLogger;
import com.metamatrix.connector.api.TypeFacility;
import com.metamatrix.connector.exception.ConnectorException;
import com.metamatrix.connector.jdbc.extension.FunctionModifier;
import com.metamatrix.connector.jdbc.extension.impl.BasicFunctionModifier;
import com.metamatrix.connector.jdbc.extension.impl.DropFunctionModifier;
import com.metamatrix.connector.language.IElement;
import com.metamatrix.connector.language.IExpression;
import com.metamatrix.connector.language.IFunction;
import com.metamatrix.connector.language.ILanguageFactory;
import com.metamatrix.connector.language.ILiteral;
import com.metamatrix.connector.language.IScalarSubquery;
import com.metamatrix.connector.language.ISelectSymbol;
import com.metamatrix.connector.metadata.runtime.Element;
import com.metamatrix.connector.metadata.runtime.MetadataID;
import com.metamatrix.connector.metadata.runtime.RuntimeMetadata;

/**
 */
public class OracleConvertModifier extends BasicFunctionModifier implements FunctionModifier {     
    private static DropFunctionModifier DROP_MODIFIER = new DropFunctionModifier();
    private ILanguageFactory langFactory;
    private RuntimeMetadata metadata;
    private ConnectorLogger logger;
    
    public OracleConvertModifier(ILanguageFactory langFactory, RuntimeMetadata metadata, ConnectorLogger logger) {
        this.langFactory = langFactory;
        this.metadata = metadata;
        this.logger = logger;
    }

    /**
     * Intentially return null, rely on the SQLStringVisitor being used by caller
     * (Oracle or Oracle8 SQLConversionVisitor (SQLConversionVisitor))
     * @see com.metamatrix.connector.jdbc.extension.FunctionModifier#translate(com.metamatrix.connector.language.IFunction)
     */
    public List translate(IFunction function) {
        return null;
    }    
    
    public IExpression modify(IFunction function) {
        IExpression[] args = function.getParameters();
        IExpression modified = null;

        if (args[0] != null && args[0] instanceof ILiteral && ((ILiteral)args[0]).getValue() == null ) {
            if (args[1] != null && args[1] instanceof ILiteral) {
                // This is a convert(null, ...) or cast(null as ...)
                modified = convertNull(function);
                return modified;
            }
        } 
        
        if (args[1] != null && args[1] instanceof ILiteral) {
            String target = ((String)((ILiteral)args[1]).getValue()).toLowerCase();
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
    
    /** 
     * In Oracle 8i only, there are cases where a null in the select clause
     * must be wrapped by a cast function, casting to the appropriate type.
     * This happens when a Union is submitted to Oracle 8i.  A null must
     * be explicitly casted to the type of it's corresponding symbol in 
     * another branch of the Union.
     * 
     * For example, the following query would fail in 8i:
     * 
     * <code>select null from TableX Union select dateColumn from TableY</code>
     * 
     * The above query would have to be rewritten as
     * 
     * <code>select cast(null as date) from TableX Union select dateColumn from TableY</code>
     * 
     * This isn't necessary, though, for textual types (string, char, etc.) so the
     * cast/convert function will be dropped.
     * 
     * (Date is Oracle type for timestamps and dates.)
     * 
     * @param function IFunction to be converted
     */
    private IExpression convertNull(IFunction function) {
        IExpression convert = null;
        IExpression[] args = function.getParameters();
        String typeName = null;
        Class functionClass = null;
        
        String target = ((String)((ILiteral)args[1]).getValue()).toLowerCase();
        if (target.equals("string")) {  //$NON-NLS-1$ 
            convert = DROP_MODIFIER.modify(function);         
        } else if (target.equals("short")) {  //$NON-NLS-1$ 
            typeName = "Number"; //$NON-NLS-1$
            functionClass = Integer.class;
        } else if (target.equals("integer")) { //$NON-NLS-1$ 
            typeName = "Number"; //$NON-NLS-1$
            functionClass = Integer.class;
        } else if (target.equals("long")) { //$NON-NLS-1$ 
            typeName = "Number"; //$NON-NLS-1$
            functionClass = Integer.class;
        } else if (target.equals("biginteger")) { //$NON-NLS-1$ 
            typeName = "Number"; //$NON-NLS-1$
            functionClass = Integer.class;
        } else if (target.equals("float")) { //$NON-NLS-1$ 
            typeName = "float"; //$NON-NLS-1$
            functionClass = Float.class;
        } else if (target.equals("double")) { //$NON-NLS-1$ 
            typeName = "Number"; //$NON-NLS-1$
            functionClass = Integer.class;
        } else if (target.equals("bigdecimal")) { //$NON-NLS-1$ 
            typeName = "float"; //$NON-NLS-1$
            functionClass = Float.class; 
        } else if (target.equals("date")) { //$NON-NLS-1$ 
            typeName = TypeFacility.RUNTIME_NAMES.DATE; 
            functionClass = java.sql.Date.class;
        } else if (target.equals("time")) { //$NON-NLS-1$ 
            typeName = TypeFacility.RUNTIME_NAMES.DATE; 
            functionClass = java.sql.Time.class;
        } else if (target.equals("timestamp")) { //$NON-NLS-1$ 
            typeName = TypeFacility.RUNTIME_NAMES.DATE; 
            functionClass = java.sql.Timestamp.class;
        } else if (target.equals("char")) { //$NON-NLS-1$ 
            convert = DROP_MODIFIER.modify(function);         
        } else if (target.equals("boolean")) {  //$NON-NLS-1$ 
            typeName = "Number"; //$NON-NLS-1$
            functionClass = Integer.class;
        } else if (target.equals("byte")) {  //$NON-NLS-1$ 
            typeName = "Number"; //$NON-NLS-1$
            functionClass = Integer.class;
        } else {
            convert = DROP_MODIFIER.modify(function);         
        }
            
        if (convert == null) {
            // cast (NULL as ...) -- > cast(NULL as ...)
            // or
            // convert (NULL, ...) -- > cast(NULL as ...)
            convert = langFactory.createFunction("cast", //$NON-NLS-1$
                new IExpression[] {
                    args[0],
                    langFactory.createLiteral(typeName, String.class)}, 
                    functionClass);                       
        }

        return convert;
    }

    private IExpression convertToDate(IFunction function) {
        IExpression convert = null;
        IExpression[] args = function.getParameters();
        Class srcType = args[0].getType();
        int srcCode = getSrcCode(srcType);

        switch(srcCode) {
            case STRING:
                // convert(STRING, date) --> to_date(STRING, format)
                String format = "YYYY-MM-DD";  //$NON-NLS-1$ 
                convert = dateTypeHelper("to_date", new IExpression[] {args[0],  //$NON-NLS-1$ 
                    langFactory.createLiteral(format, String.class)}, java.sql.Date.class);
                break;
            case TIMESTAMP:
                // convert(TSELEMENT, date) --> trunc(TSELEMENT) 
                convert = dateTypeHelper("trunc", new IExpression[] {args[0]}, java.sql.Date.class);  //$NON-NLS-1$ 
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
        IExpression[] args = function.getParameters();
        Class srcType = args[0].getType();
        String format = "YYYY-MM-DD HH24:MI:SS";  //$NON-NLS-1$ 
        
        int srcCode = getSrcCode(srcType);
        switch(srcCode) {
            case STRING:
                //convert(STRING, time) --> to_date('1970-01-01 ' || to_char(timevalue, 'HH24:MI:SS'), 'YYYY-MM-DD HH24:MI:SS')
                IFunction inner0 = langFactory.createFunction("to_char",  //$NON-NLS-1$
                    new IExpression[] { 
                        args[0],
                        langFactory.createLiteral("HH24:MI:SS", String.class)},  //$NON-NLS-1$
                        String.class); 
                        
                IExpression prependedPart0 = langFactory.createFunction("||",  //$NON-NLS-1$
                new IExpression[] {
                    langFactory.createLiteral("1970-01-01 ", String.class),  //$NON-NLS-1$
                    inner0},
                    String.class);    
                    
                convert = langFactory.createFunction("to_date",  //$NON-NLS-1$
                    new IExpression[] {prependedPart0,
                        langFactory.createLiteral(format, String.class)}, 
                        java.sql.Time.class);   
                break;                                                                 
            case TIMESTAMP:
                // convert(timestamp, time) 
                // --> to_date(('1970-01-01 ' ||substr(to_char(timestampvalue, 'YYYY-MM-DD HH24:MI:SS'), 12)),  
                //         'YYYY-MM-DD HH24:MI:SS') 
                IFunction inner = langFactory.createFunction("to_char",  //$NON-NLS-1$
                    new IExpression[] { 
                        args[0],
                        langFactory.createLiteral("FXYYYY-MM-DD HH24:MI:SS", String.class)},  //$NON-NLS-1$
                        String.class); 
                
                IFunction intermediate = langFactory.createFunction("substr",  //$NON-NLS-1$
                    new IExpression[] { 
                        inner,
                        langFactory.createLiteral(new Integer(12), Integer.class)},  
                        String.class); 
                        
                IExpression prependedPart =  langFactory.createFunction("||",  //$NON-NLS-1$
                    new IExpression[] {
                        langFactory.createLiteral("1970-01-01 ", String.class),  //$NON-NLS-1$
                        intermediate},
                        String.class);
                                          
                convert = langFactory.createFunction("to_date",  //$NON-NLS-1$
                    new IExpression[] {prependedPart,
                        langFactory.createLiteral(format, String.class)}, 
                        java.sql.Time.class);                                     
                break;
            default:
                convert = DROP_MODIFIER.modify(function); 
                break;
        }
        
        return convert;
    }    
    
    /**
     * This works only for Oracle 9i.
     * @param src
     * @return IFunction
     */
    private IExpression convertToTimestamp(IFunction function) {
        IExpression convert = null;
        IExpression[] args = function.getParameters();
        Class srcType = args[0].getType();
        int srcCode = getSrcCode(srcType);
        //TODO: what is the best format for timestamp
        //String format = "YYYY-MM-DD HH24:MI:SS.fffffffff";  //$NON-NLS-1$
        String format = "YYYY-MM-DD HH24:MI:SS";  //$NON-NLS-1$
        switch(srcCode) {
            case STRING:
                // convert(STRING, timestamp) --> to_date(timestampvalue, 'YYYY-MM-DD HH24:MI:SS'))) from smalla 
                format = "YYYY-MM-DD HH24:MI:SS.FF";  //$NON-NLS-1$
                convert = dateTypeHelper("to_timestamp", new IExpression[] {args[0],  //$NON-NLS-1$ 
                    langFactory.createLiteral(format, String.class)}, java.sql.Timestamp.class);
                break;
            case TIME:
            case DATE:
                // convert(DATE, timestamp) --> to_date(to_char(DATE, 'YYYY-MM-DD HH24:MI:SS'), 'YYYY-MM-DD HH24:MI:SS')
                IFunction inner = langFactory.createFunction("to_char",  //$NON-NLS-1$
                    new IExpression[] { 
                        args[0],
                        langFactory.createLiteral(format, String.class)},  
                        String.class); 
                        
                convert = langFactory.createFunction("to_date",  //$NON-NLS-1$
                    new IExpression[] { 
                        inner,
                        langFactory.createLiteral(format, String.class)},  
                        java.sql.Timestamp.class); 
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
        IExpression[] args = function.getParameters();
        String format = null;
        boolean dateTypeFound = false;

        int srcCode = getSrcCode(function);
        switch(srcCode) { // convert(input, string) --> to_char(input)
            case BOOLEAN:
                convert = langFactory.createFunction("decode", new IExpression[]  //$NON-NLS-1$
                    {   args[0],
                        langFactory.createLiteral(new Integer(0), Integer.class),
                        langFactory.createLiteral("false", String.class), //$NON-NLS-1$
                        langFactory.createLiteral(new Integer(1), Integer.class),
                        langFactory.createLiteral("true", String.class) },  //$NON-NLS-1$
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
                convert = createStringFunction(args[0]);
                break;
            // convert(input, string) --> to_char(input, format)
            case DATE:
                format = "YYYY-MM-DD"; //$NON-NLS-1$
                convert = createStringFunction(args[0], format); 
                break;
            case TIME:
                format = "HH24:MI:SS"; //$NON-NLS-1$
                convert = createStringFunction(args[0], format); 
                break;
            case TIMESTAMP:
                if (args[0] instanceof IElement) {
                    IElement element = (IElement) args[0];
                    MetadataID id = element.getMetadataID();
                    RuntimeMetadata rmd = this.metadata;
                    try {
                        Element elemMetadata = (Element)rmd.getObject(id);
                        String nativeType = elemMetadata.getNativeType();
                        if (nativeType != null && nativeType.equalsIgnoreCase("DATE")) { //$NON-NLS-1$
                            dateTypeFound = true;
                        }
                    }
                    catch (ConnectorException e) {
                        logger.logError(e.getMessage());
                    }
                }
                else if (args[0] instanceof IScalarSubquery) {
                    IScalarSubquery scalar = (IScalarSubquery) args[0];
                    List symList = scalar.getQuery().getProjectedQuery().getSelect().getSelectSymbols();
                    Iterator iter = symList.iterator();
                    while(iter.hasNext()) {
                        ISelectSymbol symObj = (ISelectSymbol)iter.next();
                        IExpression expObj = symObj.getExpression();
                        if (expObj instanceof IElement) {
                            IElement element = (IElement)expObj;
                            MetadataID id = element.getMetadataID();
                            RuntimeMetadata rmd = this.metadata;
                            try {
                                Element elemMetadata = (Element)rmd.getObject(id);
                                String nativeType = elemMetadata.getNativeType();
                                if (nativeType != null && nativeType.equalsIgnoreCase("DATE")) { //$NON-NLS-1$
                                    dateTypeFound = true;
                                }
                            }
                            catch (ConnectorException e) {
                                logger.logError(e.getMessage());
                                
                            }
                        }
                    }
                    
                }
                    
                if (dateTypeFound) {
                    format = "YYYY-MM-DD HH24:MI:SS"; //$NON-NLS-1$  
                }
                else {
                    format = "YYYY-MM-DD HH24:MI:SS.FF"; //$NON-NLS-1$
                }            
                             
                convert = createStringFunction(args[0], format); 
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

    private IFunction dateTypeHelper(String functionName, IExpression[] args, Class target) {
        IFunction convert = langFactory.createFunction(functionName,  
            args, target);
        return convert;          
    }
       
    private IFunction booleanHelper(IFunction function) {
        // using decode(value, 'true', 1, 'false', 0)
        IExpression[] args = function.getParameters();
       
        IExpression[] modified = new IExpression[] {
            args[0],
            langFactory.createLiteral("true", String.class), //$NON-NLS-1$ 
            langFactory.createLiteral(new Byte((byte)1), Byte.class),
            langFactory.createLiteral("false", String.class), //$NON-NLS-1$ 
            langFactory.createLiteral(new Byte((byte)0), Byte.class)                        
        };
        
        return langFactory.createFunction("decode", //$NON-NLS-1$
            modified, java.lang.Boolean.class);  
    }
            
    private IExpression stringSrcHelper(IFunction function) {
        IExpression convert = null;
        IExpression[] args = function.getParameters();
        // switch the target type
        String functionName = "to_number"; //$NON-NLS-1$
        int targetCode = getTargetCode(function.getType());
        switch(targetCode) {
            case BYTE:
                convert = createFunction(functionName, args[0], Byte.class);
                break;
            case SHORT:
                convert = createFunction(functionName, args[0], Short.class);
                break;                    
            case INTEGER:
                convert = createFunction(functionName, args[0], Integer.class);
                break;
            case LONG:
                convert = createFunction(functionName, args[0], Long.class);
                break;           
            case BIGINTEGER:
                convert = createFunction(functionName, args[0], java.math.BigInteger.class);
                break;    
            case FLOAT:
                convert = createFunction(functionName, args[0], Float.class);
                break;
            case DOUBLE:
                convert = createFunction(functionName, args[0], Double.class);
                break;
            case BIGDECIMAL:
                convert = createFunction(functionName, args[0], java.math.BigDecimal.class);
                break;   
            default:
                convert = DROP_MODIFIER.modify(function);
                break;               
        }             
        return convert;
    } 
          
    private IFunction createFunction(String functionName, IExpression args0, Class targetClass) {
        IFunction created = langFactory.createFunction(functionName,
            new IExpression[] {args0}, targetClass);
        return created;            
    }

    private IFunction createStringFunction(IExpression args0, String format) {
        IFunction created = langFactory.createFunction("to_char", //$NON-NLS-1$ 
            new IExpression[] {args0, langFactory.createLiteral(format, String.class)}, 
            String.class);
        return created;            
    }
    
    private IFunction createStringFunction(IExpression args) {
        IFunction created = langFactory.createFunction("to_char", //$NON-NLS-1$ 
            new IExpression[] { args }, String.class); 
        return created;
    }
    
    private int getSrcCode(IFunction function) {
        IExpression[] args = function.getParameters();
        Class srcType = args[0].getType();
        return ((Integer) typeMap.get(srcType)).intValue();
    }
    
    private int getSrcCode(Class source) {
        return ((Integer) typeMap.get(source)).intValue();
    }
    
    private int getTargetCode(Class target) {
        return ((Integer) typeMap.get(target)).intValue();
    }           
}
