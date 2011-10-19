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

/*
 */
package org.teiid.translator.jdbc;

import java.util.List;

import org.teiid.core.types.DataTypeManager;
import org.teiid.language.Function;
import org.teiid.language.LanguageObject;


/**
 * Implementations of this interface are used to modify Teiid functions
 * coming in to the connector into alternate datasource-specific language, if
 * necessary. 
 */
public abstract class FunctionModifier {
	
    /*
     * Public sharing part for the mapping between class and type in format of Map<class->Integer>.
     */
    public static final int STRING = DataTypeManager.DefaultTypeCodes.STRING;
    public static final int CHAR = DataTypeManager.DefaultTypeCodes.CHAR;
    public static final int BOOLEAN = DataTypeManager.DefaultTypeCodes.BOOLEAN;
    public static final int BYTE = DataTypeManager.DefaultTypeCodes.BYTE;
    public static final int SHORT = DataTypeManager.DefaultTypeCodes.SHORT;
    public static final int INTEGER = DataTypeManager.DefaultTypeCodes.INTEGER;
    public static final int LONG = DataTypeManager.DefaultTypeCodes.LONG;
    public static final int BIGINTEGER = DataTypeManager.DefaultTypeCodes.BIGINTEGER;
    public static final int FLOAT = DataTypeManager.DefaultTypeCodes.FLOAT;
    public static final int DOUBLE = DataTypeManager.DefaultTypeCodes.DOUBLE;
    public static final int BIGDECIMAL = DataTypeManager.DefaultTypeCodes.BIGDECIMAL;
    public static final int DATE = DataTypeManager.DefaultTypeCodes.DATE;
    public static final int TIME = DataTypeManager.DefaultTypeCodes.TIME;
    public static final int TIMESTAMP = DataTypeManager.DefaultTypeCodes.TIMESTAMP;
    public static final int OBJECT = DataTypeManager.DefaultTypeCodes.OBJECT;
    public static final int BLOB = DataTypeManager.DefaultTypeCodes.BLOB;
    public static final int CLOB = DataTypeManager.DefaultTypeCodes.CLOB;
    public static final int XML = DataTypeManager.DefaultTypeCodes.XML;
    public static final int NULL = DataTypeManager.DefaultTypeCodes.NULL;

    public static int getCode(Class<?> source) {
        return DataTypeManager.getTypeCode(source);
    }
    
    /**
     * Return a List of translated parts ({@link LanguageObject}s and Objects), or null
     * if this FunctionModifier wishes to rely on the default translation of the
     * conversion visitor. 
     * @param function IFunction to be translated
     * @return List of translated parts, or null
     * @since 4.2
     */
    public abstract List<?> translate(Function function);
    
}
