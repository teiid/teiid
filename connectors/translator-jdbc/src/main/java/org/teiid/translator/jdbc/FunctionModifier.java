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

import org.teiid.client.BatchSerializer;
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
    public static final int STRING = BatchSerializer.STRING;
    public static final int CHAR = BatchSerializer.CHAR;
    public static final int BOOLEAN = BatchSerializer.BOOLEAN;
    public static final int BYTE = BatchSerializer.BYTE;
    public static final int SHORT = BatchSerializer.SHORT;
    public static final int INTEGER = BatchSerializer.INTEGER;
    public static final int LONG = BatchSerializer.LONG;
    public static final int BIGINTEGER = BatchSerializer.BIGINTEGER;
    public static final int FLOAT = BatchSerializer.FLOAT;
    public static final int DOUBLE = BatchSerializer.DOUBLE;
    public static final int BIGDECIMAL = BatchSerializer.BIGDECIMAL;
    public static final int DATE = BatchSerializer.DATE;
    public static final int TIME = BatchSerializer.TIME;
    public static final int TIMESTAMP = BatchSerializer.TIMESTAMP;
    public static final int OBJECT = BatchSerializer.OBJECT;
    public static final int BLOB = BatchSerializer.BLOB;
    public static final int CLOB = BatchSerializer.CLOB;
    public static final int XML = BatchSerializer.XML;
    public static final int NULL = BatchSerializer.NULL;

    public static int getCode(Class<?> source) {
        return BatchSerializer.getCode(source);
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
