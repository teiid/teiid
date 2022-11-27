/*
 * Copyright Red Hat, Inc. and/or its affiliates
 * and other contributors as indicated by the @author tags and
 * the COPYRIGHT.txt file distributed with this work.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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
    public static final int VARBINARY = DataTypeManager.DefaultTypeCodes.VARBINARY;
    public static final int GEOMETRY = DataTypeManager.DefaultTypeCodes.GEOMETRY;
    public static final int GEOGRAPHY = DataTypeManager.DefaultTypeCodes.GEOGRAPHY;
    public static final int JSON = DataTypeManager.DefaultTypeCodes.JSON;

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
