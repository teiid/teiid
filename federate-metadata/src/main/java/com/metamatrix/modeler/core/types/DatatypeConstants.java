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

package com.metamatrix.modeler.core.types;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.metamatrix.core.MetaMatrixRuntimeException;
import com.metamatrix.core.id.InvalidIDException;
import com.metamatrix.core.id.ObjectID;
import com.metamatrix.core.id.UUID;

/**
 * DatatypeConstants
 */
public class DatatypeConstants {

    /** Defines the expected built-in datatypes model URI - must be consistent with the value found in the com.metamatrix.modeler.sdt plugin.xml */
    public static final String BUILTIN_DATATYPES_URI  = "http://www.metamatrix.com/metamodels/SimpleDatatypes-instance"; //$NON-NLS-1$

    /** The value <code>"http://www.metamatrix.com/2005/XmlSchema/EnterpriseDatatypes"</code>. */
    public static final String SCHEMA_FOR_ENTERPRISE_DATATYPES_URI_2005 = "http://www.metamatrix.com/2005/XmlSchema/EnterpriseDatatypes"; //$NON-NLS-1$

    /** The value <code>"http://www.metamatrix.com/2005/XmlSchema/EnterpriseDatatypes"</code>. */
    public static final String PREFIX_FOR_ENTERPRISE_DATATYPES_URI_2005 = "mmedt";   //$NON-NLS-1$

    /** Defines the expected name of the built-in datatype archive file */
    public static final String DATATYPES_ZIP_FILE_NAME   = "builtInDatatypes.zip"; //$NON-NLS-1$

    /** Defines the expected name of the built-in datatype index file */
    public static final String DATATYPES_INDEX_FILE_NAME = "builtInDataTypes.INDEX"; //$NON-NLS-1$

    /** Defines the expected name of the built-in datatype model file */
    public static final String DATATYPES_MODEL_FILE_NAME = "builtInDataTypes.xsd"; //$NON-NLS-1$

    /** Defines the expected name of the built-in datatype model file */
    public static final String DATATYPES_MODEL_FILE_NAME_WITHOUT_EXTENSION = "builtInDataTypes"; //$NON-NLS-1$

    /** Delimiter used to separate the URI string from the URI fragment */
    public static final String URI_REFERENCE_DELIMITER = "#"; //$NON-NLS-1$

    /** UUID of the built-in datatype model */
    public static ObjectID BUILTIN_DATATYPES_MODEL_UUID = null;


    /**
     * UUIDs of the XMLSchema.xsd, MagicXMLSchema.xsd, and xml.xsd <code>http://www.w3.org/1999/XMLSchema"</code>.
     */
    public static ObjectID XML_SCHEMA_UUID_1999          = null;
    public static ObjectID XML_MAGIC_SCHEMA_UUID_1999    = null;
    public static ObjectID XML_SCHEMA_INSTANCE_UUID_1999 = null;

    /**
     * UUIDs of the XMLSchema.xsd, MagicXMLSchema.xsd, and xml.xsd <code>"http://www.w3.org/2000/10/XMLSchema"</code> schema.
     */
    public static ObjectID XML_SCHEMA_UUID_2000_10          = null;
    public static ObjectID XML_MAGIC_SCHEMA_UUID_2000_10    = null;
    public static ObjectID XML_SCHEMA_INSTANCE_UUID_2000_10 = null;

    /**
     * UUIDs of the XMLSchema.xsd, MagicXMLSchema.xsd, and XMLSchema-instance.xsd <code>"http://www.w3.org/2001/XMLSchema"</code> schema.
     */
    public static ObjectID XML_SCHEMA_UUID_2001          = null;
    public static ObjectID XML_MAGIC_SCHEMA_UUID_2001    = null;
    public static ObjectID XML_SCHEMA_INSTANCE_UUID_2001 = null;


    // Conversion map between the runtime datatype names that appear in older
    // model files and the name of the new built-in data type to be used
    private static final Map RUNTIME_TO_BUILTIN_MAP = new HashMap();

    // List of runtime type names
    private static final List RUNTIME_TYPE_NAMES = new ArrayList();

    // List of built-in datatype names
    private static final List BUILTIN_TYPE_NAMES = new ArrayList();

    // List of built-in datatype names
    private static final List METAMATRIX_EXTENDED_BUILTIN_TYPE_NAMES = new ArrayList();

    // List of primitive built-in datatype names
    private static final List PRIMITIVE_BUILTIN_TYPE_NAMES = new ArrayList();

    static {
        loadRuntimeTypeToBuildInConversionMap();
        loadRuntimeTypeNames();
        loadBuiltInTypeNames();
        loadMetaMatrixExtentedBuiltInTypeNames();
        loadPrimitiveBuiltInTypeNames();
        Collections.sort(RUNTIME_TYPE_NAMES,String.CASE_INSENSITIVE_ORDER);
        try {
            BUILTIN_DATATYPES_MODEL_UUID = UUID.stringToObject("6b862080-3019-1e20-921b-eeee28353879"); //$NON-NLS-1$
        } catch (InvalidIDException e) {
            throw new MetaMatrixRuntimeException(e);
        }
        try {
            XML_SCHEMA_UUID_1999             = UUID.stringToObject("1da96d2e-fc8c-1f0b-aa25-a4ec5e156765"); //$NON-NLS-1$
            XML_MAGIC_SCHEMA_UUID_1999       = UUID.stringToObject("2343c130-fc8c-1f0b-aa25-a4ec5e156765"); //$NON-NLS-1$
            XML_SCHEMA_INSTANCE_UUID_1999    = UUID.stringToObject("26f5d0c4-fc8c-1f0b-aa25-a4ec5e156765"); //$NON-NLS-1$
            XML_SCHEMA_UUID_2000_10          = UUID.stringToObject("60624ce0-fc7a-1f0b-a813-96dd78086d7a"); //$NON-NLS-1$
            XML_MAGIC_SCHEMA_UUID_2000_10    = UUID.stringToObject("624a93b9-fc7a-1f0b-a813-96dd78086d7a"); //$NON-NLS-1$
            XML_SCHEMA_INSTANCE_UUID_2000_10 = UUID.stringToObject("632f7658-fc7a-1f0b-a813-96dd78086d7a"); //$NON-NLS-1$
            XML_SCHEMA_UUID_2001             = UUID.stringToObject("b017498c-fc82-1f0b-9301-8cc12cf53072"); //$NON-NLS-1$
            XML_MAGIC_SCHEMA_UUID_2001       = UUID.stringToObject("a8a3ee88-fc82-1f0b-9301-8cc12cf53072"); //$NON-NLS-1$
            XML_SCHEMA_INSTANCE_UUID_2001    = UUID.stringToObject("ac653e49-fc82-1f0b-9301-8cc12cf53072"); //$NON-NLS-1$
        } catch (InvalidIDException e) {
        	throw new MetaMatrixRuntimeException(e);
        }
    }

    /**
     * The names of only MetaMatrix built-in types.  These types represent
     * MetaMatrix extensions to the XML Schema of schema built-in types
     */
    public static final class MetaMatrixExtendedBuiltInNames {
        public static final String CHAR                 = "char";//$NON-NLS-1$
        public static final String BIG_INTEGER          = "biginteger";//$NON-NLS-1$
        public static final String BIG_DECIMAL          = "bigdecimal";//$NON-NLS-1$
        public static final String TIMESTAMP            = "timestamp";//$NON-NLS-1$
        public static final String OBJECT               = "object";//$NON-NLS-1$
        public static final String NULL                 = "null";//$NON-NLS-1$
        public static final String BLOB                 = "blob";//$NON-NLS-1$
        public static final String CLOB                 = "clob";//$NON-NLS-1$
        public static final String XML_LITERAL          = "XMLLiteral";//$NON-NLS-1$
    }

    /**
     * The names of the primitive built-in datatypes (19 entries).  A primitive built-in type
     * is one in which its basetype is the UR-type of "anySimpleType"
     */
    public static final class PrimitiveBuiltInNames {
        public static final String STRING               = "string"; //$NON-NLS-1$
        public static final String BOOLEAN              = "boolean";//$NON-NLS-1$
        public static final String FLOAT                = "float";//$NON-NLS-1$
        public static final String DOUBLE               = "double";//$NON-NLS-1$
        public static final String DATE                 = "date";//$NON-NLS-1$
        public static final String TIME                 = "time";//$NON-NLS-1$
        public static final String NOTATION             = "NOTATION";//$NON-NLS-1$
        public static final String QNAME                = "QName";//$NON-NLS-1$
        public static final String ANY_URI              = "anyURI";//$NON-NLS-1$
        public static final String BASE64_BINARY        = "base64Binary";//$NON-NLS-1$
        public static final String DATE_TIME            = "dateTime";//$NON-NLS-1$
        public static final String DECIMAL              = "decimal";//$NON-NLS-1$
        public static final String DURATION             = "duration";//$NON-NLS-1$
        public static final String GDAY                 = "gDay";//$NON-NLS-1$
        public static final String GMONTH_DAY           = "gMonthDay";//$NON-NLS-1$
        public static final String GMONTH               = "gMonth";//$NON-NLS-1$
        public static final String GYEAR_MONTH          = "gYearMonth";//$NON-NLS-1$
        public static final String GYEAR                = "gYear";//$NON-NLS-1$
        public static final String HEX_BINARY           = "hexBinary";//$NON-NLS-1$
    }

    /**
     * The names of the built-in datatypes (54 entries) which includes both the
     * XML Schema of schema built-in types and the MetaMatrix built-in types
     */
    public static final class BuiltInNames {
        // XML Schema of schema built-in types
        public static final String STRING               = "string"; //$NON-NLS-1$
        public static final String BOOLEAN              = "boolean";//$NON-NLS-1$
        public static final String BYTE                 = "byte";//$NON-NLS-1$
        public static final String SHORT                = "short";//$NON-NLS-1$
        public static final String INTEGER              = "integer";//$NON-NLS-1$
        public static final String LONG                 = "long";//$NON-NLS-1$
        public static final String FLOAT                = "float";//$NON-NLS-1$
        public static final String DOUBLE               = "double";//$NON-NLS-1$
        public static final String DATE                 = "date";//$NON-NLS-1$
        public static final String TIME                 = "time";//$NON-NLS-1$
        public static final String ENTITIES             = "ENTITIES";//$NON-NLS-1$
        public static final String ENTITY               = "ENTITY";//$NON-NLS-1$
        public static final String IDREFS               = "IDREFS";//$NON-NLS-1$
        public static final String IDREF                = "IDREF";//$NON-NLS-1$
        public static final String ID                   = "ID";//$NON-NLS-1$
        public static final String NCNAME               = "NCName";//$NON-NLS-1$
        public static final String NMTOKENS             = "NMTOKENS";//$NON-NLS-1$
        public static final String NMTOKEN              = "NMTOKEN";//$NON-NLS-1$
        public static final String NOTATION             = "NOTATION";//$NON-NLS-1$
        public static final String NAME                 = "Name";//$NON-NLS-1$
        public static final String QNAME                = "QName";//$NON-NLS-1$
        public static final String ANY_TYPE             = "anyType";//$NON-NLS-1$
        public static final String ANY_SIMPLE_TYPE      = "anySimpleType";//$NON-NLS-1$
        public static final String ANY_URI              = "anyURI";//$NON-NLS-1$
        public static final String BASE64_BINARY        = "base64Binary";//$NON-NLS-1$
        public static final String DATE_TIME            = "dateTime";//$NON-NLS-1$
        public static final String DECIMAL              = "decimal";//$NON-NLS-1$
        public static final String DURATION             = "duration";//$NON-NLS-1$
        public static final String GDAY                 = "gDay";//$NON-NLS-1$
        public static final String GMONTH_DAY           = "gMonthDay";//$NON-NLS-1$
        public static final String GMONTH               = "gMonth";//$NON-NLS-1$
        public static final String GYEAR_MONTH          = "gYearMonth";//$NON-NLS-1$
        public static final String GYEAR                = "gYear";//$NON-NLS-1$
        public static final String HEX_BINARY           = "hexBinary";//$NON-NLS-1$
        public static final String INT                  = "int";//$NON-NLS-1$
        public static final String LANGUAGE             = "language";//$NON-NLS-1$
        public static final String NEGATIVE_INTEGER     = "negativeInteger";//$NON-NLS-1$
        public static final String NON_NEGATIVE_INTEGER = "nonNegativeInteger";//$NON-NLS-1$
        public static final String NON_POSITIVE_INTEGER = "nonPositiveInteger";//$NON-NLS-1$
        public static final String NORMALIZED_STRING    = "normalizedString";//$NON-NLS-1$
        public static final String POSITIVE_INTEGER     = "positiveInteger";//$NON-NLS-1$
        public static final String TOKEN                = "token";//$NON-NLS-1$
        public static final String UNSIGNED_BYTE        = "unsignedByte";//$NON-NLS-1$
        public static final String UNSIGNED_INT         = "unsignedInt";//$NON-NLS-1$
        public static final String UNSIGNED_LONG        = "unsignedLong";//$NON-NLS-1$
        public static final String UNSIGNED_SHORT       = "unsignedShort";//$NON-NLS-1$
        // MetaMatrix extensions to the XML Schema of schema built-in types
        public static final String CHAR                 = MetaMatrixExtendedBuiltInNames.CHAR;
        public static final String BIG_INTEGER          = MetaMatrixExtendedBuiltInNames.BIG_INTEGER;
        public static final String BIG_DECIMAL          = MetaMatrixExtendedBuiltInNames.BIG_DECIMAL;
        public static final String TIMESTAMP            = MetaMatrixExtendedBuiltInNames.TIMESTAMP;
        public static final String OBJECT               = MetaMatrixExtendedBuiltInNames.OBJECT;
        public static final String NULL                 = MetaMatrixExtendedBuiltInNames.NULL;
        public static final String BLOB                 = MetaMatrixExtendedBuiltInNames.BLOB;
        public static final String CLOB                 = MetaMatrixExtendedBuiltInNames.CLOB;
        public static final String XML_LITERAL          = MetaMatrixExtendedBuiltInNames.XML_LITERAL;
    }

    // all the runtime datatypes
    public static final class RuntimeTypeNames {
        public static final String STRING       = "string"; //$NON-NLS-1$
        public static final String BOOLEAN      = "boolean"; //$NON-NLS-1$
        public static final String BYTE         = "byte"; //$NON-NLS-1$
        public static final String SHORT        = "short"; //$NON-NLS-1$
        public static final String CHAR         = "char"; //$NON-NLS-1$
        public static final String INTEGER      = "integer"; //$NON-NLS-1$
        public static final String LONG         = "long"; //$NON-NLS-1$
        public static final String BIG_INTEGER  = "biginteger"; //$NON-NLS-1$
        public static final String FLOAT        = "float"; //$NON-NLS-1$
        public static final String DOUBLE       = "double"; //$NON-NLS-1$
        public static final String BIG_DECIMAL  = "bigdecimal"; //$NON-NLS-1$
        public static final String DATE         = "date"; //$NON-NLS-1$
        public static final String TIME         = "time"; //$NON-NLS-1$
        public static final String TIMESTAMP    = "timestamp"; //$NON-NLS-1$
        public static final String OBJECT       = "object"; //$NON-NLS-1$
        public static final String NULL         = "null"; //$NON-NLS-1$
        public static final String BLOB         = "blob"; //$NON-NLS-1$
        public static final String CLOB         = "clob"; //$NON-NLS-1$
        public static final String XML          = "xml"; //$NON-NLS-1$
    }

    // ==================================================================================
    //                      P U B L I C   M E T H O D S
    // ==================================================================================

    /**
     * Return the name of the default datatype given the runtime type.
     * @param runtimeTypeName the name of the runtime type
     * @return the name of the default datatype
     */
    public static String getDatatypeNamefromRuntimeType(final String runtimeTypeName) {
        return (String) RUNTIME_TO_BUILTIN_MAP.get(runtimeTypeName);
    }

    /**
     * Return the collection of runtime type names
     * @return Collection
     */
    public static Collection getRuntimeTypeNames() {
        return Collections.unmodifiableCollection(RUNTIME_TYPE_NAMES);
    }

    /**
     * Return the collection of built-in datatype names
     * @return Collection
     */
    public static Collection getBuiltInTypeNames() {
        return Collections.unmodifiableCollection(BUILTIN_TYPE_NAMES);
    }

    /**
     * Return the collection of the primitive built-in datatype names. A primitive built-in type
     * is one in which its basetype is the UR-type of "anySimpleType"
     * @return Collection
     */
    public static Collection getPrimitivedBuiltInTypeNames() {
        return Collections.unmodifiableCollection(PRIMITIVE_BUILTIN_TYPE_NAMES);
    }

    /**
     * Return the collection of MetaMatrix extended built-in datatype names.
     * These represent MetaMatrix extensions to the XML Schema of schema built-in types
     * to add the datatypes of "char", "biginteger", "bigdecimal", "timestamp",
     * "object", "null", "blob", "clob" and "XMLLiteral".
     * @return Collection
     */
    public static Collection getMetaMatrixExtendedBuiltInTypeNames() {
        return Collections.unmodifiableCollection(METAMATRIX_EXTENDED_BUILTIN_TYPE_NAMES);
    }

    // ==================================================================================
    //                         P R I V A T E   M E T H O D S
    // ==================================================================================

    private static void loadRuntimeTypeToBuildInConversionMap() {
        RUNTIME_TO_BUILTIN_MAP.put(RuntimeTypeNames.STRING,      BuiltInNames.STRING);
        RUNTIME_TO_BUILTIN_MAP.put(RuntimeTypeNames.BOOLEAN,     BuiltInNames.BOOLEAN);
        RUNTIME_TO_BUILTIN_MAP.put(RuntimeTypeNames.BYTE,        BuiltInNames.BYTE);
        RUNTIME_TO_BUILTIN_MAP.put(RuntimeTypeNames.SHORT,       BuiltInNames.SHORT);
        RUNTIME_TO_BUILTIN_MAP.put(RuntimeTypeNames.CHAR,        BuiltInNames.CHAR);
        RUNTIME_TO_BUILTIN_MAP.put(RuntimeTypeNames.INTEGER,     BuiltInNames.INT);
        RUNTIME_TO_BUILTIN_MAP.put(RuntimeTypeNames.LONG,        BuiltInNames.LONG);
        RUNTIME_TO_BUILTIN_MAP.put(RuntimeTypeNames.BIG_INTEGER, BuiltInNames.BIG_INTEGER);
        RUNTIME_TO_BUILTIN_MAP.put(RuntimeTypeNames.FLOAT,       BuiltInNames.FLOAT);
        RUNTIME_TO_BUILTIN_MAP.put(RuntimeTypeNames.DOUBLE,      BuiltInNames.DOUBLE);
        RUNTIME_TO_BUILTIN_MAP.put(RuntimeTypeNames.BIG_DECIMAL, BuiltInNames.BIG_DECIMAL);
        RUNTIME_TO_BUILTIN_MAP.put(RuntimeTypeNames.DATE,        BuiltInNames.DATE);
        RUNTIME_TO_BUILTIN_MAP.put(RuntimeTypeNames.TIME,        BuiltInNames.TIME);
        RUNTIME_TO_BUILTIN_MAP.put(RuntimeTypeNames.TIMESTAMP,   BuiltInNames.TIMESTAMP);
        RUNTIME_TO_BUILTIN_MAP.put(RuntimeTypeNames.OBJECT,      BuiltInNames.OBJECT);
        RUNTIME_TO_BUILTIN_MAP.put(RuntimeTypeNames.BLOB,        BuiltInNames.BLOB);
        RUNTIME_TO_BUILTIN_MAP.put(RuntimeTypeNames.CLOB,        BuiltInNames.CLOB);
        RUNTIME_TO_BUILTIN_MAP.put(RuntimeTypeNames.XML,         BuiltInNames.XML_LITERAL);
    }
    private static void loadRuntimeTypeNames() {
        RUNTIME_TYPE_NAMES.add(RuntimeTypeNames.STRING);
        RUNTIME_TYPE_NAMES.add(RuntimeTypeNames.BOOLEAN);
        RUNTIME_TYPE_NAMES.add(RuntimeTypeNames.BYTE);
        RUNTIME_TYPE_NAMES.add(RuntimeTypeNames.SHORT);
        RUNTIME_TYPE_NAMES.add(RuntimeTypeNames.CHAR);
        RUNTIME_TYPE_NAMES.add(RuntimeTypeNames.INTEGER);
        RUNTIME_TYPE_NAMES.add(RuntimeTypeNames.LONG);
        RUNTIME_TYPE_NAMES.add(RuntimeTypeNames.BIG_INTEGER);
        RUNTIME_TYPE_NAMES.add(RuntimeTypeNames.FLOAT);
        RUNTIME_TYPE_NAMES.add(RuntimeTypeNames.DOUBLE);
        RUNTIME_TYPE_NAMES.add(RuntimeTypeNames.BIG_DECIMAL);
        RUNTIME_TYPE_NAMES.add(RuntimeTypeNames.DATE);
        RUNTIME_TYPE_NAMES.add(RuntimeTypeNames.TIME);
        RUNTIME_TYPE_NAMES.add(RuntimeTypeNames.TIMESTAMP);
        RUNTIME_TYPE_NAMES.add(RuntimeTypeNames.OBJECT);
        RUNTIME_TYPE_NAMES.add(RuntimeTypeNames.BLOB);
        RUNTIME_TYPE_NAMES.add(RuntimeTypeNames.CLOB);
        RUNTIME_TYPE_NAMES.add(RuntimeTypeNames.XML);
    }
    private static void loadMetaMatrixExtentedBuiltInTypeNames() {
        METAMATRIX_EXTENDED_BUILTIN_TYPE_NAMES.add(MetaMatrixExtendedBuiltInNames.CHAR);
        METAMATRIX_EXTENDED_BUILTIN_TYPE_NAMES.add(MetaMatrixExtendedBuiltInNames.BIG_INTEGER);
        METAMATRIX_EXTENDED_BUILTIN_TYPE_NAMES.add(MetaMatrixExtendedBuiltInNames.BIG_DECIMAL);
        METAMATRIX_EXTENDED_BUILTIN_TYPE_NAMES.add(MetaMatrixExtendedBuiltInNames.TIMESTAMP);
        METAMATRIX_EXTENDED_BUILTIN_TYPE_NAMES.add(MetaMatrixExtendedBuiltInNames.OBJECT);
        METAMATRIX_EXTENDED_BUILTIN_TYPE_NAMES.add(MetaMatrixExtendedBuiltInNames.NULL);
        METAMATRIX_EXTENDED_BUILTIN_TYPE_NAMES.add(MetaMatrixExtendedBuiltInNames.BLOB);
        METAMATRIX_EXTENDED_BUILTIN_TYPE_NAMES.add(MetaMatrixExtendedBuiltInNames.CLOB);
        METAMATRIX_EXTENDED_BUILTIN_TYPE_NAMES.add(MetaMatrixExtendedBuiltInNames.XML_LITERAL);
    }
    private static void loadPrimitiveBuiltInTypeNames() {
        PRIMITIVE_BUILTIN_TYPE_NAMES.add(PrimitiveBuiltInNames.STRING);
        PRIMITIVE_BUILTIN_TYPE_NAMES.add(PrimitiveBuiltInNames.BOOLEAN);
        PRIMITIVE_BUILTIN_TYPE_NAMES.add(PrimitiveBuiltInNames.FLOAT);
        PRIMITIVE_BUILTIN_TYPE_NAMES.add(PrimitiveBuiltInNames.DOUBLE);
        PRIMITIVE_BUILTIN_TYPE_NAMES.add(PrimitiveBuiltInNames.DATE);
        PRIMITIVE_BUILTIN_TYPE_NAMES.add(PrimitiveBuiltInNames.TIME);
        PRIMITIVE_BUILTIN_TYPE_NAMES.add(PrimitiveBuiltInNames.NOTATION);
        PRIMITIVE_BUILTIN_TYPE_NAMES.add(PrimitiveBuiltInNames.QNAME);
        PRIMITIVE_BUILTIN_TYPE_NAMES.add(PrimitiveBuiltInNames.ANY_URI);
        PRIMITIVE_BUILTIN_TYPE_NAMES.add(PrimitiveBuiltInNames.BASE64_BINARY);
        PRIMITIVE_BUILTIN_TYPE_NAMES.add(PrimitiveBuiltInNames.DATE_TIME);
        PRIMITIVE_BUILTIN_TYPE_NAMES.add(PrimitiveBuiltInNames.DECIMAL);
        PRIMITIVE_BUILTIN_TYPE_NAMES.add(PrimitiveBuiltInNames.DURATION);
        PRIMITIVE_BUILTIN_TYPE_NAMES.add(PrimitiveBuiltInNames.GDAY);
        PRIMITIVE_BUILTIN_TYPE_NAMES.add(PrimitiveBuiltInNames.GMONTH_DAY);
        PRIMITIVE_BUILTIN_TYPE_NAMES.add(PrimitiveBuiltInNames.GMONTH);
        PRIMITIVE_BUILTIN_TYPE_NAMES.add(PrimitiveBuiltInNames.GYEAR_MONTH);
        PRIMITIVE_BUILTIN_TYPE_NAMES.add(PrimitiveBuiltInNames.GYEAR);
        PRIMITIVE_BUILTIN_TYPE_NAMES.add(PrimitiveBuiltInNames.HEX_BINARY);
    }
    private static void loadBuiltInTypeNames() {
        BUILTIN_TYPE_NAMES.add(BuiltInNames.STRING);
        BUILTIN_TYPE_NAMES.add(BuiltInNames.BOOLEAN);
        BUILTIN_TYPE_NAMES.add(BuiltInNames.BYTE);
        BUILTIN_TYPE_NAMES.add(BuiltInNames.SHORT);
        BUILTIN_TYPE_NAMES.add(BuiltInNames.CHAR);
        BUILTIN_TYPE_NAMES.add(BuiltInNames.INT);
        BUILTIN_TYPE_NAMES.add(BuiltInNames.LONG);
        BUILTIN_TYPE_NAMES.add(BuiltInNames.BIG_INTEGER);
        BUILTIN_TYPE_NAMES.add(BuiltInNames.FLOAT);
        BUILTIN_TYPE_NAMES.add(BuiltInNames.DOUBLE);
        BUILTIN_TYPE_NAMES.add(BuiltInNames.BIG_DECIMAL);
        BUILTIN_TYPE_NAMES.add(BuiltInNames.DATE);
        BUILTIN_TYPE_NAMES.add(BuiltInNames.TIME);
        BUILTIN_TYPE_NAMES.add(BuiltInNames.TIMESTAMP);
        BUILTIN_TYPE_NAMES.add(BuiltInNames.OBJECT);
        BUILTIN_TYPE_NAMES.add(BuiltInNames.INTEGER);
        BUILTIN_TYPE_NAMES.add(BuiltInNames.NULL);
        BUILTIN_TYPE_NAMES.add(BuiltInNames.BLOB);
        BUILTIN_TYPE_NAMES.add(BuiltInNames.CLOB);
        BUILTIN_TYPE_NAMES.add(BuiltInNames.ENTITIES);
        BUILTIN_TYPE_NAMES.add(BuiltInNames.ENTITY);
        BUILTIN_TYPE_NAMES.add(BuiltInNames.IDREFS);
        BUILTIN_TYPE_NAMES.add(BuiltInNames.IDREF);
        BUILTIN_TYPE_NAMES.add(BuiltInNames.ID);
        BUILTIN_TYPE_NAMES.add(BuiltInNames.NCNAME);
        BUILTIN_TYPE_NAMES.add(BuiltInNames.NMTOKENS);
        BUILTIN_TYPE_NAMES.add(BuiltInNames.NMTOKEN);
        BUILTIN_TYPE_NAMES.add(BuiltInNames.NOTATION);
        BUILTIN_TYPE_NAMES.add(BuiltInNames.NAME);
        BUILTIN_TYPE_NAMES.add(BuiltInNames.QNAME);
        BUILTIN_TYPE_NAMES.add(BuiltInNames.ANY_TYPE);
        BUILTIN_TYPE_NAMES.add(BuiltInNames.ANY_SIMPLE_TYPE);
        BUILTIN_TYPE_NAMES.add(BuiltInNames.ANY_URI);
        BUILTIN_TYPE_NAMES.add(BuiltInNames.BASE64_BINARY);
        BUILTIN_TYPE_NAMES.add(BuiltInNames.DATE_TIME);
        BUILTIN_TYPE_NAMES.add(BuiltInNames.DECIMAL);
        BUILTIN_TYPE_NAMES.add(BuiltInNames.DURATION);
        BUILTIN_TYPE_NAMES.add(BuiltInNames.GDAY);
        BUILTIN_TYPE_NAMES.add(BuiltInNames.GMONTH_DAY);
        BUILTIN_TYPE_NAMES.add(BuiltInNames.GMONTH);
        BUILTIN_TYPE_NAMES.add(BuiltInNames.GYEAR_MONTH);
        BUILTIN_TYPE_NAMES.add(BuiltInNames.GYEAR);
        BUILTIN_TYPE_NAMES.add(BuiltInNames.HEX_BINARY);
        BUILTIN_TYPE_NAMES.add(BuiltInNames.LANGUAGE);
        BUILTIN_TYPE_NAMES.add(BuiltInNames.NEGATIVE_INTEGER);
        BUILTIN_TYPE_NAMES.add(BuiltInNames.NON_NEGATIVE_INTEGER);
        BUILTIN_TYPE_NAMES.add(BuiltInNames.NON_POSITIVE_INTEGER);
        BUILTIN_TYPE_NAMES.add(BuiltInNames.NORMALIZED_STRING);
        BUILTIN_TYPE_NAMES.add(BuiltInNames.POSITIVE_INTEGER);
        BUILTIN_TYPE_NAMES.add(BuiltInNames.TOKEN);
        BUILTIN_TYPE_NAMES.add(BuiltInNames.UNSIGNED_BYTE);
        BUILTIN_TYPE_NAMES.add(BuiltInNames.UNSIGNED_INT);
        BUILTIN_TYPE_NAMES.add(BuiltInNames.UNSIGNED_LONG);
        BUILTIN_TYPE_NAMES.add(BuiltInNames.UNSIGNED_SHORT);
        BUILTIN_TYPE_NAMES.add(BuiltInNames.XML_LITERAL);
    }

    /**
     * Returns whether the given namespace is the XML Schema Enterprise Datatype namespace.
     * @param namespace a namespace.
     * @return whether the given namespace is the XML Schema Enterprise Datatype namespace.
     */
    public static boolean isSchemaEnterpriseDatatypeNamespace(final String namespace)
    {
      return
        SCHEMA_FOR_ENTERPRISE_DATATYPES_URI_2005.equals(namespace);
    }

    /**
     * Returns whether the given namespace prefix is the XML Schema Enterprise Datatype namespace prefix.
     * @param prefix a namespace prefix.
     * @return whether the given namespace prefix is the XML Schema Enterprise Datatype namespace prefix.
     */
    public static boolean isSchemaEnterpriseDatatypeNamespacePrefix(final String prefix)
    {
      return
        PREFIX_FOR_ENTERPRISE_DATATYPES_URI_2005.equals(prefix);
    }
}
