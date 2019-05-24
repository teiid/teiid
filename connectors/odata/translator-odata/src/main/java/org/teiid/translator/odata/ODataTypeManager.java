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
package org.teiid.translator.odata;

import java.sql.Time;
import java.sql.Timestamp;
import java.util.HashMap;

import org.joda.time.LocalDateTime;
import org.joda.time.LocalTime;
import org.odata4j.core.Guid;
import org.odata4j.edm.EdmCollectionType;
import org.odata4j.edm.EdmProperty.CollectionKind;
import org.odata4j.edm.EdmSimpleType;
import org.odata4j.edm.EdmType;
import org.teiid.core.types.DataTypeManager;

public class ODataTypeManager {

    private static HashMap<String, EdmSimpleType> teiidkeyed = new HashMap<String, EdmSimpleType>();
    private static HashMap<String, String> odatakeyed = new HashMap<String, String>();

    static {
        teiidkeyed.put(DataTypeManager.DefaultDataTypes.STRING, EdmSimpleType.STRING);
        teiidkeyed.put(DataTypeManager.DefaultDataTypes.BOOLEAN, EdmSimpleType.BOOLEAN);
        teiidkeyed.put(DataTypeManager.DefaultDataTypes.BYTE, EdmSimpleType.SBYTE);
        teiidkeyed.put(DataTypeManager.DefaultDataTypes.SHORT, EdmSimpleType.INT16);
        teiidkeyed.put(DataTypeManager.DefaultDataTypes.CHAR, EdmSimpleType.STRING);
        teiidkeyed.put(DataTypeManager.DefaultDataTypes.INTEGER, EdmSimpleType.INT32);
        teiidkeyed.put(DataTypeManager.DefaultDataTypes.LONG, EdmSimpleType.INT64);
        teiidkeyed.put(DataTypeManager.DefaultDataTypes.BIG_INTEGER, EdmSimpleType.INT64);
        teiidkeyed.put(DataTypeManager.DefaultDataTypes.FLOAT, EdmSimpleType.SINGLE);
        teiidkeyed.put(DataTypeManager.DefaultDataTypes.DOUBLE, EdmSimpleType.DOUBLE);
        teiidkeyed.put(DataTypeManager.DefaultDataTypes.BIG_DECIMAL, EdmSimpleType.DECIMAL);
        teiidkeyed.put(DataTypeManager.DefaultDataTypes.DATE, EdmSimpleType.DATETIME);
        teiidkeyed.put(DataTypeManager.DefaultDataTypes.TIME,  EdmSimpleType.TIME);
        teiidkeyed.put(DataTypeManager.DefaultDataTypes.TIMESTAMP,  EdmSimpleType.DATETIME);
        teiidkeyed.put(DataTypeManager.DefaultDataTypes.OBJECT,  EdmSimpleType.BINARY); //currently problematic
        teiidkeyed.put(DataTypeManager.DefaultDataTypes.BLOB, EdmSimpleType.BINARY);
        teiidkeyed.put(DataTypeManager.DefaultDataTypes.CLOB, EdmSimpleType.STRING);
        teiidkeyed.put(DataTypeManager.DefaultDataTypes.XML, EdmSimpleType.STRING);
        teiidkeyed.put(DataTypeManager.DefaultDataTypes.VARBINARY, EdmSimpleType.BINARY);

        odatakeyed.put(EdmSimpleType.STRING.getFullyQualifiedTypeName(), DataTypeManager.DefaultDataTypes.STRING);
        odatakeyed.put(EdmSimpleType.BOOLEAN.getFullyQualifiedTypeName(), DataTypeManager.DefaultDataTypes.BOOLEAN);
        odatakeyed.put(EdmSimpleType.BYTE.getFullyQualifiedTypeName(), DataTypeManager.DefaultDataTypes.SHORT);
        odatakeyed.put(EdmSimpleType.SBYTE.getFullyQualifiedTypeName(), DataTypeManager.DefaultDataTypes.BYTE);
        odatakeyed.put(EdmSimpleType.INT16.getFullyQualifiedTypeName(), DataTypeManager.DefaultDataTypes.SHORT);
        odatakeyed.put(EdmSimpleType.INT32.getFullyQualifiedTypeName(), DataTypeManager.DefaultDataTypes.INTEGER);
        odatakeyed.put(EdmSimpleType.INT64.getFullyQualifiedTypeName(), DataTypeManager.DefaultDataTypes.LONG);
        odatakeyed.put(EdmSimpleType.SINGLE.getFullyQualifiedTypeName(), DataTypeManager.DefaultDataTypes.FLOAT);
        odatakeyed.put(EdmSimpleType.DOUBLE.getFullyQualifiedTypeName(), DataTypeManager.DefaultDataTypes.DOUBLE);
        odatakeyed.put(EdmSimpleType.DECIMAL.getFullyQualifiedTypeName(), DataTypeManager.DefaultDataTypes.BIG_DECIMAL);
        odatakeyed.put(EdmSimpleType.TIME.getFullyQualifiedTypeName(), DataTypeManager.DefaultDataTypes.TIME);
        odatakeyed.put(EdmSimpleType.DATETIME.getFullyQualifiedTypeName(), DataTypeManager.DefaultDataTypes.TIMESTAMP);
        odatakeyed.put(EdmSimpleType.DATETIMEOFFSET.getFullyQualifiedTypeName(), DataTypeManager.DefaultDataTypes.TIMESTAMP);
        odatakeyed.put(EdmSimpleType.BINARY.getFullyQualifiedTypeName(), DataTypeManager.DefaultDataTypes.VARBINARY);
        odatakeyed.put(EdmSimpleType.GUID.getFullyQualifiedTypeName(), DataTypeManager.DefaultDataTypes.STRING);
    }

    public static String teiidType(String odataType) {
        if (odataType.startsWith(CollectionKind.Bag.name() + "(")
                && odataType.endsWith(")")) {
            odataType = odataType.substring(4, odataType.length() - 1);
            return odatakeyed.get(odataType)+"[]";
        } else if (odataType.startsWith(CollectionKind.List.name() + "(")
                && odataType.endsWith(")")) {
            odataType = odataType.substring(5, odataType.length() - 1);
            return odatakeyed.get(odataType)+"[]";
        } else if (odataType.startsWith(CollectionKind.Collection.name() + "(")
                && odataType.endsWith(")")) {
            odataType = odataType.substring(11, odataType.length() - 1);
            return odatakeyed.get(odataType)+"[]";
        }
        return odatakeyed.get(odataType);
    }

    public static EdmType odataType(String teiidType) {
        if (DataTypeManager.isArrayType(teiidType)) {
            return new EdmCollectionType(CollectionKind.Collection, odataType(DataTypeManager.getComponentType(teiidType)));
        }
        return teiidkeyed.get(teiidType);
    }

    public static Object convertToTeiidRuntimeType(Object value) {
        if (value == null) {
            return null;
        }
        if (DataTypeManager.getAllDataTypeClasses().contains(value.getClass())) {
            return value;
        }
        if (value instanceof LocalDateTime) {
            return new Timestamp(((LocalDateTime)value).toDateTime().getMillis());
        } else if (value instanceof Guid) {
            return value.toString();
        } else if (value instanceof LocalTime) {
            return new Time(((LocalTime)value).toDateTimeToday().getMillis());
        }
        return value;
    }
}
