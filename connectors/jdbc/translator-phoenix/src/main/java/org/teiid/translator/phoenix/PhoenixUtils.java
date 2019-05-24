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
package org.teiid.translator.phoenix;

import org.teiid.metadata.Column;
import org.teiid.translator.TypeFacility;

public class PhoenixUtils {

    public static final String QUOTE = "\""; //$NON-NLS-1$

    /*
     * Convert teiid type to phoenix type, the following types not support by phoenix
     *    object -> Any
     *    blob   -> java.sql.Blob
     *    clob   -> java.sql.Clob
     *    xml    -> java.sql.SQLXML
     */
    public static String convertType(Column column) {
        Class<?> clas = column.getJavaType();
        String typeName = TypeFacility.getDataTypeName(clas);
        return convertType(typeName);
    }

    public static String convertType(String type) {

        if(type.equals(TypeFacility.RUNTIME_NAMES.STRING)){
            return "VARCHAR"; //$NON-NLS-1$
        } else if(type.equals(TypeFacility.RUNTIME_NAMES.BOOLEAN)){
            return "BOOLEAN"; //$NON-NLS-1$
        } else if(type.equals(TypeFacility.RUNTIME_NAMES.BYTE)){
            return "TINYINT"; //$NON-NLS-1$
        }  else if(type.equals(TypeFacility.RUNTIME_NAMES.SHORT)){
            return "SMALLINT"; //$NON-NLS-1$
        } else if(type.equals(TypeFacility.RUNTIME_NAMES.CHAR)){
            return "CHAR"; //$NON-NLS-1$
        } else if(type.equals(TypeFacility.RUNTIME_NAMES.INTEGER)){
            return "INTEGER"; //$NON-NLS-1$
        } else if(type.equals(TypeFacility.RUNTIME_NAMES.LONG)){
            return "LONG"; //$NON-NLS-1$
        } else if(type.equals(TypeFacility.RUNTIME_NAMES.BIG_INTEGER)){
            return "LONG"; //$NON-NLS-1$
        } else if(type.equals(TypeFacility.RUNTIME_NAMES.FLOAT)){
            return "FLOAT"; //$NON-NLS-1$
        } else if(type.equals(TypeFacility.RUNTIME_NAMES.DOUBLE)){
            return "DOUBLE"; //$NON-NLS-1$
        } else if(type.equals(TypeFacility.RUNTIME_NAMES.BIG_DECIMAL)){
            return "DECIMAL"; //$NON-NLS-1$
        } else if(type.equals(TypeFacility.RUNTIME_NAMES.DATE)){
            return "DATE"; //$NON-NLS-1$
        } else if(type.equals(TypeFacility.RUNTIME_NAMES.TIME)){
            return "TIME"; //$NON-NLS-1$
        } else if(type.equals(TypeFacility.RUNTIME_NAMES.TIMESTAMP)){
            return "TIMESTAMP"; //$NON-NLS-1$
        } else if(type.equals(TypeFacility.RUNTIME_NAMES.VARBINARY)){
            return "VARBINARY"; //$NON-NLS-1$
        } else if(type.equals(TypeFacility.RUNTIME_NAMES.CLOB)){
            return "VARCHAR"; //$NON-NLS-1$
        }

        return "BINARY"; //$NON-NLS-1$
    }

}
