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
package org.teiid.translator.hbase.phoenix;

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
