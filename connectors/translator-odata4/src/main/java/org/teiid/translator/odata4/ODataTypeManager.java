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
package org.teiid.translator.odata4;

import java.util.HashMap;

import org.teiid.core.types.DataTypeManager;

public class ODataTypeManager {

    private static HashMap<String, String> odataTypes = new HashMap<String, String>();

    static {
        odataTypes.put("Edm.String", DataTypeManager.DefaultDataTypes.STRING);
        odataTypes.put("Edm.Boolean", DataTypeManager.DefaultDataTypes.BOOLEAN);
        odataTypes.put("Edm.Byte", DataTypeManager.DefaultDataTypes.SHORT);
        odataTypes.put("Edm.SByte", DataTypeManager.DefaultDataTypes.BYTE);
        odataTypes.put("Edm.Int16", DataTypeManager.DefaultDataTypes.SHORT);
        odataTypes.put("Edm.Int32", DataTypeManager.DefaultDataTypes.INTEGER);
        odataTypes.put("Edm.Int64", DataTypeManager.DefaultDataTypes.LONG);
        odataTypes.put("Edm.Single", DataTypeManager.DefaultDataTypes.FLOAT);
        odataTypes.put("Edm.Double", DataTypeManager.DefaultDataTypes.DOUBLE);
        odataTypes.put("Edm.Decimal", DataTypeManager.DefaultDataTypes.BIG_DECIMAL);
        odataTypes.put("Edm.Date", DataTypeManager.DefaultDataTypes.DATE);
        odataTypes.put("Edm.TimeOfDay", DataTypeManager.DefaultDataTypes.TIME);
        odataTypes.put("Edm.DateTimeOffset", DataTypeManager.DefaultDataTypes.TIMESTAMP);
        odataTypes.put("Edm.Stream", DataTypeManager.DefaultDataTypes.BLOB);
        odataTypes.put("Edm.Guid", DataTypeManager.DefaultDataTypes.STRING);
        odataTypes.put("Edm.Binary", DataTypeManager.DefaultDataTypes.VARBINARY); //$NON-NLS-1$
    }
    
    public static String teiidType(String odataType, boolean array) {
        String type =  odataTypes.get(odataType);
        if (array) {
           type +="[]";
        }
        return type;
    }
}
