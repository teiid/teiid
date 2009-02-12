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

package com.metamatrix.console.ui.views.vdb;


/** 
 * Data class to hold and connector binding name and UUID.
 * 
 * @since 4.2
 */
public class ConnectorBindingNameAndUUID {
    public static boolean contains(ConnectorBindingNameAndUUID[] array, 
            ConnectorBindingNameAndUUID element) {
        boolean matchFound = false;
        int i = 0;
        while ((!matchFound) && (i < array.length)) {
            if (element.equals(array[i])) {
                matchFound = true;
            } else {
                i++;
            }
        }
        return matchFound;
    }
    
    private String bindingName;
    private String uuid;
    
    public ConnectorBindingNameAndUUID(String name, String uuid) {
        super();
        this.bindingName = name;
        this.uuid = uuid;
    }
    
    public String getBindingName() {
        return bindingName;
    }
    
    public String getUUID() {
        return uuid;
    }
    
    public boolean equals(Object obj) {
        boolean same = false;
        if (obj == this) {
            same = true;
        } else if (obj instanceof ConnectorBindingNameAndUUID) {
            ConnectorBindingNameAndUUID that = (ConnectorBindingNameAndUUID)obj;
            boolean namesSame = this.bindingName.equals(that.getBindingName());
            boolean uuidsSame = this.uuid.equals(that.getUUID());
            same = (namesSame && uuidsSame);
        }
        return same;
    }
}
