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

package com.metamatrix.common.object;

import java.util.HashMap;
import java.util.Map;

public class PropertyAccessPolicyImpl implements PropertyAccessPolicy {
    private Map propObjects = null;
    private Map propObjectPolicies = null;

    public PropertyAccessPolicyImpl() {
        this.propObjects = new HashMap();
        this.propObjectPolicies = new HashMap();
    }

    public boolean isReadOnly(PropertiedObject obj) {
        Object readPrivilege = this.propObjects.get(obj);
        return readPrivilege == null ? PropertyAccessPolicy.DEFAULT_READ_ONLY_PRIVILEGE : ((Boolean)readPrivilege).booleanValue();
    }

    public boolean isReadOnly(PropertiedObject obj, PropertyDefinition def) {
        Object policies = this.propObjectPolicies.get(obj);
        if (policies != null) {
            Object readPrivilege = ((HashMap)policies).get(def);
            return readPrivilege == null ? PropertyAccessPolicy.DEFAULT_READ_ONLY_PRIVILEGE : ((Boolean)readPrivilege).booleanValue();
        }
        return PropertyAccessPolicy.DEFAULT_READ_ONLY_PRIVILEGE;
    }

    public void setReadOnly(PropertiedObject obj, PropertyDefinition def, boolean readOnly) {
        Object policies = this.propObjectPolicies.get(obj);
        if (policies == null) {
            policies = new HashMap();
        }
        ((HashMap)policies).put(def, Boolean.valueOf(readOnly));
    }

    public void setReadOnly(PropertiedObject obj, boolean readOnly) {
        this.propObjects.put(obj, Boolean.valueOf(readOnly));
    }

    public void reset(PropertiedObject obj) {
        this.propObjects.remove(obj);
        this.propObjectPolicies.remove(obj);
    }
}
