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

package com.metamatrix.common.types.basic;

import com.metamatrix.common.types.AbstractTransform;
import com.metamatrix.common.types.DataTypeManager;
import com.metamatrix.common.types.Transform;
import com.metamatrix.common.types.TransformationException;
import com.metamatrix.core.CorePlugin;

public class ObjectToAnyTransform extends AbstractTransform {

    private Class targetClass;
    
    public ObjectToAnyTransform(Class targetClass) {
        this.targetClass = targetClass;
    }
    
    /**
     * Type of the incoming value.
     * @return Source type
     */
    public Class getSourceType() {
        return DataTypeManager.DefaultDataClasses.OBJECT;
    }
    
    public Class getTargetType() {
        return targetClass;
    }

    public Object transform(Object value) throws TransformationException {
        if(value == null || targetClass.isAssignableFrom(value.getClass())) {
            return value;
        }
        
        Transform transform = DataTypeManager.getTransform(value.getClass(), getTargetType());
        
        if (transform == null || transform instanceof ObjectToAnyTransform) {
            Object[] params = new Object[] { getSourceType(), targetClass, value};
            throw new TransformationException(CorePlugin.Util.getString("ObjectToAnyTransform.Invalid_value", params)); //$NON-NLS-1$
        }
        
        try {
            return transform.transform(value);    
        } catch (TransformationException e) {
            Object[] params = new Object[] { getSourceType(), targetClass, value};
            throw new TransformationException(e, CorePlugin.Util.getString("ObjectToAnyTransform.Invalid_value", params)); //$NON-NLS-1$
        }
    }
    
    /** 
     * @see com.metamatrix.common.types.AbstractTransform#isNarrowing()
     */
    public boolean isNarrowing() {
        return true;
    }
}
