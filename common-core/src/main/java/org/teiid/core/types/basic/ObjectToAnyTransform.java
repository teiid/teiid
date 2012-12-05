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

package org.teiid.core.types.basic;

import org.teiid.core.CorePlugin;
import org.teiid.core.types.DataTypeManager;
import org.teiid.core.types.Transform;
import org.teiid.core.types.TransformationException;

public class ObjectToAnyTransform extends Transform {

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

    public Object transformDirect(Object value) throws TransformationException {
        if(targetClass.isAssignableFrom(value.getClass())) {
            return value;
        }
        
        Transform transform = DataTypeManager.getTransform(value.getClass(), getTargetType());
        boolean valid = true;
        if (transform instanceof ObjectToAnyTransform) {
        	Object v1 = DataTypeManager.convertToRuntimeType(value, true);
        	if (v1 != value) {
				try {
					return transformDirect(v1);
				} catch (TransformationException e) {
					throw new TransformationException(
							CorePlugin.Event.TEIID10076, e, CorePlugin.Util.gs(CorePlugin.Event.TEIID10076, getSourceType(),
									targetClass, value));
				}
        	}
        	valid = false;
        }
        
        if (transform == null || !valid) {
            Object[] params = new Object[] { getSourceType(), targetClass, value};
              throw new TransformationException(CorePlugin.Event.TEIID10076, CorePlugin.Util.gs(CorePlugin.Event.TEIID10076, params));
        }
        
        try {
            return transform.transform(value);    
        } catch (TransformationException e) {
            Object[] params = new Object[] { getSourceType(), targetClass, value};
              throw new TransformationException(CorePlugin.Event.TEIID10076, e, CorePlugin.Util.gs(CorePlugin.Event.TEIID10076, params));
        }
    }
    
    /** 
     * @see org.teiid.core.types.Transform#isExplicit()
     */
    public boolean isExplicit() {
        return true;
    }
}
