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

import java.sql.Array;
import java.sql.SQLException;

import org.teiid.core.CorePlugin;
import org.teiid.core.types.ArrayImpl;
import org.teiid.core.types.DataTypeManager;
import org.teiid.core.types.Transform;
import org.teiid.core.types.TransformationException;

public class ObjectToAnyTransform extends Transform {
	
	public static final ObjectToAnyTransform INSTANCE = new ObjectToAnyTransform(Object.class);

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
    
    @Override
    public Object transform(Object value, Class<?> targetType)
    		throws TransformationException {
    	if (value == null) {
    		return null;
    	}
        if(targetType.isAssignableFrom(value.getClass())) {
            return value;
        }
        
        Transform transform = DataTypeManager.getTransform(value.getClass(), targetType);
        boolean valid = true;
        if (transform instanceof ObjectToAnyTransform) {
        	Object v1 = DataTypeManager.convertToRuntimeType(value, true);
        	if (v1 != value) {
				try {
					return transform(v1, targetType);
				} catch (TransformationException e) {
					throw new TransformationException(
							CorePlugin.Event.TEIID10076, e, CorePlugin.Util.gs(CorePlugin.Event.TEIID10076, getSourceType(),
									targetClass, value));
				}
        	}
        	//TODO: allow casts from integer[] to string[], etc.
        	if (targetType.isArray()) {
        		if (value instanceof Array) {
	    			try {
	    				//TODO: need to use the base type information for non-ArrayImpl values
						Object array = ((Array)value).getArray();
						if (targetType.isAssignableFrom(array.getClass())) {
							if (!(value instanceof ArrayImpl)) {
								return new ArrayImpl((Object[])array);
							}
							return value;
						}
						value = array;
					} catch (SQLException e) {
						throw new TransformationException(e);
					}
        		} 
        		if (value.getClass().isArray() 
        				&& value.getClass().getComponentType().isPrimitive() 
        				&& targetType.getComponentType().isAssignableFrom(convertPrimitiveToObject(value.getClass().getComponentType()))) {
					Object[] result = new Object[java.lang.reflect.Array.getLength(value)];
					for (int i = 0; i < result.length; i++) {
						result[i] = java.lang.reflect.Array.get(value, i);
					}
					return new ArrayImpl(result);
        		}
        	}
        	valid = false;
        }
        
        if (transform == null || !valid) {
            Object[] params = new Object[] { getSourceType(), targetType, value};
              throw new TransformationException(CorePlugin.Event.TEIID10076, CorePlugin.Util.gs(CorePlugin.Event.TEIID10076, params));
        }
        
        try {
            return transform.transform(value, targetType);    
        } catch (TransformationException e) {
            Object[] params = new Object[] { getSourceType(), targetType, value};
              throw new TransformationException(CorePlugin.Event.TEIID10076, e, CorePlugin.Util.gs(CorePlugin.Event.TEIID10076, params));
        }
    }
    
    @Override
    protected Object transformDirect(Object value)
    		throws TransformationException {
    	return value;
    }
    
    /** 
     * @see org.teiid.core.types.Transform#isExplicit()
     */
    public boolean isExplicit() {
        return true;
    }
    
	/**
	 * Convert a primitive class to the corresponding object class
	 * @param clazz
	 * @return
	 */
	public static Class<?> convertPrimitiveToObject(Class<?> clazz) {
		if (!clazz.isPrimitive()) {
			return clazz;
		}
		if      ( clazz == Boolean.TYPE   ) clazz = Boolean.class;
		else if ( clazz == Character.TYPE ) clazz = Character.class;
		else if ( clazz == Byte.TYPE      ) clazz = Byte.class;
		else if ( clazz == Short.TYPE     ) clazz = Short.class;
		else if ( clazz == Integer.TYPE   ) clazz = Integer.class;
		else if ( clazz == Long.TYPE      ) clazz = Long.class;
		else if ( clazz == Float.TYPE     ) clazz = Float.class;
		else if ( clazz == Double.TYPE    ) clazz = Double.class;
		return clazz;
	}
}
