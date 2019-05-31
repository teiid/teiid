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

package org.teiid.core.types.basic;

import java.sql.Array;
import java.sql.SQLException;

import org.teiid.core.CorePlugin;
import org.teiid.core.types.ArrayImpl;
import org.teiid.core.types.DataTypeManager;
import org.teiid.core.types.DataTypeManager.DefaultDataClasses;
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
                if (value.getClass().isArray()) {
                    if (value.getClass().getComponentType().isPrimitive()
                            && targetType.getComponentType().isAssignableFrom(convertPrimitiveToObject(value.getClass().getComponentType()))) {
                        Object[] result = (Object[]) java.lang.reflect.Array.newInstance(targetType.getComponentType(), java.lang.reflect.Array.getLength(value));
                        for (int i = 0; i < result.length; i++) {
                            result[i] = java.lang.reflect.Array.get(value, i);
                        }
                        return new ArrayImpl(result);
                    }
                    Class<?> targetComponentType = targetType.getComponentType();
                    Object[] result = (Object[]) java.lang.reflect.Array.newInstance(targetComponentType, java.lang.reflect.Array.getLength(value));
                    for (int i = 0; i < result.length; i++) {
                        Object v = java.lang.reflect.Array.get(value, i);
                        if (v.getClass() == targetComponentType || DefaultDataClasses.OBJECT == targetComponentType) {
                            result[i] = v;
                        } else {
                            Transform subTransform = DataTypeManager.getTransform(v.getClass(), targetComponentType);
                            if (subTransform == null) {
                                valid = false;
                                break;
                            }
                            result[i] = subTransform.transform(v, targetComponentType);
                        }
                    }
                    if (valid) {
                        return new ArrayImpl(result);
                    }
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
     */
    public static Class<?> convertPrimitiveToObject(Class<?> clazz) {
        if (!clazz.isPrimitive()) {
            return clazz;
        }
        if      ( clazz == Boolean.TYPE   ) { clazz = Boolean.class; }
        else if ( clazz == Character.TYPE ) { clazz = Character.class; }
        else if ( clazz == Byte.TYPE      ) { clazz = Byte.class; }
        else if ( clazz == Short.TYPE     ) { clazz = Short.class; }
        else if ( clazz == Integer.TYPE   ) { clazz = Integer.class; }
        else if ( clazz == Long.TYPE      ) { clazz = Long.class; }
        else if ( clazz == Float.TYPE     ) { clazz = Float.class; }
        else if ( clazz == Double.TYPE    ) { clazz = Double.class; }
        return clazz;
    }
}
