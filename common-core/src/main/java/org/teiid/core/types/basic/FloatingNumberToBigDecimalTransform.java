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

import java.math.BigDecimal;

import org.teiid.core.types.DataTypeManager;
import org.teiid.core.types.Transform;
import org.teiid.core.types.TransformationException;
import org.teiid.core.util.PropertiesUtils;


public class FloatingNumberToBigDecimalTransform extends Transform {

    public static final boolean PRESERVE_APPROXIMATE_SCALE = PropertiesUtils.getHierarchicalProperty("org.teiid.preserveApproximateScale", false, Boolean.class); //$NON-NLS-1$

    private Class<?> sourceType;

    public FloatingNumberToBigDecimalTransform(Class<?> sourceType) {
        this.sourceType = sourceType;
    }

    /**
     * This method transforms a value of the source type into a value
     * of the target type.
     * @param value Incoming value of source type
     * @return Outgoing value of target type
     * @throws TransformationException if value is an incorrect input type or
     * the transformation fails
     */
    public Object transformDirect(Object value) throws TransformationException {
        BigDecimal result = BigDecimal.valueOf(((Number)value).doubleValue());
        if (PRESERVE_APPROXIMATE_SCALE) {
            result = result.setScale(Math.max(result.scale(), (value instanceof Double ? 16 : 8) - result.precision()));
        }
        return result;
    }

    /**
     * Type of the incoming value.
     * @return Source type
     */
    public Class<?> getSourceType() {
        return sourceType;
    }

    /**
     * Type of the outgoing value.
     * @return Target type
     */
    public Class<?> getTargetType() {
        return DataTypeManager.DefaultDataClasses.BIG_DECIMAL;
    }

}
