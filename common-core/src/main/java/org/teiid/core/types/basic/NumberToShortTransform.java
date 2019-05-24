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

import org.teiid.core.types.DataTypeManager;
import org.teiid.core.types.TransformationException;

public class NumberToShortTransform extends NumberToNumberTransform {

    private boolean isNarrowing;

    public NumberToShortTransform(Class<?> sourceType, boolean isNarrowing) {
        super(Short.MIN_VALUE, Short.MAX_VALUE, sourceType);
        this.isNarrowing = isNarrowing;
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
        if (isNarrowing) {
            checkValueRange(value);
        }
        return Short.valueOf(((Number)value).shortValue());
    }

    /**
     * Type of the outgoing value.
     * @return Target type
     */
    public Class<?> getTargetType() {
        return DataTypeManager.DefaultDataClasses.SHORT;
    }

    @Override
    public boolean isExplicit() {
        return isNarrowing;
    }

}
