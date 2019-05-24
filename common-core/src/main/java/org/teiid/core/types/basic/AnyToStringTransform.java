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
import org.teiid.core.types.Transform;
import org.teiid.core.types.TransformationException;

/**
 * This class can be subclassed to do a simple anything to String by
 * just calling toString().  Just extend and implement getSouceType().
 */
public class AnyToStringTransform extends Transform {

    private Class<?> sourceType;

    public AnyToStringTransform(Class<?> sourceType) {
        this.sourceType = sourceType;
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
        return String.class;
    }

    /**
     * This method transforms a value of the source type into a value
     * of the target type.
     * @param value Incoming value - Integer
     * @return Outgoing value - String
     * @throws TransformationException if value is an incorrect input type or
     * the transformation fails
     */
    public Object transformDirect(Object value) throws TransformationException {
        String result = value.toString();
        if (result != null && result.length() > DataTypeManager.MAX_STRING_LENGTH) {
            return result.substring(0, DataTypeManager.MAX_STRING_LENGTH);
        }
        return result;
    }

}
