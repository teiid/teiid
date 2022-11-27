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

package org.teiid.core.types;


/**
 * This interface represents the transformation from one data type to
 * another.  For instance, from java.lang.String to java.lang.Integer
 * where java.lang.String is the the source type, "java.lang.String"
 * is the source name, etc.
 */
public abstract class Transform {

    /**
     * This method transforms a value of the source type into a value
     * of the target type.
     * @param value Incoming value of source type
     * @return Outgoing value of target type
     * @throws TransformationException if value is an incorrect input type or
     * the transformation fails
     */
    public Object transform(Object value, Class<?> targetType) throws TransformationException {
        if (value == null) {
            return null;
        }
        return transformDirect(value);
    }


    protected abstract Object transformDirect(Object value) throws TransformationException;

    /**
     * Type of the incoming value.
     * @return Source type
     */
    public abstract Class<?> getSourceType();

    /**
     * Name of the source type.
     * @return Name of source type
     */
    public String getSourceTypeName() {
        return DataTypeManager.getDataTypeName(getSourceType());
    }

    /**
     * Type of the outgoing value.
     * @return Target type
     */
    public abstract Class<?> getTargetType();

    /**
     * Name of the target type.
     * @return Name of target type
     */
    public String getTargetTypeName() {
        return DataTypeManager.getDataTypeName(getTargetType());
    }

    /**
     * Get nice display name for GUIs.
     * @return Display name
     */
    public String getDisplayName() {
        return getSourceTypeName() + " to " + getTargetTypeName(); //$NON-NLS-1$
    }

    /**
     * Get description.
     * @return Description of transform
     */
    public String getDescription() {
        return getDisplayName();
    }

    public boolean isExplicit() {
        return false;
    }

    /**
     * Override Object.toString() to do getDisplayName() version.
     * @return String representation of object
     */
    public String toString() {
        return getDisplayName();
    }

}
