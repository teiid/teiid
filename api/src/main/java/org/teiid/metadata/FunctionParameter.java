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

package org.teiid.metadata;


/**
 * A function parameter defines the name and description of an input or output
 * parameter for a function.  The name should not be null, but that is not
 * verified by this class.  The type string used in the function
 * parameter should be one of the standard type names defined in
 * {@link org.teiid.core.types.DataTypeManager.DefaultDataTypes}.
 */
public class FunctionParameter extends BaseColumn {
    private static final long serialVersionUID = -4696050948395485266L;

    public static final String OUTPUT_PARAMETER_NAME = "result"; //$NON-NLS-1$

    private boolean isVarArg;
    private FunctionMethod parent;

    /**
     * Construct a function parameter with no attributes.
     */
    public FunctionParameter() {
    }

    /**
     * Construct a function parameter with no description.
     * @param name Name
     * @param type Type from standard set of types
     */
    public FunctionParameter(String name, String type) {
        this(name, type, null);
    }

    /**
     * Construct a function parameter with all attributes.
     * @param name Name
     * @param type Type from standard set of types
     * @param description Description
     */
    public FunctionParameter(String name, String type, String description) {
        this(name, type, description, false);
    }

    public FunctionParameter(String name, String type, String description, boolean vararg) {
        setName(name);
        setType(type);
        setDescription(description);
        this.isVarArg = vararg;
    }

    /**
     * Get description of parameter
     * @return Description
     */
    public String getDescription() {
        return getAnnotation();
    }

    /**
     * Set description of parameter
     * @param description Description
     */
    public void setDescription(String description) {
        this.setAnnotation(description);
    }

    /**
     * Get type of parameter
     * @return Type name
     * @see org.teiid.core.types.DataTypeManager.DefaultDataTypes
     * @deprecated use {@link #getRuntimeType()}
     */
    public String getType() {
        return getRuntimeType();
    }

    /**
     * Set type of parameter
     * @param type Type of parameter
     * @see org.teiid.core.types.DataTypeManager.DefaultDataTypes
     * @deprecated use {@link #setRuntimeType(String)}
     */
    public void setType(String type) {
        setRuntimeType(type);
    }

    /**
     * Return hash code for this parameter.  The hash code is based only
     * on the type of the parameter.  Changing the type of the parameter
     * after placing this object in a hashed collection will likely cause
     * the object to be lost.
     * @return Hash code
     */
    public int hashCode() {
        if(this.getRuntimeType() == null) {
            return 0;
        }
        return this.getRuntimeType().hashCode();
    }

    /**
     * Compare with other object for equality.  Equality is based on whether
     * the type is the same as the other parameter.
     * @return True if equal to obj
     */
    public boolean equals(Object obj) {
        if(obj == this) {
            return true;
        }
        if(!(obj instanceof FunctionParameter)) {
            return false;
        }
        FunctionParameter other = (FunctionParameter) obj;
        if(other.getType() == null) {
            return (this.getType() == null);
        }
        return other.getType().equals(this.getType()) && this.isVarArg == other.isVarArg;
    }

    /**
     * Return string version for debugging purposes
     * @return String representation of function parameter
     */
    public String toString() {
        return getRuntimeType() + (isVarArg?"... ":" ") + super.toString(); //$NON-NLS-1$ //$NON-NLS-2$
    }

    public void setVarArg(boolean isVarArg) {
        this.isVarArg = isVarArg;
    }

    public boolean isVarArg() {
        return isVarArg;
    }

    public void setParent(FunctionMethod functionMethod) {
        this.parent = functionMethod;
    }

    @Override
    public FunctionMethod getParent() {
        return parent;
    }

}
