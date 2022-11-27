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
 * Represents a procedure parameter
 */
public class ProcedureParameter extends BaseColumn {

    private static final long serialVersionUID = 3484281155208939073L;

    public enum Type {
        In,
        InOut,
        Out,
        ReturnValue
    }

    private Type type;
    private boolean optional;
    private Procedure procedure;
    private boolean isVarArg;

    public void setType(Type type) {
        this.type = type;
    }

    public Type getType() {
        return type;
    }

    @Deprecated
    public void setOptional(boolean optional) {
        this.optional = optional;
    }

    @Deprecated
    public boolean isOptional() {
        return optional;
    }

    public void setProcedure(Procedure procedure) {
        this.procedure = procedure;
    }

    @Override
    public Procedure getParent() {
        return this.procedure;
    }

    public void setVarArg(boolean isVarArg) {
        this.isVarArg = isVarArg;
    }

    public boolean isVarArg() {
        return isVarArg;
    }

    public String toString() {
        return getType()+(isVarArg?"... ":" ")+" "+super.toString(); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
    }

    @Override
    public String getNativeType() {
        String nativeType = super.getNativeType();
        if (nativeType != null) {
            return nativeType;
        }
        nativeType = getProperty(AbstractMetadataRecord.RELATIONAL_PREFIX + "native_type" , false); //$NON-NLS-1$
        if (nativeType != null) {
            this.setNativeType(nativeType);
        }
        return nativeType;
    }
}