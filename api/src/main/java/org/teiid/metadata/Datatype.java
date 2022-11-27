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

import org.teiid.core.TeiidRuntimeException;
import org.teiid.metadata.BaseColumn.NullType;
import org.teiid.metadata.Column.SearchType;

public class Datatype extends AbstractMetadataRecord implements Cloneable {

    private static final long serialVersionUID = -7839335802224393230L;

    public enum Type {
        Basic,
        UserDefined,
        ResultSet,
        Domain
    }

    public enum Variety {
        Atomic,
        List,
        Union,
        Complex
    }

    private static final String DEFAULT_JAVA_CLASS_NAME = "java.lang.Object";  //$NON-NLS-1$

    private int length;
    private int precisionLength;
    private int scale;
    private int radix;
    private boolean isSigned;
    private boolean isAutoIncrement;
    private boolean isCaseSensitive;
    private Type type;
    private SearchType searchType;
    private NullType nullType = NullType.Nullable;
    private String javaClassName = DEFAULT_JAVA_CLASS_NAME;
    private String runtimeTypeName;
    private String basetypeName;
    private Variety varietyType = Variety.Atomic;

    /**
     * Get the length of the type.
     *
     * For string (binary or character) types, it is the number of characters.
     * For all other types it is the byte storage size.
     * @return
     */
    public int getLength() {
        return this.length;
    }

    /**
     * @deprecated
     * @see #getPrecision()
     */
    public int getPrecisionLength() {
        return this.precisionLength;
    }

    public int getPrecision() {
        return this.precisionLength;
    }

    public int getScale() {
        return this.scale;
    }

    public int getRadix() {
        return this.radix;
    }

    public boolean isSigned() {
        return this.isSigned;
    }

    public boolean isAutoIncrement() {
        return this.isAutoIncrement;
    }

    public boolean isCaseSensitive() {
        return this.isCaseSensitive;
    }

    public Type getType() {
        return this.type;
    }

    public boolean isBuiltin() {
        return getType() == Type.Basic;
    }


    public SearchType getSearchType() {
        return this.searchType;
    }

    public NullType getNullType() {
        if (this.nullType == null) {
            return NullType.Unknown;
        }
        return this.nullType;
    }

    public String getJavaClassName() {
        return this.javaClassName;
    }

    public String getRuntimeTypeName() {
        return this.runtimeTypeName;
    }

    public String getBasetypeName() {
        return this.basetypeName;
    }

    public void setBasetypeName(String name) {
        this.basetypeName = name;
    }

    public Variety getVarietyType() {
        return this.varietyType;
    }

    /**
     * @param b
     */
    public void setAutoIncrement(boolean b) {
        isAutoIncrement = b;
    }

    /**
     * @param b
     */
    public void setCaseSensitive(boolean b) {
        isCaseSensitive = b;
    }

    /**
     * @param b
     */
    public void setSigned(boolean b) {
        isSigned = b;
    }

    /**
     * @param string
     */
    public void setJavaClassName(String string) {
        javaClassName = string;
    }

    /**
     * @param i
     */
    public void setLength(int i) {
        length = i;
    }

    /**
     * @param s
     */
    public void setNullType(NullType s) {
        nullType = s;
    }

    /**
     * @param i
     */
    public void setPrecision(int i) {
        precisionLength = i;
    }

    /**
     * @deprecated
     * @see #setPrecision(int)
     */
    public void setPrecisionLength(int i) {
        precisionLength = i;
    }

    /**
     * @param i
     */
    public void setRadix(int i) {
        radix = i;
    }

    /**
     * @param string
     */
    public void setRuntimeTypeName(String string) {
        runtimeTypeName = string;
    }

    /**
     * @param i
     */
    public void setScale(int i) {
        scale = i;
    }

    /**
     * @param s
     */
    public void setSearchType(SearchType s) {
        searchType = s;
    }

    /**
     * @param s
     */
    public void setType(Type s) {
        type = s;
    }

    /**
     * @param s
     */
    public void setVarietyType(Variety s) {
        varietyType = s;
    }

    public String toString() {
        StringBuffer sb = new StringBuffer(100);
        sb.append(getClass().getSimpleName());
        sb.append(" name="); //$NON-NLS-1$
        sb.append(getName());
        sb.append(", basetype name="); //$NON-NLS-1$
        sb.append(getBasetypeName());
        sb.append(", runtimeType="); //$NON-NLS-1$
        sb.append(getRuntimeTypeName());
        sb.append(", javaClassName="); //$NON-NLS-1$
        sb.append(getJavaClassName());
        sb.append(", ObjectID="); //$NON-NLS-1$
        sb.append(getUUID());
        return sb.toString();
    }

    @Override
    public Datatype clone() {
        try {
            return (Datatype) super.clone();
        } catch (CloneNotSupportedException e) {
            throw new TeiidRuntimeException(e);
        }
    }

}