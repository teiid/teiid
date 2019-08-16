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

package org.teiid.query.sql.symbol;

import org.teiid.core.util.HashCodeUtil;
import org.teiid.query.sql.LanguageVisitor;

/**
 * <p>This is a subclass of Symbol representing a single element.  An ElementSymbol
 * also is an expression and thus has a type.  Element symbols have a variety of
 * attributes that determine how they are displayed - a flag for displaying fully qualified
 * and an optional vdb name.
 *
 * <p>The "isExternalReference" property indicates whether the element symbol
 * refers to an element from a group outside the current command.  Typically this
 * is set to false.  Common uses when this is set to true are for variables used
 * within a command, correlated elements within a command, etc.
 */
public class ElementSymbol extends Symbol implements DerivedExpression {

    public enum DisplayMode {
        FULLY_QUALIFIED, // symbol name
        OUTPUT_NAME, // default
        SHORT_OUTPUT_NAME}

    private GroupSymbol groupSymbol;
    private Object metadataID;
    private Class<?> type;
    private boolean isExternalReference;
    private boolean isAggregate;

    private DisplayMode displayMode = DisplayMode.OUTPUT_NAME;

    /**
     * Simple constructor taking just a name.  By default will display fully qualified name
     * @param name Name of the symbol, may or may not be fully qualified
     */
    public ElementSymbol(String name) {
        super(name);
    }

    public ElementSymbol(String shortName, GroupSymbol group) {
        this(shortName, group, null);
    }

    public ElementSymbol(String shortName, GroupSymbol group, Class<?> type) {
        this.setShortName(shortName);
        this.groupSymbol = group;
        this.type = type;
    }

    /**
     * Constructor taking a name and a flag whether to display fully qualified.
     * @param name Name of the symbol
     * @param displayFullyQualified True if should display fully qualified
     */
    public ElementSymbol(String name, boolean displayFullyQualified) {
        super(name);
        setDisplayFullyQualified(displayFullyQualified);
    }

    @Override
    public String getName() {
        if (this.groupSymbol != null) {
            return this.groupSymbol.getName() + Symbol.SEPARATOR + this.getShortName();
        }
        return super.getName();
    }

    @Override
    public boolean equals(Object obj) {
        if (this.groupSymbol == null) {
            return super.equals(obj);
        }
        if (obj == this) {
            return true;
        }
        if (!(obj instanceof ElementSymbol)) {
            return false;
        }
        ElementSymbol other = (ElementSymbol)obj;
        if (other.groupSymbol == null) {
            return super.equals(obj);
        }
        return this.groupSymbol.equals(other.groupSymbol) && this.getShortName().equals(other.getShortName());
    }

    @Override
    public int hashCode() {
        if (this.groupSymbol != null) {
            return HashCodeUtil.hashCode(this.groupSymbol.hashCode(), this.getShortName().hashCode());
        }
        return super.hashCode();
    }

    public void setDisplayMode(DisplayMode displayMode) {
        if (displayMode == null) {
            this.displayMode = DisplayMode.OUTPUT_NAME;
        }
        this.displayMode = displayMode;
    }

    public DisplayMode getDisplayMode() {
        return displayMode;
    }

    /**
     * Set whether this element will be displayed as fully qualified
     * @param displayFullyQualified True if should display fully qualified
     */
    public void setDisplayFullyQualified(boolean displayFullyQualified) {
        this.displayMode = displayFullyQualified?DisplayMode.FULLY_QUALIFIED:DisplayMode.SHORT_OUTPUT_NAME;
    }

    /**
     * Get whether this element will be displayed as fully qualified
     * @return True if should display fully qualified
     */
    public boolean getDisplayFullyQualified() {
        return this.displayMode.equals(DisplayMode.FULLY_QUALIFIED);
    }

    /**
     * Set whether this element is an external reference.  An external
     * reference is an element that comes from a group outside the current
     * command context.  Typical uses would be variables and correlated
     * references in subqueries.
     * @param isExternalReference True if element is an external reference
     */
    public void setIsExternalReference(boolean isExternalReference) {
        this.isExternalReference = isExternalReference;
    }

    /**
     * Get whether this element is an external reference to a group
     * outside the command context.
     * @return True if element is an external reference
     */
    public boolean isExternalReference() {
        return this.isExternalReference;
    }

    /**
     * Set the group symbol referred to by this element symbol
     * @param symbol the group symbol to set
     */
    public void setGroupSymbol(GroupSymbol symbol) {
        this.groupSymbol = symbol;
    }

    protected void setName(String name) {
        int index = name.lastIndexOf('.');
        if (index > 0) {
            if (this.groupSymbol != null) {
                throw new AssertionError("Attempt to set an invalid name"); //$NON-NLS-1$
            }
            GroupSymbol gs = new GroupSymbol(new String(name.substring(0, index)));
            this.setGroupSymbol(gs);
            name = new String(name.substring(index + 1));
        } else {
            this.groupSymbol = null;
        }
        super.setShortName(name);
    }

    /**
     * Get the group symbol referred to by this element symbol, may be null before resolution
     * @return Group symbol referred to by this element, may be null
     */
    public GroupSymbol getGroupSymbol() {
        return this.groupSymbol;
    }

    /**
     * Get the metadata ID reference
     * @return Metadata ID reference, may be null before resolution
     */
    public Object getMetadataID() {
        return this.metadataID;
    }

    /**
     * Set the metadata ID reference for this element
     * @param metadataID Metadata ID reference
     */
    public void setMetadataID(Object metadataID) {
        this.metadataID = metadataID;
    }

    /**
     * Get the type of the symbol
     * @return Type of the symbol, may be null before resolution
     */
    public Class<?> getType() {
        return this.type;
    }

    /**
     * Set the type of the symbol
     * @param type New type
     */
    public void setType(Class<?> type) {
        this.type = type;
    }

    public void acceptVisitor(LanguageVisitor visitor) {
        visitor.visit(this);
    }

    /**
     * Return a deep copy of this object.
     * @return Deep copy of this object
     */
    public ElementSymbol clone() {
        ElementSymbol copy = new ElementSymbol(getShortName(), null);
        if(getGroupSymbol() != null) {
            copy.setGroupSymbol(getGroupSymbol().clone());
        }
        copy.setMetadataID(getMetadataID());
        copy.setType(getType());
        copy.setIsExternalReference(isExternalReference());
        copy.outputName = this.outputName;
        copy.setDisplayMode(this.getDisplayMode());
        copy.isAggregate = isAggregate;
        return copy;
    }

    public boolean isAggregate() {
        return isAggregate;
    }

    public void setAggregate(boolean isAggregate) {
        this.isAggregate = isAggregate;
    }

}
