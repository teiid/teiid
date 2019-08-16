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

import org.teiid.core.util.EquivalenceUtil;
import org.teiid.core.util.HashCodeUtil;
import org.teiid.query.QueryPlugin;
import org.teiid.query.metadata.TempMetadataAdapter;
import org.teiid.query.metadata.TempMetadataID;
import org.teiid.query.sql.LanguageVisitor;

/**
 * <p>This is the server's representation of a metadata group symbol.  The group
 * symbol has a name, an optional definition, and a reference to a real
 * metadata ID.  Typically, a GroupSymbol will be created only from a name and
 * possibly a definition if the group has an alias.  The metadata ID is
 * discovered only when resolving the query.
 *
 * <p>For example, if the original string contained a FROM clause such as
 * "FROM Group1 AS G, Group2", there would be two GroupSymbols created.  The
 * first would have name=G, definition=Group1 and the second would have
 * name=Group2, definition=null.
 */
public class GroupSymbol extends Symbol implements Comparable<GroupSymbol> {

    public static final String TEMP_GROUP_PREFIX = "#"; //$NON-NLS-1$

    /** Definition of the symbol, may be null */
    private String definition;

    /** Actual metadata ID */
    private Object metadataID;

    private boolean isTempTable;
    private boolean isGlobalTable;
    private boolean isProcedure;

    private String outputDefinition;
    //possible qualifier, not included with the short name
    //due do legacy choice this is ambiguous with schema and may be part of the name
    //TODO: refactor to be a proper schema reference
    private String qualifier;

    private Object checkMatViewStatus;

    /**
     * Construct a symbol with a name.
     * @param name Name of the symbol
     * @throws IllegalArgumentException If name is null
     */
    public GroupSymbol(String name) {
        super(name);
    }

    /**
     * Construct a symbol with a name.
     * @param name Name of the symbol
     * @param definition Definition of the symbol, may be null
     * @throws IllegalArgumentException If name is null
     */
    public GroupSymbol(String name, String definition) {
        super(name);
        setDefinition(definition);
    }

    private GroupSymbol(String schema, String shortName, String definition) {
        this.qualifier = schema;
        this.setShortName(shortName);
        this.setDefinition(definition);
    }

    public Object getModelMetadataId() {
        if (getMetadataID() instanceof TempMetadataID) {
            return ((TempMetadataID)getMetadataID()).getTableData().getModel();
        }
        return null;
    }

    public String getNonCorrelationName() {
        if (this.definition == null) {
            return this.getName();
        }
        return this.getDefinition();
    }

    /**
     * Get the definition for the group symbol, which may be null
     * @return Group definition, may be null
     */
    public String getDefinition() {
        return definition;
    }

    /**
     * Set the definition for the group symbol, which may be null
     * @param definition Definition
     */
    public void setDefinition(String definition) {
        this.definition = definition;
        this.outputDefinition = definition;
    }

    /**
     * Get the metadata ID that this group symbol resolves to.  If
     * the group symbol has not been resolved yet, this will be null.
     * If the symbol has been resolved, this will never be null.
     * @return Metadata ID object
     */
    public Object getMetadataID() {
        return metadataID;
    }

    public void acceptVisitor(LanguageVisitor visitor) {
        visitor.visit(this);
    }

    /**
     * Set the metadata ID that this group symbol resolves to.  It cannot
     * be null.
     * @param metadataID Metadata ID object
     * @throws IllegalArgumentException If metadataID is null
     */
    public void setMetadataID(Object metadataID) {
        if(metadataID == null) {
            throw new IllegalArgumentException(QueryPlugin.Util.getString("ERR.015.010.0016")); //$NON-NLS-1$
        }
        if (this.isImplicitTempGroupSymbol()) {
            this.isTempTable = true;
        }
        this.metadataID = metadataID;
    }

    /**
     * Returns true if this symbol has been completely resolved with respect
     * to actual runtime metadata.  A resolved symbol has been validated that
     * it refers to actual metadata and will have references to the real metadata
     * IDs if necessary.  Different types of symbols determine their resolution
     * in different ways, so this method is abstract and must be implemented
     * by subclasses.
     * @return True if resolved with runtime metadata
     */
    public boolean isResolved() {
        return (metadataID != null);
    }

    /**
     * Returns true if this is a symbol for a temporary (implicit or explicit) group
     * May return false for explicit temp tables prior to resolving.
     * see {@link #isTempTable()}
     * @return
     * @since 5.5
     */
    public boolean isTempGroupSymbol() {
        return isTempTable || (metadataID == null && isImplicitTempGroupSymbol());
    }

    public boolean isImplicitTempGroupSymbol() {
        return isTempGroupName(getNonCorrelationName());
    }

    /**
     * Compare two groups and give an ordering.
     * @param o Other group
     * @return -1, 0, or 1 depending on how this compares to group
     */
    public int compareTo(GroupSymbol o) {
        return getName().compareTo(o.getName());
    }

    /**
     * Return a deep copy of this object.
     * @return Deep copy of the object
     */
    public GroupSymbol clone() {
        GroupSymbol copy = new GroupSymbol(qualifier, getShortName(), getDefinition());
        copy.metadataID = this.metadataID;
        copy.setIsTempTable(isTempTable);
        copy.setProcedure(isProcedure);
        copy.outputDefinition = this.outputDefinition;
        copy.outputName = this.outputName;
        copy.isGlobalTable = isGlobalTable;
        copy.checkMatViewStatus = checkMatViewStatus;
        return copy;
    }

    /**
     * Compare group symbols
     * @param obj Other object to compare
     * @return True if equivalent
     */
    public boolean equals(Object obj) {
        if(this == obj) {
            return true;
        }

        if(!(obj instanceof GroupSymbol)) {
            return false;
        }
        GroupSymbol other = (GroupSymbol) obj;

        if (this.qualifier == null || other.qualifier == null) {
            return this.getName().equals(other.getName());
        }
        return EquivalenceUtil.areEqual(this.qualifier, other.qualifier) && this.getShortName().equals(other.getShortName());
    }

    public boolean hasAlias() {
        return getDefinition() != null;
    }

    public void setIsTempTable(boolean isTempTable) {
        this.isTempTable = isTempTable;
    }

    public static boolean isTempGroupName(String name) {
        if (name == null)
            return false;
        return name.startsWith(TEMP_GROUP_PREFIX);
    }

    /**
     * Returns if this is a Temp Table
     * Set after resolving.
     * @return
     */
    public boolean isTempTable() {
        return this.isTempTable;
    }

    /**
     * Returns if this is a pushed Common Table
     * Set after resolving and initial common table planning
     * @return
     */
    public boolean isPushedCommonTable() {
        return isTempTable && TempMetadataAdapter.getActualMetadataId(metadataID) == metadataID;
    }

    public boolean isProcedure() {
        return this.isProcedure;
    }

    public void setProcedure(boolean isProcedure) {
        this.isProcedure = isProcedure;
    }

    public String getOutputDefinition() {
        return this.outputDefinition == null?this.getDefinition():this.outputDefinition;
    }

    public void setOutputDefinition(String outputDefinition) {
        this.outputDefinition = outputDefinition;
    }

    public boolean isGlobalTable() {
        return isGlobalTable;
    }

    public void setGlobalTable(boolean isGlobalTable) {
        this.isGlobalTable = isGlobalTable;
    }

    @Override
    public String getName() {
        if (this.qualifier != null) {
            return this.qualifier + Symbol.SEPARATOR + this.getShortName();
        }
        return super.getName();
    }

    @Override
    public int hashCode() {
        if (this.qualifier != null) {
            return HashCodeUtil.hashCode(this.qualifier.hashCode(), this.getShortName().hashCode());
        }
        return super.hashCode();
    }

    public void setName(String name) {
        int index = name.indexOf('.');
        if (index > 0) {
            this.qualifier = new String(name.substring(0, index));
            name = new String(name.substring(index + 1));
        } else {
            this.qualifier = null;
        }
        super.setShortName(name);
    }

    public void setCheckMatStatus(Object viewMatadataId) {
        this.checkMatViewStatus = viewMatadataId;
    }

    public Object getCheckMatViewStatus() {
        return this.checkMatViewStatus;
    }
}
