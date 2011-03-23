/*
 * JBoss, Home of Professional Open Source.
 * See the COPYRIGHT.txt file distributed with this work for information
 * regarding copyright ownership.  Some portions may be licensed
 * to Red Hat, Inc. under one or more contributor license agreements.
 * 
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 2.1 of the License, or (at your option) any later version.
 * 
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 * 
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA
 * 02110-1301 USA.
 */

package org.teiid.query.sql.symbol;

import org.teiid.core.types.DataTypeManager;
import org.teiid.core.util.EquivalenceUtil;
import org.teiid.core.util.HashCodeUtil;
import org.teiid.core.util.StringUtil;
import org.teiid.query.QueryPlugin;
import org.teiid.query.sql.LanguageVisitor;

/**
 * <p>This is the server's representation of a metadata group symbol.  The group
 * symbol has a name, an optional definition, and a reference to a real
 * metadata ID.  Typically, a GroupSymbol will be created only from a name and
 * possibly a definition if the group has an alias.  The metadata ID is
 * discovered only when resolving the query.</p>
 *
 * <p>For example, if the original string contained a FROM clause such as
 * "FROM Group1 AS G, Group2", there would be two GroupSymbols created.  The
 * first would have name=G, definition=Group1 and the second would have
 * name=Group2, definition=null.</p>
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
    
    private Object modelMetadataId;
    
    private String outputDefinition;
    private String schema;
    private String canonicalSchema;
    
    /**
     * Cloning constructor 
     * @param name
     * @param canonicalName
     * @param definition
     * @since 4.3
     */
    protected GroupSymbol(String name, String canonicalName, String definition) {
        super(name, canonicalName);
        setDefinition(definition);
    }

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
	 * @param name Definition of the symbol, may be null
	 * @throws IllegalArgumentException If name is null
	 */
	public GroupSymbol(String name, String definition) {
		super(name);
		setDefinition(definition);
	}
	
	public Object getModelMetadataId() {
		return modelMetadataId;
	}
	
	public void setModelMetadataId(Object modelMetadataId) {
		this.modelMetadataId = modelMetadataId;
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
	 * @param meatdataID Metadata ID object
	 * @throws IllegalArgumentException If metadataID is null
	 */
	public void setMetadataID(Object metadataID) {
		if(metadataID == null) {
            throw new IllegalArgumentException(QueryPlugin.Util.getString("ERR.015.010.0016")); //$NON-NLS-1$
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
        return isTempTable || isImplicitTempGroupSymbol();
    }
    
    public boolean isImplicitTempGroupSymbol() {
        return isTempGroupName(getNonCorrelationName());
    }

	/**
	 * Compare two groups and give an ordering.
	 * @param other Other group
	 * @return -1, 0, or 1 depending on how this compares to group
	 */
    public int compareTo(GroupSymbol o) {
    	return getCanonicalName().compareTo(o.getCanonicalName());
	}

	/**
	 * Return a deep copy of this object.
	 * @return Deep copy of the object
	 */
	public GroupSymbol clone() {
		GroupSymbol copy = new GroupSymbol(getShortName(), getCanonical(), getDefinition());
		copy.schema = this.schema;
		copy.canonicalSchema = this.canonicalSchema;
		if(getMetadataID() != null) {
			copy.setMetadataID(getMetadataID());
		}
        copy.setIsTempTable(isTempTable);
        copy.setProcedure(isProcedure);
        copy.setOutputDefinition(this.getOutputDefinition());
        copy.outputName = this.outputName;
        copy.isGlobalTable = isGlobalTable;
        copy.modelMetadataId = modelMetadataId;
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

		if (this.schema == null || other.schema == null) {
			return this.getCanonicalName().equals(other.getCanonicalName());
		}
		return EquivalenceUtil.areEqual(this.getCanonicalSchema(), other.getCanonicalSchema()) && this.getShortCanonicalName().equals(other.getShortCanonicalName());
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

    public boolean isProcedure() {
        return this.isProcedure;
    }

    public void setProcedure(boolean isProcedure) {
        this.isProcedure = isProcedure;
    }
    
    public String getOutputDefinition() {
        return this.outputDefinition;
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
    	if (this.schema != null) {
    		return this.schema + SingleElementSymbol.SEPARATOR + this.getShortName();
    	}
    	return super.getName();
    }
    
    @Override
    public String getCanonicalName() {
    	if (this.schema != null) {
    		return this.getCanonicalSchema() + SingleElementSymbol.SEPARATOR + this.getShortCanonicalName();
    	}
    	return super.getCanonicalName();
    }
    
    @Override
    public int hashCode() {
    	if (this.schema != null) {
    		return HashCodeUtil.hashCode(this.getCanonicalSchema().hashCode(), this.getShortCanonicalName().hashCode());
    	}
    	return super.hashCode();
    }
    
    public void setName(String name) {
    	int index = name.indexOf('.');
    	if (index > 0) {
    		this.schema = new String(name.substring(0, index));
    		name = new String(name.substring(index + 1));
    	} else {
    		this.schema = null;
    	}
    	this.canonicalSchema = null;
    	super.setShortName(name);
    }

	private String getCanonicalSchema() {
		if (this.canonicalSchema == null && this.schema != null) {
			this.canonicalSchema = DataTypeManager.getCanonicalString(StringUtil.toUpperCase(this.schema));
		}
		return canonicalSchema;
	}
	
	public String getSchema() {
		return schema;
	}
}
