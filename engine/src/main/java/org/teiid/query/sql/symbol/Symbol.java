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
import org.teiid.core.util.StringUtil;
import org.teiid.query.QueryPlugin;
import org.teiid.query.sql.LanguageObject;
import org.teiid.query.sql.visitor.SQLStringVisitor;


/**
 * This is the server's representation of a metadata symbol.  The only thing
 * a symbol has to have is a name.  This name relates only to how a symbol is
 * specified in a user's query and does not necessarily relate to any
 * actual metadata identifier (although it may).  Subclasses of this class
 * provide specialized instances of symbol for various circumstances in a
 * user's query.  In the context of a single query, a symbol's name has
 * a unique meaning although it may be used more than once in some circumstances.
 */
public abstract class Symbol implements LanguageObject {

	/** 
	 * Name of the symbol
	 * 
	 * Prior to resolving it is the name as entered in the query,
	 * after resolving it is the fully qualified name.
	 */
	private String name;

	/** 
	 * upper case of name
	 */
	private String canonicalName;
	
	/**
	 * Prior to resolving null, after resolving it is the exact string
	 * entered in the query.
	 * 
	 * The AliasGenerator can also set this value as necessary for the data tier.
	 */
    private String outputName;
    
    /**
     * Constructor to be used for cloning instances. Calls to String.toUpperCase() to generate the canonical names
     * were the big performance hit during cloning, so this method allows subclasses to simply pass in the canonical name
     * to avoid toUpperCase().
     * @param name
     * @param canonicalName
     * @since 4.3
     */
    protected Symbol(String name, String canonicalName) {
        this.name = name;
        this.canonicalName = canonicalName;
    }

	/**
	 * Construct a symbol with a name.
	 * @param name Name of the symbol
	 * @throws IllegalArgumentException If name is null
	 */
	public Symbol(String name) {
		setName(name);
	}

	/**
	 * Change the symbol's name.  This will change the symbol's hash code
	 * and canonical name!!!!!!!!!!!!!!!!!  If this symbol is in a hashed
	 * collection, it will be lost!
	 * @param name New name
	 */
	public void setName(String name) {
		if(name == null) {
            throw new IllegalArgumentException(QueryPlugin.Util.getString("ERR.015.010.0017")); //$NON-NLS-1$
		}
		this.name = DataTypeManager.getCanonicalString(name);
		this.outputName = null;
        // Canonical name is lazily created
        this.canonicalName = null;
	}

	/**
	 * Get the name of the symbol
	 * @return Name of the symbol, never null
	 */
	public String getName() {
		return this.name;
	}

	/**
	 * Get canonical name for comparisons
	 * @return Canonical name for comparisons
	 */
	public String getCanonicalName() {
        // PERFORMANCE1: String.toUpperCase() is an expensive call, and calls from this class are the largest component
        // of the total toUpperCase() calls, so we are lazily performing the toUpperCase and calculating the hash.
        computeCanonicalNameAndHash();
        return this.canonicalName;
	}
	
	public void setCanonicalName(String canonicalName) {
		this.canonicalName = canonicalName;
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
	public abstract boolean isResolved();

	/**
	 * Returns string representation of this symbol.
	 * @return String representing the symbol
	 */
	public String toString() {
		return SQLStringVisitor.getSQLString(this);
	}

	/**
	 * Return a hash code for this symbol.
	 * @return Hash code
	 */
	public int hashCode() {
        computeCanonicalNameAndHash();
		// Return cached hash code
		return this.canonicalName.hashCode();
	}

	/**
	 * Compare the symbol based ONLY on name.  Symbols are not compared based on
	 * their underlying physical metadata IDs but rather on their representation
	 * in the context of a particular query.  Case is not important when comparing
	 * symbol names.
	 * @param obj Other object
	 * @return True if other obj is a Symbol (or subclass) and name is equal
	 */
	public boolean equals(Object obj) {
		if(obj == this) {
			return true;
		}

		if(!(obj instanceof Symbol)) {
			return false;
		}
		return ((Symbol)obj).getCanonicalName().equals(getCanonicalName());
	}

	/**
	 * Return a copy of this object.
	 */
	public abstract Object clone();
    
    protected String getCanonical() {
        return canonicalName;
    }
    
    private void computeCanonicalNameAndHash() {
        if (canonicalName == null) {
            canonicalName = DataTypeManager.getCanonicalString(StringUtil.toUpperCase(name));
        }
    }
    
    public String getOutputName() {
        return this.outputName == null ? getName() : this.outputName;
    }

    public void setOutputName(String outputName) {
        this.outputName =outputName;
    }

}
