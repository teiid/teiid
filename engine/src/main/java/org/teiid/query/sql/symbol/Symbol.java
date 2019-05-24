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

import org.teiid.core.types.DataTypeManager;
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
    private String shortName;

    /**
     * Prior to resolving null, after resolving it is the exact string
     * entered in the query.
     *
     * The AliasGenerator can also set this value as necessary for the data tier.
     */
    protected String outputName;

    /**
     * Character used to delimit name components in a symbol
     */
    public static final String SEPARATOR = "."; //$NON-NLS-1$

    /**
     * Construct a symbol with a name.
     * @param name Name of the symbol
     * @throws IllegalArgumentException If name is null
     */
    public Symbol(String name) {
        this.setName(name);
    }

    public Symbol() {

    }

    protected void setName(String name) {
        setShortName(name);
    }

    /**
     * Change the symbol's name.  This will change the symbol's hash code
     * and canonical name!!!!!!!!!!!!!!!!!  If this symbol is in a hashed
     * collection, it will be lost!
     * @param name New name
     */
    public void setShortName(String name) {
        if(name == null) {
            throw new IllegalArgumentException(QueryPlugin.Util.getString("ERR.015.010.0017")); //$NON-NLS-1$
        }
        this.shortName = DataTypeManager.getCanonicalString(name);
        this.outputName = null;
    }

    /**
     * Get the name of the symbol
     * @return Name of the symbol, never null
     */
    public String getName() {
        return getShortName();
    }

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
        return this.shortName.hashCode();
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
        String otherFqn = ((Symbol)obj).getName();
        String thisFqn = getName();
        return thisFqn.equals(otherFqn);
    }

    /**
     * Return a copy of this object.
     */
    public abstract Object clone();

    public String getOutputName() {
        return this.outputName == null ? getName() : this.outputName;
    }

    public void setOutputName(String outputName) {
        this.outputName = outputName;
    }

    /**
     * Get the short name of the element
     * @return Short name of the symbol (un-dotted)
     */
    public final String getShortName() {
        return shortName;
    }

    public static String getShortName(Expression ex) {
        if (ex instanceof Symbol) {
            return ((Symbol)ex).getShortName();
        }
        return "expr"; //$NON-NLS-1$
    }

    public static String getName(Expression ex) {
        if (ex instanceof Symbol) {
            return ((Symbol)ex).getName();
        }
        return "expr"; //$NON-NLS-1$
    }

    public static String getOutputName(Expression ex) {
        if (ex instanceof Symbol) {
            return ((Symbol)ex).getOutputName();
        }
        return "expr"; //$NON-NLS-1$
    }

    public static String getShortName(String name) {
        int index = name.lastIndexOf(Symbol.SEPARATOR);
        if(index >= 0) {
            return name.substring(index+1);
        }
        return name;
    }

}
