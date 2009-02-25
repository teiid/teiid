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

package org.teiid.connector.language;

/**
 * Represents an item in the SELECT clause.  Typically the SELECT clause 
 * contains expressions which are optionally given an output name.  
 */
public interface ISelectSymbol extends ILanguageObject {
    
    /**
     * Determine whether this symbol is named with an alias.
     * @return True if an alias exists
     */
    boolean hasAlias();
    
    /**
     * Return output column name.  This may be the alias name, a name 
     * derived from the expression, or a default name assigned to an 
     * expression.
     * @return Name of the output column
     */
    String getOutputName();
    
    /**
     * Get the expression referenced by this symbol.
     * @return The expression
     */
    IExpression getExpression();

    /**
     * Set whether this symbol is named with an alias.
     * @param alias True if an alias exists
     */
    void setAlias(boolean alias);
    
    /**
     * Set output column name.  This may be the alias name, a name 
     * derived from the expression, or a default name assigned to an 
     * expression.
     * @param name Name of the output column
     */
    void setOutputName(String name);
    
    /**
     * Set the expression referenced by this symbol.
     * @param expression The expression
     */
    void setExpression(IExpression expression);
    
}
