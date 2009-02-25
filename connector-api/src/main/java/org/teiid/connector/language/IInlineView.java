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
 * An inline view represents a subquery in the FROM clause that defines a 
 * query-able context for the outer query.  An inline view must be named, which allows
 * them to be treated like aliased groups.
 * 
 */
public interface IInlineView extends IGroup, ISubqueryContainer {

    String getName();

    void setName(String name);
    
    /**
     * Sets the string that represents the query used within the inline view.
     *  
     * @param output
     * @since 5.0
     */
    void setOutput(String output);
    
    /**
     * Gets the output for the subquery once it has been preprocessed.
     * Returns null if the subquery has not yet been converted to output form.
     *  
     * @return
     * @since 4.3
     */
    String getOutput();
}
