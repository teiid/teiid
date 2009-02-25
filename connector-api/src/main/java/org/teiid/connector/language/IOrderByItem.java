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
 * Represents a single item in the ORDER BY clause.
 */
public interface IOrderByItem extends ILanguageObject {

    public static final boolean ASC = true;
    public static final boolean DESC = false;

    /**
     * Get name of the output column to order by
     * @return Name of output column, as specified in {@link ISelectSymbol#getOutputName()}.
     */
    String getName();
    
    /**
     * Get direction of whether to sort ascending or descending.
     * @return {@link #ASC} for ascending or {@link #DESC} for descending
     */
    boolean getDirection();

    /**
     * Set name of the output column to order by
     * @param name Name of output column, as specified in {@link ISelectSymbol#getOutputName()}.
     */
    void setName(String name);
    
    /**
     * Set direction of whether to sort ascending or descending.
     * @param direction {@link #ASC} for ascending or {@link #DESC} for descending
     */
    void setDirection(boolean direction);

    /**
     * Get the element referred to by this item
     * @return The element, may be null
     */
    IElement getElement();
    
    /**
     * Set the new element for this order by
     * @param element New element
     */
    void setElement(IElement element);

}
