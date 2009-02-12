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

package com.metamatrix.jdbc.api;

import java.util.List;

/**
 * This interface is used to plug in display-specific information for the 
 * {@link TextOutputVisitor}.       
 */
public interface DisplayHelper {

    /**
     * Get the name to use for this plan node.  An obvious implementation
     * is to use the node's type. 
     * @param node The node
     * @return The name, based on the node's information
     */
    String getName(PlanNode node); 
    
    /**
     * Get the description to use for this plan node, which is usually displayed
     * after the name as a short description.  The description should pull out 
     * the most important information for a particular node type.
     * @param node The node
     * @return The description
     */
    String getDescription(PlanNode node);
    
    /**
     * Determine the proper order to display the properties of the node.  The 
     * returned List contains the property names as found in the node's 
     * properties in the preferred order.  If properties are omitted from the list,
     * they will not be displayed.  Thus, this method can also filter what properties
     * are shown.
     * @param node The node
     * @return List of property names
     */ 
    List getOrderedProperties(PlanNode node);
    
    /**
     * Get display name for a particular property name.  
     * @param property The property name, as used in the node's properties
     * @return The desired display name for the property
     */ 
    String getPropertyName(String property); 
    
    /**
     * A max description length so that the description can be truncated when necessary.
     * @param length Max string length
     */
    void setMaxDescriptionLength(int length);
    
}
