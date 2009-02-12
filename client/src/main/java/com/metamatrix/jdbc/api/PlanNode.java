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
import java.util.Map;

/**
 * Represents one node in a query plan tree.  Every node has a list of child nodes
 * and a Map of property name to property value.  Property names are always strings
 * and property values should always be Java primitive types such as String, Integer, 
 * Boolean, and Lists of those types.  In some cases, a property value may itself also be 
 * a PlanNode.  
 */
public interface PlanNode {

    /**
     * Get the parent node for this node.
     * @return Parent node or null if this node is the root
     */
    PlanNode getParent();

    /**
     * Get the children of this component, which are always of type PlanNode.   
     * @return List of PlanNode
     */
    List getChildren();
    
    /**
     * Get the properties for this component.  Property names are always String.  
     * Property values are typically String, Integer, Boolean, a List of one of 
     * those primitive types, or another PlanNode in rare cases.  
     */
    Map getProperties();
    
}
