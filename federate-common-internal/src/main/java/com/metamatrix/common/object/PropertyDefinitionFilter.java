/*
 * JBoss, Home of Professional Open Source.
 * Copyright (C) 2008 Red Hat, Inc.
 * Copyright (C) 2000-2007 MetaMatrix, Inc.
 * Licensed to Red Hat, Inc. under one or more contributor 
 * license agreements.  See the copyright.txt file in the
 * distribution for a full listing of individual contributors.
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

package com.metamatrix.common.object;

public interface PropertyDefinitionFilter {
    
    /** The filter is not applicable to the PropertyDefinition */
    public static final int NA      = 0;
    /** Treat this PropertyDefinition as hidden */
    public static final int HIDE    = 1;
    /** Treat this PropertyDefinition as enabled */
    public static final int ENABLE  = 2;
    /** Treat this PropertyDefinition as disabled */
    public static final int DISABLE = 3;
    /** Treat this PropertyDefinition as expert */
    public static final int EXPERT  = 4;
    /** Remove this PropertyDefinition */
    public static final int REMOVE  = 5;

    /**
     * Determine whether the given PropertiedObject is applicable to this filter.
     * If the filter is not applicable the calling method does not need to 
     * check all associated PropertyDefinition instances.
     * @param obj the PropertiedObject instance to check
     * @return true if filter applies, or false otherwise.
     */
    boolean canFilter(PropertiedObject obj);

    /**
     * Return the filter code indicating how this PropertyDefinition associated
     * with the specified PropertiedObject instance should be treated by the 
     * calling method. 
     * @param editor PropertiedObjectEditor instance to use in the filter.
     * @param obj the PropertiedObject instance the definition is associated with.
     * @param defn the PropertyDefinition instance to check.
     * @return int the filter code (one of HIDE, ENABLE, DISABLE); never null.
     */
    int filter(PropertiedObjectEditor editor, PropertiedObject obj, PropertyDefinition defn);

}
