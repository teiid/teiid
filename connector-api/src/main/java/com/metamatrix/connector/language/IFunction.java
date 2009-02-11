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

package com.metamatrix.connector.language;

/**
 * Represents a function in the language.  A function has a name and 0..n 
 * Expressions that are parameters.  
 */
public interface IFunction extends IExpression {

    /**
     * Get name of the function
     * @return Function name
     */
    String getName();

    /**
     * Get the parameters used in this function.
     * @return Array of IExpressions defining the parameters
     */
    IExpression[] getParameters();
    
    /**
     * Set name of the function
     * @param name Function name
     */
    void setName(String name);

    /**
     * Set the parameters used in this function.
     * @param parameters Array of IExpressions defining the parameters
     */
    void setParameters(IExpression[] parameters);
    
}
