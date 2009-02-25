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

import java.util.List;

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
    List<IExpression> getParameters();
    
    /**
     * Set name of the function
     * @param name Function name
     */
    void setName(String name);

}
