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

package com.metamatrix.data.language;

/**
 * Represents an expression in the language.  Subinterfaces define specific 
 * types of expressions, such as literal values, element references, and 
 * functions.   
 */
public interface IExpression extends ILanguageObject {

    /**
     * Determine the type returned by this expression.  The connector should
     * return an object of this type if this expression is used in a SELECT 
     * clause.  
     * 
     * @return The type, as defined by a Java class
     */
    Class getType();

    /**
     * Set the type returned by this expression.  The connector should
     * return an object of this type if this expression is used in a SELECT 
     * clause.  
     * 
     * @param type The type, as defined by a Java class
     */
    void setType(Class type);
    
}
