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

/*
 */
package com.metamatrix.connector.jdbc.extension;

import java.util.List;

import com.metamatrix.data.language.IExpression;
import com.metamatrix.data.language.IFunction;

/**
 * Implementations of this interface are used to modify metamatrix functions
 * coming in to the connector into alternate datasource-specific language, if
 * necessary. 
 */
public interface FunctionModifier {
    
    /**
     * Takes an IFunction and returns the datasource-specific IExpression,
     * or can possibly return the unmodified function parameter itself. 
     * @param function
     * @return IExpression or unmodified function
     * @since 4.2
     */
    IExpression modify(IFunction function);
    
    /**
     * Return a List of translated parts (LanguageObjects and Strings), or null
     * if this FunctionModifier wishes to rely on the default translation of the
     * conversion visitor. 
     * @param function IFunction to be translated
     * @return List of translated parts, or null
     * @since 4.2
     */
    List translate(IFunction function);
}
