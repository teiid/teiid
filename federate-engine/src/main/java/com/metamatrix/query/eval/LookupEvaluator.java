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

package com.metamatrix.query.eval;

import com.metamatrix.api.exception.MetaMatrixComponentException;
import com.metamatrix.common.buffer.BlockedException;
import com.metamatrix.query.util.CommandContext;

/** 
 * @since 5.0
 */
public interface LookupEvaluator {

    /**
     * Lookup a value from a cached code table.  If the code table is not loaded, it will be 
     * loaded on the first query.  Code tables should be cached based on a combination of
     * the codeTableName, returnElementName, and keyElementName.  If the table is not loaded,
     * a request will be made and the method should throw a BlockedException.
     * 
     * @param context Context for processing
     * @param codeTableName Name of the code table - must be a physical table 
     * @param returnElementName Name of the element to be returned in the code table
     * @param keyElementName Name of the key element in the code table
     * @param keyValue Key value to look up 
     * @return Return value for the specified key value, or null if not found
     * @throws BlockedException If code table must be loaded
     * @throws MetaMatrixComponentException If an unexpected error occurs
     */
    public abstract Object lookupCodeValue(CommandContext context,
                                           String codeTableName,
                                           String returnElementName,
                                           String keyElementName,
                                           Object keyValue) throws BlockedException,
                                                           MetaMatrixComponentException;

}