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

package com.metamatrix.common.pooling.util;

import java.util.Collection;

import com.metamatrix.common.pooling.api.exception.ResourcePoolException;
import com.metamatrix.core.MetaMatrixCoreException;
import com.metamatrix.core.util.ReflectionHelper;

public class PoolingUtil {

    /**
     * Helper method to create an instance of the class using the appropriate
     * constructor based on the ctorObjs passed.
     * @param className is the class to instantiate
     * @param ctorObjs are the objects to pass to the constructor; optional, nullable
     * @return Object is the instance of the class
     * @throws ResourcePoolException if an error occurrs instantiating the class
     */

    public static final Object create(String className, Collection ctorObjs) throws ResourcePoolException {
        try {
            return ReflectionHelper.create(className, ctorObjs, Thread.currentThread().getContextClassLoader());
        } catch ( MetaMatrixCoreException e ) {
            throw new ResourcePoolException(e);
        }

    }
}
