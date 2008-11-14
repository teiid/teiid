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

package com.metamatrix.common.pooling.resource;

import com.metamatrix.common.config.api.ResourceDescriptor;
import com.metamatrix.common.pooling.NoOpResourceInterface;
import com.metamatrix.common.pooling.api.Resource;
import com.metamatrix.common.pooling.api.ResourceAdapter;
import com.metamatrix.common.pooling.api.exception.ResourcePoolException;
import com.metamatrix.common.pooling.util.PoolingUtil;

public class GenericResourceAdapter implements ResourceAdapter {

    public static final String RESOURCE_CLASS_NAME = "generic.resource.class.name"; //$NON-NLS-1$

    public GenericResourceAdapter() {
    }

    public Resource createResource(Object physicalResource) throws ResourcePoolException{

        NoOpResourceInterface no = convertToInterface(physicalResource);

        GenericResource gr = new GenericResource(no);
        

        return gr;

    }


    public Object createPhysicalResourceObject(ResourceDescriptor descriptor) throws ResourcePoolException{
//        Resource resource = null;

        String resourceClassName = descriptor.getProperty(RESOURCE_CLASS_NAME);

        Object obj = PoolingUtil.create(resourceClassName, null);

        // do this for testing
        convertToInterface(obj);

        return obj;
/*

        try {
             resource = (Resource) obj;

        } catch(ClassCastException e) {
            throw new ResourcePoolException(e, "The specified Resource subclass (\"" + resourceClassName + "\") doesn't implement " + Resource.class.getName() );
        }

        return resource;
*/
    }

    public void closePhyicalResourceObject(Resource object) throws ResourcePoolException {

    }

    private NoOpResourceInterface convertToInterface(Object obj) throws ResourcePoolException {
          if (obj instanceof NoOpResourceInterface) {
              return (NoOpResourceInterface) obj;

          }
          throw new ResourcePoolException("Unable to convert physical resource object because it " + //$NON-NLS-1$
                    "is not an instance of NoOpResourceInterface - instance is " + obj.getClass().getName()); //$NON-NLS-1$
    }

} 
