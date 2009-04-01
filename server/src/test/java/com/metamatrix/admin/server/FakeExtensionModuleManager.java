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

package com.metamatrix.admin.server;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Properties;

import com.metamatrix.api.exception.MetaMatrixComponentException;
import com.metamatrix.common.connection.ManagedConnectionException;
import com.metamatrix.common.extensionmodule.ExtensionModuleDescriptor;
import com.metamatrix.common.extensionmodule.ExtensionModuleManager;
import com.metamatrix.common.extensionmodule.exception.DuplicateExtensionModuleException;
import com.metamatrix.common.extensionmodule.exception.ExtensionModuleNotFoundException;
import com.metamatrix.common.extensionmodule.exception.ExtensionModuleOrderingException;
import com.metamatrix.common.extensionmodule.exception.InvalidExtensionModuleTypeException;
import com.metamatrix.common.extensionmodule.spi.ExtensionModuleTransaction;
import com.metamatrix.core.util.DateUtil;


/**
 * Fake implementation that creates fake data for testing the Admin API. 
 * @since 4.3
 */
public class FakeExtensionModuleManager extends ExtensionModuleManager {
    
    public FakeExtensionModuleManager() {
    }

    public ExtensionModuleDescriptor addSource(String principalName,
                                               String type,
                                               String sourceName,
                                               byte[] source,
                                               String description,
                                               boolean enabled) throws DuplicateExtensionModuleException,
                                                               InvalidExtensionModuleTypeException,
                                                               MetaMatrixComponentException {
        return null;
    }

    public ExtensionModuleTransaction getReadTransaction() throws ManagedConnectionException {
        return null;
    }

    public byte[] getSource(String sourceName) throws ExtensionModuleNotFoundException,
                                              MetaMatrixComponentException {
        if (sourceName.equals("extensionModule1")) { //$NON-NLS-1$
            return "bytes1".getBytes(); //$NON-NLS-1$
        } else if (sourceName.equals("extensionModule2")){ //$NON-NLS-1$
            return "bytes2".getBytes(); //$NON-NLS-1$
        } else {
            return null;
        }
    }

    public ExtensionModuleDescriptor getSourceDescriptor(String sourceName) throws ExtensionModuleNotFoundException,
                                                                           MetaMatrixComponentException {
        return null;
    }

    public List getSourceDescriptors() throws MetaMatrixComponentException {
        List results = new ArrayList();
        
        ExtensionModuleDescriptor descriptor1 = new ExtensionModuleDescriptor();
        descriptor1.setName("extensionModule1"); //$NON-NLS-1$
        descriptor1.setCreatedBy("testUser1"); //$NON-NLS-1$
        descriptor1.setCreationDate(DateUtil.getCurrentDateAsString());
        descriptor1.setLastUpdatedDate(DateUtil.getCurrentDateAsString());
        descriptor1.setEnabled(true);
        descriptor1.setType("testType1"); //$NON-NLS-1$
        descriptor1.setDescription("description1"); //$NON-NLS-1$
        results.add(descriptor1);
        
        ExtensionModuleDescriptor descriptor2 = new ExtensionModuleDescriptor();
        descriptor2.setName("extensionModule2"); //$NON-NLS-1$
        descriptor2.setCreatedBy("testUser2"); //$NON-NLS-1$
        descriptor2.setCreationDate(DateUtil.getCurrentDateAsString());
        descriptor2.setLastUpdatedDate(DateUtil.getCurrentDateAsString());
        descriptor2.setEnabled(true);
        descriptor2.setType("testType2"); //$NON-NLS-1$
        descriptor2.setDescription("description2"); //$NON-NLS-1$
        results.add(descriptor2);
        
        
        return results;

    }

    public List getSourceDescriptors(String type) throws InvalidExtensionModuleTypeException,
                                                 MetaMatrixComponentException {
        return null;
    }

    public List getSourceNames() throws MetaMatrixComponentException {
        return null;
    }

    public Collection getSourceTypes() throws MetaMatrixComponentException {
        return null;
    }

    public ExtensionModuleTransaction getWriteTransaction() throws ManagedConnectionException {
        return null;
    }

    public void init() {
    }

    protected void init(Properties env) {
    }

    public boolean isSourceInUse(String sourceName) throws MetaMatrixComponentException {
        return false;
    }

    public void removeSource(String principalName,
                             String sourceName) throws ExtensionModuleNotFoundException,
                                               MetaMatrixComponentException {
    }

    public List setEnabled(String principalName,
                           Collection sourceNames,
                           boolean enabled) throws ExtensionModuleNotFoundException,
                                           MetaMatrixComponentException {
        return null;
    }

    public List setSearchOrder(String principalName,
                               List sourceNames) throws ExtensionModuleOrderingException,
                                                MetaMatrixComponentException {
        return null;
    }

    public ExtensionModuleDescriptor setSource(String principalName,
                                               String sourceName,
                                               byte[] source) throws ExtensionModuleNotFoundException,
                                                             MetaMatrixComponentException {
        return null;
    }

    public ExtensionModuleDescriptor setSourceDescription(String principalName,
                                                          String sourceName,
                                                          String description) throws ExtensionModuleNotFoundException,
                                                                             MetaMatrixComponentException {
        return null;
    }

    public ExtensionModuleDescriptor setSourceName(String principalName,
                                                   String sourceName,
                                                   String newName) throws ExtensionModuleNotFoundException,
                                                                  MetaMatrixComponentException {
        return null;
    }

    

   
    
}