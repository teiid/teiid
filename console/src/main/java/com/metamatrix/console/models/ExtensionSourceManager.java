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

package com.metamatrix.console.models;

import java.io.File;
import java.io.FileOutputStream;
import java.util.*;

import com.metamatrix.admin.api.objects.ExtensionModule;
import com.metamatrix.common.extensionmodule.ExtensionModuleDescriptor;
import com.metamatrix.common.extensionmodule.exception.*;
import com.metamatrix.console.connections.ConnectionInfo;
import com.metamatrix.console.ui.views.extensionsource.ExtensionSourceDetailInfo;
import com.metamatrix.console.ui.views.extensionsource.NewExtensionSourceInfo;
import com.metamatrix.console.util.ExternalException;
import com.metamatrix.core.util.DateUtil;
import com.metamatrix.platform.admin.api.ExtensionSourceAdminAPI;

public class ExtensionSourceManager extends Manager {
    public ExtensionSourceManager(ConnectionInfo connection) {
        super(connection);
    }

    public void init() {
        super.init();
    }

    public void deleteModule(String moduleName) throws ExternalException,
            ExtensionModuleNotFoundException {
        ExtensionSourceAdminAPI api = ModelManager.getExtensionSourceAPI(
        		getConnection());
        try {
            api.removeSource(moduleName);
        } catch (ExtensionModuleNotFoundException ex) {
            throw ex;
        } catch (Exception ex) {
            throw new ExternalException("Remove extension module " + moduleName, //$NON-NLS-1$
                    ex);
        }
    }

    public void modifyModule(String existingModuleName, String newModuleName,
            String newDescription, Boolean enabled, byte[] contents)
            throws ExtensionModuleNotFoundException, ExternalException {
        ExtensionSourceAdminAPI api = ModelManager.getExtensionSourceAPI(
        		getConnection());
        String moduleName = existingModuleName;
        if ((newModuleName != null) && (!newModuleName.equals(
                existingModuleName))) {
            try {
                api.setSourceName(existingModuleName, newModuleName);
            } catch (ExtensionModuleNotFoundException ex) {
                throw ex;
            } catch (Exception ex) {
                throw new ExternalException(
                        "Attempt to rename an extension module", ex); //$NON-NLS-1$
            }
            moduleName = newModuleName;
        }
        if (newDescription != null) {
            try {
                api.setSourceDescription(moduleName, newDescription);
            } catch (ExtensionModuleNotFoundException ex) {
                throw ex;
            } catch (Exception ex) {
                throw new ExternalException(
                        "Attempt to change extension module description", ex); //$NON-NLS-1$
            }
        }
        if (enabled != null) {
            try {
                Collection moduleNames = new ArrayList(1);
                moduleNames.add(moduleName);
                api.setEnabled(moduleNames, enabled.booleanValue());
            } catch (ExtensionModuleNotFoundException ex) {
                throw ex;
            } catch (Exception ex) {
                throw new ExternalException(
                        "Attempt to change extension module enabled flag", ex); //$NON-NLS-1$
            }
        }
        if (contents != null) {
            try {
                api.setSource(moduleName, contents);
            } catch (ExtensionModuleNotFoundException ex) {
                throw ex;
            } catch (Exception ex) {
                throw new ExternalException(
                        "Attempt to change extension module contents", ex); //$NON-NLS-1$
            }
        }
    }

    public void replaceModule(String moduleName, byte[] replacementContents)
            throws ExtensionModuleNotFoundException, ExternalException {
        modifyModule(moduleName, null, null, null, replacementContents);
    }

    public void exportToFile(String moduleName, File target) throws
            ExtensionModuleNotFoundException, ExternalException {
        try {
            Collection<ExtensionModule> modules = getConnection().getServerAdmin().getExtensionModules(moduleName);
            if (modules.size() != 1) {
            	throw new ExtensionModuleNotFoundException(moduleName);
            }
            FileOutputStream stream = new FileOutputStream(target);
            stream.write(modules.iterator().next().getFileContents());
            stream.close();
        } catch (ExtensionModuleNotFoundException ex) {
            throw ex;
        } catch (Exception ex) {
            throw new ExternalException("Error exporting extension module", ex); //$NON-NLS-1$
        }
    }

    public void addModule(NewExtensionSourceInfo info)
            throws DuplicateExtensionModuleException, ExternalException {
        ExtensionSourceAdminAPI api = ModelManager.getExtensionSourceAPI(
        		getConnection());
        try {
            api.addSource(info.getModuleType(), info.getModuleName(),
                    info.getFileContents(), info.getDescription(),
                    info.isEnabled());
        } catch (DuplicateExtensionModuleException ex) {
            throw ex;
        } catch (Exception ex) {
            throw new ExternalException("Attempt to add extension module", ex); //$NON-NLS-1$
        }
    }

    public ExtensionSourceDetailInfo getDetailForModule(String moduleName)
            throws ExtensionModuleNotFoundException, ExternalException {
        ExtensionSourceAdminAPI api = ModelManager.getExtensionSourceAPI(
        		getConnection());
        try {
            ExtensionModuleDescriptor descriptor = api.getSourceDescriptor(
                    moduleName);
            String creationDateAsString = descriptor.getCreationDate();
            Date creationDate = DateUtil.convertStringToDate(
                    creationDateAsString);
            String lastUpdateDateAsString = descriptor.getLastUpdatedDate();
            Date lastUpdateDate = DateUtil.convertStringToDate(
                    lastUpdateDateAsString);
            String createdBy = descriptor.getCreatedBy();
            String lastUpdatedBy = descriptor.getLastUpdatedBy();
            ExtensionSourceDetailInfo detail = new ExtensionSourceDetailInfo(
                    descriptor.getName(), descriptor.getType(),
                    descriptor.getDescription(), descriptor.isEnabled(),
                    creationDate, createdBy, lastUpdateDate, lastUpdatedBy);
            return detail;
        } catch (ExtensionModuleNotFoundException ex) {
            throw ex;
        } catch (Exception ex) {
            throw new ExternalException(
                    "Error retrieving detail for extension module", ex); //$NON-NLS-1$
        }
    }

    public void reorderModules(java.util.List /*<String>*/ modules)
            throws ExtensionModuleOrderingException, ExternalException {
        ExtensionSourceAdminAPI api = ModelManager.getExtensionSourceAPI(
        		getConnection());
        try {
            api.setSearchOrder(modules);
        } catch (ExtensionModuleOrderingException ex) {
            throw ex;
        } catch (Exception ex) {
            throw new ExternalException(
                    "Error changing extension module search order", ex); //$NON-NLS-1$
        }
    }

    public ExtensionSourceDetailInfo[] getModules() throws ExternalException {
        ExtensionSourceAdminAPI api = ModelManager.getExtensionSourceAPI(
        		getConnection());
        ExtensionSourceDetailInfo[] details = null;
        java.util.List /*<ExtensionSourceDescriptor>*/ descriptors = null;
        try {
            descriptors = api.getSourceDescriptors();
            int numModules = descriptors.size();
            details = new ExtensionSourceDetailInfo[numModules];
            Iterator it = descriptors.iterator();
            for (int i = 0; it.hasNext(); i++) {
                ExtensionModuleDescriptor descriptor =
                        (ExtensionModuleDescriptor)it.next();
                Date creationDate = DateUtil.convertStringToDate(
                        descriptor.getCreationDate());
                Date lastUpdateDate = DateUtil.convertStringToDate(
                        descriptor.getLastUpdatedDate());
                details[i] = new ExtensionSourceDetailInfo(descriptor.getName(),
                        descriptor.getType(), descriptor.getDescription(),
                        descriptor.isEnabled(), creationDate,
                        descriptor.getCreatedBy(), lastUpdateDate,
                        descriptor.getLastUpdatedBy());
            }
        } catch (Exception ex) {
            throw new ExternalException("Retrieve extension modules", ex); //$NON-NLS-1$
        }
        return details;
    }

    public String[] getModuleTypes() throws ExternalException {
        String[] result = null;
        ExtensionSourceAdminAPI api = ModelManager.getExtensionSourceAPI(
        		getConnection());
        try {
            Collection types = api.getSourceTypes();
            result = new String[types.size()];
            Iterator it = types.iterator();
            for (int i = 0; it.hasNext(); i++) {
                result[i] = (String)it.next();
            }
        } catch (Exception ex) {
            throw new ExternalException("Get extension module types", ex); //$NON-NLS-1$
        }
        return result;
    }

    public boolean moduleExists(String moduleName) throws ExternalException {
        ExtensionSourceAdminAPI api = ModelManager.getExtensionSourceAPI(
        		getConnection());
        boolean exists = false;
        try {
            exists = api.isSourceExists(moduleName);
        } catch (Exception ex) {
            throw new ExternalException("Error calling getSourceDescriptor()", //$NON-NLS-1$
                    ex);
        }
        return exists;
    }
}
