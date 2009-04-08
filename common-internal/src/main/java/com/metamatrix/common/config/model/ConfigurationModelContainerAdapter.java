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

package com.metamatrix.common.config.model;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Collection;
import java.util.Properties;

import com.metamatrix.common.CommonPlugin;
import com.metamatrix.common.config.api.ConfigurationID;
import com.metamatrix.common.config.api.ConfigurationModelContainer;
import com.metamatrix.common.config.api.ConfigurationObjectEditor;
import com.metamatrix.common.config.api.exceptions.ConfigurationException;
import com.metamatrix.common.config.util.ConfigObjectsNotResolvableException;
import com.metamatrix.common.config.util.ConfigurationPropertyNames;
import com.metamatrix.common.config.xml.XMLConfigurationImportExportUtility;
import com.metamatrix.common.util.ApplicationInfo;
import com.metamatrix.common.util.ErrorMessageKeys;

public class ConfigurationModelContainerAdapter {


    public ConfigurationModelContainer readConfigurationModel(String file, ConfigurationID configID)throws ConfigurationException {
        FileInputStream fis = null;
        try {
            fis = new FileInputStream(file);
            
            return readConfigurationModel(fis, configID);
            
        } catch (ConfigurationException ce) {
            throw ce;
            
        } catch(Exception ioe) {
            throw new ConfigurationException(ioe, ErrorMessageKeys.CONFIG_0016, CommonPlugin.Util.getString(ErrorMessageKeys.CONFIG_0016, configID.getFullName()));
        } finally {
            try {
                if (fis != null) {
                    fis.close();
                } 
            }catch (Exception e) {
                    
            }
        }
        
        
    }
    
    public ConfigurationModelContainer readConfigurationModel(InputStream inputStream, ConfigurationID configID) throws ConfigurationException {

        Collection configObjects = null;
        ConfigurationObjectEditor editor = new BasicConfigurationObjectEditor(false);
        try {
        	XMLConfigurationImportExportUtility io = new XMLConfigurationImportExportUtility();

            configObjects = io.importConfigurationObjects(inputStream, editor, configID.getFullName());

            ConfigurationModelContainerImpl configModel = new ConfigurationModelContainerImpl();
            configModel.setConfigurationObjects(configObjects);


            return configModel;

        } catch(Exception ioe) {
            throw new ConfigurationException(ioe, ErrorMessageKeys.CONFIG_0016, CommonPlugin.Util.getString(ErrorMessageKeys.CONFIG_0016, configID.getFullName()));
        }

    }


    public void writeConfigurationModel(String file, ConfigurationModelContainer model, String principalName) throws ConfigurationException {
        
        FileOutputStream fos = null;
        try {
            fos = new FileOutputStream(file);
            writeConfigurationModel(fos, model, principalName);
            
        } catch (ConfigurationException ce) {
            throw ce;
        } catch(Exception ioe) {
            throw new ConfigurationException(ioe, ErrorMessageKeys.CONFIG_0017, CommonPlugin.Util.getString(ErrorMessageKeys.CONFIG_0017, model.getConfiguration().getID()));
        } finally {
            try {
                if (fos != null) {
                    fos.close();
                } 
            }catch (Exception e) {
                    
            }
        }
    }
    public void writeConfigurationModel(OutputStream out,ConfigurationModelContainer model, String principalName) throws ConfigurationException {
        try {

        	XMLConfigurationImportExportUtility io = new XMLConfigurationImportExportUtility();

            Properties props = new Properties();
            props.put(ConfigurationPropertyNames.APPLICATION_CREATED_BY, "Configuration Import_Export Utility"); //$NON-NLS-1$
            props.put(ConfigurationPropertyNames.APPLICATION_VERSION_CREATED_BY, ApplicationInfo.getInstance().getMajorReleaseNumber()); 
            props.put(ConfigurationPropertyNames.USER_CREATED_BY, principalName);

            io.exportConfiguration(out, model.getAllObjects(), props);

        } catch(Exception ioe) {
            throw new ConfigurationException(ioe, ErrorMessageKeys.CONFIG_0017, CommonPlugin.Util.getString(ErrorMessageKeys.CONFIG_0017, model.getConfiguration().getID()));
        }

    }

    public void validateModel(ConfigurationModelContainer model) throws  ConfigObjectsNotResolvableException {
            XMLConfigurationImportExportUtility io = new XMLConfigurationImportExportUtility();
            io.resolveConfigurationObjects(model.getAllObjects());
    }

}
