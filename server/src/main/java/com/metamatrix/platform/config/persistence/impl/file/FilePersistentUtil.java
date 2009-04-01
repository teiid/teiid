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

package com.metamatrix.platform.config.persistence.impl.file;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Properties;

import com.metamatrix.common.config.api.ConfigurationID;
import com.metamatrix.common.config.api.ConfigurationModelContainer;
import com.metamatrix.common.config.api.exceptions.ConfigurationException;
import com.metamatrix.common.config.model.ConfigurationModelContainerAdapter;
import com.metamatrix.platform.config.persistence.api.PersistentConnection;
import com.metamatrix.platform.config.persistence.api.PersistentConnectionFactory;

public class FilePersistentUtil {
	



    public static ConfigurationModelContainer readModel(String fileName, String filePath, ConfigurationID configID) throws Exception {
         
            Properties props = createProperties(fileName, filePath);
                                               
    		return readModel(props, configID);
    }    
 
    public static ConfigurationModelContainer readModel(Properties props, ConfigurationID configID) throws Exception {
            
            PersistentConnectionFactory pf = new PersistentConnectionFactory(props);            
            
            PersistentConnection readin = pf.createPersistentConnection(true); 
                       
            ConfigurationModelContainer model = readin.read(configID); 
            readin.close();
            return model;

	}
	
	private static Properties createProperties(String fileName, String filePath) {
		
      	//	File configFile = new File(fileName);
      		          
            Properties props = new Properties();
             
            props.setProperty(FilePersistentConnection.CONFIG_NS_FILE_NAME_PROPERTY,  fileName );
            if (filePath != null) {
	            props.setProperty(FilePersistentConnection.CONFIG_FILE_PATH_PROPERTY,  filePath );
            }

			return props;
	}
	
	public static void writeModel(String fileName, String filePath, ConfigurationModelContainer model, String principal) throws ConfigurationException {
                          
        String loc;
            
        if (filePath == null || filePath.length() == 0) {
            loc = fileName;        
        } else {
            if (filePath.endsWith(File.separator)) {
            } else {
                filePath = filePath + File.separator;   
            }
                
            loc = filePath + fileName;
                
        }
                    
                                       
        ConfigurationModelContainerAdapter adapter = new ConfigurationModelContainerAdapter();
                    
        FileOutputStream os = null;                    
        try {
            os = new FileOutputStream(loc);
            adapter.writeConfigurationModel(os,
                                model, 
                                principal);
            os.flush();
        } catch (ConfigurationException e) {
            throw e;
        } catch (Exception e) {
            throw new ConfigurationException(e);
        } finally {
            if (os != null) {
                try {
                    os.close();
                } catch (IOException e1) {

                }                                
            }
        }
	}
		
            
		 

}
