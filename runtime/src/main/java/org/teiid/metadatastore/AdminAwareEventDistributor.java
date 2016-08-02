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
package org.teiid.metadatastore;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.Collection;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import org.teiid.adminapi.Admin;
import org.teiid.adminapi.AdminException;
import org.teiid.adminapi.PropertyDefinition;
import org.teiid.adminapi.Translator;
import org.teiid.logging.LogConstants;
import org.teiid.logging.LogManager;
import org.teiid.metadata.DataWrapper;
import org.teiid.metadata.MetadataException;
import org.teiid.metadata.Server;
import org.teiid.runtime.RuntimePlugin;

public class AdminAwareEventDistributor extends BaseEventDistributor {
    // Do NOT hold any state of the store here. Treat like servlet, so that rollback is 
    // manageable in case of exception 
    private Admin admin;
    
    public AdminAwareEventDistributor(Admin admin) {
        this.admin = admin;
    }

    @Override
    public void createDataWrapper(String dbName, String version, DataWrapper dataWrapper) {
        if (dataWrapper.getType() != null) {
            // This is override translator
            checkIfTranslatorExists(dataWrapper.getType());
        } else {
            checkIfTranslatorExists(dataWrapper.getName());
        }
    }

    private void checkIfTranslatorExists(String wrapperName) {
        try {
            Translator t = admin.getTranslator(wrapperName);
            if (t == null) {
                throw new MetadataException(RuntimePlugin.Event.TEIID40150,
                        RuntimePlugin.Util.gs(RuntimePlugin.Event.TEIID40150, wrapperName));
            }
        } catch (AdminException e) {
            throw new MetadataException(e);
        }
    }
    
    private void checkIfServerTypeExists(String type) {
        try {
            boolean found = false;
            Set<String> serverTypes = admin.getDataSourceTemplateNames();
            if (serverTypes != null && !serverTypes.isEmpty()) {
                if (serverTypes.contains(type)) {
                    found = true;
                }
            }
            if (!found) {
                throw new MetadataException(RuntimePlugin.Event.TEIID40151,
                        RuntimePlugin.Util.gs(RuntimePlugin.Event.TEIID40151, type, serverTypes));
            }
        } catch (AdminException e) {
            throw new MetadataException(e);
        }  
    }
    
    private boolean doesDataSourceExists(String datasourceName) {
        try {
            boolean found = false;
            Collection<String> datasources = admin.getDataSourceNames();
            if (datasources != null && !datasources.isEmpty()) {
                if (datasources.contains(datasourceName)) {
                    found = true;
                }
            }
            return found;
        } catch (AdminException e) {
            throw new MetadataException(e);
        }  
    }    

    @Override
    public void dropDataWrapper(String dbName, String version, String dataWrapperName, boolean override) {
    }

    @Override
    public void createServer(String dbName, String version, Server server) {
    	if(server.isVirtual()) {
    		return;
    	}
    	
        Properties p = new Properties();
        p.putAll(server.getProperties());
                
        try {
            try {
            	checkIfServerTypeExists(server.getType());
            } catch (MetadataException e) {
                String library = server.getProperty("library", false);
                if (library != null) {
                	List<String> deployments = admin.getDeployments();
                	if (!deployments.contains(server.getType())) {
                		deployLibrary(server.getType(), library);
                	}
                	checkIfServerTypeExists(server.getType());
                } else {
                	throw e;
                }
            }
            
            // if JNDI is provided consider this database is already created by other means
            // and skip creating the data source.
            String jndiName = server.getJndiName();
            if (jndiName == null) {
                if (!doesDataSourceExists(server.getName())) {
                    Collection<? extends PropertyDefinition> defns = admin.getTemplatePropertyDefinitions(server.getType());
                    for (PropertyDefinition def : defns) {
                        // use all the default names if not overridden
                        String key = def.getName();
                        if (key.equalsIgnoreCase("managedconnectionfactory-class")
                                && server.getProperty("class-name", false) == null) {
                            key = "class-name";
                            p.put(key, def.getDefaultValue());
                        }
						if (def.isRequired() && server.getProperty(def.getName(), false) == null
								&& def.getDefaultValue() == null) {
							throw new MetadataException(RuntimePlugin.Event.TEIID40152, RuntimePlugin.Util
									.gs(RuntimePlugin.Event.TEIID40152, def.getName(), server.getName()));
                        }
                    }
                    admin.createDataSource(server.getName(), server.getType(), p);
                }
            }
        } catch (AdminException e) {
            throw new MetadataException(e);            
        }
    }

    private void deployLibrary(String name, String pathOrUri) {
    	try {
    		if (!pathOrUri.endsWith(".jar") && !pathOrUri.endsWith("rar")) {
    			LogManager.logWarning(LogConstants.CTX_METASTORE,
    					RuntimePlugin.Util.gs(RuntimePlugin.Event.TEIID40159, name, pathOrUri));
    			return;
    		}
			InputStream in = null;
			if (pathOrUri.contains("://")) {
				// url
				URI url = URI.create(pathOrUri);
				in = url.toURL().openStream();
			} else {
				// consider this as file path
				File f = new File(pathOrUri);
				if (f.exists() && f.isFile()) {
					in = new FileInputStream(f);
				}
			}
			admin.deploy(name, in);
		} catch (AdminException | IOException e) {
			LogManager.logWarning(LogConstants.CTX_METASTORE,
					RuntimePlugin.Util.gs(RuntimePlugin.Event.TEIID40158, name, pathOrUri));
		}
	}

	@Override
    public void dropServer(String dbName, String version, Server server) {
    	if(server.isVirtual()) {
    		return;
    	}
        try {
            // only for servers created with this interface.
            if (!doesDataSourceExists(server.getName())) {
                throw new MetadataException(RuntimePlugin.Event.TEIID40153,
                        RuntimePlugin.Util.gs(RuntimePlugin.Event.TEIID40153, server.getName()));
            }
            admin.deleteDataSource(server.getName());
        } catch (AdminException e) {
            throw new MetadataException(e);
        }
    }
}
