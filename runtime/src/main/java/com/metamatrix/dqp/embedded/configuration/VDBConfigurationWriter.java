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

package com.metamatrix.dqp.embedded.configuration;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URL;
import java.net.URLConnection;
import java.util.Properties;

import com.metamatrix.api.exception.MetaMatrixComponentException;
import com.metamatrix.common.application.exception.ApplicationInitializationException;
import com.metamatrix.common.config.util.ConfigurationPropertyNames;
import com.metamatrix.common.protocol.URLHelper;
import com.metamatrix.common.vdb.api.VDBArchive;
import com.metamatrix.common.vdb.api.VDBDefn;
import com.metamatrix.core.util.FileUtils;
import com.metamatrix.dqp.embedded.DQPEmbeddedPlugin;


/**
 * Utility to export VDBs to files or streams.
 * @since 4.3
 */
public class VDBConfigurationWriter {
    private static final String DEF = ".def"; //$NON-NLS-1$
    
    
    /**
     * Convert the supplied VDBDefn object into a VDB archive where the DEF file
     * and VDB file are inside single file. Then write to the supplied file 
     * @param vdb
     * @param vdbFileURL
     * @throws ApplicationInitializationException
     * @since 4.3
     */
    public static void write(VDBArchive vdb, URL vdbFileURL) 
        throws MetaMatrixComponentException{
        
        // write single DEF/VDB file
        OutputStream out = null;
        try {
            String vdbFile= vdbFileURL.toString()+"?action=write"; //$NON-NLS-1$
            vdbFileURL = URLHelper.buildURL(vdbFile);
            
            URLConnection conn = vdbFileURL.openConnection();
            // if there is already file existing then we have an issue
            out = conn.getOutputStream();
            vdb.write(out);
        } catch (Exception e) {
            throw new MetaMatrixComponentException(e);
        } finally {
            if (out != null) {
                try{out.close();}catch(IOException e) {}
            }
        }
    }    
    
    /** 
     * @return
     * @since 4.3
     */
    static Properties getPropertiesForExporting() {
        Properties properties = new Properties();
        properties.put(ConfigurationPropertyNames.APPLICATION_CREATED_BY, "EmbeddedAdmin"); //$NON-NLS-1$
        properties.put(ConfigurationPropertyNames.APPLICATION_VERSION_CREATED_BY, "4.3"); //$NON-NLS-1$
        properties.put(ConfigurationPropertyNames.USER_CREATED_BY, "dqpadmin"); //$NON-NLS-1$
        return properties;
    }     
    
    /**
     * Delete the VDB specified by the URL 
     * @param vdb
     * @param url
     * @throws MetaMatrixComponentException
     * @since 4.3
     */
    public static void deleteVDB(VDBArchive vdb, URL url) throws MetaMatrixComponentException {       

        String urlPath = url.toString()+"?action=delete"; //$NON-NLS-1$
        InputStream in = null;
        
        // this can be DEF file or VDB file
        try {
            url = URLHelper.buildURL(urlPath);        
            in = url.openStream();
            if (in != null) {
                // now delete file from the extensions directory..        
                throw new MetaMatrixComponentException(DQPEmbeddedPlugin.Util.getString("VDBConfigurationWriter.vdb_delete_failed", new Object[] {vdb.getName(), vdb.getVersion()})); //$NON-NLS-1$
            }
        }catch(IOException e) {
            DQPEmbeddedPlugin.logInfo("VDBConfigurationWriter.vdb_delete", new Object[] {vdb.getName(), vdb.getVersion()}); //$NON-NLS-1$            
        } finally {
            if (in != null) {
                try {in.close();}catch(IOException e) {}
            }
        }         
        
        VDBDefn def = vdb.getConfigurationDef();
    	
        // If previous one is a DEF file, we also need to delete the VDB file        
        if (url.getPath().endsWith(DEF) && def.getFileName() != null) {
            try {
                url = URLHelper.buildURL(url, def.getFileName());
                urlPath = url.toString()+"?action=delete"; //$NON-NLS-1$
                
                url = URLHelper.buildURL(urlPath);        
                in = url.openStream();
                if (in != null) {
                    // now delete file from the extensions directory..        
                    throw new MetaMatrixComponentException(DQPEmbeddedPlugin.Util.getString("VDBConfigurationWriter.vdb_delete_failed", new Object[] {vdb.getName(), vdb.getVersion()})); //$NON-NLS-1$
                }
            }catch(IOException e) {
                DQPEmbeddedPlugin.logInfo("VDBConfigurationWriter.vdb_delete", new Object[] {vdb.getName(), vdb.getVersion()}); //$NON-NLS-1$            
            } finally {
                if (in != null) {
                    try {in.close();}catch(IOException e) {}
                }
            }
        }  
        
        if (vdb.getDeployDirectory().exists()) {
        	FileUtils.removeDirectoryAndChildren(vdb.getDeployDirectory());
        }
        
    }
}
