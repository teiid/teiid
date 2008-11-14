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

package com.metamatrix.dqp.embedded.configuration;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URL;
import java.net.URLConnection;

import com.metamatrix.api.exception.MetaMatrixComponentException;
import com.metamatrix.common.config.api.ExtensionModule;
import com.metamatrix.common.protocol.URLHelper;
import com.metamatrix.dqp.embedded.DQPEmbeddedPlugin;


/** 
 * Utility to abstract the Extension Module's persistence.
 * @since 4.3
 */
public class ExtensionModuleWriter {
    
    public static void write(ExtensionModule extModule, URL extModuleURL) throws MetaMatrixComponentException {
        OutputStream out = null;
        try {
            String extFile = extModuleURL.toString()+"?action=write"; //$NON-NLS-1$
            extModuleURL = URLHelper.buildURL(extFile);
                        
            URLConnection conn = extModuleURL.openConnection();
            out =  conn.getOutputStream();
            out.write(extModule.getFileContents());
            DQPEmbeddedPlugin.logInfo("ExtensionModuleWriter.ext_module_save", new Object[] {extModule.getFullName(), extModuleURL}); //$NON-NLS-1$
        } catch (IOException e) {
            throw new MetaMatrixComponentException(e);
        } finally {
            if (out != null) {
                try{out.close();}catch(IOException e) {}
            }
        }
    }
    
    /**
     * Delete the extension module from the file system. 
     * @param extModule
     * @since 4.3
     */
    public static void deleteModule(URL extModuleURL) throws MetaMatrixComponentException{
        String extensionPath = extModuleURL.toString()+"?action=delete"; //$NON-NLS-1$

        InputStream in = null;
        try {
            extModuleURL = URLHelper.buildURL(extensionPath);        
            in = extModuleURL.openStream();
            if (in != null) {
                // now delete file from the extensions directory..        
                throw new MetaMatrixComponentException(DQPEmbeddedPlugin.Util.getString("ExtensionModuleWriter.ext_module_delete_failed", new Object[] {extModuleURL})); //$NON-NLS-1$
            }
            DQPEmbeddedPlugin.logInfo("ExtensionModuleWriter.ext_module_delete", new Object[] {extModuleURL.getPath(), extModuleURL}); //$NON-NLS-1$
        }catch(FileNotFoundException e) {
            // this is what we should expect if open the stream.
            DQPEmbeddedPlugin.logInfo("ExtensionModuleWriter.ext_module_delete", new Object[] {extModuleURL.getPath(), extModuleURL}); //$NON-NLS-1$                                    
        }catch(IOException e) {
            throw new MetaMatrixComponentException(DQPEmbeddedPlugin.Util.getString("ExtensionModuleWriter.ext_module_delete_failed", new Object[] {extModuleURL})); //$NON-NLS-1$            
        } finally {
            if (in != null) {
                try {in.close();}catch(IOException e) {}
            }
        }        
    }                                              
}
