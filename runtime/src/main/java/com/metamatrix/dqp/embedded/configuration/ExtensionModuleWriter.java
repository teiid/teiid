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
    
    public static void write(ExtensionModule extModule, URL[] contexts) throws MetaMatrixComponentException {
        OutputStream out = null;
        try {
            String extFile = extModule.getFullName()+"?action=write"; //$NON-NLS-1$
            // NOTE: only write to the very first context.
            URL extModuleURL = URLHelper.buildURL(contexts[0], extFile);
                        
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
    public static void deleteModule(String extModuleName, URL[] contexts) throws MetaMatrixComponentException{

        URL extModuleURL = null;
		try {
			extModuleURL = ExtensionModuleReader.resolveExtensionModule(ExtensionModuleReader.MM_JAR_PROTOCOL+":"+extModuleName, contexts); //$NON-NLS-1$
			if (extModuleURL == null) {
				throw new MetaMatrixComponentException(DQPEmbeddedPlugin.Util.getString("ExtensionModuleReader.ext_module_does_not_exist", extModuleName)); //$NON-NLS-1$
			}
		} catch (IOException e) {
			throw new MetaMatrixComponentException(DQPEmbeddedPlugin.Util.getString("ExtensionModuleReader.ext_module_does_not_exist", extModuleName)); //$NON-NLS-1$
		}
        
        String extFile = extModuleURL.toString()+"?action=delete"; //$NON-NLS-1$
        
        InputStream in = null;
        try {
            extModuleURL = URLHelper.buildURL(extFile);        
            in = extModuleURL.openStream();
            if (in != null) {
                // now delete file from the extensions directory..        
                throw new MetaMatrixComponentException(DQPEmbeddedPlugin.Util.getString("ExtensionModuleWriter.ext_module_delete_failed", new Object[] {extModuleURL})); //$NON-NLS-1$
            }
            DQPEmbeddedPlugin.logInfo("ExtensionModuleWriter.ext_module_delete", new Object[] {extModuleName, extModuleURL}); //$NON-NLS-1$
        }catch(FileNotFoundException e) {
            // this is what we should expect if open the stream.
            DQPEmbeddedPlugin.logInfo("ExtensionModuleWriter.ext_module_delete", new Object[] {extModuleName, extModuleURL}); //$NON-NLS-1$                                    
        }catch(IOException e) {
            throw new MetaMatrixComponentException(DQPEmbeddedPlugin.Util.getString("ExtensionModuleWriter.ext_module_delete_failed", new Object[] {extModuleURL})); //$NON-NLS-1$            
        } finally {
            if (in != null) {
                try {in.close();}catch(IOException e) {}
            }
        }
    }                                              
}
