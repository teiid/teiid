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
import java.io.ObjectInputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;

import com.metamatrix.api.exception.MetaMatrixComponentException;
import com.metamatrix.common.config.api.ExtensionModule;
import com.metamatrix.common.config.model.BasicExtensionModule;
import com.metamatrix.common.protocol.URLHelper;
import com.metamatrix.common.util.ByteArrayHelper;
import com.metamatrix.dqp.embedded.DQPEmbeddedPlugin;


/** 
 * @since 4.3
 */
public class ExtensionModuleReader {
    public static final String MM_JAR_PROTOCOL = "extensionjar"; //$NON-NLS-1$
    
    /**
     * Load the extension module from the file system
     * @return
     * @since 4.3
     */
    public static ExtensionModule loadExtensionModule(String extModuleName, URL[] contexts) throws MetaMatrixComponentException{
        byte[] contents = null;
        InputStream in =  null;
        try {
        	URL extModuleURL = resolveExtensionModule(MM_JAR_PROTOCOL+":"+extModuleName, contexts); //$NON-NLS-1$
        	if (extModuleURL != null) {
	            in = extModuleURL.openStream();                               
	            contents = ByteArrayHelper.toByteArray(in);
        	}
        	else {
        		throw new MetaMatrixComponentException(DQPEmbeddedPlugin.Util.getString("ExtensionModuleReader.ext_module_does_not_exist", extModuleName)); //$NON-NLS-1$
        	}
        } catch (FileNotFoundException e) {
            throw new MetaMatrixComponentException(DQPEmbeddedPlugin.Util.getString("ExtensionModuleReader.ext_module_does_not_exist", extModuleName)); //$NON-NLS-1$
        } catch (IOException e) {
            throw new MetaMatrixComponentException(e, DQPEmbeddedPlugin.Util.getString("ExtensionModuleReader.ext_module_failed_to_read", extModuleName)); //$NON-NLS-1$
        } finally {
            if (in != null) {
                try{in.close();}catch(IOException e) {}
            }
        }
        return new BasicExtensionModule(extModuleName, ExtensionModule.JAR_FILE_TYPE, "Jar File", contents); //$NON-NLS-1$            
    }
    
    /**
     * Load all the Extension modules from the given directory 
     * @param extModuleDirectory
     * @return list of Extension Modules {@link com.metamatrix.common.config.api.ExtensionModule}
     * @since 4.3
     */
    public static List<ExtensionModule> loadExtensionModules(URL[] contexts) throws MetaMatrixComponentException{
            
        ObjectInputStream in =  null;
        List<ExtensionModule> extModuleList = new ArrayList<ExtensionModule>();
        
        for (URL extensionPathURL: contexts) {
        	String extensionPath = extensionPathURL.toString()+"?action=list&filter=.jar"; //$NON-NLS-1$
                
	        try {
	            extensionPathURL = URLHelper.buildURL(extensionPath);        
	            in = new ObjectInputStream(extensionPathURL.openStream());
	            String[] jarFiles = (String[])in.readObject();
	            for (int i = 0; i < jarFiles.length; i++) {
	                String jarName = jarFiles[i];
	                jarName = jarName.substring(jarName.lastIndexOf('/')+1);
	                byte[] contents = null;                
	                try {
	                    URL jarFileURL = URLHelper.buildURL(jarFiles[i]);
	                    InputStream jarStream = jarFileURL.openStream();
	                    contents = ByteArrayHelper.toByteArray(jarStream);
	                    jarStream.close();
	                } catch (IOException e) {
	                    throw new MetaMatrixComponentException(e, DQPEmbeddedPlugin.Util.getString("ExtensionModuleReader.ext_module_failed_to_read", new Object[] {jarFiles[i]})); //$NON-NLS-1$
	                }
	                extModuleList.add(new BasicExtensionModule(jarName, ExtensionModule.JAR_FILE_TYPE, "Jar File", contents)); //$NON-NLS-1$
	            }
	        } catch (FileNotFoundException e) {
	            // if the file not found then it means no extensions directory and no extension 
	            // modules, just return a empty list.
	        } catch (IOException e) {
	            throw new MetaMatrixComponentException(e);
	        } catch(ClassNotFoundException e){
	        	throw new MetaMatrixComponentException(e);
	        } finally {
	            if (in != null) {
	                try{in.close();}catch(IOException e) {}
	            }
	        }
        }
        return extModuleList;
    }
    
    public static URL resolveExtensionModule(String extModule, URL[] contexts) throws MalformedURLException {
		int idx = extModule.indexOf(MM_JAR_PROTOCOL);
		if (idx != -1) {
			for (URL context : contexts) {
				URL entry = URLHelper.buildURL(context, extModule.substring(idx + MM_JAR_PROTOCOL.length() + 1));
				if (urlExists(entry)) {
					return entry;
				}
			}

		} else {
			return new URL(extModule);
		}
		return null;
	}
    
    private static boolean urlExists(URL url) {
        InputStream in = null;
        try {
            in = url.openStream();
            in.close();
            return true;
        } catch (IOException e) {
        }
    	return false;
    }
}
