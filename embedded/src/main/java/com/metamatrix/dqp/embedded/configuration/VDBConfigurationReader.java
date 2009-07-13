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

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

import com.metamatrix.api.exception.MetaMatrixComponentException;
import com.metamatrix.common.protocol.URLHelper;
import com.metamatrix.common.vdb.api.VDBArchive;
import com.metamatrix.core.util.ArgCheck;
import com.metamatrix.core.util.ObjectConverterUtil;
import com.metamatrix.core.vdb.VdbConstants;
import com.metamatrix.dqp.embedded.DQPEmbeddedPlugin;
import com.metamatrix.vdb.runtime.BasicVDBDefn;


/** 
 * This is reader class for DEF file. This is light weight implementation of 
 * DEF reader,which does not come with any heavy weight as the config API 
 * or modeler stuff. At the same time it may not have all the flexibility.
 * 
 * This is mainly used in the DQP configuration, it builds the Admin API
 * objects as out puts.
 * @since 4.3
 */
public class VDBConfigurationReader {

    /**
     *  Load the VDB from the contents given.
     * @param defContents
     * @param vdbContents
     * @return VDB - return the loaded VDB object with its configuration
     * @since 4.3
     */
    public static VDBArchive loadVDB(String name, char[] defContents, byte[] vdbContents) 
        throws MetaMatrixComponentException{
    	
    	if (defContents == null || vdbContents == null) {
    		throw new IllegalArgumentException("VDB Content provided can not be null");
    	}
    	
    	VDBArchive archive = null;
    	
        try {
        	// DEF Contents.
            InputStream defStream = ObjectConverterUtil.convertToInputStream(new String(defContents));
        	BasicVDBDefn def = VDBArchive.readFromDef(defStream);
        	if (name != null) {
        		def.setName(name);
        	}
        	
            archive = new VDBArchive(new ByteArrayInputStream(vdbContents));
       		archive.updateConfigurationDef(def);
        		
        	return archive;
        } catch (Exception e) {
            throw new MetaMatrixComponentException(e);
        }
    }
    
    /**
     * Load the combined VDB file and DEF file archive  
     * @param name
     * @param vdbContents
     * @return
     * @throws Exception
     * @since 4.3
     */
    public static VDBArchive loadVDB(String name, byte[] vdbContents) 
        throws MetaMatrixComponentException{
    	
    	ArgCheck.isNotNull(vdbContents);
    	
        try {
        	VDBArchive archive = new VDBArchive(new ByteArrayInputStream(vdbContents));
        	archive.getConfigurationDef().setName(name);
        	archive.updateConfigurationDef(archive.getConfigurationDef());
            return archive;
        } catch (Exception e) {
            throw new MetaMatrixComponentException(e);
        }
    }   
    
    /**
     * Load the VDB at the given URL. If this is DEF file look for same named VDB file 
     * @param vdbURL
     * @return
     * @throws MetaMatrixComponentException
     * @since 4.3
     */
    public static VDBArchive loadVDB(URL vdbURL, File deployDirectory) throws MetaMatrixComponentException{        
        try {
            VDBArchive vdb = VDBArchive.loadVDB(vdbURL, deployDirectory);
            
            if (vdb.getVDBValidityErrors() != null) {                    
                String[] errors = vdb.getVDBValidityErrors();
                StringBuffer sb = new StringBuffer();
                for (int i = 0; i < errors.length; i++) {
                    sb.append("-").append(errors[i]).append(";"); //$NON-NLS-1$ //$NON-NLS-2$                
                } // for            
                DQPEmbeddedPlugin.logError("VDBReader.validityErrors", new Object[] {vdbURL, sb}); //$NON-NLS-1$
            }
            return vdb;
        } catch (IOException e) {
            throw new MetaMatrixComponentException(e, DQPEmbeddedPlugin.Util.getString("VDBReader.Archive_not_Found", vdbURL)); //$NON-NLS-1$
        }
    }
        
    
    /**
     * Load the VDBS at the given URLS. If the URL is does not point to a DEF or VDB then
     * look for more VDB at that directory level and load any VDBs at that level. 
     * @param vdbURLs
     * @return HashMap map of objects with (URL, VDBDefn)
     */
    public static HashMap<URL, VDBArchive> loadVDBS(URL[] urls, File deployDirectory) throws MetaMatrixComponentException{
        
        HashMap vdbs = new HashMap();
        ArrayList vdbURLs = new ArrayList();
        // First get a comprehensive list of all the VDBs at the given URL lists
        for (int i = 0; i < urls.length; i++) {
            String vdblocation = urls[i].toString().toLowerCase();
            if (vdblocation.endsWith(VdbConstants.VDB) || vdblocation.endsWith(VdbConstants.DEF)) {
                vdbURLs.add(urls[i]);
            }
            else {
                // now we have given a directory location, so look for all the files 
                // in the given location.
                vdbURLs.addAll(getVDBResources(urls[i]));                
            }            
        }    
        
        // now that we have the absolute paths to the all the VDBs available
        // load all of them.
        for (Iterator i = vdbURLs.iterator(); i.hasNext();) {
            URL vdbURL = (URL)i.next();
            VDBArchive vdb = loadVDB(vdbURL, deployDirectory);

            // Only valid vdb files get loaded into dqp engine.
            if (vdb.getVDBValidityErrors() == null) {
                vdbs.put(vdbURL, vdb);
            }
        }                
        return vdbs;
    }
    
    
    static List getVDBResources(URL vdbRepositoryURL) throws MetaMatrixComponentException {        
        ObjectInputStream in =  null;
        ArrayList urlList = new ArrayList();
        String vdblocation = vdbRepositoryURL.toString()+"?action=list&filter=.vdb,.def"; //$NON-NLS-1$                
        try {
            vdbRepositoryURL = URLHelper.buildURL(vdblocation);        
            in = new ObjectInputStream(vdbRepositoryURL.openStream());
            String[] urls = (String[])in.readObject();
            for (int i = 0; i < urls.length; i++) {
                
                boolean add = true;
                URL vdbFileURL = URLHelper.buildURL(urls[i]);
                if (add) {
                    urlList.add(vdbFileURL);
                }
            }             
            return urlList;
        } catch(Exception e) {
            throw new MetaMatrixComponentException(e);
        }finally {
            if (in != null) {
                try{in.close();}catch(IOException e) {}
            }
        }        
    }
}
