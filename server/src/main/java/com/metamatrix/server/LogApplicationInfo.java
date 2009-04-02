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

/**
 * 
 */
package com.metamatrix.server;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import java.util.jar.Attributes;
import java.util.jar.Manifest;

import com.metamatrix.common.config.CurrentConfiguration;
import com.metamatrix.common.config.ResourceNames;
import com.metamatrix.common.config.api.ConfigurationModelContainer;
import com.metamatrix.common.config.api.VMComponentDefn;
import com.metamatrix.common.extensionmodule.ExtensionModuleDescriptor;
import com.metamatrix.common.extensionmodule.ExtensionModuleManager;
import com.metamatrix.common.extensionmodule.ExtensionModuleTypes;
import com.metamatrix.common.util.ApplicationInfo;
import com.metamatrix.common.util.ByteArrayHelper;
import com.metamatrix.common.util.PropertiesUtils;
import com.metamatrix.core.util.FileUtils;
import com.metamatrix.core.util.TempDirectory;
import com.metamatrix.core.util.ZipFileUtil;

class LogApplicationInfo implements Runnable {
	String applFileName = null;
    String logPath = null;
    String hostName;
    String processName;
    
    public LogApplicationInfo(String hostName, String processName, String path, String fileName) {
		applFileName = fileName;
        logPath = path;
        this.hostName = hostName;
        this.processName = processName;
    }
    
    public void run() {
        InputStream is = null;
        try {
                           
            ConfigurationModelContainer configmodel = CurrentConfiguration.getInstance().getConfigurationModel();
            VMComponentDefn deployedVM = configmodel.getConfiguration().getVMForHost(this.hostName, this.processName);

            Properties configprops = null;
            
            ApplicationInfo info = ApplicationInfo.getInstance();
            StringBuffer sb = new StringBuffer();

            sb.append(CurrentConfiguration.getInstance().getHostInfo());
                            
            sb.append("\n---- System Properties ----\n");  //$NON-NLS-1$             
            sb.append(PropertiesUtils.prettyPrint(System.getProperties()));

            sb.append("\n---- MM Global Properties ----\n");  //$NON-NLS-1$                             
            sb.append(PropertiesUtils.prettyPrint(configmodel.getConfiguration().getProperties()));
            
            sb.append("\n---- Host Properties ----\n");  //$NON-NLS-1$             
            configprops = configmodel.getHost(this.hostName).getProperties();
            sb.append(PropertiesUtils.prettyPrint(configprops));
            
            sb.append("\n# of Processors: " + java.lang.Runtime.getRuntime().availableProcessors());//$NON-NLS-1$  
            sb.append("\nMax Avail memory: " + java.lang.Runtime.getRuntime().maxMemory());//$NON-NLS-1$               
            sb.append("\nFree memory: " + java.lang.Runtime.getRuntime().freeMemory());//$NON-NLS-1$  
            
            sb.append("\n\n---- VM Properties ----\n");  //$NON-NLS-1$             
            sb.append(PropertiesUtils.prettyPrint(deployedVM.getProperties()));
            
            sb.append("\n---- JGroups Resource Properties ----\n");  //$NON-NLS-1$             
            configprops = configmodel.getResource(ResourceNames.JGROUPS).getProperties();
            sb.append(PropertiesUtils.prettyPrint(configprops));
            
            sb.append(info.getClasspathInfo());
            
            
            sb.append("\n\n---- Extension Jars Manifest Info ----\n");  //$NON-NLS-1$             

            logManifestInfoForExtensionModules(sb);
            
            is = ByteArrayHelper.toInputStream(sb.toString().getBytes());
            
            File logFile = null;
            if (logPath == null || logPath.length() == 0 ) {            
                logFile = new File(applFileName);
            } else {
                logFile = new File(logPath, applFileName);
            }                
               
            FileUtils.write(is, logFile);
            
            is.close();
            
        } catch (Exception e) {
        	e.printStackTrace();
            System.err.println("Error writing application info to " + applFileName + ", msg: " + e.getMessage()); //$NON-NLS-1$ //$NON-NLS-2$
            //Ignore, we are dieing anyway.
        } finally {
            if (is != null) {
                try {
                    is.close();
                } catch (IOException err) {
                }
            }
        }
        
    }
    
    private static final String APPLICATION_PRODUCT_INFORMATION       = "Product Information"; //$NON-NLS-1$
    
    private void logManifestInfoForExtensionModules(StringBuffer sb) {
        final Random random = new Random(System.currentTimeMillis());

        TempDirectory tempDir = null;
        try {
            // must write the jarfile to the filesystem for
            // the ZipFileUtil to look inside
            tempDir = new TempDirectory(System.currentTimeMillis(), random.nextLong());
            tempDir.create();
        
            ExtensionModuleManager em = ExtensionModuleManager.getInstance();
            List descriptors = em.getSourceDescriptors(ExtensionModuleTypes.JAR_FILE_TYPE);
            if (descriptors != null) {
                for (Iterator it=descriptors.iterator(); it.hasNext();) {
                    ExtensionModuleDescriptor md = (ExtensionModuleDescriptor) it.next();
                    sb.append("\n == Jar: " + md.getName() + " ===== " +  APPLICATION_PRODUCT_INFORMATION );  //$NON-NLS-1$ //$NON-NLS-2$  
                    
                    
                    byte[] jardata = em.getSource(md.getName());
                    
                    
                    File f = new File(tempDir.getPath(), md.getName());
                    
                    FileUtils.write(jardata, f);
                    
                    Manifest m = ZipFileUtil.getManifest(f);
                    
                    if (m != null) {
                        // only print the manifest info for MetaMatrix related jars that have
                        // the product information section
                        Attributes manifestAttributes = m.getAttributes(APPLICATION_PRODUCT_INFORMATION);
                        if(manifestAttributes == null || manifestAttributes.isEmpty()){
                            continue;
                            
                        } 
                                                    
                        for (Iterator ita = manifestAttributes.keySet().iterator(); ita.hasNext();) {
                            Object n =  ita.next();
                            Object v = manifestAttributes.get(n);
                            
                            sb.append("\n");//$NON-NLS-1$
                            sb.append("   ");//$NON-NLS-1$
                            sb.append(n.toString());
                            sb.append(":        "); //$NON-NLS-1$
                            sb.append(v.toString());
                            
                        }
                        sb.append("\n");//$NON-NLS-1$
                        
                    } 
                }
            }
        } catch(Exception e) {
            sb.append("**** Error: Unable to list manifest - msg: " + e.getMessage());//$NON-NLS-1$

        } finally {
            if (tempDir != null) {
                tempDir.remove();
            }
        }
        
    }
}