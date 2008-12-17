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
package com.metamatrix.installer.anttask.extensions;

/**
 * This utility provides the ability to import extension files.
 * 
 * There are 2 options for importing:
 * <li>One file at a time</li>
 * <li>Many files at one time</li>
 * 
 * <p>
 * To import one file at a time, call the following methods:
 * - setExtensionpath:  this is the file to import
 * - setExtensionname:  this is the name to call the extension
 * - setExtensiontype:  this is the type of file that is represented (i.e. JAR File, Configuration Model, etc.)
 * (optional) - setExtensionDescription:  provide a description of the extension file
 * </p>
 * 
 * <p>
 * to import many files at one time, the following methods need to be called:
 * - setExtensionpath:  this is the directory location for the file that are to be loaded
 * - setExtensiondescriptor:  this is the descriptor file that contains all the information needed to load a set of files
 * </p>
 * 
 * 
 */

import java.io.File;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;

import org.apache.tools.ant.BuildException;
import org.apache.tools.ant.Project;
import org.apache.tools.ant.Task;

import com.metamatrix.api.exception.MetaMatrixException;
import com.metamatrix.common.extensionmodule.ExtensionModuleInstallUtil;
import com.metamatrix.core.util.FileUtils;

public class ExtensionModuleImportTask extends Task {
    
    private String extpath;
    private String extName;
    private String extType;
    private String extDesc;
        
    private String extDescriptor=null;
	
	/**
	 * Constructor for ExtensionModuleExportTask.
	 */
	public ExtensionModuleImportTask() {
		super();
	}

    
    public void setExtensionpath(String extensionpath) {       
        this.extpath = extensionpath;
        
    }
    
    public void setExtensionname(String extensionName) {       
        this.extName = extensionName;
    }
    
    public void setExtensiontype(String extensionType) {  
        this.extType = extensionType;
    } 
    
    public void setExtensiondescription(String description) {  
        this.extDesc = description;
    }        
    
    public void setExtensiondescriptor(String extensionDescFile) {  
        this.extDescriptor = extensionDescFile;
    }   
    
    
	/**
	 * @see com.metamatrix.installer.transformation.apiimpl.BasicTask#process()
	 */
   public void execute() throws BuildException {
           
       if ( extpath == null || extpath.length() == 0)  {
           throw new BuildException("extensionpath was not specified for ExtensionModuleImportTask.");
       }      
       
       // if no descriptor file is specified, then the type must be
       if (extDescriptor == null && extType == null) {
           throw new BuildException("Either extensiontype or extensiondescriptor must be specified for ExtensionModuleImportTask.");
           
       }
       
       String extFullPath = null;
       boolean useDescriptor = false;
       if (extDescriptor != null) {
           // first use just the extDescriptor setting, it may have the full path
           File f = new File(extDescriptor);
           if (!f.exists()) {
               String fullPath = f.getAbsolutePath();
               // now add the extpath to find the descriptor
               f = new File(extpath, extDescriptor);                    
               if (!f.exists()) {
                   throw new BuildException("extensiondescriptor file " + fullPath + " was not found to import extension modules.");
               }
           }
           useDescriptor = true;
       } else { 
           File f = new File(extpath);
           
           if ( extName == null || extName.length() == 0)  {
           // when the extName isn't specified, check the extpath to get the name
               if (!f.isDirectory()) {
                   extName = f.getName();
                   extFullPath = extpath;
               } else {
                   throw new BuildException("extensionname was not specified and the extensionpath didn't specify a filename for ExtensionModuleImportTask.");
                   
               }
           } else if (f.isDirectory()) {
               extFullPath = FileUtils.buildDirectoryPath(new String[] {
                                                            extpath, extName
                                            });

           } else {
               extFullPath = extpath;
           }

           
           if (this.extDesc == null || this.extDesc.length() == 0) {
               this.extDesc = this.extName;
           }
           
       }
                  
        
        try {
        	
            ExtensionModuleInstallUtil util = InstallExtensionUtil.createExtensionModuleInstallUtil(new Properties());
            if (useDescriptor) {
                List <Object> modulesAdded = InstallExtensionUtil.importExtensionModulesFromDescriptor(util, extpath, extDescriptor);
               this.getProject().log("Imported or Updated the following extensions:");
               for (Iterator <Object> it=modulesAdded.iterator(); it.hasNext();) {
                    this.getProject().log("\t" + it.next().toString());
                }
            } else {

                InstallExtensionUtil.importExtensionModule(util, extName, extType, extDescriptor, extFullPath);
                
            }
            
        } catch (MetaMatrixException err) {
            this.getProject().log("Lading extension module error " +  err.getMessage(), Project.MSG_ERR);
            throw new BuildException(err);
		}
        
        if (extDescriptor != null) {
            this.getProject().log("All Extension modules in descriptor   " +  extDescriptor + " have been imported from " + extpath);
        } else {
            this.getProject().log("Extension module " +  extName + " has been imported from " + extpath);
        }
        
	}
    
    

}

