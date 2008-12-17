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

import java.io.File;

import org.apache.tools.ant.BuildException;

import com.metamatrix.platform.config.util.CurrentConfigHelper;

/**
 * The task loads the configuration file into the extension manager.
 * 
 * Because the repository is presumed not loaded prior to this running, this uses
 * the {@link CurrentConfigHelper} to load a configuration into memory in order for the
 * correct resources to be loaded for the extension module to work.
 * 
 * @author van halbert
 *
 */
public class ExtensionConfigImportTask extends ExtensionModuleImportTask {
	private String configFilePath = null;
	
    public void setExtensionpath(String extensionpath) {       
        super.setExtensionpath(extensionpath);
        
        this.configFilePath = extensionpath;
        
    }	
   
	
	@Override
	public void execute() throws BuildException {
		// 
		try {
			File f = new File(this.configFilePath);
			
			if (! f.exists()) {
				throw new BuildException("Configuration file " + f.getAbsolutePath() + " does not exist");
			}
			String filename = f.getName();
			String path = f.getParent();
			
			CurrentConfigHelper.initConfig(filename, path, this.getClass().getName());
		} catch (Exception e) {
			throw new BuildException(e);
		}

		super.execute();
		
	}

	
	
}
