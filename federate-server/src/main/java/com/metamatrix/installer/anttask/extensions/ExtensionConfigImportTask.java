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
