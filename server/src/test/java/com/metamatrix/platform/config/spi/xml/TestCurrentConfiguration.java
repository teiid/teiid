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

package com.metamatrix.platform.config.spi.xml;

import java.util.Properties;

import com.metamatrix.common.config.CurrentConfiguration;
import com.metamatrix.common.config.api.ConfigurationModelContainer;
import com.metamatrix.common.config.api.Host;
import com.metamatrix.platform.config.BaseTest;
import com.metamatrix.platform.config.util.CurrentConfigHelper;

public class TestCurrentConfiguration extends BaseTest {
    
	private static String PRINCIPAL = "TestCurrentConfiguration";       //$NON-NLS-1$

    public TestCurrentConfiguration(String name) {
        super(name);
        
        printMessages = false;
    }
    
	protected void init(String cfgFile) throws Exception {
		CurrentConfigHelper.initXMLConfig(cfgFile, this.getPath(), PRINCIPAL);
		
	}    
    
    public void testValidateConfiguration() throws Exception {
    	
    	printMsg("Starting testValidateConfiguration");    	 //$NON-NLS-1$

        init(CONFIG_FILE);
		    	                		    		    			    		    			    		
        validConfigurationModel();
    		
		printMsg("Completed testValidateConfiguration"); //$NON-NLS-1$
    }
    
    public void testSystemInitialization() throws Exception {
    	
    	printMsg("Starting testSystemInitialization");    	 //$NON-NLS-1$

        init(CONFIG_FILE);
	    			    		    			    		
        validConfigurationModel();

		Properties configProps = CurrentConfiguration.getInstance().getProperties();	
		if (configProps == null || configProps.isEmpty()) {
			fail("No Global Configuration Properties were found"); //$NON-NLS-1$
		}
 	
		printMsg("Completed testSystemInitialization"); //$NON-NLS-1$
    	
    }
    
    
    public void testCurrentHost() throws Exception {
        printMsg("Starting testCurrentHost");       //$NON-NLS-1$

    	System.setProperty(CurrentConfiguration.CONFIGURATION_NAME, "DummyHost"); //$NON-NLS-1$ 
        System.setProperty("metamatrix.vmname", "MetaMatrixProcess"); //$NON-NLS-1$ //$NON-NLS-2$

        init(CONFIG_FILE);
                                                            
        Host host = CurrentConfiguration.getInstance().getDefaultHost();
        
        if (!host.getFullName().equals("DummyHost")) { //$NON-NLS-1$
            fail("DummyHost host was not the default host in the configuration");//$NON-NLS-1$
        }
        
        printMsg("Completed testCurrentHost"); //$NON-NLS-1$        
    }  
    
    public void testMultiHostConfig() throws Exception {
    	
    	printMsg("Starting testMultiHostConfig");    	 //$NON-NLS-1$

        init("config_multihost.xml");
	    			    		    			    		
        validConfigurationModel();

		int hostcnt = CurrentConfiguration.getInstance().getConfiguration().getHosts().size();	
		if (hostcnt <= 1 ) {
			fail("Multiple hosts were not found " + hostcnt); //$NON-NLS-1$
		}
 	
		printMsg("Completed testMultiHostConfig"); //$NON-NLS-1$
    	
    }
    
     
 
    
    private void validConfigurationModel() throws Exception {
        ConfigurationModelContainer ccm = CurrentConfiguration.getInstance().getConfigurationModel();
        if (ccm == null) {
            fail("Configuration Model was not obtained from CurrentConfiguration.getInstance()."); //$NON-NLS-1$
        }
        
        
        HelperTestConfiguration.validateModelContents(ccm);
       
    }
    

}
