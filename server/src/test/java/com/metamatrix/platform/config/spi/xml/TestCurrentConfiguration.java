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
import com.metamatrix.common.config.api.Configuration;
import com.metamatrix.common.config.api.ConfigurationModelContainer;
import com.metamatrix.common.config.api.Host;
import com.metamatrix.common.util.VMNaming;
import com.metamatrix.platform.config.BaseTest;
import com.metamatrix.platform.config.CurrentConfigHelper;

public class TestCurrentConfiguration extends BaseTest {

    protected static final String CONFIG_30_FILE = "config_30.xml"; //$NON-NLS-1$
    
   
	
	private static String PRINCIPAL = "TestCurrentConfiguration";       //$NON-NLS-1$
//		
//	private static boolean  oneTime = true;

    public TestCurrentConfiguration(String name) {
        super(name);
        
        printMessages = false;
        
        System.setProperty("metamatrix.encryption.jce.provider","none"); //$NON-NLS-1$ //$NON-NLS-2$$
        
             
    }
    

	protected void init(String cfgFile) throws Exception {
		
	
			CurrentConfigHelper.initConfig(cfgFile, this.getPath(), PRINCIPAL);
		
	}    
	
    
    public void testValidateConfiguration() {
    	
    	printMsg("Starting testValidateConfiguration");    	 //$NON-NLS-1$

    	try {
            init(CONFIG_FILE);
    		    	                		    		    			    		    			    		
            validConfigurationModel();
    		
     	} catch (Exception e) {
    		fail(e.getMessage());
     	}
    		printMsg("Completed testValidateConfiguration"); //$NON-NLS-1$
     	

    }
    
    public void testSystemInitialization() {
    	
    	printMsg("Starting testSystemInitialization");    	 //$NON-NLS-1$

    	try {
            
            init(CONFIG_FILE);
   		    			    		    			    		
    		CurrentConfiguration.getInstance().performSystemInitialization(true); 

            validConfigurationModel();

    		
    		Properties configProps = CurrentConfiguration.getInstance().getProperties();	
    		if (configProps == null || configProps.isEmpty()) {
    			fail("No Global Configuration Properties were found"); //$NON-NLS-1$
    		}
    		
  		    		   		
    		
     	} catch (Exception e) {
     		e.printStackTrace();
    		fail(e.getMessage());
     	}
     	
    		printMsg("Completed testSystemInitialization"); //$NON-NLS-1$
    	
    }
    
    
    public void testCurrentHost() {
        
        printMsg("Starting testCurrentHost");       //$NON-NLS-1$

        try {
            System.setProperty("metamatrix.vmname", "MetaMatrixProcess"); //$NON-NLS-1$ //$NON-NLS-2$
            VMNaming.setup("DummyHost", null, null); //$NON-NLS-1$

            init(CONFIG_FILE);
                                                                
            CurrentConfiguration.getInstance().performSystemInitialization(true); 

            Host host = CurrentConfiguration.getInstance().getDefaultHost();
            
            if (!host.getFullName().equals("DummyHost")) { //$NON-NLS-1$
                fail("DummyHost host was not the default host in the configuration");//$NON-NLS-1$
            }
                            
            
        } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
        
            printMsg("Completed testCurrentHost"); //$NON-NLS-1$        
    }   
    
     
    public void test30SystemInitialization() {
        
        printMsg("**** Starting test30SystemInitialization");       //$NON-NLS-1$

        try {
            init(CONFIG_30_FILE);
                                                                
            CurrentConfiguration.getInstance().performSystemInitialization(true); 

            Configuration config = CurrentConfiguration.getInstance().getConfiguration();
            
            if (config == null) {
                fail("Configuration was not obtained from CurrentConfiguration after system initialization is performed."); //$NON-NLS-1$
            }
            
            HelperTestConfiguration.validateConfigContents(config);
            
            
            Properties configProps = CurrentConfiguration.getInstance().getProperties();  
            if (configProps == null || configProps.isEmpty()) {
                fail("No Global Configuration Properties were found"); //$NON-NLS-1$
            }
            
                            
            
        } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
        
            printMsg("**** Completed test30SystemInitialization"); //$NON-NLS-1$
        
    }
    
    private void validConfigurationModel() throws Exception {
        ConfigurationModelContainer ccm = CurrentConfiguration.getInstance().getConfigurationModel();
        if (ccm == null) {
            fail("Configuration Model was not obtained from CurrentConfiguration.getInstance()."); //$NON-NLS-1$
        }
        
        
        HelperTestConfiguration.validateModelContents(ccm);
       
    }
    

}
