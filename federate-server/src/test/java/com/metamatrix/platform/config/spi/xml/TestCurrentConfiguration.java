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

package com.metamatrix.platform.config.spi.xml;

import java.util.Properties;

import com.metamatrix.common.config.CurrentConfiguration;
import com.metamatrix.common.config.ResourceNames;
import com.metamatrix.common.config.api.Configuration;
import com.metamatrix.common.config.api.ConfigurationModelContainer;
import com.metamatrix.common.config.api.Host;
import com.metamatrix.common.config.api.VMComponentDefn;
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
        
        printMessages = true;
        
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
   		    			    		    			    		
    		CurrentConfiguration.performSystemInitialization(true); 

            validConfigurationModel();
//    		Configuration config = CurrentConfiguration.getConfiguration();
//    		
//    		if (config == null) {
//    			fail("Configuration was not obtained from CurrentConfiguration after system initialization is performed."); //$NON-NLS-1$
//    		}
//    		
//    		HelperTestConfiguration.validateConfigContents(config);
    		
    		Properties props = CurrentConfiguration.getResourceProperties(ResourceNames.CONFIGURATION_SERVICE);
    		if (props == null || props.isEmpty()) {
    			fail("No Resource Properties were found for " + ResourceNames.CONFIGURATION_SERVICE); //$NON-NLS-1$
    		}
    		
    		Properties configProps = CurrentConfiguration.getProperties();	
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
            VMNaming.setLogicalHostName("DummyHost"); //$NON-NLS-1$

            init(CONFIG_FILE);
                                                                
            CurrentConfiguration.performSystemInitialization(true); 

            Host host = CurrentConfiguration.getHost();
            
            if (!host.getFullName().equals("DummyHost")) { //$NON-NLS-1$
                fail("DummyHost host was not the default host in the configuration");//$NON-NLS-1$
            }
                            
            
        } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
        
            printMsg("Completed testCurrentHost"); //$NON-NLS-1$        
    }   
    
    public void testFindHostByFullyQualifiedName() {
        
        printMsg("Starting testFindHostByFullyQualifiedName");       //$NON-NLS-1$

        try {
            init(CONFIG_FILE);
                                                                
            CurrentConfiguration.performSystemInitialization(true); 

            Host host = CurrentConfiguration.findHost("slwxp141.quadrian.com"); //$NON-NLS-1$
            
            if (host == null) { //$NON-NLS-1$
                fail(" host was not found in configuration");//$NON-NLS-1$
            }
                            
            
        } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
        
            printMsg("Completed testFindHostByFullyQualifiedName"); //$NON-NLS-1$        
    }   
    
    public void testFindHostByShortName() {
        
        printMsg("Starting testFindHostByShortName");       //$NON-NLS-1$

        try {
            init(CONFIG_FILE);
                                                                
            CurrentConfiguration.performSystemInitialization(true); 

            Host host = CurrentConfiguration.findHost("slwxp141"); //$NON-NLS-1$
            
            if (host == null) { //$NON-NLS-1$
                fail(" host was not found in configuration");//$NON-NLS-1$
            }
                            
            
        } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
        
            printMsg("Completed testFindHostByShortName"); //$NON-NLS-1$        
    }   
    
    public void testFindHostByPhysicalAddress() {
        
        printMsg("Starting testFindHostByPhysicalAddress");       //$NON-NLS-1$

        try {
            init(CONFIG_FILE);
                                                                
            CurrentConfiguration.performSystemInitialization(true); 

            Host host = CurrentConfiguration.findHost("dummyhost.quadrian.com"); //$NON-NLS-1$
            
            if (host == null) { //$NON-NLS-1$
                fail("host was not found in configuration");//$NON-NLS-1$
            }
                            
            
        } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
        
            printMsg("Completed testFindHostByPhysicalAddress"); //$NON-NLS-1$        
    }      
    
    public void testFindHostByBindingAddress() {
        
        printMsg("Starting testFindHostByBindingAddress");       //$NON-NLS-1$

        try {
            init(CONFIG_FILE);
                                                                
            CurrentConfiguration.performSystemInitialization(true); 

            Host host = CurrentConfiguration.findHost("192.168.10.166"); //$NON-NLS-1$
            
            if (host == null) { //$NON-NLS-1$
                fail("host was not found in configuration");//$NON-NLS-1$
            }
                            
            
        } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
        
            printMsg("Completed testFindHostByBindingAddress"); //$NON-NLS-1$        
    }       
    
    public void testCurrentVM() {
        
        printMsg("Starting testCurrentVM");       //$NON-NLS-1$

        try {
            System.setProperty("metamatrix.vmname", "MetaMatrixProcess"); //$NON-NLS-1$ //$NON-NLS-2$
            VMNaming.setLogicalHostName("DummyHost"); //$NON-NLS-1$

            init(CONFIG_FILE);
                                                                
            CurrentConfiguration.performSystemInitialization(true); 

            VMComponentDefn vm = CurrentConfiguration.getVM();
            
            if (vm.getFullName().equals("MetaMatrixProcess")) { //$NON-NLS-1$
                fail("MetaMatrixProcess process was not the default vm in the configuration");//$NON-NLS-1$
            }
                            
            
        } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
        
            printMsg("Completed testCurrentVM"); //$NON-NLS-1$        
    }     
    
    public void test30SystemInitialization() {
        
        printMsg("**** Starting test30SystemInitialization");       //$NON-NLS-1$

        try {
            init(CONFIG_30_FILE);
                                                                
            CurrentConfiguration.performSystemInitialization(true); 

            Configuration config = CurrentConfiguration.getConfiguration();
            
            if (config == null) {
                fail("Configuration was not obtained from CurrentConfiguration after system initialization is performed."); //$NON-NLS-1$
            }
            
            HelperTestConfiguration.validateConfigContents(config);
            
            Properties props = CurrentConfiguration.getResourceProperties(ResourceNames.CONFIGURATION_SERVICE);
            if (props == null || props.isEmpty()) {
                fail("No Resource Properties were found for " + ResourceNames.CONFIGURATION_SERVICE); //$NON-NLS-1$
            }
            
            Properties configProps = CurrentConfiguration.getProperties();  
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
        ConfigurationModelContainer ccm = CurrentConfiguration.getConfigurationModel();
        if (ccm == null) {
            fail("Configuration Model was not obtained from CurrentConfiguration."); //$NON-NLS-1$
        }
        
        
        HelperTestConfiguration.validateModelContents(ccm);
       
    }
    

}
