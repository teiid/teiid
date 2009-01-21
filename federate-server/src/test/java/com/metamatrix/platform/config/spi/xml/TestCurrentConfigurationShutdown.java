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

import com.metamatrix.common.config.CurrentConfiguration;
import com.metamatrix.common.config.StartupStateController;
import com.metamatrix.common.config.api.ConfigurationModelContainer;
import com.metamatrix.platform.config.BaseTest;
import com.metamatrix.platform.config.CurrentConfigHelper;

public class TestCurrentConfigurationShutdown extends BaseTest {

	
	private static String PRINCIPAL = "TestCurrentConfigurationShutdown";       //$NON-NLS-1$


    public TestCurrentConfigurationShutdown(String name) {
        super(name);
      	printMessages = true;
             
    }
    
    public void testSystemShutdown() {
    	
    	printMsg("Starting testSystemShutdown");    	 //$NON-NLS-1$

    	try {
			CurrentConfigHelper.initConfig(CONFIG_FILE, this.getPath(), PRINCIPAL);		   		    			    		    			    		
//    		CurrentConfiguration.getInstance().performSystemInitialization(true); 

            validConfigurationModel();
//    		Configuration config = CurrentConfiguration.getInstance().getConfiguration();
//    		
//    		if (config == null) {
//    			fail("Configuration was not obtained from CurrentConfiguration after system initialization is performed."); //$NON-NLS-1$
//    		}
//    		
//    		HelperTestConfiguration.validateConfigContents(config);

			printMsg("Call Configuration to Shutdown System"); //$NON-NLS-1$

			CurrentConfiguration.getInstance().indicateSystemShutdown();

			printMsg("Shutdown System"); //$NON-NLS-1$
			
			XMLConfigurationMgr mgr = XMLConfigurationMgr.getInstance();
			
			printMsg("Check System State"); //$NON-NLS-1$
			int state = mgr.getServerStartupState();
			if (state != StartupStateController.STATE_STOPPED) {

				String lbl;
				if (state == StartupStateController.STATE_STARTED) {
					lbl = StartupStateController.STATE_STARTED_LABEL;
				} else if (state == StartupStateController.STATE_STARTING) {
					lbl = StartupStateController.STATE_STARTING_LABEL;
				} else {
					lbl = "UNDEFINED STATE CODE of " + state; //$NON-NLS-1$
				} 
				
				fail("Server State was not set to " + StartupStateController.STATE_STOPPED_LABEL + //$NON-NLS-1$
					" but is currently set to " + lbl); //$NON-NLS-1$
			}				
 		    		   		
    		
     	} catch (Exception e) {
     		e.printStackTrace();
    		fail(e.getMessage());
     	}
     	
    		printMsg("Completed testSystemShutdown"); //$NON-NLS-1$
    	
    }
    
    private void validConfigurationModel() throws Exception {
        ConfigurationModelContainer ccm = CurrentConfiguration.getInstance().getConfigurationModel();
        if (ccm == null) {
            fail("Configuration Model was not obtained from CurrentConfiguration"); //$NON-NLS-1$
        }
        
        
        HelperTestConfiguration.validateModelContents(ccm);
       
    }
    
        

}

