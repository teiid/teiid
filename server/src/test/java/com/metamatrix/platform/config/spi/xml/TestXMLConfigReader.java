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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;

import com.metamatrix.common.config.CurrentConfiguration;
import com.metamatrix.common.config.api.ComponentType;
import com.metamatrix.common.config.api.ComponentTypeDefn;
import com.metamatrix.common.config.api.ComponentTypeID;
import com.metamatrix.common.config.api.Configuration;
import com.metamatrix.common.config.api.ConfigurationModelContainer;
import com.metamatrix.common.config.api.ConnectorBinding;
import com.metamatrix.common.config.api.SharedResource;
import com.metamatrix.platform.config.BaseTest;
import com.metamatrix.platform.config.util.CurrentConfigHelper;

public class TestXMLConfigReader extends BaseTest {

    private static String PRINCIPAL = "TestXMLConfigReader"; //$NON-NLS-1$

    public TestXMLConfigReader(String name) {
        super(name);

        printMessages = false;
    }

    protected void setUp() throws Exception {
    	super.setUp();
        CurrentConfigHelper.initXMLConfig(CONFIG_FILE, this.getPath(), PRINCIPAL);
    }

    public void testValidateReader() throws Exception {

        XMLConfigurationConnector reader = XMLConfigurationMgr.getInstance().getTransaction(PRINCIPAL);

        printMsg("Validate ComponentTypes Exists"); //$NON-NLS-1$
        Collection compTypes = reader.getConfigurationModel().getComponentTypes().values();

        HelperTestConfiguration.validateComponentTypes(compTypes);

        // logic used in the configserviceimpl
        Collection keepTypes = new ArrayList(5);

        int xacnt = 0;
        for (Iterator it = compTypes.iterator(); it.hasNext();) {
            ComponentType type = (ComponentType)it.next();
            if (type.getComponentTypeCode() == ComponentType.RESOURCE_COMPONENT_TYPE_CODE) {
                if (type.getID().equals(SharedResource.MISC_COMPONENT_TYPE_ID)) {
                    keepTypes.add(type);
                } 
            }


        }

        if (keepTypes.size() == 0) {
            fail("No Poolable ComponentTypes"); //$NON-NLS-1$
        }

        printMsg("Validate Resources Exists"); //$NON-NLS-1$
        Collection resources = reader.getConfigurationModel().getResources();

        HelperTestConfiguration.validateResources(resources);

        printMsg("Validate NextStartup Config"); //$NON-NLS-1$

        Configuration ns = reader.getConfigurationModel().getConfiguration();

//        HelperTestConfiguration.validateConfigContents(ns);

        int cnt = 0;
        Collection bindingsCollection = ns.getConnectorBindings();
        for (Iterator<ConnectorBinding> it=bindingsCollection.iterator(); it.hasNext();) {
        	ConnectorBinding cb = it.next();
        	if (cb.isXASupported() ) {
        		cnt++;
        	}
        }
        if (cnt < 1) {
        	fail("No XA Supported Connector Types");
        }
        

        printMsg("Validate Hosts"); //$NON-NLS-1$

        Collection hosts = reader.getConfigurationModel().getConfiguration().getHosts();

        HelperTestConfiguration.validateHosts(hosts);
    }


    public void testComponentTypeDefn() throws Exception {
        ConfigurationModelContainer cmc = CurrentConfiguration.getInstance().getConfigurationModel();

        ComponentType ct = cmc.getComponentType("Connector");//$NON-NLS-1$
        if (ct == null) {
            fail("No componenttype found for Connector");//$NON-NLS-1$
        }
        ComponentTypeDefn ctd = cmc.getComponentTypeDefinition((ComponentTypeID)ct.getID(), "ServiceClassName");//$NON-NLS-1$

        if (ctd == null) {
            fail("ComponentTypeDefn for ServiceClassName from super type Connector was not found.");//$NON-NLS-1$
        }
    }

}
