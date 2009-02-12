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
import java.util.Map;
import java.util.Properties;

import com.metamatrix.common.config.CurrentConfiguration;
import com.metamatrix.common.config.api.ComponentType;
import com.metamatrix.common.config.api.ComponentTypeDefn;
import com.metamatrix.common.config.api.ComponentTypeID;
import com.metamatrix.common.config.api.Configuration;
import com.metamatrix.common.config.api.ConfigurationModelContainer;
import com.metamatrix.common.config.api.SharedResource;
import com.metamatrix.common.connection.ManagedConnection;
import com.metamatrix.platform.config.BaseTest;
import com.metamatrix.platform.config.CurrentConfigHelper;

public class TestXMLConfigReader extends BaseTest {

    private static String PRINCIPAL = "TestXMLConfigReader"; //$NON-NLS-1$
    protected static final String CONFIG_FILE = "config.xml"; //$NON-NLS-1$

    private ManagedConnection conn;
    private XMLConfigurationConnectorFactory factory;

    public TestXMLConfigReader(String name) {
        super(name);

        printMessages = false;
    }

    protected void setUp() throws Exception {
    	super.setUp();
        CurrentConfigHelper.initConfig(CONFIG_FILE, this.getPath(), PRINCIPAL);

        factory = new XMLConfigurationConnectorFactory();

        conn = factory.createConnection(new Properties(), PRINCIPAL);
    }

    public void testValidateReader() throws Exception {

        XMLConfigurationConnector reader = (XMLConfigurationConnector)factory.createTransaction(conn, true);

        printMsg("Validate ComponentTypes Exists"); //$NON-NLS-1$
        Collection compTypes = reader.getAllComponentTypes(true);

        HelperTestConfiguration.validateComponentTypes(compTypes);

        // logic used in the configserviceimpl
        Collection keepTypes = new ArrayList(5);

        int xacnt = 0;
        for (Iterator it = compTypes.iterator(); it.hasNext();) {
            ComponentType type = (ComponentType)it.next();
            if (type.getComponentTypeCode() == ComponentType.RESOURCE_COMPONENT_TYPE_CODE) {
                if (type.getID().equals(SharedResource.JDBC_COMPONENT_TYPE_ID)) {
                    keepTypes.add(type);
                } else if (type.getID().equals(SharedResource.SEARCHBASE_COMPONENT_TYPE_ID)) {
                    keepTypes.add(type);
                }
            }

            if (type.getComponentTypeCode() == ComponentType.CONNECTOR_COMPONENT_TYPE_CODE) {
                if (type.isOfTypeXAConnector()) {
                    if (!type.isOfTypeConnector()) {
                        fail("XA Connector Type " + type.getFullName() + " must also be a connector type");//$NON-NLS-1$ //$NON-NLS-2$
                    }
                    ++xacnt;
                }
            }

        }

        if (keepTypes.size() == 0) {
            fail("No Poolable ComponentTypes"); //$NON-NLS-1$
        }

        if (xacnt != 6) {
            fail("The number XA Connector Types should have been 5, but found " + xacnt);//$NON-NLS-1$
        }

        printMsg("Validate Resources Exists"); //$NON-NLS-1$
        Collection resources = reader.getResources();

        HelperTestConfiguration.validateResources(resources);

        printMsg("Validate NextStartup Config"); //$NON-NLS-1$

        Configuration ns = reader.getDesignatedConfiguration(Configuration.NEXT_STARTUP);

        HelperTestConfiguration.validateConfigContents(ns);

        printMsg("Validate ComponentDefns"); //$NON-NLS-1$
        Map defns = reader.getComponentDefinitions(Configuration.NEXT_STARTUP_ID);

        HelperTestConfiguration.validateComponentDefns(defns.values());

        printMsg("Validate Deployed Components"); //$NON-NLS-1$

        Collection dc = reader.getDeployedComponents(Configuration.NEXT_STARTUP_ID);

        HelperTestConfiguration.validateDeployedComponents(dc);

        printMsg("Validate Hosts"); //$NON-NLS-1$

        Collection hosts = reader.getHosts();

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
