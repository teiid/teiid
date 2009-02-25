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
package com.metamatrix.connector.salesforce.test.util;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.teiid.connector.api.ConnectorEnvironment;
import org.teiid.connector.api.CredentialMap;
import org.teiid.connector.api.ExecutionContext;

import com.metamatrix.cdk.api.EnvironmentUtility;
import com.metamatrix.connector.salesforce.ConnectorState;

public class ObjectFactory {

	public static boolean DEBUG = false;
	
    public static final String VALID_USERNAME= "jdoyleoss@gmail.com";
    public static final String VALID_PASSWORD = "l3tm31nNZ4loJCls59GlDr4sZLB8N4TT";

    public static final String BOGUS_USERNAME= "bogus@gmail.com";
    public static final String BOGUS_PASSWORD = "k33pm30ut";

	public static ConnectorEnvironment getDefaultTestConnectorEnvironment() {
        Properties props = getDefaultProps(); 
        ConnectorEnvironment env = EnvironmentUtility.createEnvironment(props, DEBUG);
        return env;
     }
	
	public static ConnectorEnvironment getNoCredTestConnectorEnvironment() {
		Properties props = new Properties();
		props.put("sandbox", "false"); 
        ConnectorEnvironment env = EnvironmentUtility.createEnvironment(props, DEBUG);
        return env;
     }

	public static ConnectorEnvironment getConnectorEnvironmentBadUser() {
        Properties props = getDefaultProps();
        props.put(ConnectorState.USERNAME, BOGUS_USERNAME);
        ConnectorEnvironment env = EnvironmentUtility.createEnvironment(props, DEBUG);
        return env;
     }
	
	public static ConnectorEnvironment getConnectorEnvironmentBadPass() {
        Properties props = getDefaultProps();
        props.put(ConnectorState.PASSWORD, BOGUS_PASSWORD);
        ConnectorEnvironment env = EnvironmentUtility.createEnvironment(props, DEBUG);
        return env;
     }
	
	public static ConnectorEnvironment getConnectorEnvironmentEmptyPass() {
        Properties props = getDefaultProps();
        props.put(ConnectorState.PASSWORD, "");
        ConnectorEnvironment env = EnvironmentUtility.createEnvironment(props, DEBUG);
        return env;
     }
	
	public static Properties getDefaultProps() {
		Properties props = new Properties();
		
		props.put(ConnectorState.USERNAME, VALID_USERNAME);
		props.put(ConnectorState.PASSWORD, VALID_PASSWORD);
		props.put("sandbox", "false");
		props.put("ConnectorCapabilities", "com.metamatrix.connector.salesforce.SalesforceCapabilities");
		props.put("InLimit","-1");
		return props;
	}
	
	public static Properties getNoCredProps() {
		Properties props = new Properties();
		props.put("ConnectorCapabilities", "com.metamatrix.connector.salesforce.SalesforceCapabilities");
		props.put("sandbox", "false");
		return props;
	}

	public static ExecutionContext getDefaultSecurityContext() {
		return EnvironmentUtility.createSecurityContext("MetaMatrixAdmin");
	}
	
	public static ExecutionContext getTokenSecurityContext() {
		CredentialMap cMap = getCredentialMap();
		return EnvironmentUtility.createSecurityContext("Foo","1", "MetaMatrixAdmin", cMap);
	}
	
	public static CredentialMap getCredentialMap() {
		Map values = new HashMap();
		values.put(CredentialMap.USER_KEYWORD, VALID_USERNAME);
		values.put(CredentialMap.PASSWORD_KEYWORD, VALID_PASSWORD);
		CredentialMap cMap = new CredentialMap();
		cMap.addSystemCredentials("Connector<CDK>",values);
		return cMap;
	}
	public static ConnectorEnvironment getConnectorEnvironmentEmptyUser() {
		Properties props = getDefaultProps();
        props.put(ConnectorState.USERNAME, "");
        ConnectorEnvironment env = EnvironmentUtility.createEnvironment(props, DEBUG);
        return env;
	}
}
