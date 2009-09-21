/*
 * Copyright (c) 2000-2007 MetaMatrix, Inc.
 * All rights reserved.
 */
package org.teiid.test.testcases;

import org.teiid.test.framework.ConfigPropertyNames;
import org.teiid.test.framework.connection.ConnectionStrategy;


/** 
 * Local Transaction Test
 * 
 * Settings:
 * 
 * -	Transaction Type = local
 * -	Using Driver
 * -	Autocommit = True
 * -	TxnAutoWrap = Off
 */
public class XATransactionDatasourceFalseOffTest extends TwoSourceTransactionScenarios {
          
    public XATransactionDatasourceFalseOffTest(String name) {
		super(name);
	}

	@Override
	protected void setUp() throws Exception {
    	System.setProperty(ConfigPropertyNames.CONFIG_FILE, "xa-config.properties");
		
    	System.setProperty(ConfigPropertyNames.CONNECTION_TYPE, ConfigPropertyNames.CONNECTION_TYPES.DATASOURCE_CONNECTION);
    	System.setProperty(ConnectionStrategy.AUTOCOMMIT, "false");
    	System.setProperty(ConnectionStrategy.TXN_AUTO_WRAP, "off");
    	
		
	}


     
}
