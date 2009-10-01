/*
 * Copyright (c) 2000-2007 MetaMatrix, Inc.
 * All rights reserved.
 */
package org.teiid.test.testcases;

import org.teiid.test.framework.ConfigPropertyLoader;
import org.teiid.test.framework.ConfigPropertyNames;
import org.teiid.test.framework.ConfigPropertyNames.CONNECTION_STRATEGY_PROPS;


/** 
 * Local Transaction Test
 * 
 * Settings:
 * 
 * -	Using Driver
 * -	Autocommit = True
 * -	TxnAutoWrap = Off
 */
public class LocalTransactionDriverTrueOffTest extends TwoSourceTransactionScenarios {
          
    public LocalTransactionDriverTrueOffTest(String name) {
		super(name);
	}

	@Override
	protected void setUp() throws Exception {
		super.setUp();
		
		this.addProperty(ConfigPropertyNames.CONNECTION_TYPE, ConfigPropertyNames.CONNECTION_TYPES.DRIVER_CONNECTION);
		this.addProperty(CONNECTION_STRATEGY_PROPS.AUTOCOMMIT, "true");
		this.addProperty(CONNECTION_STRATEGY_PROPS.TXN_AUTO_WRAP, "off");
		this.addProperty(ConfigPropertyNames.CONFIG_FILE, ConfigPropertyLoader.DEFAULT_CONFIG_FILE_NAME);
   	
		
	}      
      
}
