/*
 * Copyright (c) 2000-2007 MetaMatrix, Inc.
 * All rights reserved.
 */
package org.teiid.test.testcases;

import org.teiid.test.framework.ConfigPropertyNames;
import org.teiid.test.framework.ConfigPropertyNames.CONNECTION_STRATEGY_PROPS;


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
//public class XATransactionDatasourceFalseOffTest extends SingleSourceTransactionScenarios {
   
	public class XATransactionDatasourceFalseOffTest extends TwoSourceTransactionScenarios {

	
	
    public XATransactionDatasourceFalseOffTest(String name) {
		super(name);
	}

	@Override
	protected void setUp() throws Exception {
		super.setUp();
		
		//XATransactions currently doesn't support using sqlserver 
		//{@see TEIID-559} 
		this.addProperty(ConfigPropertyNames.EXCLUDE_DATASBASE_TYPES_PROP, "sqlserver");
		
		this.addProperty(ConfigPropertyNames.CONFIG_FILE, "xa-config.properties");
		
		this.addProperty(ConfigPropertyNames.CONNECTION_TYPE, ConfigPropertyNames.CONNECTION_TYPES.DATASOURCE_CONNECTION);
		this.addProperty(CONNECTION_STRATEGY_PROPS.AUTOCOMMIT, "false");
 //   	this.addProperty(ConnectionStrategy.TXN_AUTO_WRAP, "on");
    	
		
	}


     
}
