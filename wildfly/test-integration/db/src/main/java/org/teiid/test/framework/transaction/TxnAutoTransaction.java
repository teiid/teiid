/*
 * Copyright (c) 2000-2007 MetaMatrix, Inc.
 * All rights reserved.
 */
package org.teiid.test.framework.transaction;

import org.teiid.test.framework.TransactionContainer;
import org.teiid.test.framework.TransactionQueryTestCase;
import org.teiid.test.framework.ConfigPropertyNames.CONNECTION_STRATEGY_PROPS;


/** 
 * This transction is only valid when 
 * AutoCommit = ON 
 * txnAutoWrap = Optimistic.
 */
public class TxnAutoTransaction extends TransactionContainer {
    
    private String autocommittxn = null;
    
    public TxnAutoTransaction() {
	super();
    }
    
    public TxnAutoTransaction(String autocommittxn) {
	super();
	this.autocommittxn = autocommittxn;
    }
    
    public void before(TransactionQueryTestCase test) {
	if (this.autocommittxn != null) {
	    test.getConnectionStrategy().setEnvironmentProperty(CONNECTION_STRATEGY_PROPS.TXN_AUTO_WRAP, this.autocommittxn);
	}

    }
    
    public void after(TransactionQueryTestCase test) {

    }
}
