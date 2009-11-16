/*
 * Copyright (c) 2000-2007 MetaMatrix, Inc.
 * All rights reserved.
 */
package org.teiid.test.framework.transaction;

import org.teiid.test.framework.ConfigPropertyLoader;
import org.teiid.test.framework.TransactionContainer;
import org.teiid.test.framework.TransactionQueryTest;
import org.teiid.test.framework.ConfigPropertyNames.CONNECTION_STRATEGY_PROPS;
import org.teiid.test.framework.ConfigPropertyNames.TXN_AUTO_WRAP_OPTIONS;


/** 
 * This transaction type only is valid when
 * AutoCommit = ON
 * txnAutoWrap = PESSIMISTIC
 */
public class PessimisticWrapTransaction extends TransactionContainer {
    public PessimisticWrapTransaction() {
	super();
    }
    
    public void before(TransactionQueryTest test) {
	this.setEnvironmentProperty(CONNECTION_STRATEGY_PROPS.TXN_AUTO_WRAP, TXN_AUTO_WRAP_OPTIONS.AUTO_WRAP_PESSIMISTIC);

    }
    
    public void after(TransactionQueryTest test) {

    }
    

}
