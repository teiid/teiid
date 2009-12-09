/*
 * Copyright (c) 2000-2007 MetaMatrix, Inc.
 * All rights reserved.
 */
package org.teiid.test.framework.transaction;

import org.teiid.test.framework.TransactionContainer;
import org.teiid.test.framework.TransactionQueryTestCase;
import org.teiid.test.framework.ConfigPropertyNames.CONNECTION_STRATEGY_PROPS;
import org.teiid.test.framework.ConfigPropertyNames.TXN_AUTO_WRAP_OPTIONS;


/** 
 * This transction is only valid when 
 * AutoCommit = ON 
 * txnAutoWrap = Optimistic.
 */
public class OptimisticWrapTransaction extends TransactionContainer {
    public OptimisticWrapTransaction() {
	super();
    }
    
    public void before(TransactionQueryTestCase test) {
	test.getConnectionStrategy().setEnvironmentProperty(CONNECTION_STRATEGY_PROPS.TXN_AUTO_WRAP, TXN_AUTO_WRAP_OPTIONS.AUTO_WRAP_OPTIMISTIC);

    }
    
    public void after(TransactionQueryTestCase test) {

    }
}
