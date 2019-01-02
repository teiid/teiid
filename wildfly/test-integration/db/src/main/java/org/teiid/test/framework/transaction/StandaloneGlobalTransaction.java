/*
 * Copyright (c) 2000-2007 MetaMatrix, Inc.
 * All rights reserved.
 */
package org.teiid.test.framework.transaction;

import java.util.Random;

import javax.transaction.xa.XAResource;

import org.teiid.client.xa.XidImpl;
import org.teiid.test.framework.TransactionContainer;
import org.teiid.test.framework.TransactionQueryTestCase;
import org.teiid.test.framework.exception.TransactionRuntimeException;


public class StandaloneGlobalTransaction extends TransactionContainer {
	private static Random RANDOM = new Random();
	private XidImpl xid;
	
    public StandaloneGlobalTransaction() {
        super();
    }
        
    @Override
    protected void before(TransactionQueryTestCase test) {
        try {          
        	xid = createXid();
        	XAResource xaResource = test.getConnectionStrategy().getXAConnection().getXAResource();
         	xaResource.setTransactionTimeout(120);
        	xaResource.start(xid, XAResource.TMNOFLAGS);
        } catch (Exception e) {
            throw new TransactionRuntimeException(e);
        }        
    }

	public static XidImpl createXid() {
		byte[] gid = new byte[10];
		byte[] bid = new byte[10];
		RANDOM.nextBytes(gid);
		RANDOM.nextBytes(bid);
		return new XidImpl(0, gid, bid);
	}
    
    @Override
    protected void after(TransactionQueryTestCase test) {
        boolean delistSuccessful = false;
        boolean commit = false;
        try {
            XAResource xaResource = test.getConnectionStrategy().getXAConnection().getXAResource();
            
			xaResource.end(xid, XAResource.TMSUCCESS);
            
            if (!test.exceptionExpected() && xaResource.prepare(xid) == XAResource.XA_OK) {
            	commit = true;
            }
            delistSuccessful = true;
        } catch (Exception e) {
            throw new TransactionRuntimeException(e);            
        } finally {
            try {
                if (!delistSuccessful || test.rollbackAllways()|| test.exceptionOccurred()) {
                    test.getConnectionStrategy().getXAConnection().getXAResource().rollback(xid);
                 }
                else if (commit) {
                    test.getConnectionStrategy().getXAConnection().getXAResource().commit(xid, true);
                }            
            } catch (Exception e) {
                throw new TransactionRuntimeException(e);            
            } 
        }
    }    
}
