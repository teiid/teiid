/*
 * Copyright (c) 2000-2007 MetaMatrix, Inc.
 * All rights reserved.
 */
package org.teiid.test.framework.transaction;

import java.util.Random;

import javax.sql.XAConnection;
import javax.transaction.xa.XAResource;

import org.teiid.test.framework.TransactionContainer;
import org.teiid.test.framework.TransactionQueryTest;
import org.teiid.test.framework.exception.QueryTestFailedException;
import org.teiid.test.framework.exception.TransactionRuntimeException;

import com.metamatrix.common.xa.MMXid;

public class XATransaction extends TransactionContainer {
	private static Random RANDOM = new Random();
	private MMXid xid;
	
    public XATransaction() {
        super();
    }
        
    protected void before(TransactionQueryTest test) {
        try {          
        	xid = createXid();
        	XAResource xaResource = getXAConnection().getXAResource();
        	xaResource.setTransactionTimeout(120);
        	xaResource.start(xid, XAResource.TMNOFLAGS);
        	debug("Start transaction using XID: " + xid.toString());

        } catch (Exception e) {
            throw new TransactionRuntimeException(e);
        }        
    }

	public static MMXid createXid() {
		byte[] gid = new byte[10];
		byte[] bid = new byte[10];
		RANDOM.nextBytes(gid);
		RANDOM.nextBytes(bid);
		return new MMXid(0, gid, bid);
	}
    
    protected void after(TransactionQueryTest test) {
        boolean delistSuccessful = false;
        boolean commit = false;
        
        XAResource xaResource = null;
        boolean exception = false;
        try {
        	xaResource = getXAConnection().getXAResource();
            
			xaResource.end(xid, XAResource.TMSUCCESS);
            
            if (!test.exceptionExpected() && xaResource.prepare(xid) == XAResource.XA_OK) {
            	commit = true;
            }
            delistSuccessful = true;
        } catch (Exception e) {
        	exception = true;
            throw new TransactionRuntimeException(e);            
        } finally {
            try {
                if (!delistSuccessful || test.rollbackAllways()|| test.exceptionOccurred()) {
                	xaResource.rollback(xid);
                }
                else if (commit) {
                	xaResource.commit(xid, true);
                }            
            } catch (Exception e) {
            	if (!exception) {
            		throw new TransactionRuntimeException(e); 
            	}
            } 
        }
    }    
    
  
    protected XAConnection getXAConnection() throws QueryTestFailedException {
    	return this.connStrategy.getXAConnection();
    }
}
