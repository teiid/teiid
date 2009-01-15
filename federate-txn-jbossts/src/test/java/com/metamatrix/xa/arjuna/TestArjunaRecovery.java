/*
 * JBoss, Home of Professional Open Source.
 * Copyright (C) 2008 Red Hat, Inc.
 * Copyright (C) 2000-2007 MetaMatrix, Inc.
 * Licensed to Red Hat, Inc. under one or more contributor 
 * license agreements.  See the copyright.txt file in the
 * distribution for a full listing of individual contributors.
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

package com.metamatrix.xa.arjuna;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import javax.transaction.xa.Xid;

import junit.framework.TestCase;

import com.arjuna.ats.arjuna.common.Configuration;
import com.arjuna.ats.arjuna.recovery.RecoveryConfiguration;

import com.metamatrix.common.xa.MMXid;
import com.metamatrix.dqp.service.TransactionService;


/**
 * Test that Arjuna is calling into the xa resource in event of recovery
 */
public class TestArjunaRecovery extends TestCase {
    private static final String CONN2 = "conn2";//$NON-NLS-1$
    private static final String CONN1 = "conn1";//$NON-NLS-1$


    public TestArjunaRecovery() {
    	super();
    	System.setProperty("metamatrix.config.none", "true");
    }
    
    public void testRecovery() throws Exception{
        final List done = new ArrayList();
        
        Properties props = new Properties();
        props.setProperty(TransactionService.VMNAME, "test"); //$NON-NLS-1$
        props.setProperty(TransactionService.HOSTNAME, "test"); //$NON-NLS-1$
        RecoveryConfiguration.setRecoveryManagerPropertiesFile("com/metamatrix/xa/arjuna/jbossjta-properties.xml"); //$NON-NLS-1$
        com.arjuna.ats.jta.common.Configuration.setPropertiesFile("com/metamatrix/xa/arjuna/jbossjta-properties.xml"); //$NON-NLS-1$
        Configuration.setPropertiesFile("com/metamatrix/xa/arjuna/jbossjta-properties.xml"); //$NON-NLS-1$
        com.arjuna.ats.txoj.common.Configuration.setPropertiesFile("com/metamatrix/xa/arjuna/jbossjta-properties.xml"); //$NON-NLS-1$
        
        FakeXAConnector connector_1 = new FakeXAConnector(CONN1);
        FakeXAConnector connector_2 = new FakeXAConnector(CONN2);
                
        // now setup some fake xid to recover
        FakeXAConnection connection_1 = (FakeXAConnection)connector_1.getXAConnection(null,null);
        FakeXAConnection connection_2 = (FakeXAConnection)connector_2.getXAConnection(null,null);

        final MMXid xid1 = new MMXid(1, "conn1".getBytes(), "txn1".getBytes()); //$NON-NLS-1$ //$NON-NLS-2$
        final MMXid xid2 = new MMXid(2, "conn2".getBytes(), "txn2".getBytes()); //$NON-NLS-1$ //$NON-NLS-2$
        
        // set up the resource to rollback txn
        FakeXAResource resource_1 = (FakeXAResource)connection_1.getXAResource();
        resource_1.setCallback(new RecoveryCallback() {
            public void onCommit(Xid xid) {
                fail("must not call commit"); //$NON-NLS-1$
            }
            public void onForget(Xid xid) {
                fail("must not call forget"); //$NON-NLS-1$
            }
            public void onRollback(Xid xid) {
                assertTrue(xid==xid1);
                
                synchronized (done) {
                    done.add(new Object());
                    if (done.size() == 2) {
                        done.notifyAll();
                    }
                }
            }
        });
        
        // set up resource 2, to forget the txn
        FakeXAResource resource_2 = (FakeXAResource)connection_2.getXAResource();
        resource_2.throwRollbackException = true;
        
        resource_2.setCallback(new RecoveryCallback() {
            public void onCommit(Xid xid) {
                fail("must not call commit"); //$NON-NLS-1$
            }
            public void onForget(Xid xid) {
                assertTrue(xid==xid2);
                synchronized (done) {
                    done.add(new Object());
                    if (done.size() == 2) {
                        done.notifyAll();
                    }
                }
            }
            public void onRollback(Xid xid) {
                fail("must not call rollback"); //$NON-NLS-1$
            }
        });
     
        resource_1.setRecoverableXid(xid1); 
        resource_2.setRecoverableXid(xid2); 
        
        ArjunaTransactionProvider provider = ArjunaTransactionProvider.getInstance(props);
        provider.registerRecoverySource(CONN1, connector_1);
        provider.registerRecoverySource(CONN2, connector_2);

        synchronized (done) {
            done.wait(60000);    
        }
        provider.shutdown();
        
        assertEquals(2, done.size());
    }
}
