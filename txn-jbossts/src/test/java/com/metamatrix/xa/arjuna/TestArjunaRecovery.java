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

package com.metamatrix.xa.arjuna;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;

import org.teiid.connector.api.ConnectorException;
import org.teiid.dqp.internal.transaction.TransactionProvider;

import junit.framework.TestCase;

import com.arjuna.ats.arjuna.common.Configuration;
import com.arjuna.ats.arjuna.recovery.RecoveryConfiguration;
import com.metamatrix.common.xa.MMXid;
import com.metamatrix.dqp.service.TransactionService;


/**
 * Test that Arjuna is calling into the xa resource in event of recovery
 */
public class TestArjunaRecovery extends TestCase {
	
    public static final class FakeXAConnectionSource implements
			TransactionProvider.XAConnectionSource {
		private final FakeXAConnection connection_1;

		FakeXAConnectionSource(FakeXAConnection connection_1) {
			this.connection_1 = connection_1;
		}
		
		@Override
		public XAResource getXAResource() throws SQLException {
			try {
				return connection_1.getXAResource();
			} catch (ConnectorException e) {
				throw new SQLException(e);
			}
		}
		
		@Override
		public void close() {
			connection_1.close();
		}
	}

	private static final String CONN2 = "conn2";//$NON-NLS-1$
    private static final String CONN1 = "conn1";//$NON-NLS-1$


    public TestArjunaRecovery() {
    	super();
    }
    
    public void testRecovery() throws Exception{
        final List done = new ArrayList();
        
        Properties props = new Properties();
        props.setProperty(TransactionService.PROCESSNAME, "test"); //$NON-NLS-1$
        RecoveryConfiguration.setRecoveryManagerPropertiesFile("com/metamatrix/xa/arjuna/jbossjta-properties.xml"); //$NON-NLS-1$
        com.arjuna.ats.jta.common.Configuration.setPropertiesFile("com/metamatrix/xa/arjuna/jbossjta-properties.xml"); //$NON-NLS-1$
        Configuration.setPropertiesFile("com/metamatrix/xa/arjuna/jbossjta-properties.xml"); //$NON-NLS-1$
        com.arjuna.ats.txoj.common.Configuration.setPropertiesFile("com/metamatrix/xa/arjuna/jbossjta-properties.xml"); //$NON-NLS-1$
        
        // now setup some fake xid to recover
        final FakeXAConnection connection_1 = new FakeXAConnection(CONN1);
        final FakeXAConnection connection_2 = new FakeXAConnection(CONN2);

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
        provider.registerRecoverySource(CONN1, new FakeXAConnectionSource(connection_1));
        provider.registerRecoverySource(CONN2, new FakeXAConnectionSource(connection_2));

        synchronized (done) {
            done.wait(60000);    
        }
        provider.shutdown();
        
        assertEquals(2, done.size());
    }
}
