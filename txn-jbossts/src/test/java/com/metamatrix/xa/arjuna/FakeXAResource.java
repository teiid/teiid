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

import java.util.HashSet;
import java.util.Iterator;

import javax.transaction.xa.XAException;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;

class FakeXAResource implements XAResource {
    String name;
    HashSet recoverables = new HashSet(); 
    RecoveryCallback callback;
    boolean throwRollbackException = false;
    
    FakeXAResource(String name){
        this.name = name;
    }
    
    public void commit(Xid xid,boolean onePhase) throws XAException {
        if (callback != null) {
            this.callback.onCommit(xid);
        }
    }
    
    public void end(Xid xid,int flags) throws XAException {
    }
    
    public void forget(Xid xid) throws XAException {
        if (callback != null) {
            this.callback.onForget(xid);
        }
        recoverables.remove(xid);
    }
    
    public int getTransactionTimeout() throws XAException {
        return 0;
    }
    
    public boolean isSameRM(XAResource xares) throws XAException {
        FakeXAResource res = (FakeXAResource)xares;
        return this.name.equals(res.name);
    }
    
    public int prepare(Xid xid) throws XAException {
        return 0;
    }
    
    public Xid[] recover(int flag) throws XAException {
        if (!recoverables.isEmpty()) {
            Xid[] xids = new Xid[recoverables.size()];
            int idx = 0;
            for (Iterator i = recoverables.iterator(); i.hasNext();) {
                xids[idx] = (Xid)i.next();
                idx++;
            }
            return xids;
        }
        return new Xid[] {};
    }
    
    public void rollback(Xid xid) throws XAException {
        if (throwRollbackException) {
            throw new XAException(XAException.XA_HEURCOM);
        }
        if (callback != null) {
            this.callback.onRollback(xid);
        }
        this.recoverables.remove(xid);        
    }
    
    public boolean setTransactionTimeout(int seconds) throws XAException {
        return false;
    }
     
    public void start(Xid xid, int flags) throws XAException {
    }        
    
    public void setRecoverableXid(Xid xid) {
        this.recoverables.add(xid);
    }
    
    public void setCallback(RecoveryCallback callback) {
        this.callback = callback; 
    }
}
