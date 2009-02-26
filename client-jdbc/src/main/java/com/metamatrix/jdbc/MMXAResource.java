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

package com.metamatrix.jdbc;

import java.sql.SQLException;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.transaction.xa.XAException;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;

import com.metamatrix.common.comm.exception.CommunicationException;
import com.metamatrix.common.xa.MMXid;
import com.metamatrix.common.xa.XATransactionException;

/**
 * Implementation of XAResource.
 */
public class MMXAResource implements XAResource{
	private static Logger logger = Logger.getLogger("org.teiid.jdbc"); //$NON-NLS-1$

	private MMXAConnection mmConnection;
	private int timeOut;
    
    public static MMXAResource newInstance (MMXAConnection mmConnection){
        return new MMXAResource(mmConnection);
    }
    
	public MMXAResource(MMXAConnection mmConnection){
		this.mmConnection = mmConnection;
	}	
	    
	/**
     * @see javax.transaction.xa.XAResource#commit(javax.transaction.xa.Xid, boolean)
     */
    public void commit(Xid xid, boolean onePhase) throws XAException {
    	MMXid mmXid = getMMXid(xid);
		try{
			getMMConnection().commitTransaction(mmXid, onePhase); 	
		}catch(SQLException e){
			String logMsg = JDBCPlugin.Util.getString("MMXAResource.FailedCommitTXN", xid, onePhase ? "true":"false"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
			throw handleError(e, logMsg);
		}
    }

    private XAException handleError(Exception e,String logMsg) {
        logger.log(Level.SEVERE, logMsg, e);

        if(e instanceof MMSQLException){
            Throwable ex = ((MMSQLException)e).getCause();
            if(ex instanceof XAException){
                return (XAException)ex;
            }
            if (ex instanceof XATransactionException) {
                return ((XATransactionException)ex).getXAException();
            }
        }
        return new XAException(XAException.XAER_RMERR);
    }

	/**
     * @see javax.transaction.xa.XAResource#end(javax.transaction.xa.Xid, int)
     */
    public void end(Xid xid, int flag) throws XAException {
    	MMXid mmXid = getMMXid(xid);
		try{
            getMMConnection().endTransaction(mmXid, flag); 	
		}catch(SQLException e){
            String logMsg = JDBCPlugin.Util.getString("MMXAResource.FailedEndTXN", xid, new Integer(flag)); //$NON-NLS-1$
            throw handleError(e, logMsg);
		}
    }

    /**
     * @see javax.transaction.xa.XAResource#forget(javax.transaction.xa.Xid)
     */
    public void forget(Xid xid) throws XAException {
    	MMXid mmXid = getMMXid(xid);
		try{
            getMMConnection().forgetTransaction(mmXid); 	
        }catch(SQLException e){
            String logMsg = JDBCPlugin.Util.getString("MMXAResource.FailedForgetTXN", xid); //$NON-NLS-1$
            throw handleError(e, logMsg);
        }
    }

    /**
     * @see javax.transaction.xa.XAResource#getTransactionTimeout()
     */
    public int getTransactionTimeout() throws XAException {
        return timeOut;
    }

    /**
     * @see javax.transaction.xa.XAResource#isSameRM(javax.transaction.xa.XAResource)
     */
    public boolean isSameRM(XAResource arg0) throws XAException {
    	if (arg0 == this) {
    		return true;
    	}
        if (!(arg0 instanceof MMXAResource)) {
        	return false;
        }
        MMXAResource other = (MMXAResource)arg0;
		try {
			return this.getMMConnection().isSameProcess(other.getMMConnection());
		} catch (CommunicationException e) {
			throw handleError(e, JDBCPlugin.Util.getString("MMXAResource.FailedISSameRM")); //$NON-NLS-1$
		}
    }

    /**
     * @see javax.transaction.xa.XAResource#prepare(javax.transaction.xa.Xid)
     */
    public int prepare(Xid xid) throws XAException {
    	MMXid mmXid = getMMXid(xid);
		try{
			return getMMConnection().prepareTransaction(mmXid); 	
        }catch(SQLException e){
            String logMsg = JDBCPlugin.Util.getString("MMXAResource.FailedPrepareTXN", xid); //$NON-NLS-1$
            throw handleError(e, logMsg);
        }
    }

    /**
     * @see javax.transaction.xa.XAResource#recover(int)
     */
    public Xid[] recover(int flag) throws XAException {
        try{
			return getMMConnection().recoverTransaction(flag); 	
        }catch(SQLException e){
            String logMsg = JDBCPlugin.Util.getString("MMXAResource.FailedRecoverTXN", flag); //$NON-NLS-1$
            throw handleError(e, logMsg);
        }
    }

    /**
     * @see javax.transaction.xa.XAResource#rollback(javax.transaction.xa.Xid)
     */
    public void rollback(Xid xid) throws XAException {
    	MMXid mmXid = getMMXid(xid);
		try{
            getMMConnection().rollbackTransaction(mmXid); 	
        }catch(SQLException e){
            String logMsg = JDBCPlugin.Util.getString("MMXAResource.FailedRollbackTXN", xid); //$NON-NLS-1$
            throw handleError(e, logMsg);
        }
    }

    /**
     * @see javax.transaction.xa.XAResource#setTransactionTimeout(int)
     */
    public boolean setTransactionTimeout(int seconds) throws XAException {
        timeOut = seconds;
        return true;
    }

    /**
     * @see javax.transaction.xa.XAResource#start(javax.transaction.xa.Xid, int)
     */
    public void start(Xid xid, int flag) throws XAException {
    	MMXid mmXid = getMMXid(xid);
		try{
			getMMConnection().startTransaction(mmXid, flag, timeOut); 	
        }catch(SQLException e){
            String logMsg = JDBCPlugin.Util.getString("MMXAResource.FailedStartTXN", xid, new Integer(flag)); //$NON-NLS-1$
            handleError(e, logMsg);
        }
    }
        
    private MMConnection getMMConnection() throws XAException{
    	try{
    	    return this.mmConnection.getMMConnection();
    	}catch(SQLException e){
    		throw new XAException(XAException.XAER_RMFAIL);
    	}
    }
    
    /**
	 * @param xid
	 * @return
     * @throws XAException
	 */
	private MMXid getMMXid(Xid originalXid) {
		return new MMXid(originalXid);
	}
}
