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

package com.metamatrix.platform.config.transaction;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import com.metamatrix.common.id.TransactionID;
import com.metamatrix.common.transaction.TransactionException;
import com.metamatrix.common.transaction.TransactionStatus;
import com.metamatrix.common.transaction.manager.Transaction;
import com.metamatrix.platform.config.ConfigMessages;
import com.metamatrix.platform.config.ConfigPlugin;

public class ConfigTransaction implements Transaction {



// codes 0 - 2 for Server states
	public static final int SERVER_INITIALIZATION = 1;
	public static final int SERVER_SHUTDOWN = 2;
	public static final int SERVER_FORCE_INITIALIZATION = 3;
	public static final int SERVER_STARTED = 4;

	public static final int NO_SERVER_INITIALIZATION_ACTION = -1;

	private static final int EXPIRED_TRANSACTION = -99;


// codes 10 and above for other states

//	private static final int DEFAULT_NO_DEFINED_ACTION = -1;

//	private static final String NON_TRANSACTION_ACQUIRED_BY = "ReadTransaction";

    private TransactionID txnID;
    private int status;
    private long beginTime;
//    private long timeout;        // 2 seconds
    private boolean isReadOnly;
    private Object source;
    private ConfigTransactionLock lock;
    private ConfigTransactionLockFactory lockFactory;

    private Map configurationObjects = new HashMap(3);

    // @see StartupStateController for states
    private int action = NO_SERVER_INITIALIZATION_ACTION;

    private LockExpirationThread expiredThread;



    protected ConfigTransaction(ConfigTransactionLockFactory lockFactory, TransactionID txnID, Object source, long defaultTimeoutSeconds) {
        this.txnID = txnID;
        this.status = TransactionStatus.STATUS_ACTIVE;     //???
//        this.timeout = defaultTimeoutSeconds;
        this.source = source;
		this.isReadOnly = true;
		this.beginTime = (new Date()).getTime();
		this.lockFactory = lockFactory;


    }

    protected ConfigTransaction(ConfigTransactionLockFactory lockFactory, TransactionID txnID, long defaultTimeoutSeconds) {
        this(lockFactory, txnID,null,defaultTimeoutSeconds);
    }

   protected void finalize() {
   		cleanupExpireThread();

    	lock = null;
    	if (configurationObjects != null)  {
    		configurationObjects.clear();
    		configurationObjects = null;
   		}

   		source = null;
    }

    protected void cleanupExpireThread() {
   		if (expiredThread != null) {
   			expiredThread.stopChecking();
   		}

   		expiredThread = null;


    }

    /**
     * Obtain the status of the transaction associated with this object.
     * @return The transaction status.
     * @throws TransactionException if the status for this transaction could
     * not be obtained.
     */
    public int getStatus() throws TransactionException {
        return this.status;
    }

    public boolean hasExpired() {

    	if (this.status == EXPIRED_TRANSACTION) {
    		return true;
    	}
    	return false;
    }


    public long getBeginTime() {
    	return beginTime;
    }


    /**
     * Returns the lock that is held by the transaction.
     * @return XMLConfigurationLock that is held by a specific principal.
     */
    public ConfigTransactionLock getTransactionLock() {
    	return this.lock;
    }


    /**
     * This method is implemented by this class so that the
     * actual lock can be obtained prior to the transaction beginning.
     */
    public void begin(String principal, int reason, boolean readOnly) throws TransactionException{
		setReadOnly(readOnly);
    	if (!readOnly) {
	    	this.lock = this.lockFactory.obtainConfigTransactionLock(principal, reason, false);
    		if (this.lock == null) {
    			throw new ConfigTransactionException(ConfigMessages.CONFIG_0159, ConfigPlugin.Util.getString(ConfigMessages.CONFIG_0159, principal));
    		}

    	}

    }


    /**
     * This method should only be called by the ConfigUserTransaction when
     * it is beginning its transaction and when it ends the transaction.
     * NOTE: this is set to final and is scoped as package level so that
     * only the @see {ConfigUserTransaction} can set the lock
     */
    final void setTransactionLock(ConfigTransactionLock transLock) {
    	this.lock = transLock;

	       // Create the expiration cleaner thread
        this.expiredThread = new LockExpirationThread( this );
        this.expiredThread.start();

    }

    /**
     * Returns the name that holds the lock.
     * @return String name who holds the lock
     */
    public String getLockAcquiredBy() {
    	if (lock != null) {
	   		return lock.getLockHolder();
    	}
    	return null;
    }


    /**
     * Returns the transaction id that uniquely identifies this transaction
     * @return TransactionID that identifies the transaction
     */
    public TransactionID getTransactionID() {
        return this.txnID;
    }


    public boolean isReadOnly() {
        return this.isReadOnly;
   }

    public int getAction() {
    		return this.action;
    }

    public void setAction(int actionPerformed) {
    	// only allow the setting of the action once for the duration of the transaction
    	if (action == NO_SERVER_INITIALIZATION_ACTION) {
    		this.action = actionPerformed;
    	}
    }

    /**
     * This method is called from the expire thread and
     * must print out the stack trace so that someone will know
     * it failed to release because the expection cannot be thrown
     * because its not being called from above.
     */
    void expireTransaction() {
// do these seperately to ensure both are executed

		try {
			setRollbackOnly();
		} catch (Exception e) {
			e.printStackTrace();

		}

		try {
	    	lockFactory.releaseConfigTransactionLock(this.lock);
		} catch (Exception e) {
			e.printStackTrace();

		}
     	this.status = EXPIRED_TRANSACTION;
    }


    /**
     * Call to set the transaction as read only.
     * A value of <code>true</code> will indicate the transaction
     * is a read only transaction.
     * @param readTxn value of true sets the transaction to read only
     */
    void setReadOnly( boolean readTxn ) {
        this.isReadOnly = readTxn;
    }


    public Object getSource() {
        return this.source;
    }

	/**
	 * Call to set the source of the transaction.
	 * @param source object of where the transaction originated
	 */
    public void setSource( Object source ) {
        this.source = source;
    }

    /**
     * Modify the value of the timeout value that is associated with this object.
     * @param seconds The value of the timeout in seconds. If the value is
     * zero, the transaction service restores the default value.
     * @throws TransactionException if the timeout value is unable to be set
     * for this transaction.
     */
    public void setTransactionTimeout(int seconds) throws TransactionException {
        if ( seconds <= 0 ) {
            throw new IllegalArgumentException(ConfigPlugin.Util.getString("ConfigTransaction.Timeout_value_gt_0")); //$NON-NLS-1$
        }
//        this.timeout = seconds;
    }

    /**
     * Modify the transaction such that the only possible outcome of the transaction
     * is to roll back the transaction.
     * @throws TransactionException if the rollback flag is unable to be set
     * for this transaction.
     */
    public void setRollbackOnly() throws TransactionException{
        this.status = TransactionStatus.STATUS_MARKED_ROLLBACK;
    }



    /**
     * Complete the transaction represented by this TransactionObject.
     * @throws TransactionException if the transaction is unable to commit.
     */
    public void commit() throws TransactionException{
    	cleanupExpireThread();

        if ( this.status == TransactionStatus.STATUS_MARKED_ROLLBACK ) {
            throw new TransactionException(ConfigMessages.CONFIG_0160, ConfigPlugin.Util.getString(ConfigMessages.CONFIG_0160));
        }
        if ( this.status != TransactionStatus.STATUS_ACTIVE ) {
            throw new TransactionException(ConfigMessages.CONFIG_0161, ConfigPlugin.Util.getString(ConfigMessages.CONFIG_0161));
        }
        this.status = TransactionStatus.STATUS_COMMITTING;

        if ( isReadOnly() ) {
            this.status = TransactionStatus.STATUS_COMMITTED;
            return;
        }


	     this.lockFactory.releaseConfigTransactionLock(this.lock);

/*
        Set openedModels = new HashSet();
        Iterator iter = this.edit.getEvents().iterator();
        while ( iter.hasNext() ) {
            event = (ToolkitEvent) iter.next();
            Object eventTarget = event.getTarget();
            ToolkitLog.logTrace(ToolkitLogConstants.CTX_EVENT,new Object[]{"[ToolkitTransaction.commit] Firing event ",event," with target ",eventTarget});
//System.out.println("[ToolkitTransaction.commit] Firing event " + event + " with target " + eventTarget);

            // ModelOpenEvent and ModelCloseEvent type actions
            if ( eventTarget instanceof ResourceDescriptor ) {
                if ( event instanceof ModelOpenEvent ) {
                    openedModels.add(eventTarget);
                }
            }

            // If the event target is an abstract model entity, then notify
            // its model rather than the listener ...
            else if ( eventTarget instanceof AbstractModelEntity ) {
                AbstractModelEntity entity = (AbstractModelEntity) eventTarget;

                // Mark the resource as changed only if the model was not opened with this transaction ...
                Resource resource = entity.getResource();
                if (resource != null && ! resource.isClosed()) {
                    ResourceDescriptor descriptor = resource.getResourceDescriptor();
                    if ( descriptor instanceof XMIResourceDescriptor ) {
                        if ( ! openedModels.contains(descriptor) ) {
                            resource.markAsChanged();
                        }
                        // TODO: what about non-XMI descriptors for resources that are changed ...
                        //XMIResourceDescriptor xmiDescriptor = (XMIResourceDescriptor)descriptor;
                        //DirectoryEntry entry = xmiDescriptor.getDirectoryEntry();
                        //if ( ! openedModels.contains(entry) ) {
                        //    resource.markAsChanged();
                        //}
                    }
                    resource.notifyListeners(event);
                }
            } else {
                listener.processEvent( (ToolkitEvent) event );
            }
*/
            // Fire event to indicate that the transaction is complete
/*
            if ( !iter.hasNext() && event != null ) {
                event = new TransactionCompleteEvent(event.getSource(),event.getTarget(),event.getEventID());
                listener.processEvent(event);
                ToolkitLog.logTrace(ToolkitLogConstants.CTX_EVENT,new Object[]{"[ToolkitTransaction.commit] Firing event ",event," with target ",event.getTarget()});
            }
*/
 //       }

        this.status = TransactionStatus.STATUS_COMMITTED;


    }

    /**
     * Roll back the transaction represented by this TransactionObject.
     * @throws TransactionException if the transaction is unable to roll back.
     */
    public void rollback() throws TransactionException{
		cleanupExpireThread();
         if ( isReadOnly() ) {
            return;
        }
        if (lock != null) {
	      this.lockFactory.releaseConfigTransactionLock(this.lock);
        }


        this.status = TransactionStatus.STATUS_ROLLEDBACK;
/*
        ToolkitLog.logCritical(ToolkitLogConstants.CTX_TXN,"*******************************************");
        ToolkitLog.logCritical(ToolkitLogConstants.CTX_TXN,"ToolkitTransaction.rollback not implemented");
        ToolkitLog.logCritical(ToolkitLogConstants.CTX_TXN,"*******************************************");
        ToolkitLog.logTrace(ToolkitLogConstants.CTX_TXN,"END ToolkitTransaction.rollback()");
*/
    }


     /**
     * Returns the objects that changed during this transaction
     */
    public Collection getObjects() {
        Collection objs = new ArrayList();

        Iterator it = configurationObjects.keySet().iterator();
        while (it.hasNext()) {
            Object key = it.next();
            objs.add(configurationObjects.get(key));
        }

        return objs;

    }

    /**
     * Call to add an object to the set of objects that changed during this
     * transaction.
     * For the configuration process, this object will be
     * @see {ConfigurationModelContainer Configuration}
     * @param key is the id of the configuration
     * @param value is the configuration container
     */
    public void addObjects(Object key, Object value) {
        configurationObjects.put(key, value);

    }

     /**
     * Returns the objects that changed during this transaction.  For
     * the configuration process, these objects will be
     * @see {ConfigurationModelContainer Configurations}.
     * @return Collection of objects that changed during the transaction.
     */
    public Object getObject(Object key) {
        return configurationObjects.get(key);
    }

    public boolean contains(Object key) {
        return configurationObjects.containsKey(key);
    }

	class LockExpirationThread extends Thread
	{
		private ConfigTransaction transaction;

		private boolean continueChecks = true;
		private Object object = new Object();

		LockExpirationThread( ConfigTransaction transaction )
		{
			this.transaction = transaction;
		}

		public void stopChecking() {
			synchronized(object) {
				this.continueChecks = false;
			}
		}


		public void run()
		{
				performCheck();
		}

		protected void performCheck() {
			ConfigTransactionLock checkLock = transaction.getTransactionLock();

			while( this.continueChecks ) {

				// if the time has not expired then sleep for the difference
				// when the lock should expire, plus a litte more so that it gives
				// the release process the benefit of the doubt and time to release incase
				// it is actually being released
				if (checkLock.hasExpired()) {
					synchronized(object) {
						continueChecks = false;
						transaction.expireTransaction();
					}

				} else {
					try {
						sleep( checkLock.getTimeTillExpires() + 100 );
					} catch( InterruptedException e ) {
						// ignore it
					}
				}

			}


		}
	}

}




