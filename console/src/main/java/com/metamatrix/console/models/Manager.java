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

package com.metamatrix.console.models;

import java.util.ArrayList;
import java.util.List;

import javax.swing.event.EventListenerList;

import com.metamatrix.common.util.crypto.Encryptor;
import com.metamatrix.common.util.crypto.NullCryptor;
import com.metamatrix.console.connections.ConnectionInfo;
  
/**
 * Superclass of all Managers.  Handles the collection and notification
 *of ManagerListeners with ModelChangeEvents
 * @see ModelChangeEvent
 * @see ManagerListener
 */
public abstract class Manager {

    /**
     * This is the message sent along with a ModelChangedEvent, in the event
     *that a Manager now has new data
     * @see ModelChangedEvent
     */
    public static final String MODEL_CHANGED = "Managed model changed."; //$NON-NLS-1$

    /**
     * This is the message sent along with a ModelChangedEvent, in the event
     *that a Manager's data has become stale.  In response, a ModelListener
     *will usually request some data - this would trigger another
     *ModelChangedEvent, this time with a Model Changed message
     * @see Manager#MODEL_CHANGED
     * @see ModelChangedEvent
     */
    public static final String MODEL_STALE = "Managed model data stale."; //$NON-NLS-1$

    private boolean isStale;
    private EventListenerList listeners;
    private ConnectionInfo connection;

	public Manager(ConnectionInfo connection) {
		super();
		this.connection = connection;
	}
	

    /**
     * Get a connection to the server.
     * @param reconnect If true, reconnect if not currently connected.
     * @return
     * @since 4.3
     */
	public ConnectionInfo getConnection(boolean reconnect) {
	    // this one re-connects to broken server connection.
	    connection.getServerConnection(reconnect); 
		return connection;
	}
    
    /**
     * Get a connection to the server.
     * Reconnect if not currently connected.
     * @return
     * @since 4.3
     */
    public ConnectionInfo getConnection() {
        return getConnection(true);
    }
    
	
    
    /**
     * Needs to be called by subclasses - sets up event
     *listening.  (Subclasses should call super.init() in the body of their
     *init() method.)<P>
     *
     * <B>TODO:</B> Perhaps this superclass should set itself to stale as
     *part of the initialization process (see commented code in method body).
     *That would broadcast an event to listeners which would prompt them to
     *begin calling methods on a Manager - or perhaps a start() method
     *with this code should
     *be defined that will be called after init() (allowing subclasses to
     *extend init() before start() would be called) 
     */
    public void init(){
        setListeners(new EventListenerList());
    }

    /**
     * Refresh tells a manager that it's data is stale and it should retrieve
     *real data (rather than cached data) the next time data is requested.<P>
     *
     * Currently this method simply calls setIsStale(false)
     * @see Manager#setIsStale
     */
    public void refresh(){
        setIsStale(true);
    }

    /**
     * Objects wishing to listen for ModelChangedEvents can register
     *themselves as a listener for this Manager.
     * @param listener an object implementing ManagerListener interface
     * @see ModelChangedEvent
     * @see ManagerListener
     */
    public void addManagerListener(ManagerListener listener) {
        getListeners().add(ManagerListener.class, listener);
    }

    /**
     * Objects not wishing to listen to this Manager's ModelChangedEvents
     *anymore can unregister themselves as a listener
     * @param listener an object implementing ManagerListener interface
     * @see ModelChangedEvent
     * @see ManagerListener
     */
    public void removeManagerListener(ManagerListener listener) {
        getListeners().remove(ManagerListener.class, listener);
    }

    /**
     * This method is intended to be called by subclasses to fire
     *event with a specific meaning (indicated by the message
     *parameter).
     * @param message <CODE>String</CODE> assumed to be passed in by
     *subclass using this message.  This class provides some String
     *constants which can be used for this message, and each subclass
     *will likely provide additional String constants
     * @see Manager#MODEL_CHANGED
     * @see Manager#MODEL_STALE
     */
    protected void fireModelChangedEvent(String message) {
        fireModelChangedEvent(message, ModelChangedEvent.NO_ARG);
    }

    /**
     * This method is intended to be called by subclasses to fire
     *event with a specific meaning (indicated by the message
     *parameter and the Object argument).
     * @param message <CODE>String</CODE> assumed to be passed in by
     *subclass using this message.  This class provides some String
     *constants which can be used for this message, and each subclass
     *will likely provide additional String constants
     * @param arg optional Object argument
     * @see Manager#MODEL_CHANGED
     * @see Manager#MODEL_STALE
     */
    protected void fireModelChangedEvent(String message, Object arg) {
        ModelChangedEvent e = new ModelChangedEvent(this, message, arg);

        // Guaranteed to return a non-null array
        Object[] listeners = getListeners().getListenerList();

        // Process the listeners last to first, notifying
        // those (ManagerListeners) that are interested in this event
        for (int i = listeners.length-2; i>=0; i-=2) {
            if (listeners[i]==ManagerListener.class) {
                ((ManagerListener)listeners[i+1]).modelChanged(e);
            }	       
        }
    }

    //GETTERS-SETTERS

    /**
     * Indicates whether this Manager's cached data is stale (i.e. invalid,
     *expired, dirty).  In a client-server environment, this means that
     *the Manager has decided it's cached data no longer accurately
     *reflects the up-to-date data on the server.<P>
     *
     * Listeners are notified once a Manager becomes stale - it is up to
     *a listener to request a specific service from a Manager (subclass)
     *and thus prompt a Manager to update its data.  This allows for the
     *case where a Manager becomes stale, and a listener (perhaps part of
     *a GUI that is currently not visible) does not yet need refreshed
     *data.
     *
     * @return boolean indicates whether this Manager considers it's data
     *to be stale and is due to refresh it
     */
    public boolean getIsStale(){
        return isStale;
    }

    /**
     * Sets the isStale flag.  If this flag is set to false, the Manager fires
     *a ModelChanged event with a message indicating that data is stale.
     *ManagerListener's will typically want to retrieve data from its Manager
     *in response, although in some cases maybe not (a listener may be a
     *GUI component that is currently hidden from the user).
     *
     * @param isStale boolean indicates whether this Manager considers it's
     *data to be stale and is due to refresh it
     * @see ModelChangedEvent
     * @see ManagerListener
     * @see Manager#MODEL_STALE
     */
    public void setIsStale(boolean isStale){
        this.isStale = isStale;
        if (this.isStale){
            fireModelChangedEvent(MODEL_STALE);
        }
    }

    private EventListenerList getListeners(){
        return listeners;
    }

    protected List getEventListeners() {
    	Object[] listeners = getListeners().getListeners(ManagerListener.class);
    	ArrayList list = new ArrayList();
    	int count = listeners.length;
    	for (int i=0; i < count; i++) {
    		list.add(listeners[i]);
    	}
    	return list;
    }
    
    private void setListeners(EventListenerList aList){
        listeners = aList;
    }
    
    
    /**
     * Get encryptor to use for encrypting passwords to send from the client to the server. 
     * @return
     * @since 4.3
     */
    public synchronized Encryptor getEncryptor() {
        return new NullCryptor();
    }
}
