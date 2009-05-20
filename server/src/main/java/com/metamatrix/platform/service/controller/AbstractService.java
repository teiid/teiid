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

package com.metamatrix.platform.service.controller;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.Enumeration;
import java.util.EventObject;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;

import com.metamatrix.common.application.ClassLoaderManager;
import com.metamatrix.common.comm.ClientServiceRegistry;
import com.metamatrix.common.config.CurrentConfiguration;
import com.metamatrix.common.config.api.DeployedComponentID;
import com.metamatrix.common.log.LogManager;
import com.metamatrix.common.queue.WorkerPoolStats;
import com.metamatrix.common.stats.ConnectionPoolStats;
import com.metamatrix.common.util.LogCommonConstants;
import com.metamatrix.common.util.PasswordMaskUtil;
import com.metamatrix.core.event.EventObjectListener;
import com.metamatrix.core.log.MessageLevel;
import com.metamatrix.platform.service.ServiceMessages;
import com.metamatrix.platform.service.ServicePlugin;
import com.metamatrix.platform.service.api.ServiceID;
import com.metamatrix.platform.service.api.ServiceInterface;
import com.metamatrix.platform.service.api.ServiceState;
import com.metamatrix.platform.service.api.exception.ServiceClosedException;
import com.metamatrix.platform.service.api.exception.ServiceException;
import com.metamatrix.platform.service.api.exception.ServiceNotInitializedException;
import com.metamatrix.platform.service.api.exception.ServiceStateException;
import com.metamatrix.platform.service.api.exception.ServiceSuspendedException;

/**
 * Base class for all services.
 */
//public abstract class AbstractService extends UnicastRemoteObject implements ServiceInterface, EventObjectListener {
public abstract class AbstractService implements ServiceInterface, EventObjectListener {

    private ServiceData data = new ServiceData(ServiceState.STATE_NOT_INITIALIZED);
    private Properties props;

	/**
     * Default constructor.
     */
    public AbstractService() {
    }

    //--------------------------------------------------------------
    // ServiceInterface methods
    //--------------------------------------------------------------

    /**
     * Initialize a service using the properties specified. The properties MUST contain
     * a property SERVICE_TYPE and SERVICE_SUB_TYPE which identifies the type of service. All other properties
     * are service specific. The properties object supplied MUST NOT CONTAIN DEFAULTS as
     * the default properties DO NOT serialize. If a properties object which uses defaults is
     * supplied, a call to getProperties() will return an incomplete set of properties since
     * the method is typically invoked remotely.
     *
     * @param id The ServiceID this service was registered with
     * @param deployedComponentID Unique identifier of this deployed component.
     * @param props the properties which define the service configuration
     * @param controller ServiceBusInterface which supplies resources such as event processing
     */
    public void init(ServiceID id, DeployedComponentID deployedComponentID, Properties props, ClientServiceRegistry listenerRegistry, ClassLoaderManager clManager) {

        if (props == null) {
            throw new ServiceException(ServiceMessages.SERVICE_0001, ServicePlugin.Util.getString(ServiceMessages.SERVICE_0001));
        }

        this.props = props;

        if (data.getServiceType() == null || data.getServiceType().trim().length() == 0) {
            throw new ServiceException(ServiceMessages.SERVICE_0002, ServicePlugin.Util.getString(ServiceMessages.SERVICE_0002, ServicePropertyNames.COMPONENT_TYPE_NAME ));
        }

        logMessagePrivate(ServicePlugin.Util.getString(ServiceMessages.MSG_SERVICE_0001, data.getInstanceName()));
        logMessagePrivate(ServicePlugin.Util.getString(ServiceMessages.MSG_SERVICE_0002, data.getInstanceName(), System.getProperty("java.class.path"))); //$NON-NLS-1$

        try {
            Properties resourceProps = CurrentConfiguration.getInstance().getResourceProperties(getResourceName());
            if (resourceProps != null) {
                this.props.putAll(resourceProps);
            }

            logServiceProperties(this.props);
            logMessagePrivate(ServicePlugin.Util.getString(ServiceMessages.MSG_SERVICE_0003, data.getInstanceName()));

            // Initialize!
            logMessagePrivate(ServicePlugin.Util.getString(ServiceMessages.MSG_SERVICE_0004, data.getServiceType()));
            initService(this.props);
            registerForEvents();
            logMessagePrivate(ServicePlugin.Util.getString(ServiceMessages.MSG_SERVICE_0005, getServiceType()));

            data.setStartTime(new Date());
            markAsOpen();
        } catch (Throwable e) {
            setInitException(e);
            throw new ServiceException(e, ServiceMessages.SERVICE_0004, ServicePlugin.Util.getString(ServiceMessages.SERVICE_0004, getServiceType()) );
        } 
    }

    /**
     * Return the type of service (QueryService, SubscriptionService, etc...)
     *
     * @return String representing type of service.
     */
    public final String getServiceType() {
        return data.getServiceType();
    }

    /**
     * Return current state of of the service.
     *
     *      STATE_NOT_INITIALIZED
     *      STATE_OPEN
     *      STATE_CLOSED
     *      STATE_SUSPENDED
     *
     * @return int representing current state.
     */
    public final int getCurrentState() {
        return data.getState();
    }

    /**
     * Return the time the current state was entered.
     *
     * @return Date representing time of state change.
     */
    public final Date getStateChangeTime() {
        return this.data.getStateChangeTime();
    }

    /**
     * Return id of this service.
     *
     * @return ServiceID
     */
    public final ServiceID getID() {
        return this.data.getId();
    }

    /**
     * This method will gracefully shutdown the service.
     * Sub classes of AbstractService class should implement
     * unregisterForEvents(), closeService(), waitForServiceToClear() and
     * killService to gracefully shut down the service.
     *
     */
    public final void die() {
        try {
            logMessagePrivate(ServicePlugin.Util.getString(ServiceMessages.MSG_SERVICE_0006, this.data.getId()));
            markAsClosed();
            unregisterForEvents();
            closeService();
            waitForServiceToClear();
            dieNow();
        } catch (Exception e) {
            throw new ServiceException( e, ServiceMessages.SERVICE_0005, ServicePlugin.Util.getString(ServiceMessages.SERVICE_0005, getServiceType()));
        }
    }

    /**
     * This method will shutdown the service immediately.
     * Sub classes of AbstractService class should implement
     * unregisterForEvents() and killService().
     */
    public final void dieNow(){

        try {

            if (!isClosed()) {
                markAsClosed();
                unregisterForEvents();
            }
            this.data.setInitException(null);
            killService();
        } catch (Exception e) {
            throw new ServiceException(e, ServiceMessages.SERVICE_0005, ServicePlugin.Util.getString(ServiceMessages.SERVICE_0005, getServiceType()));
        } 
    }


    /**
     * Return the properties that were used to init this service.
     *
     * @return Properties
     */
    public final Properties getProperties() {
        return this.props;
    }

    /**
     * Return time service was initializes.
     *
     * @return Date containing time service started.
     */
    public final Date getStartTime() {
        return data.getStartTime();
    }

    /**
     * Return name of the host service is running on.
     *
     * @return String Host name
     */
    public final String getHostname() {
        return this.data.getId().getHostName();
    }

    /**
     * @see com.metamatrix.platform.service.api.ServiceInterface#getProcessName()
     */
	public final String getProcessName(){
		return this.data.getId().getProcessName();
	}
	
    /**
     * Return instance name of this service.
     *
     * @return String instance name
     */
    public final String getInstanceName() {
        return this.data.getInstanceName();
    }


    /**
     * Method used to determine if service is still alive.
     */
    public final boolean isAlive() {
        return true;
    }

    /**
     * If service is not open an exception is thrown. 
     * All business methods in subclass should call this method.
     *
     * @throws ServiceNotInitialized if service has not yet been initialized.
     * @throws ServiceClosedException if service has been closed.
     * @throws ServiceSuspendedException if service is currently suspended.
     */

    public void checkState() throws ServiceStateException {

        if (data.getState() == ServiceState.STATE_OPEN) {
            return;
        }

        if (data.getState() == ServiceState.STATE_NOT_INITIALIZED) {
            throw new ServiceNotInitializedException(ServiceMessages.SERVICE_0009, ServicePlugin.Util.getString(ServiceMessages.SERVICE_0009, this.getServiceName(), data.getId()));
        }

        if (data.getState() == ServiceState.STATE_CLOSED) {
            throw new ServiceClosedException(ServiceMessages.SERVICE_0010, ServicePlugin.Util.getString(ServiceMessages.SERVICE_0010, this.getServiceName(), data.getId()));
        }

        if (data.getState() == ServiceState.STATE_DATA_SOURCE_UNAVAILABLE) {
            throw new ServiceClosedException(ServiceMessages.SERVICE_0069, ServicePlugin.Util.getString(ServiceMessages.SERVICE_0069, this.getServiceName(), data.getId()));
        }

        throw new ServiceStateException(ServiceMessages.SERVICE_0012, ServicePlugin.Util.getString(ServiceMessages.SERVICE_0012, this.getServiceName(), data.getId()));
    }

    /**
     * Helper method to set state to open.
     */
    private final void markAsOpen() {
    	updateState(ServiceState.STATE_OPEN);
    }

    /**
     * Helper method to set state to closed.
     */
    private final void markAsClosed() {
    	updateState(ServiceState.STATE_CLOSED);
    }
    
    /**
     * Return true if service has been initialized.
     */
    public final boolean isInitialized() {
        return data.getState() != ServiceState.STATE_NOT_INITIALIZED;
    }

    /**
     * Return true if service is open
     */
    public final boolean isOpen() {
        return data.getState() == ServiceState.STATE_OPEN;
    }

    /**
     * Return true if service is closed.
     */
    public final boolean isClosed() {
        return data.getState() == ServiceState.STATE_CLOSED;
    }


    //--------------------------------------------------------------
    // Methods to be subclassed
    //--------------------------------------------------------------

    /**
     * Perform initialization and commence processing. This method is called only once.
     */
    protected abstract void initService(Properties props)
	    throws Exception;

    /**
     * Close the service to new work if applicable. After this method is called
     * the service should no longer accept new work to perform but should continue
     * to process any outstanding work. This method is called by die().
     */
    protected abstract void closeService() throws Exception;

    /**
     * Wait until the service has completed all outstanding work. This method
     * is called by die() just before calling dieNow().
     */
    protected abstract void waitForServiceToClear() throws Exception;

    /**
     * Terminate all processing and reclaim resources. This method is called by dieNow()
     * and is only called once.
     */
    protected abstract void killService();

    /**
    * This method should be overridden when the extending class when it will use a
    * resource name other than the ComponentTypeName for looking up its resource
    * connection properties.  Generally, for services the resource name will
    * be the ComponentTypeName.  For other types (i.e., connectors, non-exposed functionality, etc.),
    * the resource name will probably be defined as a static variable called RESOURCE_NAME.
    */
    protected String getResourceName() {
        return data.getServiceType();
    }

    /**
     * This method is called when a ServiceEvent that service has registered for
     * is received via the MessageBus.
     */
    public void processEvent(EventObject obj) {
    }

    /**
     * Register for events. Subclassers should register for all events.
     */
     protected void registerForEvents() throws Exception {
     }

    /**
     * UnRegister for events. Subclassers should register for all events.
     */
     protected void unregisterForEvents() throws Exception {
     }

    /**
     * Returns a list of QueueStats objects that represent the queues in
     * this service.
     * If there are no queues, null is returned.
     */
    public Collection getQueueStatistics() {
        return null;
    }

    /**
     * Returns a QueueStats object that represent the queue in
     * this service.
     * If there is no queue with the given name, null is returned.
     */
    public WorkerPoolStats getQueueStatistics(String name) {
        return null;
    }

    //--------------------------------------------------------------
    // Helper methods
    //--------------------------------------------------------------

   /**
     * Log a message. Due to a problem with logging to the logging subsystem when
     * logMessage is overridden by the subclasser, this class uses these private
     * log helpers to ensure that messages get logged somewhere.
     */
    private void logMessagePrivate(String s) {
        LogManager.logInfo(LogCommonConstants.CTX_SERVICE, s);
    }

    /**
     * Log the properties the service is started with.
     */
    private void logServiceProperties(Properties props) {
        if (!LogManager.isMessageToBeRecorded(LogCommonConstants.CTX_SERVICE, MessageLevel.DETAIL)) {
            return;
        }

        List names = new ArrayList();
        Enumeration enumeration = props.propertyNames();
        while ( enumeration.hasMoreElements() ) {
            String name = (String) enumeration.nextElement();
            names.add(name);
        }
        Collections.sort(names);

        StringBuffer log = new StringBuffer();
        for (Iterator nIt=names.iterator(); nIt.hasNext(); ) {
          String name = (String) nIt.next();

          String value = null;
          if (PasswordMaskUtil.doesNameEndWithPasswordSuffix(name)){
                value = PasswordMaskUtil.MASK_STRING;
          } else {
                value = props.getProperty(name);
                value= saveConvert(value, false);
          }

          name = saveConvert(name, true);

          logMessagePrivate( ServicePlugin.Util.getString(ServiceMessages.SERVICE_0007, name, value));
          log.append(ServicePlugin.Util.getString(ServiceMessages.SERVICE_0007, name, value));
          if (nIt.hasNext()) {
        	  log.append('\n');
          }
        }
        
        LogManager.logDetail(LogCommonConstants.CTX_SERVICE, log.toString());


    }

//    private static final String keyValueSeparators = "=: \t\r\n\f";

//    private static final String strictKeyValueSeparators = "=:";

    private static final String specialSaveChars = "=: \t\r\n\f#!"; //$NON-NLS-1$

//    private static final String whiteSpaceChars = " \t\r\n\f";


    /*
     * Converts unicodes to encoded &#92;uxxxx
     * and writes out any of the characters in specialSaveChars
     * with a preceding slash
     */
    private String saveConvert(String theString, boolean escapeSpace) {
        int len = theString.length();
        StringBuffer outBuffer = new StringBuffer(len*2);

        for(int x=0; x<len; x++) {
            char aChar = theString.charAt(x);
            switch(aChar) {
        case ' ':
            if (x == 0 || escapeSpace)
            outBuffer.append('\\');

            outBuffer.append(' ');
            break;
                case '\\':outBuffer.append('\\'); outBuffer.append('\\');
                          break;
                case '\t':outBuffer.append('\\'); outBuffer.append('t');
                          break;
                case '\n':outBuffer.append('\\'); outBuffer.append('n');
                          break;
                case '\r':outBuffer.append('\\'); outBuffer.append('r');
                          break;
                case '\f':outBuffer.append('\\'); outBuffer.append('f');
                          break;
                default:
                    if ((aChar < 0x0020) || (aChar > 0x007e)) {
                        outBuffer.append('\\');
                        outBuffer.append('u');
                        outBuffer.append(toHex((aChar >> 12) & 0xF));
                        outBuffer.append(toHex((aChar >>  8) & 0xF));
                        outBuffer.append(toHex((aChar >>  4) & 0xF));
                        outBuffer.append(toHex( aChar        & 0xF));
                    } else {
                        if (specialSaveChars.indexOf(aChar) != -1)
                            outBuffer.append('\\');
                        outBuffer.append(aChar);
                    }
            }
        }
        return outBuffer.toString();
    }

    /**
     * Convert a nibble to a hex character
     * @param   nibble  the nibble to convert.
     */
    private static char toHex(int nibble) {
    return hexDigit[(nibble & 0xF)];
    }

    /** A table of hex digits */
    private static final char[] hexDigit = {
    '0','1','2','3','4','5','6','7','8','9','A','B','C','D','E','F'
    };

    /**
     * Update state and stateChangedTime with new state;
     * If newState == state then do nothing.
     *
     * @param int new state of service
     */
    public synchronized void updateState(int newState) {
        data.updateState(newState);
    }

    /**
     * Set the initializaton exception
     *
     * @param Throwable 
     *      */
    public void setInitException(Throwable error) {
    	this.data.setInitException(error);
    	if (error != null) {
    		data.updateState(ServiceState.STATE_INIT_FAILED);
    	}
    }

    public Throwable getInitException() {
    	return this.data.getInitException();
    }
    
    /**
     * Return name of service (instance name)
     */
    protected String getServiceName() {
        return data.getInstanceName();
    }
    
    @Override
    public ServiceData getServiceData() {
    	return data;
    }
    

	public Collection<ConnectionPoolStats> getConnectionPoolStats() {
		return Collections.EMPTY_LIST;
	}
    
    
}
