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

//#############################################################################
package com.metamatrix.console.ui.views.deploy.event;

import java.util.EventObject;

import com.metamatrix.common.config.api.Configuration;
import com.metamatrix.common.config.api.DeployedComponent;
import com.metamatrix.common.config.api.Host;
import com.metamatrix.common.config.api.ProductServiceConfig;
import com.metamatrix.common.config.api.ProductType;
import com.metamatrix.common.config.api.ServiceComponentDefn;
import com.metamatrix.common.config.api.VMComponentDefn;

/**
 * The <code>ConfigurationChangeEvent</code> is used to notify
 * {@link ConfigurationChangeListener}s that a change in a
 * {@link Configuration} has occurred.
 * @since Golden Gate
 * @version 1.0
 * @author Dan Florian
 */
public class ConfigurationChangeEvent
    extends EventObject {

    ///////////////////////////////////////////////////////////////////
    // CONSTANTS
    ///////////////////////////////////////////////////////////////////

    /** Indicates something in a configuration has been deleted. */
    public final static int DELETED = 0x001;

    /** Indicates something in a configuration has been modified. */
    public final static int MODIFIED = 0x002;

    /** Indicates something new has been added to a configuration. */
    public final static int NEW = 0x0004;

    /**
     * Indicates the configuration will be refreshed after notifying
     * all listeners.
     */
    public final static int REFRESH_START = 0x0008;

    /** Indicates the configuration refresh has ended. */
    public final static int REFRESH_END = 0x0010;

    /** Source of the event is a configuration. */
    protected final static int CONFIGURATION = 0x0020;

    /** Source of the event is a host. */
    protected final static int HOST = 0x0040;

    /** Source of the event is a process. */
    protected final static int PROCESS = 0x0080;

    /** Source of the event is a deployed PSC. */
    protected final static int DEPLOYED_PSC = 0x0100;

    /** Source of the event is a deployed service. */
    protected final static int DEPLOYED_SERVICE = 0x0200;

    /** Source of the event is a product. */
    protected final static int PRODUCT = 0x0400;

    /** Source of the event is a PSC definition. */
    protected final static int PSC_DEFN = 0x0800;

    /** Source of the event is a service definition. */
    protected final static int SERVICE_DEFN = 0x1000;

    ///////////////////////////////////////////////////////////////////
    // FIELDS
    ///////////////////////////////////////////////////////////////////

    /** The event type. */
    protected int type;

    /** Ancestor objects starting with the parent. */
    protected Object[] ancestors;

    /** The configuration where the change occurred. */
    private Configuration config;

    ///////////////////////////////////////////////////////////////////
    // CONSTRUCTORS
    ///////////////////////////////////////////////////////////////////

    /**
     * Constructs a <code>ConfigurationChangeEvent</code> of the given type.
     * @param theType the event type
     * @param theChangedObject the object whose state has changed
     * @param theConfig the configuration where the change occurred
     * @throws IllegalArgumentException if type is not valid, if the
     * changed object is <code>null</code>, or if the configuration
     * is <code>null</code>.
     */
    protected ConfigurationChangeEvent(
        int theType,
        Object theChangedObject,
        Configuration theConfig) {

        super(theChangedObject);
        if (theChangedObject == null) {
            throw new IllegalArgumentException("Object cannot be null."); //$NON-NLS-1$
        }
        if (theConfig == null) {
            throw new IllegalArgumentException("Configuration cannot be null."); //$NON-NLS-1$
        }
        if ((theType != DELETED) && (theType != MODIFIED) &&
            (theType != NEW) && (theType != REFRESH_START) &&
            (theType != REFRESH_END)) {
            throw new IllegalArgumentException(
                "Invalid event type <" + theType + ">."); //$NON-NLS-1$ //$NON-NLS-2$
        }

        config = theConfig;
        type = theType;
    }

    /**
     * Constructs a <code>ConfigurationChangeEvent</code> of the given type.
     * @param theType the event type
     * @param theChangedObject the object whose state has changed
     * @param theConfig the configuration where the change occurred
     * @param theAncestors the ancestor objects starting with the parent
     * @throws IllegalArgumentException if type is not valid, if the
     * changed object is <code>null</code>, or if the configuration
     * is <code>null</code>.
     */
    public ConfigurationChangeEvent(
        int theType,
        Object theChangedObject,
        Configuration theConfig,
        Object[] theAncestors) {

        this(theType, theChangedObject, theConfig);
        setAncestors(theAncestors);
    }

    /**
     * Constructs a refresh <code>ConfigurationChangeEvent</code>. The type
     * must be either {@link #REFRESH_START} or {@link REFRESH_END}. This
     * event has no configuration and no ancestors.
     * @param theType the refresh event type
     * @param theSource the object whose state has changed
     * @throws IllegalArgumentException if type is not a refresh type
     */
    public ConfigurationChangeEvent(
        int theRefreshType,
        Object theSource) {

        super(theSource);
        if ((theRefreshType != REFRESH_START) &&
            (theRefreshType != REFRESH_END)) {
            throw new IllegalArgumentException(
                "Invalid refresh event type <" + theRefreshType + ">."); //$NON-NLS-1$ //$NON-NLS-2$
        }
        type = theRefreshType;
    }

    ///////////////////////////////////////////////////////////////////
    // METHODS
    ///////////////////////////////////////////////////////////////////

    /**
     * Gets the ancestors of the changed object.
     * @return the ancestors
     */
    public Object[] getAncestors() {
        return ancestors;
    }

    /**
     * Gets the configuration where the change took place.
     * @return the configuration
     */
    public Configuration getConfiguration() {
        return config;
    }

    /**
     * Gets the deployed PSC if either a deployed PSC was the event source
     * or an ancestor.
     * @return the deployed PSC or <code>null</code>
     */
    public ProductServiceConfig getDeployedPsc() {
        ProductServiceConfig psc = null;
        if (isDeployedPscChange()) {
            psc = (ProductServiceConfig)getSource();
        }
        else {
            int index = getAncestorIndex(DEPLOYED_PSC);
            if (index != -1) {
                psc = (ProductServiceConfig)ancestors[index];
            }
        }
        return psc;
    }

    /**
     * Gets the deployed service if either a deployed service was the event
     * source or an ancestor.
     * @return the deployed service or <code>null</code>
     */
    public DeployedComponent getDeployedService() {
        DeployedComponent service = null;
        if (isDeployedServiceChange()) {
            service = (DeployedComponent)getSource();
        }
        return service;
    }

    /**
     * Gets the host if either a host was the event source or an ancestor.
     * @return the host or <code>null</code>
     */
    public Host getHost() {
        Host host = null;
        if (isHostChange()) {
            host = (Host)getSource();
        }
        else {
            int index = getAncestorIndex(HOST);
            if (index != -1) {
                host = (Host)ancestors[index];
            }
        }
        return host;
    }

    /**
     * Gets the process if either a process was the event source or an ancestor.
     * @return the process or <code>null</code>
     */
    public VMComponentDefn getProcess() {
        VMComponentDefn process = null;
        if (isProcessChange()) {
            process = (VMComponentDefn)getSource();
        }
        else {
            int index = getAncestorIndex(PROCESS);
            if (index != -1) {
                process = (VMComponentDefn)ancestors[index];
            }
        }
        return process;
    }

    /**
     * Gets the product if either a product was the event source or an ancestor.
     * @return the product or <code>null</code>
     */
    public ProductType getProduct() {
        ProductType product = null;
        if (isProductChange()) {
            product = (ProductType)getSource();
        }
        else {
            int index = getAncestorIndex(PRODUCT);
            if (index != -1) {
                product = (ProductType)ancestors[index];
            }
        }
        return product;
    }

    /**
     * Gets the PSC definition if either a PSC definition was the event source
     * or an ancestor.
     * @return the PSC definition or <code>null</code>
     */
    public ProductServiceConfig getPscDefinition() {
        ProductServiceConfig psc = null;
        if (isPscDefinitionChange()) {
            psc = (ProductServiceConfig)getSource();
        }
        else {
            int index = getAncestorIndex(PSC_DEFN);
            if (index != -1) {
                psc = (ProductServiceConfig)ancestors[index];
            }
        }
        return psc;
    }

    /**
     * Gets the service definition if either a service definition was the event
     * source or an ancestor.
     * @return the service definition or <code>null</code>
     */
    public ServiceComponentDefn getServiceDefinition() {
        ServiceComponentDefn service = null;
        if (isServiceDefinitionChange()) {
            service = (ServiceComponentDefn)getSource();
        }
        return service;
    }

    /**
     * Gets the event type.
     * @return the event type
     */
    public int getType() {
        return type;
    }

    /**
     * Gets a string representation of the event type.
     * @return a string representation of the event type
     */
    private String getTypeText() {
        StringBuffer txt = new StringBuffer();
        if (isDeleted()) {
            txt.append("delete"); //$NON-NLS-1$
        }
        else if (isModified()) {
            txt.append("modify"); //$NON-NLS-1$
        }
        else if (isNew()) {
            txt.append("new"); //$NON-NLS-1$
        }
        else if (isRefreshStart()) {
            txt.append("refresh start"); //$NON-NLS-1$
        }
        else if (isRefreshEnd()) {
            txt.append("refresh end"); //$NON-NLS-1$
        }
        else {
            txt.append("unknown"); //$NON-NLS-1$
        }

        txt.append(":"); //$NON-NLS-1$

        if (isConfigurationChange()) {
            txt.append("configuration"); //$NON-NLS-1$
        }
        else if (isConfigurationChange()) {
            txt.append("configuration"); //$NON-NLS-1$
        }
        else if (isDeployedPscChange()) {
            txt.append("deployed PSC"); //$NON-NLS-1$
        }
        else if (isDeployedServiceChange()) {
            txt.append("deployed service"); //$NON-NLS-1$
        }
        else if (isHostChange()) {
            txt.append("host"); //$NON-NLS-1$
        }
        else if (isProcessChange()) {
            txt.append("process"); //$NON-NLS-1$
        }
        else if (isProductChange()) {
            txt.append("product"); //$NON-NLS-1$
        }
        else if (isPscDefinitionChange()) {
            txt.append("PSC definition"); //$NON-NLS-1$
        }
        else if (isServiceDefinitionChange()) {
            txt.append("service definition"); //$NON-NLS-1$
        }
        else {
            txt.append("unknown"); //$NON-NLS-1$
        }
        return txt.toString();
    }

    /**
     * Indicates if the changed object was the configuration object.
     * @return <code>true</code> if the changed object was the configuration object;
     * <code>false</code> otherwise.
     */
    public boolean isConfigurationChange() {
        return ((type & CONFIGURATION) == CONFIGURATION);
    }

    /**
     * Indicates if the changed object has been deleted.
     * @return <code>true</code> if the changed object has been deleted;
     * <code>false</code> otherwise.
     */
    public boolean isDeleted() {
        return ((type & DELETED) == DELETED);
    }

    /**
     * Indicates if the changed object was a deployed PSC object.
     * @return <code>true</code> if the changed object was a deployed PSC object;
     * <code>false</code> otherwise.
     */
    public boolean isDeployedPscChange() {
        return ((type & DEPLOYED_PSC) == DEPLOYED_PSC);
    }

    /**
     * Indicates if the changed object was a deployed service object.
     * @return <code>true</code> if the changed object was a deployed service object;
     * <code>false</code> otherwise.
     */
    public boolean isDeployedServiceChange() {
        return ((type & DEPLOYED_SERVICE) == DEPLOYED_SERVICE);
    }

    /**
     * Indicates if the changed object has been modified.
     * @return <code>true</code> if the changed object has been modified;
     * <code>false</code> otherwise.
     */
    public boolean isModified() {
        return ((type & MODIFIED) == MODIFIED);
    }

    /**
     * Indicates if the changed object is new.
     * @return <code>true</code> if the changed object is new;
     * <code>false</code> otherwise.
     */
    public boolean isNew() {
        return ((type & NEW) == NEW);
    }

    /**
     * Indicates if the changed object was a host object.
     * @return <code>true</code> if the changed object was a host object;
     * <code>false</code> otherwise.
     */
    public boolean isHostChange() {
        return ((type & HOST) == HOST);
    }

    /**
     * Indicates if a refresh of the configuration is starting.
     * @return <code>true</code> if a configuration refresh is starting;
     * <code>false</code> otherwise.
     */
    public boolean isRefreshEnd() {
        return ((type & REFRESH_END) == REFRESH_END);
    }

    /**
     * Indicates if a refresh of the configuration has just ended.
     * @return <code>true</code> if a configuration refresh has just ended;
     * <code>false</code> otherwise.
     */
    public boolean isRefreshStart() {
        return ((type & REFRESH_START) == REFRESH_START);
    }

    /**
     * Indicates if the changed object was a process object.
     * @return <code>true</code> if the changed object was a process object;
     * <code>false</code> otherwise.
     */
    public boolean isProcessChange() {
        return ((type & PROCESS) == PROCESS);
    }

    /**
     * Indicates if the changed object was a product object.
     * @return <code>true</code> if the changed object was a product object;
     * <code>false</code> otherwise.
     */
    public boolean isProductChange() {
        return ((type & PRODUCT) == PRODUCT);
    }

    /**
     * Indicates if the changed object was a PSC definition object.
     * @return <code>true</code> if the changed object was a PSC definition object;
     * <code>false</code> otherwise.
     */
    public boolean isPscDefinitionChange() {
        return ((type & PSC_DEFN) == PSC_DEFN);
    }

    /**
     * Indicates if the changed object was a service definition object.
     * @return <code>true</code> if the changed object was a service Definition object;
     * <code>false</code> otherwise.
     */
    public boolean isServiceDefinitionChange() {
        return ((type & SERVICE_DEFN) == SERVICE_DEFN);
    }

    /**
     * Gets a string representation of the event.
     * @return the string representation of the event
     */
    public String paramString() {
        StringBuffer ancestorTxt = new StringBuffer();
        if (ancestors == null) {
            ancestorTxt.append("None"); //$NON-NLS-1$
        }
        else {
            for (int i=0; i<ancestors.length; i++) {
                ancestorTxt.append(ancestors[i]);
                if (i < ancestors.length-1) {
                    ancestorTxt.append(", "); //$NON-NLS-1$
                }
            }
        }
        return new StringBuffer().append("type=").append(getTypeText()) //$NON-NLS-1$
                                 .append(", source=").append(getSource()) //$NON-NLS-1$
                                 .append(", ancestors=").append(ancestorTxt) //$NON-NLS-1$
                                 .toString();
    }

    /**
     * Sets the ancestor object of the changed object.
     * @param theAncestors the ancestor objects
     */
    protected void setAncestors(Object[] theAncestors) {
        ancestors = theAncestors;
        Object source = getSource();

        if (source instanceof ProductServiceConfig) {
            if (ancestors[0] instanceof ProductType) {
                type |= PSC_DEFN;
            }
            else {
                type |= DEPLOYED_PSC;
            }
        }
        else if (source instanceof ServiceComponentDefn) {
            type |= SERVICE_DEFN;
        }
        else if (source instanceof VMComponentDefn) {
            type |= PROCESS;
        }
        else if (source instanceof Host) {
            type |= HOST;
        }
        else if (source instanceof ProductType) {
            type |= PRODUCT;
        }
        else if (source instanceof Configuration) {
            type |= CONFIGURATION;
        }
        else if (source instanceof DeployedComponent) {
            type |= DEPLOYED_SERVICE;
        }
    }

    /**
     * Gets the ancestor index of the ancestor type.
     * @return the ancestor index or -1 if no ancestor of that type
     */
    protected int getAncestorIndex(int theAncestorType) {
        int index = -1;
        if (theAncestorType == HOST) {
            if (isProcessChange()) {
                index = 0;
            }
            else if (isDeployedPscChange()) {
                index = 1;
            }
            else if (isDeployedServiceChange()) {
                index = 2;
            }
        }
        else if (theAncestorType == DEPLOYED_PSC) {
            if (isDeployedServiceChange()) {
                index = 0;
            }
        }
        else if (theAncestorType == PROCESS) {
            if (isDeployedPscChange()) {
                index = 0;
            }
            else if (isDeployedServiceChange()) {
                index = 1;
            }
        }
        else if (theAncestorType == PRODUCT) {
            if (isPscDefinitionChange()) {
                index = 0;
            }
            else if (isServiceDefinitionChange()) {
                index = 1;
            }
        }
        else if (theAncestorType == PSC_DEFN) {
            if (isServiceDefinitionChange()) {
                index = 0;
            }
        }
        return index;
    }

}
