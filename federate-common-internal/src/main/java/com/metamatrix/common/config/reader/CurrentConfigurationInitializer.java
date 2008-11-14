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

package com.metamatrix.common.config.reader;

import com.metamatrix.common.config.StartupStateException;
import com.metamatrix.common.config.api.exceptions.ConfigurationException;

/**
 * <p>
 * This interface defines a self-contained initializer for the system configurations,
 * and should be used <i>only</i> by the {@link com.metamatrix.common.config.CurrentConfiguration CurrentConfiguration}
 * framework.  As such, this is an extremely low-level implementation that may
 * <i>not</i> use anything but <code>com.metamatrix.common.util</code> components
 * and only components that do not use {@link com.metamatrix.common.log.LogManager LogManager}.
 * </p>
 * <p>
 * Each class that implements this interface must supply a no-arg constructor.
 * </p>
 * <p>An initializer is used by {@link com.metamatrix.common.config.CurrentConfiguration}
 * (via {@link com.metamatrix.common.config.StartupStateController}) to
 * perform any initialization of system configurations that needs to happen
 * at startup.  Specifically, the <i>operational</i> configuration from
 * any previous system run must be deleted, and then the <i>next-startup</i>
 * configuration must be copied to make the new operational configuration.
 * </p>
 *
 * @see com.metamatrix.common.config.StartupStateController
 * @see com.metamatrix.common.config.CurrentConfig
 */
public interface CurrentConfigurationInitializer {

    /**
     * This method should be called <i>only</i> by
     * {@link com.metamatrix.common.config.CurrentConfiguration}
     * to initialize the system configurations
     * in the configuration database during bootstrapping.
     * This method should attempt to put the system state into
     * {@link com.metamatrix.common.config.StartupStateController#STATE_STARTING}, and then
     * commence with initialization.  If the state is already
     * {@link com.metamatrix.common.config.StartupStateController#STATE_STARTING}, then another
     * MetaMatrixController is already currently in the process of
     * starting the system, and a {@link StartupStateException}
     * should be thrown.  If this method returns without an
     * exception, then the system state should be in state
     * {@link com.metamatrix.common.config.StartupStateController#STATE_STARTING}, and the calling
     * code should proceed with startup.
     * @param forceInitialization if the system is in a state other than
     * {@link com.metamatrix.common.config.StartupStateController#STATE_STOPPED}, and the
     * administrator thinks the system actually crashed and is
     * not really running, he can choose to force the
     * initialization.  Otherwise, if the system is in one of these states,
     * an exception will be thrown.
     * @throws StartupStateException if the system is
     * not in a state in which initialization can proceed.  This
     * exception should indicate the current system state.
     * @throws ConfigurationException if an error occurred in communication
     * with the configuration data source
     */
     void beginSystemInitialization(boolean forceInitialization) throws StartupStateException, ConfigurationException;

    /**
     * If the {@link #beginSystemInitialization} method executes without
     * an exception, this method can be called.
     * This should put the system into a state of
     * {@link com.metamatrix.common.config.StartupStateController#STATE_STARTED}.
     * {@link com.metamatrix.common.config.CurrentConfiguration} should <i>only</i>
     * call this method if the {@link #beginSystemInitialization} method
     * executed without exception.  This method should throw an exception if
     * the system is not currently in a state of
     * {@link com.metamatrix.common.config.StartupStateController#STATE_STARTING}.
     * {@link #beginSystemInitialization}
     * @throws StartupStateException if the system is
     * not in a state in which initialization can be finished, namely
     * {@link com.metamatrix.common.config.StartupStateController#STATE_STARTING}.
     * This exception should indicate the
     * current system state.
     * @throws ConfigurationException if an error occurred in communication
     * with the configuration data source
     */
    void finishSystemInitialization() throws StartupStateException, ConfigurationException;
    
    
    /**
     * This method should be called <i>only</i> by
     * {@link com.metamatrix.platform.util.MetaMatrixController}
     * to initialize the system configurations during bootstrapping.
     * This method will attempt to put the system state into
     * {@link StartupStateController#STATE_STARTING}, and then
     * commence with initialization.  If the state is already
     * {@link StartupStateController#STATE_STARTING}, then another
     * MetaMatrixController is already currently in the process of
     * starting the system, and a {@link StartupStateException}
     * will be thrown.  If this method returns without an
     * exception, then the system state will be in state
     * {@link StartupStateController#STATE_STARTING}, and the calling
     * code should proceed with startup.
     * @param forceInitialization if the system is in a state other than
     * {@link StartupStateController#STATE_STOPPED}, and the
     * administrator thinks the system actually crashed and is
     * not really running, he can choose to force the
     * initialization.  Otherwise, if the system is in one of these states,
     * an exception will be thrown.
     * @throws StartupStateException if the system is
     * not in a state in which initialization can proceed.  This
     * exception will indicate the current system state.
     * @throws ConfigurationException if the current configuration and/or
     * bootstrap properties could not be obtained
     * 
     * NOTE: This method replaces the begin... and finish.. SystemInitialization methods
     * for the new configuration implementations.
     */
    void performSystemInitialization(boolean forceInitialization) throws StartupStateException, ConfigurationException;
    

    /**
     * This will put the system into a state of
     * {@link com.metamatrix.common.config.StartupStateController#STATE_STOPPED}.
     * @throws ConfigurationException if an error occurred in communication
     * with the configuration data source
     */
    void indicateSystemShutdown() throws ConfigurationException;

}

