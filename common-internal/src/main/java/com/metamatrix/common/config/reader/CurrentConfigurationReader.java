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

import java.util.Properties;

import com.metamatrix.common.config.StartupStateController;
import com.metamatrix.common.config.StartupStateException;
import com.metamatrix.common.config.api.ConfigurationModelContainer;
import com.metamatrix.common.config.api.exceptions.ConfigurationConnectionException;
import com.metamatrix.common.config.api.exceptions.ConfigurationException;

/**
 * <p>
 * This interface defines a self-contained reader for the current configuration,
 * and should be used <i>only</i> by the {@link com.metamatrix.common.config.CurrentConfiguration CurrentConfiguration}
 * framework.  As such, this is an extremely low-level implementation that may
 * <i>not</i> use anything but <code>com.metamatrix.common.util</code> components
 * and only components that do not use {@link com.metamatrix.common.logging.LogManager LogManager}.
 * </p>
 * <p>
 * Each class that implements this interface must supply a no-arg constructor.
 * </p>
 */
public interface CurrentConfigurationReader {

    /**
     * This method should connect to the repository that holds the current
     * configuration, using the specified properties.  The implementation
     * may <i>not</i> use logging but instead should rely upon returning
     * an exception in the case of any errors.
     * @param env the environment properties that define the information
     * @throws ConfigurationConnectionException if there is an error establishing the connection.
     */
    void connect( Properties env ) throws ConfigurationConnectionException;

    /**
     * This method should close the connection to the repository that holds the current
     * configuration.  The implementation may <i>not</i> use logging but
     * instead should rely upon returning an exception in the case of any errors.
     * @throws Exception if there is an error establishing the connection.
     */
    void close() throws Exception;

    // ------------------------------------------------------------------------------------
    //                     C O N F I G U R A T I O N   I N F O R M A T I O N
    // ------------------------------------------------------------------------------------

    /**
     * Obtain the next startup configuration model.  The implementation
     * may <i>not</i> use logging but instead should rely upon returning
     * an exception in the case of any errors.
     * @return the serializable Configuration instance
     * @throws ConfigurationException if an error occurred within or during
     * communication with the repository.
     */
    ConfigurationModelContainer getConfigurationModel() throws ConfigurationException;

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

