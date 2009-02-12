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

package com.metamatrix.common.config;

import com.metamatrix.common.config.api.exceptions.ConfigurationException;

/**
 * <p>This tool, which is to be used <i>only</i> by the
 * {@link com.metamatrix.platform.util.MetaMatrixController} during
 * system startup, works with {@link CurrentConfiguration} to
 * initialize system state, and also the system configurations, in the
 * configuration database.</p>
 *
 * <p>All {@link com.metamatrix.platform.util.MetaMatrixController MetaMatrixControllers}
 * will call {@link #beginSystemInitialization},
 * in an attempt to be the first one to start system initialization.
 * System startup state ensures that initialization happens
 * <i>exactly</i> once.</p>
 *
 * <p>The {@link #beginSystemInitialization} method will attempt to simultaneously
 * read, then set, the system startup state as a transaction.  The state will
 * be set to {@link #STATE_STARTING} by this method, but only under the
 * following conditions:
 * <ul>
 * <li>The boolean "forceInitialization" parameter is false, and the
 * system startup state is currently {@link #STATE_STOPPED}.</li>
 * <li>The boolean "forceInitialization" parameter is true (the
 * system startup state can be any state).</li>
 * </ul>
 * </p>
 * <p>If the {@link #beginSystemInitialization} method executes without
 * an exception, the client should call {@link #finishSystemInitialization}
 * after doing any startup work.  This will put the system into a
 * state of {@link #STATE_STARTED}.  MetaMatrixController should <i>only</i>
 * call this method if the {@link #beginSystemInitialization} method
 * executed without exception.</p>
 */
final public class StartupStateController {

    /**
     * This state indicates the system is stopped, and ready to
     * be started.
     */
    public final static int STATE_STOPPED = 0;

    /**
     * descriptive label for {@link #STATE_STOPPED} state
     */
    public final static String STATE_STOPPED_LABEL = "STOPPED"; //$NON-NLS-1$

    /**
     * This state indicates the system is in the process of starting.
     * If a MetaMatrixController encounters this state, it is because
     * another MetaMatrixController is in the process of starting the
     * system. However, this state also may be stored this way in the
     * database because the system crashed and did not reset it.
     * At the discretion of the system administrator, he can choose
     * to force a system start even if the system is in this state,
     * if he believes it is because the system crashed.
     */
    public final static int STATE_STARTING = 1;

    /**
     * descriptive label for {@link #STATE_STARTING} state
     */
    public final static String STATE_STARTING_LABEL = "STARTING"; //$NON-NLS-1$

    /**
     * This state indicates that either the system is successfully
     * started, or this state also may be stored this way in the
     * database because the system crashed and did not reset it.
     * At the discretion of the system administrator, he can choose
     * to force a system start even if the system is in this state,
     * if he believes it is because the system crashed.
     */
    public final static int STATE_STARTED = 2;

    /**
     * descriptive label for {@link #STATE_STARTED} state
     */
    public final static String STATE_STARTED_LABEL = "STARTED"; //$NON-NLS-1$
    
    
    public final static String getStateLabel(int state) {
    	switch(state) {
    		case STATE_STARTED:
    			return STATE_STARTED_LABEL;
    		case STATE_STARTING:
    			return STATE_STARTING_LABEL;
    		case STATE_STOPPED:
    			return STATE_STOPPED_LABEL;
    		default: return "InvalidServerState " + state; //$NON-NLS-1$
    	}
    }

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
    
 	public final static void performSystemInitialization(boolean forceInitialization) throws StartupStateException, ConfigurationException{
        CurrentConfiguration.getInstance().performSystemInitialization(forceInitialization);
    }

}

