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
package com.metamatrix.console.ui.views.runtime.util;

import java.awt.Color;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;

import javax.swing.Icon;
import javax.swing.border.EmptyBorder;

import com.metamatrix.console.connections.ConnectionInfo;
import com.metamatrix.console.ui.util.property.GuiComponentFactory;
import com.metamatrix.console.ui.util.property.PropertyProvider;
import com.metamatrix.console.ui.views.runtime.OperationsPanel;
import com.metamatrix.platform.admin.api.runtime.HostData;
import com.metamatrix.platform.admin.api.runtime.PSCData;
import com.metamatrix.platform.admin.api.runtime.ProcessData;
import com.metamatrix.platform.admin.api.runtime.ServiceData;
import com.metamatrix.platform.service.api.ServiceState;
import com.metamatrix.toolbox.ui.widget.TextFieldWidget;

public final class RuntimeMgmtUtils
    implements ServiceStateConstants {
    
    public static final int ODBC_UNAVAILABLE_SERVICE_STATE = 99;

    ///////////////////////////////////////////////////////////////////////////
    // CONSTANTS
    ///////////////////////////////////////////////////////////////////////////

    public static final EmptyBorder EMPTY_BORDER;
    public static final String PROPS =
        "com/metamatrix/console/ui/views/runtime/data/ui";
    public static final SimpleDateFormat DATE_FORMATTER;

    // [SERVICE STATES][OPERATIONS]
    public static final boolean[][] VALID_SERV_OPS;

    ///////////////////////////////////////////////////////////////////////////
    // FIELDS
    ///////////////////////////////////////////////////////////////////////////

    private static PropertyProvider propProvider;

    ///////////////////////////////////////////////////////////////////////////
    // INITIALIZER
    ///////////////////////////////////////////////////////////////////////////

    static {
        // first setup property provider
        ArrayList propFiles = new ArrayList();
        propFiles.add(PROPS);
        propFiles.add(PropertyProvider.COMMON_PROP);
        propFiles.add(GuiComponentFactory.TYPE_DEFS_PROP);
        propProvider = new PropertyProvider(propFiles);

        int inset = getInt("emptyinsets", 10);
        EMPTY_BORDER = new EmptyBorder(inset, inset, inset, inset);

        String pattern = getString("datepattern", true);
        if (pattern == null) {
            pattern = "MMM dd, yyyy hh:mm:ss";
        }
        DATE_FORMATTER = new SimpleDateFormat(pattern);

        VALID_SERV_OPS = new boolean[7][TOTAL_OPERATIONS];
        VALID_SERV_OPS[OPEN] = new boolean[TOTAL_OPERATIONS];
        VALID_SERV_OPS[OPEN][START_ORDINAL_POSITION] = false;
        VALID_SERV_OPS[OPEN][STOP_ORDINAL_POSITION] = true;
        VALID_SERV_OPS[OPEN][STOP_NOW_ORDINAL_POSITION] = true;
        VALID_SERV_OPS[OPEN][SHOWQUEUE_ORDINAL_POSITION] = true;

        VALID_SERV_OPS[CLOSED] = new boolean[TOTAL_OPERATIONS];
        VALID_SERV_OPS[CLOSED][START_ORDINAL_POSITION] = true;
        VALID_SERV_OPS[CLOSED][STOP_ORDINAL_POSITION] = false;
        VALID_SERV_OPS[CLOSED][STOP_NOW_ORDINAL_POSITION] = false;
        VALID_SERV_OPS[CLOSED][SHOWQUEUE_ORDINAL_POSITION] = false;

        VALID_SERV_OPS[FAILED] = new boolean[TOTAL_OPERATIONS];
        VALID_SERV_OPS[FAILED][START_ORDINAL_POSITION] = true;
        VALID_SERV_OPS[FAILED][STOP_ORDINAL_POSITION] = false;
        VALID_SERV_OPS[FAILED][STOP_NOW_ORDINAL_POSITION] = false;
        VALID_SERV_OPS[FAILED][SHOW_SERVICE_ERROR_ORDINAL_POSITION] = true;

        VALID_SERV_OPS[INIT_FAILED] = new boolean[TOTAL_OPERATIONS];
        VALID_SERV_OPS[INIT_FAILED][START_ORDINAL_POSITION] = true;
        VALID_SERV_OPS[INIT_FAILED][STOP_ORDINAL_POSITION] = false;
        VALID_SERV_OPS[INIT_FAILED][STOP_NOW_ORDINAL_POSITION] = false;
        VALID_SERV_OPS[INIT_FAILED][SHOW_SERVICE_ERROR_ORDINAL_POSITION] = true;

        VALID_SERV_OPS[NOT_INITIALIZED] = new boolean[TOTAL_OPERATIONS];
        VALID_SERV_OPS[NOT_INITIALIZED][START_ORDINAL_POSITION] = false;
        VALID_SERV_OPS[NOT_INITIALIZED][STOP_ORDINAL_POSITION] = false;
        VALID_SERV_OPS[NOT_INITIALIZED][STOP_NOW_ORDINAL_POSITION] = false;
        VALID_SERV_OPS[NOT_INITIALIZED][SHOW_SERVICE_ERROR_ORDINAL_POSITION] = false;

        VALID_SERV_OPS[NOT_REGISTERED] = new boolean[TOTAL_OPERATIONS];
        VALID_SERV_OPS[NOT_REGISTERED][START_ORDINAL_POSITION] = false;
        VALID_SERV_OPS[NOT_REGISTERED][STOP_ORDINAL_POSITION] = false;
        VALID_SERV_OPS[NOT_REGISTERED][STOP_NOW_ORDINAL_POSITION] = false;
        VALID_SERV_OPS[NOT_REGISTERED][SHOW_SERVICE_ERROR_ORDINAL_POSITION] = false;
        
        VALID_SERV_OPS[DATA_SOURCE_UNAVAILABLE] = new boolean[TOTAL_OPERATIONS];
        VALID_SERV_OPS[DATA_SOURCE_UNAVAILABLE][START_ORDINAL_POSITION] = false;
        VALID_SERV_OPS[DATA_SOURCE_UNAVAILABLE][STOP_ORDINAL_POSITION] = true;
        VALID_SERV_OPS[DATA_SOURCE_UNAVAILABLE][STOP_NOW_ORDINAL_POSITION] = true;
        VALID_SERV_OPS[DATA_SOURCE_UNAVAILABLE][SHOWQUEUE_ORDINAL_POSITION] = true;
        VALID_SERV_OPS[DATA_SOURCE_UNAVAILABLE][SHOW_SERVICE_ERROR_ORDINAL_POSITION] = true;
    }

    ///////////////////////////////////////////////////////////////////////////
    // CONSTRUCTORS
    ///////////////////////////////////////////////////////////////////////////

    /** Don't allow no arg construction. */
    private RuntimeMgmtUtils() {}

    ///////////////////////////////////////////////////////////////////////////
    // METHODS
    ///////////////////////////////////////////////////////////////////////////

    public static TextFieldWidget createTextField(String theType) {
        return GuiComponentFactory.createTextField(theType);
    }

    public static boolean getBoolean(String theKey) {
        return propProvider.getBoolean(theKey);
    }

    public static Color getColor(final String theKey) {
        return (Color)propProvider.getObject(theKey);
    }

    public static Icon getIcon(String theKey) {
        return propProvider.getIcon(theKey);
    }

    public static int getInt(
        String theKey,
        int theDefault) {

        return propProvider.getInt(theKey, theDefault);
    }

    public static int getMnemonic(String theKey) {
        String key = propProvider.getString(theKey, true);
        return (key == null) ? 0 : (int)key.charAt(0);
    }
    
    public static boolean isSameHost(HostData theHost, ConnectionInfo connection) {
        return connection.isConnectedHost(theHost.getName());
    }
    
    public static boolean isSameHost(ProcessData theProcess, ConnectionInfo connection) {
        return connection.isConnectedHost(theProcess.getHostName(), theProcess.getPort());
    }    

    
    public static boolean[] getOperationsEnablements(HostData theHost) {

        // booleans default to false so just set true operations
        boolean[] enablements = new boolean[OperationsPanel.TOTAL_OPERATIONS];
        if (theHost.isRegistered()) {
            enablements[STOP_ORDINAL_POSITION] = true;
            enablements[STOP_NOW_ORDINAL_POSITION] = true;
        }
        else {
            enablements[START_ORDINAL_POSITION] = true;
        }
        return enablements;
    }

    public static boolean[] getOperationsEnablements(ProcessData theProcess) {

        // booleans default to false so just set true operations
        boolean[] enablements = new boolean[OperationsPanel.TOTAL_OPERATIONS];
        if (theProcess.isRegistered()) {
            enablements[STOP_ORDINAL_POSITION] = true;
            enablements[STOP_NOW_ORDINAL_POSITION] = true;
        }
        else {
            enablements[START_ORDINAL_POSITION] = true;
        }
        return enablements;
    }

    public static boolean[] getOperationsEnablements(
        PSCData thePsc,
        ProcessData theProcess) {

        // booleans default to false so just set true operations
        boolean[] enablements = new boolean[OperationsPanel.TOTAL_OPERATIONS];
        if (thePsc.isRegistered()) {
            enablements[STOP_ORDINAL_POSITION] = true;
            enablements[STOP_NOW_ORDINAL_POSITION] = true;
        }
        else {
            if (theProcess.isRegistered()) {
                enablements[START_ORDINAL_POSITION] = true;
            }
        }
        
        // if has one suspended service then resume can be enabled
        // if has one open/running service then suspend can be enabled
        //if there is one that is not open and not suspended, enable start
        Collection services = thePsc.getServices();
        if ((services != null) && (!services.isEmpty())) {
            Iterator servItr = services.iterator();
            while (servItr.hasNext()) {
                ServiceData service = (ServiceData)servItr.next();
                int state = service.getCurrentState();

                if(state != ServiceState.STATE_OPEN){
                    enablements[START_ORDINAL_POSITION] = true;
                } else if (state == ServiceState.STATE_INIT_FAILED || 
                                state == ServiceState.STATE_FAILED ||
                                state == ServiceState.STATE_DATA_SOURCE_UNAVAILABLE) {
                    enablements[SHOW_SERVICE_ERROR_ORDINAL_POSITION] = true;
                }
            }
        }
        return enablements;
    }
    

    public static boolean[] getOperationsEnablements(ServiceData theService) {
        return getServiceOperationsEnablements(theService.getCurrentState());
    }

    public static boolean[] getServiceOperationsEnablements(int theState) {
        boolean[] enablements = VALID_SERV_OPS[theState];
        return enablements;
    }

    public static Color getServiceStateColor(int theState) {
        Color color = null;
        if (theState == ServiceState.STATE_OPEN) {
            color = getColor("state.open.color");
        } else if (theState == ServiceState.STATE_CLOSED) {
            color = getColor("state.closed.color");
        } else if (theState == ServiceState.STATE_FAILED) {
            color = getColor("state.failed.color");
        } else if (theState == ServiceState.STATE_INIT_FAILED) {
            color = getColor("state.initfailed.color");
        } else if (theState == ServiceState.STATE_NOT_INITIALIZED) {
            color = getColor("state.notinit.color");
        } else if (theState == ServiceState.STATE_NOT_REGISTERED) {
            color = getColor("state.notregisteredservice.color");
        } else if (theState == ServiceState.STATE_DATA_SOURCE_UNAVAILABLE) {
            color = getColor("state.datasourceunavailable.color");
        }
        return color;
    }

    public static String getServiceStateText(int theState) {
        String stateTxt = getString("state.unknown");
        if (theState == ServiceState.STATE_OPEN) {
            stateTxt = getString("state.open");
        } else if (theState == ServiceState.STATE_CLOSED) {
            stateTxt = getString("state.closed");
        } else if (theState == ServiceState.STATE_FAILED) {
            stateTxt = getString("state.failed");
        } else if (theState == ServiceState.STATE_INIT_FAILED) {
            stateTxt = getString("state.initfailed");
        } else if (theState == ServiceState.STATE_NOT_INITIALIZED) {
            stateTxt = getString("state.notinit");
        } else if (theState == ServiceState.STATE_NOT_REGISTERED) {
            stateTxt = getString("state.notregistered");
        } else if (theState == ServiceState.STATE_DATA_SOURCE_UNAVAILABLE) {
            stateTxt = getString("state.datasourceunavailable");
        } else if (theState == ODBC_UNAVAILABLE_SERVICE_STATE) {
            // this is a hack, because there are too many places to update
            // to add in checking for another state
            // and this is only for odbc
            stateTxt = getString("state.odbcsourceunavailable");
        }
        return stateTxt;
    }

    public static String getServiceStateToolTip(ServiceData theService) {
        boolean registered = theService.isRegistered();
        boolean deployed = theService.isDeployed();
        String txt = null;
        if (registered && deployed) {
            txt = getString("state.synched.tip", true);
        }
        else if (registered && !deployed) {
            txt = getString("state.notdeployed.tip", true);
        }
        else if (!registered && deployed) {
            txt = getString("state.notregistered.tip", true);
        }
        return txt;
    }

    public static Color getStateColor(
        boolean theDeployedFlag,
        boolean theRegisteredFlag) {

        Color color = null;
        if (theRegisteredFlag && theDeployedFlag) {
            color = getColor("state.synched.color");
        }
        else if (theRegisteredFlag && !theDeployedFlag) {
            color = getColor("state.notdeployed.color");
        }
        else if (!theRegisteredFlag && theDeployedFlag) {
            color = getColor("state.notregistered.color");
        }
        return color;
    }

    public static String getString(String theKey) {
        return propProvider.getString(theKey);
    }

    public static String getString(
        String theKey,
        boolean theReturnNullFlag) {

        return propProvider.getString(theKey, theReturnNullFlag);
    }

    public static String getString(
        String theKey,
        Object[] theArgs) {

        return propProvider.getString(theKey, theArgs);
    }

}
