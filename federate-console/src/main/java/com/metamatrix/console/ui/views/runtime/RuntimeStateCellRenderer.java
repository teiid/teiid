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

//#############################################################################
package com.metamatrix.console.ui.views.runtime;

import java.awt.Color;
import java.awt.Component;

import javax.swing.Icon;
import javax.swing.JTree;

import com.metamatrix.console.ui.views.runtime.util.RuntimeMgmtUtils;

import com.metamatrix.platform.admin.api.runtime.HostData;
import com.metamatrix.platform.admin.api.runtime.PSCData;
import com.metamatrix.platform.admin.api.runtime.ProcessData;
import com.metamatrix.platform.admin.api.runtime.ServiceData;
import com.metamatrix.platform.service.api.ServiceInterface;

import com.metamatrix.toolbox.ui.widget.tree.DefaultTreeCellRenderer;
import com.metamatrix.toolbox.ui.widget.tree.DefaultTreeNode;

public final class RuntimeStateCellRenderer extends DefaultTreeCellRenderer {

    ///////////////////////////////////////////////////////////////////////////
    // CONSTANTS
    ///////////////////////////////////////////////////////////////////////////

    private static final Icon HOST_ICON;
    private static final Icon PROCESS_ICON;
    private static final Icon SERVICE_ICON;
    private static final Icon PSC_ICON;

    ///////////////////////////////////////////////////////////////////////////
    // FIELDS
    ///////////////////////////////////////////////////////////////////////////

    private static Color saveBackNonSelectColor;
    private static Color saveBackSelectColor;
    private static Color saveForeNonSelectColor;
    private static Color saveForeSelectColor;

    ///////////////////////////////////////////////////////////////////////////
    // INITIALIZER
    ///////////////////////////////////////////////////////////////////////////

    static {
        HOST_ICON = RuntimeMgmtUtils.getIcon("icon.host");
        PROCESS_ICON = RuntimeMgmtUtils.getIcon("icon.process");
        SERVICE_ICON = RuntimeMgmtUtils.getIcon("icon.service");
        PSC_ICON = RuntimeMgmtUtils.getIcon("icon.psc");
    }

    ///////////////////////////////////////////////////////////////////////////
    // CONSTRUCTORS
    ///////////////////////////////////////////////////////////////////////////

    public RuntimeStateCellRenderer() {
        if (saveBackSelectColor == null) {
            saveBackNonSelectColor = getBackgroundNonSelectionColor();
            saveBackSelectColor = getBackgroundSelectionColor();
            saveForeNonSelectColor = getTextNonSelectionColor();
            saveForeSelectColor = getTextSelectionColor();
        }
    }

    ///////////////////////////////////////////////////////////////////////////
    // METHODS
    ///////////////////////////////////////////////////////////////////////////

    public Component getTreeCellRendererComponent(
        final JTree tree, final Object value, boolean isSelected,
        final boolean isExpanded, final boolean isLeaf, final int row,
        final boolean hasFocus) {

        if (value == null) {
            return this;
        }

        Object userObj = ((DefaultTreeNode)value).getContent();
        boolean deployed = false;
        boolean registered = false;
        String stateTxt = null;
        Color txtColor = null;
        Icon icon = null;

        if (userObj instanceof ServiceData) {
            icon = SERVICE_ICON;
            ServiceData service = (ServiceData)userObj;
            deployed = service.isDeployed();
            registered = service.isRegistered();
            int state = service.getCurrentState();
            txtColor = RuntimeMgmtUtils.getServiceStateColor(state);                

            // hack for odbc service to display a more appropriate
            // text when the actual MMODBC service is not running
            if (service.getName().equalsIgnoreCase("ODBCService") &&
                state == ServiceInterface.STATE_DATA_SOURCE_UNAVAILABLE) {
                state = RuntimeMgmtUtils.ODBC_UNAVAILABLE_SERVICE_STATE;
            } 
            
            stateTxt = RuntimeMgmtUtils.getServiceStateText(state);
        }
        else {
            if (userObj instanceof PSCData) {
                icon = PSC_ICON;
            }
            else if (userObj instanceof ProcessData) {
                icon = PROCESS_ICON;
            }
            else if (userObj instanceof HostData) {
                icon = HOST_ICON;
            }
        }

        // set background color
        Color color = RuntimeMgmtUtils.getStateColor(deployed, registered);
        if (color == null) {
            setBackgroundSelectionColor(saveBackSelectColor);
            setBackgroundNonSelectionColor(saveBackNonSelectColor);
        }
        else {
            setBackgroundSelectionColor(color);
            setBackgroundNonSelectionColor(color);
        }

        // set foreground color
        if (txtColor == null) {
            setTextSelectionColor(saveForeSelectColor);
            setTextNonSelectionColor(saveForeNonSelectColor);
        }
        else {
            setTextSelectionColor(txtColor);
            setTextNonSelectionColor(txtColor);
        }

        // call super to set all background/foreground colors for isSelected and
        // hasFocus
        super.getTreeCellRendererComponent(
             tree, value, isSelected, isExpanded, isLeaf, row, hasFocus);

        setIcon(icon);

        if (userObj instanceof ServiceData) {
            setText(getText() +
                    RuntimeMgmtUtils.getString("state.msg",
                                               new Object[] {stateTxt}));
        }

        return this;
    }

}
