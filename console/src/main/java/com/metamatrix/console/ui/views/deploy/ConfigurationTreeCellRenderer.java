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
package com.metamatrix.console.ui.views.deploy;

import java.awt.Component;

import javax.swing.Icon;
import javax.swing.JTree;

import com.metamatrix.common.config.api.Configuration;
import com.metamatrix.common.config.api.ConfigurationID;
import com.metamatrix.common.config.api.DeployedComponent;
import com.metamatrix.common.config.api.ServiceComponentDefn;
import com.metamatrix.common.config.api.VMComponentDefn;
import com.metamatrix.console.connections.ConnectionInfo;
import com.metamatrix.console.models.ConfigurationManager;
import com.metamatrix.console.models.ModelManager;
import com.metamatrix.console.ui.views.deploy.model.ConfigurationTreeModel;
import com.metamatrix.console.ui.views.deploy.util.DeployPkgUtils;
import com.metamatrix.toolbox.ui.widget.tree.DefaultTreeCellRenderer;
import com.metamatrix.toolbox.ui.widget.tree.DefaultTreeNode;

public final class ConfigurationTreeCellRenderer
    extends DefaultTreeCellRenderer {

    ///////////////////////////////////////////////////////////////////////////
    // CONSTANTS
    ///////////////////////////////////////////////////////////////////////////

//    private static final Icon NEXT_CONFIG_ICON;
    private static final Icon HOST_ICON;
    private static final Icon PROCESS_ICON;
    private static final Icon SERVICE_ICON;
//    private static final Icon DEPLOYMENTS_ICON;

    ///////////////////////////////////////////////////////////////////////////
    // INITIALIZER
    ///////////////////////////////////////////////////////////////////////////

    static {
 //       NEXT_CONFIG_ICON = DeployPkgUtils.getIcon("icon.nextstartup"); //$NON-NLS-1$
        HOST_ICON = DeployPkgUtils.getIcon("icon.host"); //$NON-NLS-1$
        PROCESS_ICON = DeployPkgUtils.getIcon("icon.process"); //$NON-NLS-1$
        SERVICE_ICON = DeployPkgUtils.getIcon("icon.service"); //$NON-NLS-1$
//        DEPLOYMENTS_ICON = DeployPkgUtils.getIcon("icon.deployments"); //$NON-NLS-1$
//        CONNECTOR_ICON = DeployPkgUtils.getIcon("icon.connector"); //$NON-NLS-1$
//        METAMATRIX_SERVER_ICON = DeployPkgUtils.getIcon("icon.mmserver"); //$NON-NLS-1$
//        PLATFORM_ICON = DeployPkgUtils.getIcon("icon.platform"); //$NON-NLS-1$
    }

    private ConnectionInfo connectionInfo;

    ///////////////////////////////////////////////////////////////////////////
    // METHODS
    ///////////////////////////////////////////////////////////////////////////

	public ConfigurationTreeCellRenderer(ConnectionInfo connectionInfo) {
		super();
        this.connectionInfo = connectionInfo;
	}

    public Component getTreeCellRendererComponent(
        final JTree tree,
        final Object value,
        boolean isSelected,
        final boolean isExpanded,
        final boolean isLeaf,
        final int row,
        final boolean hasFocus) {

        if (value == null) {
            return this;
        }
        // call super to set all background/foreground colors for
        // isSelected, hasFocus
        super.getTreeCellRendererComponent(
            tree, value, isSelected, isExpanded, isLeaf, row, hasFocus);

        // set the icon
        Object userObj = ((DefaultTreeNode)value).getContent();
        if (userObj instanceof Configuration) {
//                ConfigurationID configId =
//                    (ConfigurationID)((Configuration)userObj).getID();
        } else if (userObj instanceof ServiceComponentDefn) {
            setIcon(SERVICE_ICON);
		} else if (userObj instanceof VMComponentDefn) {
            setIcon(PROCESS_ICON);
        } else if (userObj instanceof ConfigurationTreeModel.HostWrapper) {
            setIcon(HOST_ICON);
        } else if (userObj instanceof DeployedComponent) {
            setIcon(SERVICE_ICON);
        }
        return this;
    }
    
    private ConfigurationManager getConfigurationManager() {
        return ModelManager.getConfigurationManager(connectionInfo);
    }
    
    
}