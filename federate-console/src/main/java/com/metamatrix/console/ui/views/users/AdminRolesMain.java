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

package com.metamatrix.console.ui.views.users;

import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.awt.Insets;
import java.util.Collections;

import com.metamatrix.console.connections.ConnectionInfo;
import com.metamatrix.console.models.GroupsManager;
import com.metamatrix.console.notification.RuntimeUpdateNotification;
import com.metamatrix.console.ui.layout.BasePanel;
import com.metamatrix.console.ui.layout.WorkspacePanel;

public class AdminRolesMain extends BasePanel implements WorkspacePanel {
    private GroupsTabMainPanel panel;
    private GroupsManager mgr;

    public AdminRolesMain(GroupsManager mgr, boolean seesEnterprise,
            boolean viewsPrincipals, boolean modifiesPrincipals,
            boolean viewsRoles, boolean modifiesRoles, boolean resetsPassword) {
        super();
        this.mgr = mgr;
        panel = new GroupsTabMainPanel(mgr, seesEnterprise, viewsPrincipals,
                modifiesPrincipals, viewsRoles, modifiesRoles);
        add(panel);
        GridBagLayout layout = new GridBagLayout();
        setLayout(layout);
        layout.setConstraints(panel, new GridBagConstraints(0, 0, 1, 1, 1.0, 1.0,
                GridBagConstraints.CENTER, GridBagConstraints.BOTH,
                new Insets(0, 0, 0, 0), 0, 0));
    }

    public void refreshData(){
        panel.refreshData();
    }

    public void createComponent() throws Exception {
        panel.createComponent();
    }

    public java.util.List /*<Action>*/ resume() {
        return Collections.EMPTY_LIST;
        //return panel.resume();
    }

    public String getTitle() {
        return "Admin Roles";
    }
    
    public ConnectionInfo getConnection() {
    	return mgr.getConnection();
    }
    
    public void receiveUpdateNotification(RuntimeUpdateNotification notification) {
    	//TODO
    }
}


