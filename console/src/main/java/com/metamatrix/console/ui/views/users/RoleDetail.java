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

package com.metamatrix.console.ui.views.users;

import java.awt.Dimension;
import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.awt.Insets;

import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.JTextArea;

import com.metamatrix.console.connections.ConnectionInfo;
import com.metamatrix.console.models.GroupsManager;
import com.metamatrix.console.ui.layout.BasePanel;
import com.metamatrix.console.ui.util.ConsoleCellRenderer;
import com.metamatrix.console.ui.util.IconLabel;
import com.metamatrix.console.ui.util.RepaintController;
import com.metamatrix.toolbox.ui.widget.TitledBorder;

/**
 * Panel to the details for a role.  This includes its name, description,
 * and a list of the principals to whom it has been assigned, possibly with
 * the capability to assign or deassign.
 */
public class RoleDetail extends BasePanel {

	private RoleDisplay roleDisplay;

    //API call handler:
    private GroupsManager manager;

    //Sub-panel showing list of principals assigned to:
    private RoleMembershipPanel rolePanel;

    //To be informed if change made, hence repaint needed:
    private RepaintController repaintController;

    /**
     * Constructor.  Also creates the component.
     *
     * @param r role description
     * @param mgr  API communications handler
     * @param rOnly Is information displayed to be read-only?
     * @param rc object to be explicitly informed when a repaint is needed, because of possible Swing repaint problems
     * @throws Exception  If API call failed to get list of principals role is already assigned to
     */
    public RoleDetail(RoleDisplay rd, GroupsManager mgr, boolean rOnly,
            RepaintController rc, ConnectionInfo conn)
            throws Exception {
        super();
		roleDisplay = rd;
        manager = mgr;
        repaintController = rc;
        init();
        repaintController.repaintNeeded();
    }
    
    public void deregister() {
        if(rolePanel != null) {
            rolePanel.deregister();
        }
    }

    /**
     * Create the component
     */
    private void init() throws Exception {
        GridBagLayout layout = new GridBagLayout();
        setLayout(layout);	
        String headerText = "Role: " + roleDisplay.getDisplayName();
        IconLabel header = new IconLabel(ConsoleCellRenderer.ROLE_ICON,
                headerText);
        add(header);
        layout.setConstraints(header, new GridBagConstraints(0, 0, 1, 1,
                0.0, 0.0, GridBagConstraints.WEST, GridBagConstraints.NONE,
                new Insets(5, 5, 5, 5), 0, 0));
        JTextArea descriptionArea = new JTextArea();
        descriptionArea.setColumns(30);
        descriptionArea.setRows(4);
        descriptionArea.setPreferredSize(new Dimension(150, 68));
        descriptionArea.setLineWrap(true);
        descriptionArea.setText(roleDisplay.getDescription());
        descriptionArea.setBorder(new TitledBorder(""));
        descriptionArea.setBackground((new JPanel()).getBackground());
        JScrollPane descriptionJSP = new JScrollPane(descriptionArea);
        add(descriptionJSP);
        layout.setConstraints(descriptionJSP, new GridBagConstraints(0, 1, 1, 1,
                0.0, 0.2, GridBagConstraints.WEST, GridBagConstraints.BOTH,
                new Insets(0, 5, 0, 5), 0, 0));
        //Create panel showing principals currently assigned to.  API call
        //involved, so can throw exceptions.
        rolePanel = new RoleMembershipPanel(roleDisplay, manager);
        //rolePanel.setAddButtonText("Assign...");
        add(rolePanel);
        layout.setConstraints(rolePanel, new GridBagConstraints(0, 2, 1, 1,
                1.0, 0.8, GridBagConstraints.CENTER, GridBagConstraints.BOTH,
                new Insets(5, 5, 5, 5), 0, 0));
    }

}
