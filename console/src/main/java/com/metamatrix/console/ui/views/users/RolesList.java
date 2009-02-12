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

import java.util.Iterator;
import java.util.Map;

import javax.swing.ListSelectionModel;
import javax.swing.event.ListSelectionEvent;
import javax.swing.event.ListSelectionListener;

import com.metamatrix.admin.api.server.AdminRoles;
import com.metamatrix.api.exception.ComponentNotFoundException;
import com.metamatrix.api.exception.security.AuthorizationException;

import com.metamatrix.console.models.GroupsManager;
import com.metamatrix.console.ui.util.StringListBasedListModel;
import com.metamatrix.console.util.ExternalException;
import com.metamatrix.console.util.StaticQuickSorter;


import com.metamatrix.toolbox.ui.widget.ListWidget;

/**
 * Extension to ListWidget to display the list of roles that apply to the Console.
 * Through an interface it notifies caller-designated object when the selected role has
 * changed.
 */
public class RolesList extends ListWidget implements ListSelectionListener {
    //API communications handler:
    private GroupsManager manager;

    //Object to be notified when list selection changes:
    private NotifyOnSelectionChange controller;

    //Flag to temporarily disable notification on list selection change.
    //Starts out with notification enabled:
    private boolean ignoreValueChange = false;

// Constructors / initialization methods:

    /**
     * Constructor.  Also creates the component.
     *
     * @param mgr     handler of API communications
     * @param ctrlr   to be notified when list selection changes
     * @throws ComponentNotFoundException  possible if manager call to get all roles fails
     * @throws AuthorizationException      possible if manager call to get all roles fails
     * @throws ExternalException           possible if manager call to get all roles fails
     */
    public RolesList(GroupsManager mgr, NotifyOnSelectionChange ctrlr,
            boolean makeSelection)
            throws ComponentNotFoundException, AuthorizationException,
            ExternalException {
        super();
        manager = mgr;
        controller = ctrlr;
        init(makeSelection);
    }

    /**
     * Create the component.
     */
    private void init(boolean makeSelection) throws ComponentNotFoundException,
            AuthorizationException, ExternalException {
        //Get all roles.  Can throw exceptions.
        Map roles = manager.getRoles();

        String[] roleNames = new String[roles.size()];
        Iterator it = roles.entrySet().iterator();
        for (int i = 0; it.hasNext(); i++) {
            Map.Entry me = (Map.Entry)it.next();
			RoleDisplay roleDisplay = (RoleDisplay)me.getValue();
            roleNames[i] = roleDisplay.getDisplayName();
        }
        //Sort roles names alphabetically.
        String[] sortedRoleNames = StaticQuickSorter.quickStringSort(roleNames);
        //Set list model to this set of names
        setModel(new StringListBasedListModel(sortedRoleNames, false));
        addListSelectionListener(this);
        getSelectionModel().setSelectionMode(ListSelectionModel.SINGLE_SELECTION);
        if (makeSelection) {
            String readRole = AdminRoles.RoleName.ADMIN_READONLY;
            String displayName = manager.getRoleDisplayName(readRole);
            if (displayName != null) {
                int numElems = getModel().getSize();
                int i = 0;
                boolean found = false;
                while ((!found) && (i < numElems)) {
                    String itemString = getModel().getElementAt(i).toString();
                    if (itemString.equals(displayName)) {
                        this.setSelectedIndex(i);
                        found = true;
                    } else {
                        i++;
                    }
                }
            }
        }
    }
    
//Set methods

    /**
     * Method to temporarily disable/enable handling of list selection changes.
     *
     * @param flag  new true or false value
     */
    public void setIgnoreValueChange(boolean flag) {
        ignoreValueChange = flag;
    }

//Processing methods

    /**
     * Method required by ListSelectionListener.  Selection has changed.
     *
     * @param ev  event describing the change
     */
    public void valueChanged(ListSelectionEvent ev) {
        //Do we need to handle this?
        if ((!ignoreValueChange) && (!ev.getValueIsAdjusting())) {
            controller.selectionChanged(this, this);
        }
    }

    public void refreshData(){
        if ((!ignoreValueChange)) {
            controller.selectionChanged(this, this);
        }
    }
}




