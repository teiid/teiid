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

import java.awt.Component;
import java.util.Collection;

import com.metamatrix.console.models.GroupsManager;

/**
 * Interface to be implemented by a class controlling a GroupAccumulatorPanel.
 * The GroupAccumulatorPanel has an 'add' button, and a 'remove' button for which
 * it displays a confirmation dialog.  The methods are in response to and 'add'
 * or 'remove' request.
 */
public interface GroupAccumulatorController {
    //Method called when 'Add' button has been pressed.
    public Collection addPressed(Component callingPanel);

    //Method called when 'Remove' has been pressed, and user has responded 'yes'
    //to a confirmation dialog.  In removalItems, the first of each pair is the
    //user, group, or role being removed.  The second of each pair is the user
    //or group from which it is being removed.
    public boolean removeConfirmed(Component callingPanel, Collection removedMMPrincipals);
    
    public GroupsManager getGroupsManager();
}
