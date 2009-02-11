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

package com.metamatrix.console.ui.layout;

import com.metamatrix.console.connections.ConnectionInfo;
import com.metamatrix.console.notification.RuntimeUpdateNotification;

public interface WorkspacePanel {
    /**
     * Called when the panel has just regained focus.  It needs to return a List
     * of Actions that will put the Actions menu back into the state it was
     * when the panel last had focus.
     *
     * Also, the panel may want to perform other processing at this time such as
     * restarting a timer.
     *
     * @return  List of Actions to be placed in the Actions menu.
     */
    java.util.List /*<Action>*/ resume();

    /**
     * Return a title that may be displayed in the frame's title bar and/or in
     * a panel above this panel.
     */
    String getTitle();
    
    ConnectionInfo getConnection();
    
    void receiveUpdateNotification(RuntimeUpdateNotification notification);
}
