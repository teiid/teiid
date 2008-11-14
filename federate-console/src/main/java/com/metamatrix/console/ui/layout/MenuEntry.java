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

import javax.swing.Action;
import javax.swing.JMenu;

/**
 * Class to represent the state that the menu bar should be put in.
 */
public class MenuEntry {

    //public static final String FILE_MENU            = "File";
    //public static final String EDIT_MENU            = "Edit";
    //public static final String PREFS_MENU           = "Preferences";
    //public static final String VIEW_MENU            = "View";
    //public static final String ACTIONS_MENU         = "Actions";
    //public static final String HELP_MENU            = "Help";


    // these constants define the permanent menu items
    public static final String FILE_EXIT_MENUITEM   = "file.exit"; //$NON-NLS-1$
    public static final String FILE_PRINT_MENUITEM  = "file.print"; //$NON-NLS-1$

    public static final String PREFS_URLS_MENUITEM  = "prefs.urls"; //$NON-NLS-1$
    public static final String PREFS_REFRESH_RATES_MENUITEM
                                                    = "prefs.refreshrates"; //$NON-NLS-1$
	public static final String PREFS_LOGGING_MENUITEM =
			"prefs.logging";                                                     //$NON-NLS-1$

	public static final String CONNECTIONS_ADD_CONN_MENUITEM = 
			"connections.addconnection"; //$NON-NLS-1$
	public static final String CONNECTIONS_REMOVE_CONN_MENUITEM = 
			"connections.removeconnection"; //$NON-NLS-1$
			
    public static final String VIEW_REFRESH_MENUITEM
                                                    = "view.refresh"; //$NON-NLS-1$

    public static final String HELP_ABOUTTHECONSOLE_MENUITEM
                                                    = "help.abouttheconsole"; //$NON-NLS-1$

    public static final String SEPARATOR_LIT        = "separator"; //$NON-NLS-1$
    public static final String MENU_LIT             = "fullmenu"; //$NON-NLS-1$

    // a common ID is used for all Action menu menuitems:
    public static final String ACTION_MENUITEM      = "action"; //$NON-NLS-1$



    public static final int ACTION                  = 1;
    public static final int MENU                    = 2;
    public static final int SEPARATOR               = 3;

    private int iMenuObjectType     = 0;

    private String sID              = "Unknown"; //$NON-NLS-1$
    private Action actAction        = null;
    private JMenu mnuMenu           = null;

    // a default separator object to use when creating menus
    public static final MenuEntry DEFAULT_SEPARATOR
        = new MenuEntry(SEPARATOR);


    public MenuEntry(String sID, Action actAction) {
        super();
        this.sID        = sID;
        this.actAction  = actAction;
        iMenuObjectType = ACTION;
    }

    public MenuEntry(JMenu mnu) {
        super();
        this.sID        = MENU_LIT;
        this.mnuMenu    = mnu;
        iMenuObjectType = MENU;
    }

    public MenuEntry(int iType) {
        super();
        if (iType != SEPARATOR)
            throw new IllegalArgumentException("If type is not separator, action is required"); //$NON-NLS-1$
        iMenuObjectType = iType;
        this.sID = SEPARATOR_LIT;
    }

    public String getID() {
        return sID;
    }

    public int getType() {
        return iMenuObjectType;
    }

    public Action getAction() {
        return actAction;
    }

    public JMenu getMenu() {
        return mnuMenu;
    }

    public static MenuEntry getSeparator() {
        return DEFAULT_SEPARATOR;
    }
}
