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

package com.metamatrix.console.util;

import java.awt.Component;

import javax.swing.JFrame;
import javax.swing.JOptionPane;

import com.metamatrix.console.ui.dialog.PendingChangesDialog;
import com.metamatrix.console.ui.util.CenteredOptionPane;

/**
 * This class consists of convenient static methods for displaying common
 *and oft-used dialog boxes, such as confirmation dialogs, message dialogs,
 *etc.  Confirm dialogs are in yes/no mode, and return a boolean indicating
 *if yes was selected.<P>
 *
 * TODO - add company icons to dialog boxes
 */
public class DialogUtility {
	public static final String CONFIRM_DELETE_HDR = "Confirm Deletion";
	public static final int YES = JOptionPane.YES_OPTION;
    public static final int NO = JOptionPane.NO_OPTION;
    public static final int CANCEL = JOptionPane.CANCEL_OPTION;

    private static Component defaultComponent = null;

    /**
     * Set the default java.awt.Component which is used if <I>null</I>
     *is passed in for an argument on any of the public methods.  If this is
     *not set, and null is passed in later, Swing will still produce the
     *JDialog, but it may not be in reference to the proper "parent"
     *Component.
     */
    public static void setDefaultComponent(Component c) {
        defaultComponent = c;
    }

    public static JFrame getDefaultComponent() {
        return (JFrame)defaultComponent;
    }

    /**
     * Generic implementation of a yes/no dialog box, used by other methods
     *of this class (which simply supply the necessary text and title)
     * @param c java.awt.Component needed by the Dialog Window
     * @param message Object displayed in window (usually a String, but
     *JOptionPane will take an Object)
     * @param title String displayed in title bar
     * @return boolean indicating if yes was selected
     */
    public static boolean yesNoDialog(Component c, Object message, String title){
        if (c == null){
            c = defaultComponent;
        }
        int i;
        i = JOptionPane.showConfirmDialog(c,
                                          message,
                                          title,
                                          JOptionPane.YES_NO_OPTION);
        if (i==JOptionPane.YES_OPTION) {
            return true;
        }
        return false;
    }

    /**
     * Put up a dialog with YES or NO options.
     *
     * @return one of YES or NO
     */
    public static int displayYesNoDialog(Component c, String header, String message) {
        String formattedMessage = StaticUtilities.insertLineBreaks(message,
                StaticUtilities.PREFERRED_MODAL_DIALOG_TEXT_WIDTH,
                StaticUtilities.MAX_MODAL_DIALOG_TEXT_WIDTH);
        Component comp = c;
        if (comp == null) {
            comp = defaultComponent;
        }
        int val = CenteredOptionPane.showConfirmDialog(comp, formattedMessage, 
        		header, JOptionPane.YES_NO_OPTION);
        int response = -1;
        switch (val) {
            case JOptionPane.YES_OPTION:
                response = YES;
                break;
            case JOptionPane.NO_OPTION:
                response = NO;
                break;
        }
        return response;
    }

	/**
     * Static method to show a dialog inquiring about saving the pending changes.
     *
     * @param msg   The message to be displayed
     * @return      One of: DialogUtility.YES, DialogUtility.NO, or DialogUtility.CANCEL
     */
    public static int showPendingChangesDialog(String msg, String url,
    		String userName) {
    	PendingChangesDialog dlg = new PendingChangesDialog(
    			(JFrame)defaultComponent, msg, url, userName);
    	dlg.show();
    	int response = dlg.getResponse();
    	return response;
    }
}
