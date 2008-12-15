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

package com.metamatrix.toolbox.ui.widget;

import java.awt.Component;
import java.awt.Dialog;
import java.awt.Frame;
import java.awt.Window;

import javax.swing.SwingUtilities;

import com.metamatrix.common.util.ApplicationInfo;
import com.metamatrix.toolbox.ToolboxPlugin;

/**
 * @since 2.0
 */
public class AboutDialog extends DialogWindow {
    //############################################################################################################################
    //# Constants                                                                                                                #
    //############################################################################################################################
    
    private static final String TITLE = ToolboxPlugin.Util.getString("AboutDialog.About__1") + //$NON-NLS-1$
                                        ApplicationInfo.getInstance().getMainComponent().getTitle();
    
    //############################################################################################################################
    //# Static Methods                                                                                                           #
    //############################################################################################################################
    
    /**
     * @since 2.0
     */
    public static void show(final Component parent) {
        Window owner;
        if (parent instanceof Window) {
            owner = (Window)parent;
        } else {
            owner = SwingUtilities.windowForComponent(parent);
        }
        AboutDialog dlg;
        if (owner instanceof Frame) {
            dlg = new AboutDialog((Frame)owner);
        } else if (owner instanceof Dialog) {
            dlg = new AboutDialog((Dialog)owner);
        } else {
            throw new IllegalArgumentException(ToolboxPlugin.Util.getString("AboutDialog.Parent_parameter_must_be_within_a_Dialog_(_2") + Dialog.class + ToolboxPlugin.Util.getString("AboutDialog.)_or_Frame_(_3") + //$NON-NLS-1$ //$NON-NLS-2$
                                               Frame.class + ")"); //$NON-NLS-1$
        }
        dlg.pack();
        dlg.setLocationRelativeTo(parent);
        dlg.setVisible(true);
    }
    
    public static void show(final Component parent, final String title, final AboutPanel aboutPanel) {
        Window owner;
        if (parent instanceof Window) {
            owner = (Window)parent;
        } else {
            owner = SwingUtilities.windowForComponent(parent);
        }
        AboutDialog dlg;
        if (owner instanceof Frame) {
            dlg = new AboutDialog((Frame)owner, title, aboutPanel);
        } else if (owner instanceof Dialog) {
            dlg = new AboutDialog((Dialog)owner, title, aboutPanel);
        } else {
            throw new IllegalArgumentException(ToolboxPlugin.Util.getString("AboutDialog.Parent_parameter_must_be_within_a_Dialog_(_2") + Dialog.class + ToolboxPlugin.Util.getString("AboutDialog.)_or_Frame_(_3") + //$NON-NLS-1$ //$NON-NLS-2$
                                               Frame.class + ")"); //$NON-NLS-1$
        }
        dlg.pack();
        dlg.setLocationRelativeTo(parent);
        dlg.setVisible(true);
    }
    
    //############################################################################################################################
    //# Constructors                                                                                                             #
    //############################################################################################################################

    /**
     * @param owner    The window that displayed the dialog
     * @since 2.0
     */
    public AboutDialog(final Frame owner) {
        super(owner, TITLE, new AboutPanel());
    }

    /**
     * @param owner    The window that displayed the dialog
     * @since 2.0
     */
    public AboutDialog(final Dialog owner) {
        super(owner, TITLE, new AboutPanel());
    }
    
    /**
     * Constructor for Alternate Panel
     */
    public AboutDialog(final Frame owner, final String title, final AboutPanel aboutPanel) {
        super(owner, title, aboutPanel);
    }

    /**
     * Constructor for Alternate Panel
     */
    public AboutDialog(final Dialog owner, final String title, final AboutPanel aboutPanel) {
        super(owner, title, aboutPanel);
    }
}
