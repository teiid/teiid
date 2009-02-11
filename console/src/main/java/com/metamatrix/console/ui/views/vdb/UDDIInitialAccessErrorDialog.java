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

package com.metamatrix.console.ui.views.vdb;

import java.awt.Frame;
import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.awt.Insets;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.awt.event.WindowAdapter;
import java.awt.event.WindowEvent;

import javax.swing.JButton;
import javax.swing.JDialog;
import javax.swing.JLabel;

import com.metamatrix.console.ConsolePlugin;
import com.metamatrix.console.ui.util.IconComponent;
import com.metamatrix.console.ui.util.property.Icons;
import com.metamatrix.console.util.ExceptionUtility;
import com.metamatrix.console.util.StaticUtilities;
import com.metamatrix.toolbox.ui.widget.ButtonWidget;
import com.metamatrix.toolbox.ui.widget.LabelWidget;


/** 
 * @since 4.2
 */
public class UDDIInitialAccessErrorDialog extends JDialog {
    private final static String VIEW_ERROR_DIALOG = ConsolePlugin.Util.getString(
            "WSDLWizardRunner.viewErrorDialog"); //$NON-NLS-1$
    private final static String OK = ConsolePlugin.Util.getString("General.OK"); //$NON-NLS-1$
    private final static String TITLE = ConsolePlugin.Util.getString(
            "UDDIInitialAccessErrorDialog.title"); //$NON-NLS-1$
    private final static String LINE1 = ConsolePlugin.Util.getString(
            "UDDIInitialAccessErrorDialog.line1"); //$NON-NLS-1$
    private final static String LINE2 = ConsolePlugin.Util.getString(
            "UDDIInitialAccessErrorDialog.line2"); //$NON-NLS-1$
    
    private Throwable t;
    
    public UDDIInitialAccessErrorDialog(Frame owner, Throwable t) {
        super(owner, TITLE, true);
        this.t = t;
        init();
    }
    
    private void init() {
        this.addWindowListener(new WindowAdapter() {
            public void windowClosing(WindowEvent ev) {
                okPressed();
            }
        });
        GridBagLayout layout = new GridBagLayout();
        this.getContentPane().setLayout(layout);
        IconComponent errorIcon = new IconComponent(Icons.ERROR_ICON);
        this.getContentPane().add(errorIcon);
        layout.setConstraints(errorIcon, new GridBagConstraints(0, 0, 1, 2, 0.0, 0.0,
                GridBagConstraints.CENTER, GridBagConstraints.NONE, new Insets(4, 4, 4, 4),
                0, 0));
        JLabel line1 = new LabelWidget(LINE1);
        this.getContentPane().add(line1);
        layout.setConstraints(line1, new GridBagConstraints(1, 0, 1, 1, 0.0, 0.0,
                GridBagConstraints.WEST, GridBagConstraints.NONE, new Insets(4, 10, 0, 10), 0, 0));
        JLabel line2 = new LabelWidget(LINE2);
        this.getContentPane().add(line2);
        layout.setConstraints(line2, new GridBagConstraints(1, 1, 1, 1, 0.0, 0.0,
                GridBagConstraints.WEST, GridBagConstraints.NONE, new Insets(0, 10, 4, 10), 0, 0));
        JButton viewButton = new ButtonWidget(VIEW_ERROR_DIALOG);
        viewButton.addActionListener(new ActionListener() {
            public void actionPerformed(ActionEvent ev) {
                viewPressed();
            }
        });
        this.getContentPane().add(viewButton);
        layout.setConstraints(viewButton, new GridBagConstraints(0, 2, 2, 1, 0.0, 0.0,
                GridBagConstraints.CENTER, GridBagConstraints.NONE, new Insets(10, 10, 10, 10),
                0, 0));
        JButton okButton = new ButtonWidget("   " + OK + "   ");
        okButton.addActionListener(new ActionListener() {
            public void actionPerformed(ActionEvent ev) {
                okPressed();
            }
        });
        this.getContentPane().add(okButton);
        layout.setConstraints(okButton, new GridBagConstraints(0, 3, 2, 1, 0.0, 0.0,
                GridBagConstraints.CENTER, GridBagConstraints.NONE, new Insets(10, 10, 10, 10),
                0, 0));
        this.pack();
        this.setLocation(StaticUtilities.centerFrame(this.getSize()));
    }
    
    private void okPressed() {
        this.dispose();
    }
    
    private void viewPressed() {
        okPressed();
        ExceptionUtility.showMessage(TITLE, t);
    }
}
