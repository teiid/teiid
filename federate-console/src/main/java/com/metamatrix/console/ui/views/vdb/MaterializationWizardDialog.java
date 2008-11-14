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

import java.awt.GridLayout;
import java.awt.event.WindowAdapter;
import java.awt.event.WindowEvent;

import javax.swing.JDialog;
import javax.swing.JFrame;
import javax.swing.JPanel;
import javax.swing.border.EmptyBorder;

import com.metamatrix.console.ConsolePlugin;
import com.metamatrix.console.connections.ConnectionInfo;
import com.metamatrix.console.util.StaticUtilities;

/**
 * JDialog that contains the MaterializationWizard 
 * @since 4.3
 */
public class MaterializationWizardDialog extends JDialog {

    MaterializationWizard wizard = null;
    private ConnectionInfo connection = null;

    public MaterializationWizardDialog(JFrame parentFrame,
                           String vdbName,
                           String vdbVersion,
                           ConnectionInfo connection) {
        super(parentFrame);
        this.connection = connection;
        
        this.setModal(true);
        initThis(vdbName, vdbVersion);
        
        this.pack();
        this.setLocation(StaticUtilities.centerFrame(this.getSize()));
    }


    private void initThis(String vdbName, String vdbVersion) {

        getRootPane().setPreferredSize(new java.awt.Dimension(650, 650));

        setTitle(ConsolePlugin.Util.getString("MaterializationWizardDialog.title")); //$NON-NLS-1$);
        setModal(true);

        JPanel content = (JPanel)getContentPane();
        content.setLayout(new GridLayout(1, 1));
        content.setBorder(new EmptyBorder(10, 10, 10, 10));

        addWindowListener(new WindowAdapter() {

            public void windowClosing(WindowEvent event) {
                dispose();
            }
        });

        wizard = new MaterializationWizard(this, vdbName, vdbVersion, connection);

        content.add(wizard);
    }

    public void processFinishButton() {
        dispose();
    }

    public void processCancelButton() {
        dispose();
    }

    
}
