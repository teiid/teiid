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

package com.metamatrix.console.ui;

import java.awt.BorderLayout;
import java.awt.Color;

import javax.swing.JPanel;
import javax.swing.SwingUtilities;

import com.metamatrix.toolbox.ui.widget.ButtonWidget;
import com.metamatrix.toolbox.ui.widget.LabelWidget;
import com.metamatrix.toolbox.ui.widget.TextFieldWidget;

public class StatusPanel extends JPanel {

    public static final String DEFAULT_STATUS_TEXT = "READY"; //$NON-NLS-1$
    public static final String BUSY_STATUS_TEXT = "PLEASE WAIT..."; //$NON-NLS-1$

    private LabelWidget statusLabel = new LabelWidget("  status:  "); //$NON-NLS-1$
    private TextFieldWidget statusField = new TextFieldWidget(" " + DEFAULT_STATUS_TEXT); //$NON-NLS-1$
    private ButtonWidget busyIndicator = new ButtonWidget();

    public void createComponent() {
        this.setLayout(new BorderLayout());
        this.add(statusLabel, BorderLayout.WEST);
        this.add(statusField, BorderLayout.CENTER);
        busyIndicator.setBackground(Color.green);
        this.add(busyIndicator, BorderLayout.EAST);
        statusField.setEditable( false );
        statusField.setBackground( Color.white );
    }

    public void setStatusText(String text) {
        statusField.setText(" " + text); //$NON-NLS-1$
    }

    public void clearStatusText() {
        statusField.setText(DEFAULT_STATUS_TEXT);
    }

    /**
     * Puts a request on the event-queue thread to render this object's
     *status panel as busy.
     */
    public void startBusySyncronize(){
        busyIndicator.setBackground(Color.red);
        setStatusText(BUSY_STATUS_TEXT);
    }

    public void startBusy() {
        SwingUtilities.invokeLater(new Runnable(){
            public void run(){
                busyIndicator.setBackground(Color.red);
                setStatusText(BUSY_STATUS_TEXT);
            }
        });
    }

    /**
     * Puts a request on the event-queue thread to render this object's
     *status panel as not busy.
     */

    public void endBusySyncronize(){
        busyIndicator.setBackground(Color.green);
        setStatusText(DEFAULT_STATUS_TEXT);
    }

    public void endBusy() {
        SwingUtilities.invokeLater(new Runnable(){
            public void run(){
                busyIndicator.setBackground(Color.green);
                setStatusText(DEFAULT_STATUS_TEXT);
            }

        });
    }

}

