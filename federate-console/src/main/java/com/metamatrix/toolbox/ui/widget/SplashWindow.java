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

import java.awt.Dimension;
import java.awt.Font;
import java.awt.Toolkit;

import javax.swing.BoxLayout;
import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.JWindow;

import com.metamatrix.toolbox.ui.UIDefaults;

/**
 * @since 2.0
 */
public class SplashWindow extends JWindow {
    //############################################################################################################################
    //# Constants                                                                                                                #
    //############################################################################################################################
    
    public static final String PROPERTY_PREFIX = "SplashWindow.";
    public static final String STATUS_MESSAGE_BACKGROUND_COLOR_PROPERTY = PROPERTY_PREFIX + "statusMessageBackground";
    public static final String STATUS_MESSAGE_FOREGROUND_COLOR_PROPERTY = PROPERTY_PREFIX + "statusMessageForeground";
    
    //############################################################################################################################
    //# Variables                                                                                                                #
    //############################################################################################################################
    
    private SplashPanel splashPanel;
    private JLabel status;
    
    //############################################################################################################################
    //# Constructors                                                                                                             #
    //############################################################################################################################
    
    /**
     * @since 2.0
     */
    public SplashWindow() {
        this(null);
    }
    
    /**
     * @since 2.0
     */
    public SplashWindow(final String message) {
        initializeSplashWindow(message);
    }
    
    //############################################################################################################################
    //# Methods                                                                                                                  #
    //############################################################################################################################

    /**
     * @since 2.0
     */
    protected void initializeSplashWindow(String message) {
        if (message == null) {
            message = " ";
        }
        splashPanel = new SplashPanel();
        final JPanel statusPanel = new JPanel(null);
        statusPanel.setLayout(new BoxLayout(statusPanel, BoxLayout.Y_AXIS));
        final UIDefaults dflts = UIDefaults.getInstance();
        statusPanel.setBackground(dflts.getColor(STATUS_MESSAGE_BACKGROUND_COLOR_PROPERTY));
        status = new JLabel(message, JLabel.CENTER);
        status.setForeground(dflts.getColor(STATUS_MESSAGE_FOREGROUND_COLOR_PROPERTY));
        final Font font = dflts.getFont("Label.font");
        status.setFont(font.deriveFont((float)font.getSize() - 1));
        status.setMaximumSize(new Dimension(Short.MAX_VALUE, status.getPreferredSize().height));
        status.setAlignmentX(0.5f);
        statusPanel.add(status);
        splashPanel.add(statusPanel);
        getContentPane().add(splashPanel);
        pack();
        final Dimension screenSize = Toolkit.getDefaultToolkit().getScreenSize();
        final Dimension size = getPreferredSize();
        setLocation((screenSize.width - size.width) / 2, (screenSize.height - size.height) / 2);
    }
    
    /**
     * @since 2.0
     */
    public void setStatusMessage(String message) {
        if (message == null) {
            message = " ";
        }
        status.setText(message);
        splashPanel.paintImmediately(splashPanel.getBounds());
    }
    
    /**
     * @since 2.0
     */
    public void setVisible(final boolean visible) {
        super.setVisible(visible);
        if (visible) {
            splashPanel.paintImmediately(splashPanel.getBounds());
        }
    }
    
}
