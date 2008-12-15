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

import java.awt.Cursor;
import java.awt.Event;
import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.awt.Insets;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.awt.event.KeyEvent;
import java.awt.event.MouseAdapter;
import java.awt.event.MouseEvent;

import javax.swing.ImageIcon;
import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.JTextArea;
import javax.swing.KeyStroke;

import com.metamatrix.common.util.ApplicationInfo;
import com.metamatrix.console.ConsolePlugin;
import com.metamatrix.toolbox.ToolboxPlugin;
import com.metamatrix.toolbox.ui.widget.AboutPanel;
import com.metamatrix.toolbox.ui.widget.ButtonWidget;
import com.metamatrix.toolbox.ui.widget.DialogPanel;
import com.metamatrix.toolbox.ui.widget.DialogWindow;
import com.metamatrix.toolbox.ui.widget.SpacerWidget;
import com.metamatrix.toolbox.ui.widget.SplashPanel;
import com.metamatrix.toolbox.ui.widget.util.BrowserControl;
import com.metamatrix.toolbox.ui.widget.util.IconFactory;

/**
 * @since 2.0
 */
public class ConsoleAboutPanel extends AboutPanel {
	
    //############################################################################################################################
    //# Constructors                                                                                                             #
    //############################################################################################################################
    
    /**
     * @since 2.0
     */
    public ConsoleAboutPanel() {
        super();
    }
    
    //############################################################################################################################
    //# Methods                                                                                                                  #
    //############################################################################################################################
    
    /**
     * @since 2.0
     */
    protected ButtonWidget createCancelButton() {
        return null;
    }
    
    /**
     * @since 2.0
     */
    protected void initializeAboutPanel() {
        final ApplicationInfo info = ApplicationInfo.getInstance();
        //final String url = ConsolePlugin.Util.getString("ConsoleAboutPanel.url");  //$NON-NLS-1$
        final String alternateSplash = ConsolePlugin.Util.getString("Console.alternateSplash");  //$NON-NLS-1$
        ImageIcon splashIcon = null;
        if(alternateSplash!=null&&alternateSplash.trim().length()>0) {
            splashIcon = IconFactory.getIconForImageFile(alternateSplash); 
        }
        JPanel urlPnl = getUrlPanel();
        JPanel panel = null;
        if(splashIcon!=null) {
        	panel = new JPanel();
            GridBagLayout layout = new GridBagLayout();
            panel.setLayout(layout);
            JLabel label = new JLabel(splashIcon);
        	panel.add(label);
            panel.add(urlPnl);
            layout.setConstraints(label, new GridBagConstraints(0, 0, 1, 1, 1.0,1.0,
                    GridBagConstraints.NORTH, GridBagConstraints.BOTH, new Insets(0, 0, 0, 0), 0, 0));
            layout.setConstraints(urlPnl, new GridBagConstraints(0, 1, 1, 1, 0.0,0.0,
                    GridBagConstraints.SOUTH, GridBagConstraints.BOTH, new Insets(0, 0, 0, 0), 0, 0));
        } else {
        	panel = new SplashPanel();
            panel.add(urlPnl);
        }
        
        setContent(panel);
        registerKeyboardAction(new ActionListener() {
            public void actionPerformed(final ActionEvent event) {
                final JTextArea box = new JTextArea(info.toString());
                box.setLineWrap(false);
                box.setEditable(false);
                final DialogPanel panel = new DialogPanel(new JScrollPane(box)) {
                    protected ButtonWidget createCancelButton() {
                        return null;
                    }
                };
                panel.addNavigationSpacer(SpacerWidget.createHorizontalExpandableSpacer());
                String details = ToolboxPlugin.Util.getString("AboutPanel.Application_Build_Details_5"); //$NON-NLS-1$
                DialogWindow.show(ConsoleAboutPanel.this, details, panel); 
            }
        }, KeyStroke.getKeyStroke(KeyEvent.VK_D, Event.ALT_MASK), WHEN_IN_FOCUSED_WINDOW);
    }
    
    private JPanel getUrlPanel() {
        final String url = ConsolePlugin.Util.getString("ConsoleAboutPanel.url");  //$NON-NLS-1$
        final JLabel label = new JLabel("<html><a href='" + url + "'>" + url + "</a></html>"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
        label.addMouseListener(new MouseAdapter() {
        	Cursor prevCursor = null;
            public void mouseClicked(final MouseEvent event) {
                BrowserControl.displayURL(url);
            }
            public void mouseEntered(MouseEvent theEvent) {
            	prevCursor = ConsoleAboutPanel.this.getCursor();
            	ConsoleAboutPanel.this.setCursor(Cursor.getPredefinedCursor(Cursor.HAND_CURSOR));
            }
            public void mouseExited(MouseEvent theEvent) {
            	ConsoleAboutPanel.this.setCursor(prevCursor);
            }
        });
        JPanel pnl = new JPanel();
        pnl.setOpaque(false);
        pnl.add(label);
    	return pnl;
    }
}
