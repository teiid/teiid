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

import java.awt.Color;
import java.awt.Dimension;
import java.awt.Font;
import java.awt.Frame;
import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.awt.GridLayout;
import java.awt.Insets;
import java.awt.Toolkit;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.awt.event.WindowAdapter;
import java.awt.event.WindowEvent;

import javax.swing.JButton;
import javax.swing.JDialog;
import javax.swing.JLabel;
import javax.swing.JList;
import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.JTextArea;

import com.metamatrix.console.ConsolePlugin;
import com.metamatrix.console.util.StaticUtilities;
import com.metamatrix.toolbox.ui.widget.ButtonWidget;

/** 
 * Dialog displayed when a VDB import contains connector binding names that already exist.
 * The dialog informs the user that bindings already exist and that the existing versions
 * will be used.  The user is prompted as to whether or not to proceed, with 'Yes' and 
 * 'Cancel' buttons.
 * 
 * @since 4.2
 */
public class BindingsAlreadyExistDlg extends JDialog {
    private final static Color TEXT_BACKGROUND;
    private final static Color TEXT_FOREGROUND;
    private final static Font TEXT_FONT;
    
    static {
        JLabel dummyLabel = new JLabel();
        TEXT_BACKGROUND = dummyLabel.getBackground();
        TEXT_FOREGROUND = dummyLabel.getForeground();
        TEXT_FONT = dummyLabel.getFont();
    }
    
    private final static String HEADER = ConsolePlugin.Util.getString(
            "BindingsAlreadyExistDlg.header"); //$NON-NLS-1$
    private final static String UPPER_TEXT = ConsolePlugin.Util.getString(
            "BindingsAlreadyExistDlg.alreadyExistText"); //$NON-NLS-1$
    private final static String LOWER_TEXT = ConsolePlugin.Util.getString(
            "BindingsAlreadyExistDlg.oldUsed"); //$NON-NLS-1$
    private final static String PROCEED = ConsolePlugin.Util.getString(
            "BindingsAlreadyExistDlg.proceed"); //$NON-NLS-1$
    private final static String YES = ConsolePlugin.Util.getString(
            "General.Yes"); //$NON-NLS-1$
    private final static String CANCEL = ConsolePlugin.Util.getString(
            "General.Cancel"); //$NON-NLS-1$                                                                      
    
    private boolean canceling = false;
    
    public BindingsAlreadyExistDlg(Frame parent, String[] names) {
        super(parent);
        this.setTitle(HEADER);
        this.setModal(true);
        initialize(names);
        this.addWindowListener(new WindowAdapter() {
            public void windowClosing(WindowEvent ev) {
                cancelPressed();
            }
        });
    }
    
    public boolean proceeding() {
        return (!canceling);
    }
    
    private void cancelPressed() {
        canceling = true;
        this.dispose();
    }
    
    private void yesPressed() {
        this.dispose();
    }
    
    private void initialize(String[] names) {
        GridBagLayout layout = new GridBagLayout();
        this.getContentPane().setLayout(layout);
        JTextArea upperText = new JTextArea(UPPER_TEXT);
        upperText.setEditable(false);
        upperText.setLineWrap(true);
        upperText.setWrapStyleWord(true);
        upperText.setForeground(TEXT_FOREGROUND);
        upperText.setBackground(TEXT_BACKGROUND);
        upperText.setFont(TEXT_FONT);
        this.getContentPane().add(upperText);
        layout.setConstraints(upperText, new GridBagConstraints(0, 0, 1, 1, 1.0, 0.0,
                GridBagConstraints.CENTER, GridBagConstraints.BOTH, new Insets(4, 4, 4, 4),
                0, 0));
        JList list = new JList(names);
        list.setFont(TEXT_FONT);
        list.setBackground(TEXT_BACKGROUND);
        list.setForeground(TEXT_FOREGROUND);
        JScrollPane scrollPane = new JScrollPane(list);
        this.getContentPane().add(scrollPane);
        layout.setConstraints(scrollPane, new GridBagConstraints(0, 1, 1, 1, 1.0, 1.0,
                GridBagConstraints.CENTER, GridBagConstraints.BOTH, 
                new Insets(4, 4, 4, 4), 0, 0));
        JTextArea lowerText = new JTextArea(LOWER_TEXT);
        lowerText.setEditable(false);
        lowerText.setLineWrap(true);
        lowerText.setWrapStyleWord(true);
        lowerText.setForeground(TEXT_FOREGROUND);
        lowerText.setBackground(TEXT_BACKGROUND);
        lowerText.setFont(TEXT_FONT);
        this.getContentPane().add(lowerText);
        layout.setConstraints(lowerText, new GridBagConstraints(0, 2, 1, 1, 1.0, 0.0,
                GridBagConstraints.CENTER, GridBagConstraints.BOTH, new Insets(4, 4, 4, 4),
                0, 0));
        JLabel proceedLabel = new JLabel(PROCEED);
        this.getContentPane().add(proceedLabel);
        layout.setConstraints(proceedLabel, new GridBagConstraints(0, 3, 1, 1, 0.0, 0.0,
                GridBagConstraints.CENTER, GridBagConstraints.NONE, new Insets(6, 4, 6, 4),
                0, 0));
        JPanel buttonsPanel = new JPanel(new GridLayout(1, 2, 5, 0));
        JButton yesButton = new ButtonWidget(YES);
        yesButton.addActionListener(new ActionListener() {
            public void actionPerformed(ActionEvent ev) {
                yesPressed();
            }
        });
        buttonsPanel.add(yesButton);
        JButton cancelButton = new ButtonWidget(CANCEL);
        cancelButton.addActionListener(new ActionListener() {
            public void actionPerformed(ActionEvent ev) {
                cancelPressed();
            }
        });
        buttonsPanel.add(cancelButton);
        this.getContentPane().add(buttonsPanel);
        layout.setConstraints(buttonsPanel, new GridBagConstraints(0, 4, 1, 1, 0.0, 0.0,
                GridBagConstraints.CENTER, GridBagConstraints.NONE, new Insets(6, 6, 10, 6),
                0, 0));
        this.pack();
        Dimension oldSize = this.getSize();
        Dimension newSize = new Dimension(Math.max(oldSize.width,
                (int)(Toolkit.getDefaultToolkit().getScreenSize().width * 0.4)),
                oldSize.height);
        this.setSize(newSize);
        this.setLocation(StaticUtilities.centerFrame(this.getSize()));
    }
}
