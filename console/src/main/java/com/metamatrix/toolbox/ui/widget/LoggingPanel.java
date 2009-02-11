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

import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.awt.Insets;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;

import javax.swing.BorderFactory;
import javax.swing.Box;
import javax.swing.BoxLayout;
import javax.swing.ButtonGroup;
import javax.swing.JComboBox;
import javax.swing.JFrame;
import javax.swing.JLabel;
import javax.swing.JList;
import javax.swing.JPanel;
import javax.swing.JRadioButton;
import javax.swing.JScrollPane;
import javax.swing.border.Border;
import javax.swing.border.EmptyBorder;
import javax.swing.event.ListSelectionEvent;
import javax.swing.event.ListSelectionListener;

import com.metamatrix.common.log.LogConfiguration;
import com.metamatrix.common.log.LogManager;
import com.metamatrix.common.log.config.BasicLogConfiguration;
import com.metamatrix.core.log.MessageLevel;

import com.metamatrix.toolbox.preference.UserPreferences;
/**
 * AddPanel is a JDialog that panels in the User tab can use to add Accounts, Groups, and
 * Roles to a given info object.
 *
 * The user tab panel instantiates an AddPanel, configures it, and shows it.  The AddPanel
 * then calls back on the user tab panel using the AddPanelCallback interface to hand
 * the user tab panel a Collection of IDs that should be added to the current info object.
 */
public class LoggingPanel extends JPanel implements ActionListener, ListSelectionListener {

    private static final String LOGGING_TITLE = "Message Logging";

    private JList logList;
    private JComboBox cb;

    private ButtonGroup listGroup = new ButtonGroup();
    private JRadioButton logAllContextsButton;
    private JRadioButton logSelectedContextsButton;

    public LoggingPanel(java.util.List availContextsList) {
        buildLoggingPanel(availContextsList);
    }

    private void buildLoggingPanel(java.util.List availContextsList) {

        UserPreferences preferences = UserPreferences.getInstance();

        this.setLayout(new BoxLayout(this, BoxLayout.Y_AXIS));
        Border b = BorderFactory.createCompoundBorder(new EmptyBorder(2,2,2,2),
                                                      new TitledBorder(LOGGING_TITLE));
        this.setBorder(b);
        this.add(Box.createVerticalStrut(20));
        JPanel hiP = new JPanel();
        hiP.add(new JLabel("Message Type Filter : "));

        cb = new JComboBox(MessageLevel.getDisplayNames().toArray());
        cb.setSelectedIndex(Integer.parseInt(preferences.getProperties().getProperty(BasicLogConfiguration.LOG_LEVEL_PROPERTY_NAME, "3")));
        cb.addActionListener(this);
        hiP.add(cb);
        this.add(hiP);
        this.add(Box.createVerticalStrut(20));
        JPanel loP = new JPanel(new GridBagLayout());
        loP.setBorder(new EmptyBorder(0,2,0,5));
        GridBagConstraints c = new GridBagConstraints();

        logAllContextsButton = new JRadioButton("All",true);
        logSelectedContextsButton = new JRadioButton("All Except : ");

        listGroup.add(logAllContextsButton);
        listGroup.add(logSelectedContextsButton);
        logAllContextsButton.setActionCommand("All");
        logSelectedContextsButton.setActionCommand("Select");

        logList = new JList(availContextsList.toArray());
        logList.setEnabled(false);
        logList.setFixedCellWidth(200);
        logList.getSelectionModel().addListSelectionListener(this);
        logAllContextsButton.addActionListener(this);
        logSelectedContextsButton.addActionListener(this);

        JScrollPane jSP = new JScrollPane(logList);
        c.gridx = 0;
        c.gridy = 0;
        c.weightx = 1.0;
        c.weighty = 1.0;
        c.insets = new Insets(5, 5, 5, 5);
        loP.add(new JLabel("Log Contexts"), c);
        c.gridx = 1;
        c.anchor = GridBagConstraints.WEST;
        loP.add(logAllContextsButton, c);
        c.gridy = 1;
        c.anchor = GridBagConstraints.NORTHWEST;
        loP.add(logSelectedContextsButton, c);
        c.gridx = 2;
        c.anchor = GridBagConstraints.CENTER;
        loP.add(jSP, c);
        this.add(loP);

        // Determine whether any contexts have been removed.  If so, set accordingly
        Collection removedContexts = preferences.getValues(BasicLogConfiguration.LOG_CONTEXT_PROPERTY_NAME, ';');
        if(removedContexts.size()!=0) {
            Object[] allContexts = availContextsList.toArray();
            logSelectedContextsButton.setSelected(true);
            logList.setEnabled(true);
            int removedIndices[] = new int[allContexts.length];
            int ind=0;
            for(int i=0; i<allContexts.length; i++) {
                if(removedContexts.contains(allContexts[i])) {
                  removedIndices[ind]=i;
                  ind++;
                }
            }
            logList.setSelectedIndices(removedIndices);
        }
    }

    public void actionPerformed(ActionEvent e) {
        if ( e.getSource() == cb ) {
            UserPreferences preferences = UserPreferences.getInstance();
            preferences.setValue(BasicLogConfiguration.LOG_LEVEL_PROPERTY_NAME, new Integer(cb.getSelectedIndex()).toString());
        } else {
            if ( e.getActionCommand().equals("All") ) {
                logList.clearSelection();
                logList.setEnabled(false);
            } else if ( e.getActionCommand().equals("Select") ) {
                logList.setEnabled(true);
            }
        }
    }

    public void valueChanged(ListSelectionEvent e) {
        UserPreferences preferences = UserPreferences.getInstance();
        if ( logAllContextsButton.isSelected() ) {
            preferences.setValue(BasicLogConfiguration.LOG_CONTEXT_PROPERTY_NAME, "");
        } else {
            Collection selectedContexts = Arrays.asList(logList.getSelectedValues());
            preferences.setValues(BasicLogConfiguration.LOG_CONTEXT_PROPERTY_NAME, selectedContexts, ';');
        }
    }

//    /**
//     * Removes logging contexts.  Contexts not selected in the logging JList when
//     * the "remove all except" JRadioButton is checked are removed.
//     */
//    private void stopSelectedLoggingContexts() {
//        Object[] selectedContexts = logList.getSelectedValues();
//        java.util.List list = Arrays.asList(selectedContexts);
//        UserPreferences.getInstance().setValues(BasicLogConfiguration.LOG_CONTEXT_PROPERTY_NAME, list, ';');
//        removeLoggingContexts(list);
//    }

    public void removeLoggingContexts(Collection contexts) {
        LogConfiguration config = LogManager.getLogConfigurationCopy();
        config.recordAllContexts();
        config.discardContexts(contexts);
        LogManager.setLogConfiguration(config);
    }

    public void clearLoggingContexts() {
        LogConfiguration config = LogManager.getLogConfigurationCopy();
        config.recordAllContexts();
        LogManager.setLogConfiguration(config);
    }

    public Collection getRemovedLoggingContexts() {
        LogConfiguration config = LogManager.getLogConfigurationCopy();
        return config.getDiscardedContexts();
    }



    public static void main(String[] args) {

        JFrame frame = new JFrame("User Preferences Test");
        frame.setLocation(100,100);
        frame.setSize(600,500);
        frame.addWindowListener(new java.awt.event.WindowAdapter() {
           public void windowClosing(java.awt.event.WindowEvent e) {System.exit(0);}
        });
        java.util.List contexts = new ArrayList();
        contexts.add("fake context");
        frame.getContentPane().add(new LoggingPanel(contexts));
        frame.setVisible(true);



    }




}




