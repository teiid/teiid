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

package com.metamatrix.console.ui.views.logsetup;

import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.awt.GridLayout;
import java.awt.Insets;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;

import javax.swing.JPanel;

import com.metamatrix.console.util.StaticQuickSorter;

import com.metamatrix.toolbox.ui.widget.ButtonWidget;

public class MessageLevelPanel extends JPanel {
    private ConfigurationLogSetUpPanelController controller;
    private String[] copySourceNames;
    private MessageLevelChangeNotifyee notifyee;
    private MessageLevelCheckBoxes checkBoxPanel;
    private ButtonWidget[] copyFrom = null;

    public MessageLevelPanel(ConfigurationLogSetUpPanelController controller,
            String[] copySourceNames,
            MessageLevelChangeNotifyee notifyee, String[] messageLevelNames,
            int initialMessageLevel, boolean enabled) {
        super();
        this.controller = controller;
        this.copySourceNames = copySourceNames;
        this.notifyee = notifyee;
        init(messageLevelNames, initialMessageLevel, enabled);
    }

    private void init(String[] messageLevelNames,
            int initialMessageLevel, boolean enabled) {
        GridBagLayout layout = new GridBagLayout();
        this.setLayout(layout);
        checkBoxPanel = new MessageLevelCheckBoxes(notifyee, messageLevelNames,
                initialMessageLevel, true, enabled);
        this.add(checkBoxPanel);
        layout.setConstraints(checkBoxPanel, new GridBagConstraints(0, 0, 1, 1,
                0.0, 0.0, GridBagConstraints.CENTER, GridBagConstraints.NONE,
                new Insets(0, 0, 6, 0), 0, 0));
        if (enabled && (copySourceNames.length > 0)) {
            copyFrom = new ButtonWidget[copySourceNames.length];
            JPanel buttonsPanel = new JPanel(new GridLayout(1,
                    copySourceNames.length, 6, 0));
            for (int i = 0; i < copyFrom.length; i++) {
                copyFrom[i] = new ButtonWidget("Copy from " + copySourceNames[i]); //$NON-NLS-1$
                final int index = i;
                copyFrom[i].addActionListener(new ActionListener() {
                    public void actionPerformed(ActionEvent ev) {
                        copyFromPressed(index);
                    }
                });
                buttonsPanel.add(copyFrom[i]);
            }
//BWP 02/26/02  For now, leaving copy buttons off the panel
//            this.add(buttonsPanel);
//            layout.setConstraints(buttonsPanel, new GridBagConstraints(0, 1, 1, 1,
//                    0.0, 0.0, GridBagConstraints.CENTER, GridBagConstraints.NONE,
//                    new Insets(6, 50, 0, 0), 0, 0));
        }
    }

    private void copyFromPressed(int buttonIndex) {
        int level = controller.getMessageLevelFrom(copySourceNames[buttonIndex]);
        setLevel(level);
        notifyee.messageLevelsChanged();
    }

    public int getCurrentLevel() {
        boolean[] values = checkBoxPanel.getCurrentValues();
        int highestLevel = -1;
        int loc = values.length - 1;
        while ((loc >= 0) && (highestLevel < 0)) {
            if (values[loc]) {
                highestLevel = loc;
            } else {
                loc--;
            }
        }
        return highestLevel;
    }

    public void setLevel(int level) {
        checkBoxPanel.setValuesUpThrough(level);
    }

    public void setCopyButtonState(String sourceName, boolean newState) {
        if ((copySourceNames != null) && (copyFrom != null)) {
            int index = StaticQuickSorter.unsortedStringArrayIndex(copySourceNames,
                    sourceName);
            if (index >= 0) {
                copyFrom[index].setEnabled(newState);
            }
        }
    }
}


