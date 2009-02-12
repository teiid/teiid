/*
 * JBoss, Home of Professional Open Source.
 * See the COPYRIGHT.txt file distributed with this work for information
 * regarding copyright ownership.  Some portions may be licensed
 * to Red Hat, Inc. under one or more contributor license agreements.
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
import java.awt.Insets;
import java.awt.event.ItemEvent;
import java.awt.event.ItemListener;

import javax.swing.JCheckBox;
import javax.swing.JPanel;

import com.metamatrix.toolbox.ui.widget.CheckBox;

public class MessageLevelCheckBoxes extends JPanel implements ItemListener {
    private final static int GAP_BETWEEN_ITEMS = 4;
    private MessageLevelChangeNotifyee notifyee;
    private JCheckBox[] checkBoxes;
    private boolean programmaticChange = false;

    //Upon a level receiving a checkmark, should we also check any lower levels?
    private boolean checkAllLowerLevels;

    public MessageLevelCheckBoxes(MessageLevelChangeNotifyee notifyee,
            boolean checkAllLowerLevels, String[] levelNames,
            boolean[] levelInitialValues, boolean hideNone, boolean enabled) {
        super();
        this.notifyee = notifyee;
        this.checkAllLowerLevels = checkAllLowerLevels;
        if (levelNames.length != levelInitialValues.length) {
            throw new RuntimeException("Error in MessageLevelCheckBoxes constructor"); //$NON-NLS-1$
        }
        init(levelNames, levelInitialValues, hideNone, enabled);
    }

    public MessageLevelCheckBoxes(MessageLevelChangeNotifyee notifyee,
            String[] levelNames, int initialLevel, boolean hideNone,
            boolean enabled) {
        super();
        this.notifyee = notifyee;
        this.checkAllLowerLevels = true;
        boolean[] levelInitialValues = new boolean[levelNames.length];
        for (int i = 0; i <= initialLevel; i++) {
            levelInitialValues[i] = true;
        }
        for (int i = initialLevel + 1; i < levelInitialValues.length; i++) {
            levelInitialValues[i] = false;
        }
        init(levelNames, levelInitialValues, hideNone, enabled);
    }

    private void init(String[] levelNames, boolean[] levelInitialValues,
            boolean hideNone, boolean enabled) {
        GridBagLayout layout = new GridBagLayout();
        this.setLayout(layout);
        checkBoxes = new JCheckBox[levelNames.length];
        for (int i = 0; i < levelNames.length; i++) {
            checkBoxes[i] = new CheckBox(levelNames[i]);
            checkBoxes[i].setSelected(levelInitialValues[i]);
            checkBoxes[i].addItemListener(this);
            checkBoxes[i].setEnabled(enabled);
            this.add(checkBoxes[i]);
            layout.setConstraints(checkBoxes[i], new GridBagConstraints(i, 0,
                    1, 1, 1.0, 1.0, GridBagConstraints.CENTER,
                    GridBagConstraints.NONE, new Insets(0,
                    (i == 0 ? 0 : GAP_BETWEEN_ITEMS), 0,
                    (i == levelNames.length - 1 ? 0 : GAP_BETWEEN_ITEMS)),
                    0, 0));
            if (hideNone) {
                if (levelNames[i].equalsIgnoreCase("None")) { //$NON-NLS-1$
                    checkBoxes[i].setVisible(false);
                }
            }
        }
    }

    public void itemStateChanged(ItemEvent ev) {
        if (!programmaticChange) {
            if (checkAllLowerLevels) {
                Object source = ev.getSource();
                int matchLoc = -1;
                int loc = 0;
                while (matchLoc < 0) {
                    if (source == checkBoxes[loc]) {
                        matchLoc = loc;
                    } else {
                        loc++;
                    }
                }
                if (checkBoxes[loc].isSelected()) {
                    programmaticChange = true;
                    for (int i = 0; i < loc; i++) {
                        checkBoxes[i].setSelected(true);
                    }
                    programmaticChange = false;
                } else {
                    programmaticChange = true;
                    for (int i = loc + 1; i < checkBoxes.length; i++) {
                        checkBoxes[i].setSelected(false);
                    }
                    programmaticChange = false;
                }
            }
            notifyee.messageLevelsChanged();
        }
    }

    public boolean[] getCurrentValues() {
        boolean[] currentValues = new boolean[checkBoxes.length];
        for (int i = 0; i < currentValues.length; i++) {
            currentValues[i] = checkBoxes[i].isSelected();
        }
        return currentValues;
    }

    public void setValues(boolean[] newValues) {
        programmaticChange = true;
        for (int i = 0; i < newValues.length; i++) {
            checkBoxes[i].setSelected(newValues[i]);
        }
        programmaticChange = false;
    }

    public void setValuesUpThrough(int index) {
        for (int i = 0; i <= index; i++) {
            checkBoxes[i].setSelected(true);
        }
        for (int i = index + 1; i < checkBoxes.length; i++) {
            checkBoxes[i].setSelected(false);
        }
    }
}
