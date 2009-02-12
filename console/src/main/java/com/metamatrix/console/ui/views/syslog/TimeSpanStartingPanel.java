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

package com.metamatrix.console.ui.views.syslog;

import java.awt.GridBagConstraints;
import java.awt.Insets;
import java.util.Date;

import javax.swing.event.DocumentListener;

import com.metamatrix.toolbox.ui.widget.LabelWidget;
import com.metamatrix.toolbox.ui.widget.TextFieldWidget;

public class TimeSpanStartingPanel extends TimeSpanStartingOrEndingPanel {
    private TextFieldWidget startTimeTextField;

    public TimeSpanStartingPanel(
            TimeSpanButtonSelectionListener buttonSelectionListener,
            DocumentListener numUnitsListener,
            Date systemStartTime) {
        super(buttonSelectionListener, numUnitsListener, systemStartTime,
                "Starting", "before specified end time",
                TimeSpanPanel.STARTING_PANEL_INITIAL_INDEX);
        init(systemStartTime);
    }

    private void init(Date systemStartTime) {
        LabelWidget startTimeLabel = new LabelWidget("At system start time:");
        firstRowPanel.add(startTimeLabel);
        firstRowLayout.setConstraints(startTimeLabel, new GridBagConstraints(
                0, 0, 1, 1, 0.0, 0.0, GridBagConstraints.WEST,
                GridBagConstraints.NONE, new Insets(0, 0, 0, 0), 0, 0));
        startTimeTextField = new TextFieldWidget(
                TimeSpanStartingOrEndingPanel.DATE_TIME_TEXT_FIELD_WIDTH);
        startTimeTextField.setEditable(false);
        startTimeTextField.setText(formatDate(systemStartTime));
        startTimeTextField.setMinimumSize(startTimeTextField.getPreferredSize());
        firstRowPanel.add(startTimeTextField);
        firstRowLayout.setConstraints(startTimeTextField, new GridBagConstraints(
                1, 0, 1, 1, 1.0, 0.0, GridBagConstraints.WEST,
                GridBagConstraints.NONE, new Insets(0, 2, 0, 0), 0, 0));
        secondItemCalendarPanel.setDate(systemStartTime);
    }

    public void resetStartTime(Date systemStartTime) {
        startTimeTextField.setText(formatDate(systemStartTime));
    }
    
    protected void buttonPressed() {
        super.buttonPressed();
        //Will be null the first time this is called from within superclass, so
        //do a check
        if (startTimeTextField != null) {
            if (secondItemRadioButton.isSelected() ||
                    thirdItemRadioButton.isSelected()) {
                startTimeTextField.setEnabled(false);
            } else if (firstItemRadioButton.isSelected()) {
                startTimeTextField.setEnabled(true);
            }
        }
    }
}

