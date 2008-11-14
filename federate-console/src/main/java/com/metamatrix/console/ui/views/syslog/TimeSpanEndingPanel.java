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

package com.metamatrix.console.ui.views.syslog;

import java.awt.GridBagConstraints;
import java.awt.Insets;
import java.util.Date;

import javax.swing.event.DocumentListener;

import com.metamatrix.toolbox.ui.widget.LabelWidget;

public class TimeSpanEndingPanel extends TimeSpanStartingOrEndingPanel {
    public TimeSpanEndingPanel(
            TimeSpanButtonSelectionListener buttonSelectionListener,
            DocumentListener numUnitsListener, Date currentTime) {
        super(buttonSelectionListener, numUnitsListener, currentTime, "Ending",
                "after specified start time",
                TimeSpanPanel.ENDING_PANEL_INITIAL_INDEX);
        init(currentTime);
    }

    private void init(Date currentTime) {
        LabelWidget nowLabel = new LabelWidget("Now");
        firstRowPanel.add(nowLabel);
        firstRowLayout.setConstraints(nowLabel, new GridBagConstraints(
                0, 0, 1, 1, 1.0, 0.0, GridBagConstraints.WEST,
                GridBagConstraints.NONE, new Insets(0, 0, 0, 0), 0, 0));
        secondItemCalendarPanel.setDate(currentTime);
    }
}

