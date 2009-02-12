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

import java.awt.Component;
import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.awt.Insets;
import java.util.Date;

import javax.swing.JPanel;
import javax.swing.event.ChangeEvent;
import javax.swing.event.ChangeListener;
import javax.swing.event.DocumentEvent;
import javax.swing.event.DocumentListener;

import com.metamatrix.console.util.QCDateTime;

public class TimeSpanPanel extends JPanel
        implements TimeSpanButtonSelectionListener, DocumentListener {

    public final static int NOW_INDEX = 1;
    public final static int SYSTEM_START_INDEX = 1;
    public final static int SUBPANEL_CONFIGURABLE_INDEX = 1;
    public final static int SPECIFIED_TIME_INDEX = 2;
    public final static int BEFORE_AFTER_INDEX = 3;

    public final static int STARTING_PANEL_INITIAL_INDEX = BEFORE_AFTER_INDEX;
    public final static int ENDING_PANEL_INITIAL_INDEX = NOW_INDEX;

    private Date startingDateTime;
    private Date currentDateTime;
    private TimeSpanPanelValidityListener listener;
    private boolean isNowValid = true;
    private TimeSpanStartingPanel startingPanel;
    private TimeSpanEndingPanel endingPanel;
    private int startingPanelCurrentSelection = STARTING_PANEL_INITIAL_INDEX;
    private int endingPanelCurrentSelection = NOW_INDEX;
    private boolean initialized = false;
    
    public TimeSpanPanel(Date starting, Date current,
            TimeSpanPanelValidityListener lstnr) {
        super();
        startingDateTime = starting;
        currentDateTime = current;
        listener = lstnr;
        init();
    }

    private void init() {
        startingPanel = new TimeSpanStartingPanel(this, this, startingDateTime);
        endingPanel = new TimeSpanEndingPanel(this, this, currentDateTime);
        startingPanel.addCalendarChangeListener(new ChangeListener() {
            public void stateChanged(ChangeEvent ev) {
                calendarChanged(ev);
            }
        });
        endingPanel.addCalendarChangeListener(new ChangeListener() {
            public void stateChanged(ChangeEvent ev) {
                calendarChanged(ev);
            }
        });
        GridBagLayout layout = new GridBagLayout();
        this.setLayout(layout);
        this.add(startingPanel);
        this.add(endingPanel);
        layout.setConstraints(startingPanel, new GridBagConstraints(0, 0, 1, 1,
                1.0, 1.0, GridBagConstraints.CENTER, GridBagConstraints.BOTH,
                new Insets(0, 0, 0, 0), 0, 0));
        layout.setConstraints(endingPanel, new GridBagConstraints(1, 0, 1, 1,
                1.0, 1.0, GridBagConstraints.CENTER, GridBagConstraints.BOTH,
                new Insets(0, 0, 0, 0), 0, 0));
        startingPanel.pressButton(STARTING_PANEL_INITIAL_INDEX);
        initialized = true;
    }

    public void reset() {
        startingPanel.pressButton(STARTING_PANEL_INITIAL_INDEX);
        endingPanel.pressButton(ENDING_PANEL_INITIAL_INDEX);
        startingPanel.resetUnits();
        endingPanel.resetUnits();
        resetStartTime(startingDateTime);
        startingPanel.setCalendar(startingDateTime);
        currentDateTime = new Date();
        endingPanel.setCalendar(currentDateTime);
    }

    public void resetStartTime(Date systemStartTime) {
        startingPanel.resetStartTime(systemStartTime);
        startingDateTime = systemStartTime;
    }

    public boolean endsNow() {
        return (endingPanelCurrentSelection == NOW_INDEX);
    }

    public Date getStartingTime() {
        Date startTime = null;
        switch (startingPanelCurrentSelection) {
            case SYSTEM_START_INDEX:
                startTime = startingDateTime;
                break;
            case SPECIFIED_TIME_INDEX:
                startTime = startingPanel.getSecondItemCalendarPanelTime();
                break;
            case BEFORE_AFTER_INDEX:
                Date endTime = getEndingTime();
                long endTimeLong = endTime.getTime();
                int numUnits = startingPanel.getThirdItemNumUnits().intValue();
                long numMillisecondsPerUnit = 0;
                if (startingPanel.isMinutesSelected()) {
                    numMillisecondsPerUnit = 60 * 1000;
                } else if (startingPanel.isHoursSelected()) {
                    numMillisecondsPerUnit = 60 * 60 * 1000;
                } else if (startingPanel.isDaysSelected()) {
                    numMillisecondsPerUnit = 24 * 60 * 60 * 1000;
                }
                long millisecondsToSubtract = numUnits * numMillisecondsPerUnit;
                long adjustedTime = endTimeLong - millisecondsToSubtract;
                startTime = new Date(adjustedTime);
                break;
        }
        return startTime;
    }

    public Date getEndingTime() {
        Date endTime = null;
        switch (endingPanelCurrentSelection) {
            case NOW_INDEX:
                endTime = new Date();
                break;
            case SPECIFIED_TIME_INDEX:
                endTime = endingPanel.getSecondItemCalendarPanelTime();
                break;
            case BEFORE_AFTER_INDEX:
                Date startTime = getStartingTime();
                long startTimeLong = startTime.getTime();
                int numUnits = endingPanel.getThirdItemNumUnits().intValue();
                long numMillisecondsPerUnit = 0;
                if (endingPanel.isMinutesSelected()) {
                    numMillisecondsPerUnit = 60 * 1000;
                } else if (endingPanel.isHoursSelected()) {
                    numMillisecondsPerUnit = 60 * 60 * 1000;
                } else if (endingPanel.isDaysSelected()) {
                    numMillisecondsPerUnit = 24 * 60 * 60 * 1000;
                }
                long millisecondsToAdd = numUnits * numMillisecondsPerUnit;
                long adjustedTime = startTimeLong + millisecondsToAdd;
                endTime = new Date(adjustedTime);
                break;
        }
        return endTime;
    }

    public void buttonSelectionChanged(Component sender, int selection) {
        boolean handled = false;
        if (sender == startingPanel) {
            startingPanelCurrentSelection = selection;
            endingPanel.setThirdButtonEnabled((selection != BEFORE_AFTER_INDEX));
            handled = true;
        } else if (sender == endingPanel) {
            endingPanelCurrentSelection = selection;
            startingPanel.setThirdButtonEnabled((selection !=
                    BEFORE_AFTER_INDEX));
            handled = true;
        }
        if (handled) {
            checkForValidityChange();
            checkForChangeFromOriginal();
        }
    }

    public void calendarChanged(ChangeEvent ev) {
        checkForChangeFromOriginal();
    }

    private void checkForValidityChange() {
        if (listener != null) {
            boolean valid = true;
            if ((startingPanelCurrentSelection == BEFORE_AFTER_INDEX) &&
                    (endingPanelCurrentSelection == BEFORE_AFTER_INDEX)) {
                valid = false;
            }
            if (valid) {
                if (startingPanelCurrentSelection == BEFORE_AFTER_INDEX) {
                    if (startingPanel != null) {
                        Integer n = startingPanel.getThirdItemNumUnits();
                        if (n == null) {
                            valid = false;
                        } else {
                            int num = n.intValue();
                            if (num <= 0) {
                                valid = false;
                            }
                        }
                    }
                }
            }
            if (valid) {
                if (endingPanelCurrentSelection == BEFORE_AFTER_INDEX) {
                    if (endingPanel != null) {
                        Integer n = endingPanel.getThirdItemNumUnits();
                        if (n == null) {
                            valid = false;
                        } else {
                            int num = n.intValue();
                            if (num <= 0) {
                                valid = false;
                            }
                        }
                    }
                }
            }
            if (valid != isNowValid) {
                isNowValid = valid;
                listener.timeSpanValidityChanged(isNowValid);
            }
        }
    }

    
    public boolean changedFromOriginal() {
        if (startingPanelCurrentSelection != STARTING_PANEL_INITIAL_INDEX) {
            return true;
        } 
        
        if (endingPanelCurrentSelection != ENDING_PANEL_INITIAL_INDEX) {
            return true;
        } 
        
        Date startingPanelCalendarDate = startingPanel.getSecondItemCalendarPanelTime();
        QCDateTime startingPanelDateForComparison = new QCDateTime(startingPanelCalendarDate, false, true);
        QCDateTime startingDateTimeForComparison = new QCDateTime(startingDateTime, false, true);
        if (!startingPanelDateForComparison.equals(startingDateTimeForComparison)) {
            return true;
        }
        
        Date endingPanelCalendarDate = endingPanel.getSecondItemCalendarPanelTime();
        QCDateTime endingPanelDateForComparison = new QCDateTime(endingPanelCalendarDate, false, true);
        QCDateTime endingDateTimeForComparison = new QCDateTime(currentDateTime, false, true);
        if (!endingPanelDateForComparison.equals(endingDateTimeForComparison)) {
            return true;
        } 
        
        //TODO: why is this not returning true???
        if (startingPanel.unitsChanged()) {
            return true;
        }
        if (endingPanel.unitsChanged()) {
            return true;        
        }
        
        
        
        return false;
    }
    
    public void checkForChangeFromOriginal() {
        boolean changed = changedFromOriginal();
        listener.timeSpanChangedFromOriginal(changed);
    }

    

    public void changedUpdate(DocumentEvent ev) {
        numUnitsTextFieldChanged(ev);
    }

    public void removeUpdate(DocumentEvent ev) {
        numUnitsTextFieldChanged(ev);
    }

    public void insertUpdate(DocumentEvent ev) {
        numUnitsTextFieldChanged(ev);
    }

    private void numUnitsTextFieldChanged(DocumentEvent ev) {
        checkForValidityChange();
        
        if (initialized) {
            checkForChangeFromOriginal();
        }
    }


}
