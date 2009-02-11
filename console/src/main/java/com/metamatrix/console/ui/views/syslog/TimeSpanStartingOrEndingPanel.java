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
import java.awt.GridBagLayout;
import java.awt.GridLayout;
import java.awt.Insets;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.text.SimpleDateFormat;
import java.util.Date;

import javax.swing.ButtonGroup;
import javax.swing.JComboBox;
import javax.swing.JPanel;
import javax.swing.JRadioButton;
import javax.swing.JTextField;
import javax.swing.border.TitledBorder;
import javax.swing.event.ChangeEvent;
import javax.swing.event.ChangeListener;
import javax.swing.event.DocumentListener;

import com.metamatrix.console.ui.layout.ConsoleMainFrame;
import com.metamatrix.toolbox.ui.widget.CalendarPanel;
import com.metamatrix.toolbox.ui.widget.LabelWidget;
import com.metamatrix.toolbox.ui.widget.TextFieldWidget;

public abstract class TimeSpanStartingOrEndingPanel extends JPanel {
    public final static int DATE_TIME_TEXT_FIELD_WIDTH = 17;
    private final static int VERTICAL_GAP_BETWEEN_ROWS = 6;
    private final static int LEFT_MARGIN = 10;
    private final static int RIGHT_MARGIN = 10;
    private final static int RADIO_BUTTON_RIGHT_MARGIN = 0;
    private final static String UNITS_FIELD_INITIAL_TEXT = "30"; //$NON-NLS-1$
    private final static int UNITS_TYPE_INITIAL_INDEX = UnitsTypeSelector.MINUTES_LOC;

    private TimeSpanButtonSelectionListener buttonSelectionListener;
    private DocumentListener numUnitsListener;
    protected CalendarPanel secondItemCalendarPanel = 
            new CalendarPanel(ConsoleMainFrame.getInstance());
    protected Date calendarPanelStartingDate;
    private TextFieldWidget thirdItemNumUnitsField;
    protected JRadioButton firstItemRadioButton = new JRadioButton(""); //$NON-NLS-1$
    protected JRadioButton secondItemRadioButton = new JRadioButton(""); //$NON-NLS-1$
    protected JRadioButton thirdItemRadioButton = new JRadioButton(""); //$NON-NLS-1$
    private JRadioButton lastButtonSelected = null;
    private boolean programmaticButtonSelection = false;
    protected JPanel firstRowPanel;
    protected GridBagLayout firstRowLayout;
    private UnitsTypeSelector unitsTypeSelector = new UnitsTypeSelector();
    private SimpleDateFormat format = new SimpleDateFormat("MMM dd, yyyy HH:mm"); //$NON-NLS-1$

    public TimeSpanStartingOrEndingPanel(
            TimeSpanButtonSelectionListener buttonListener,
            DocumentListener unitsListener, Date secondItemDate,
            String borderTitle, String thirdItemPhrase,
            int initiallySelectedButton) {
        super();
        buttonSelectionListener = buttonListener;
        numUnitsListener = unitsListener;
        calendarPanelStartingDate = secondItemDate;
        init(borderTitle, thirdItemPhrase, initiallySelectedButton);
    }

    private void init(String borderTitle, String thirdItemPhrase,
            int initiallySelectedButton) {
        this.setBorder(new TitledBorder(borderTitle));
        this.setLayout(new GridLayout(3, 1, 0, VERTICAL_GAP_BETWEEN_ROWS));
        ButtonGroup buttonGroup = new ButtonGroup();
        for (int i = 0; i < 3; i++) {
            JPanel curRowPanel = new JPanel();
            GridBagLayout layout = new GridBagLayout();
            curRowPanel.setLayout(layout);
            switch (i) {
                case 0:
                    buttonGroup.add(firstItemRadioButton);
                    curRowPanel.add(firstItemRadioButton);
                    layout.setConstraints(firstItemRadioButton,
                            new GridBagConstraints(
                            0, 0, 1, 1, 0.0, 0.0, GridBagConstraints.WEST,
                            GridBagConstraints.NONE,
                            new Insets(0, LEFT_MARGIN, 0, RADIO_BUTTON_RIGHT_MARGIN),
                            0, 0));
                    firstItemRadioButton.addActionListener(new ActionListener() {
                        public void actionPerformed(ActionEvent ev) {
                            buttonPressed();
                        }
                    });
                    JPanel firstRowInfoPanel = new JPanel();
                    firstRowLayout = new GridBagLayout();
                    firstRowInfoPanel.setLayout(firstRowLayout);
                    curRowPanel.add(firstRowInfoPanel);
                    layout.setConstraints(firstRowInfoPanel,
                            new GridBagConstraints(
                            1, 0, 1, 1, 1.0, 0.0, GridBagConstraints.WEST,
                            GridBagConstraints.NONE,
                            new Insets(0, 0, 0, RIGHT_MARGIN), 0, 0));
                    this.add(curRowPanel);
                    firstRowPanel = firstRowInfoPanel;
                    break;
                case 1:
                    buttonGroup.add(secondItemRadioButton);
                    curRowPanel.add(secondItemRadioButton);
                    layout.setConstraints(secondItemRadioButton,
                            new GridBagConstraints(
                            0, 0, 1, 1, 0.0, 0.0, GridBagConstraints.WEST,
                            GridBagConstraints.NONE,
                            new Insets(0, LEFT_MARGIN, 0, RADIO_BUTTON_RIGHT_MARGIN),
                            0, 0));
                    secondItemRadioButton.addActionListener(new ActionListener() {
                        public void actionPerformed(ActionEvent ev) {
                            buttonPressed();
                        }
                    });
                    JPanel secondRowInfoPanel = new JPanel();
                    curRowPanel.add(secondRowInfoPanel);
                    layout.setConstraints(secondRowInfoPanel,
                            new GridBagConstraints(
                            1, 0, 1, 1, 1.0, 0.0, GridBagConstraints.WEST,
                            GridBagConstraints.NONE,
                            new Insets(0, 0, 0, RIGHT_MARGIN), 0, 0));
                    GridBagLayout secondRowLayout = new GridBagLayout();
                    secondRowInfoPanel.setLayout(secondRowLayout);
                    LabelWidget secondRowAt = new LabelWidget("At:"); //$NON-NLS-1$
                    secondRowInfoPanel.add(secondRowAt);
                    secondRowLayout.setConstraints(secondRowAt,
                            new GridBagConstraints(0, 0, 1, 1, 0.0, 0.0,
                            GridBagConstraints.WEST, GridBagConstraints.NONE,
                            new Insets(0, 0, 0, 0), 0, 0));
                    secondRowInfoPanel.add(secondItemCalendarPanel);
                    secondRowLayout.setConstraints(secondItemCalendarPanel,
                            new GridBagConstraints(1, 0, 1, 1, 1.0, 0.0,
                            GridBagConstraints.WEST, GridBagConstraints.NONE,
                            new Insets(0, 6, 0, RIGHT_MARGIN), 0, 0));
                    secondItemCalendarPanel.addChangeListener(new ChangeListener() {
                        public void stateChanged(ChangeEvent ev) {
                            calendarPanelStateChanged();
                        }
                    });
                    setCalendar(calendarPanelStartingDate);
                    this.add(curRowPanel);
                    break;
                case 2:
                    buttonGroup.add(thirdItemRadioButton);
                    curRowPanel.add(thirdItemRadioButton);
                    layout.setConstraints(thirdItemRadioButton,
                            new GridBagConstraints(0, 0, 1, 1, 0.0, 0.0,
                            GridBagConstraints.WEST, GridBagConstraints.NONE,
                            new Insets(0, LEFT_MARGIN, 0, RADIO_BUTTON_RIGHT_MARGIN),
                            0, 0));
                    thirdItemRadioButton.addActionListener(new ActionListener() {
                        public void actionPerformed(ActionEvent ev) {
                            buttonPressed();
                        }
                    });
                    JPanel thirdRowInfoPanel = new JPanel();
                    curRowPanel.add(thirdRowInfoPanel);
                    layout.setConstraints(thirdRowInfoPanel,
                            new GridBagConstraints(1, 0, 1, 1, 1.0, 0.0,
                            GridBagConstraints.WEST, GridBagConstraints.NONE,
                            new Insets(0, 0, 0, 0), 0, 0));
                    GridBagLayout thirdRowLayout = new GridBagLayout();
                    thirdRowInfoPanel.setLayout(thirdRowLayout);
                    thirdItemNumUnitsField = new TextFieldWidget(5);
                    thirdItemNumUnitsField.getDocument().addDocumentListener(
                            numUnitsListener);
                    try {
                        thirdItemNumUnitsField.setValidCharacters("0123456789"); //$NON-NLS-1$
                    } catch (Exception ex) {
                        //Cannot occur
                    }
                    thirdItemNumUnitsField.setHorizontalAlignment(
                            JTextField.RIGHT);
                    thirdItemNumUnitsField.setMinimumSize(
                            thirdItemNumUnitsField.getPreferredSize());
                    thirdRowInfoPanel.add(thirdItemNumUnitsField);
                    thirdRowLayout.setConstraints(thirdItemNumUnitsField,
                            new GridBagConstraints(0, 0, 1, 1, 1.0, 0.0,
                            GridBagConstraints.WEST,
                            GridBagConstraints.NONE,
                            new Insets(0, 0, 0, 0), 0, 0));
                    
                    
                    thirdRowInfoPanel.add(unitsTypeSelector);
                    thirdRowLayout.setConstraints(unitsTypeSelector,
                            new GridBagConstraints(1, 0, 1, 1, 0.0, 0.0,
                            GridBagConstraints.WEST,
                            GridBagConstraints.NONE,
                            new Insets(0, 4, 0, 0), 0, 0));
                    resetUnits();
                    unitsTypeSelector.addActionListener(new ActionListener() {
                        public void actionPerformed(ActionEvent ev) {
                            buttonPressed();
                        }
                    });
                    
                    LabelWidget thirdRowRightLabel = new LabelWidget(
                            thirdItemPhrase);
                    thirdRowInfoPanel.add(thirdRowRightLabel);
                    thirdRowLayout.setConstraints(thirdRowRightLabel,
                            new GridBagConstraints(2, 0, 1, 1, 0.0, 0.0,
                            GridBagConstraints.WEST, GridBagConstraints.NONE,
                            new Insets(0, 2, 0, RIGHT_MARGIN), 0, 0));
                    this.add(curRowPanel);
                    break;
            }
        }
        switch (initiallySelectedButton) {
            case 1:
                firstItemRadioButton.setSelected(true);
                lastButtonSelected = firstItemRadioButton;
                break;
            case 2:
                secondItemRadioButton.setSelected(true);
                lastButtonSelected = secondItemRadioButton;
                break;
            case 3:
                thirdItemRadioButton.setSelected(true);
                lastButtonSelected = thirdItemRadioButton;
                break;
        }
        buttonPressed();
    }

    public void resetUnits() {
        if ((thirdItemNumUnitsField != null) && (UNITS_FIELD_INITIAL_TEXT !=
                null)) {
            thirdItemNumUnitsField.setText(UNITS_FIELD_INITIAL_TEXT);
        }
        if (unitsTypeSelector != null) {
            unitsTypeSelector.setSelectedIndex(UNITS_TYPE_INITIAL_INDEX);
        }
    }

    protected String formatDate(Date date) {
        return format.format(date);
    }

    private void calendarPanelStateChanged() {
        Date calDate = getSecondItemCalendarPanelTime();
        if (calDate == null) {
            setCalendar(calendarPanelStartingDate);
        }
    }

    public Date getSecondItemCalendarPanelTime() {
        return secondItemCalendarPanel.getTimestamp();
    }

    public void setCalendar(Date date) {
        calendarPanelStartingDate = date;
        if (secondItemCalendarPanel != null) {
            secondItemCalendarPanel.setDate(date);
        }
    }
    
    public void pressButton(int buttonIndicator) {
        switch (buttonIndicator) {
            case TimeSpanPanel.SUBPANEL_CONFIGURABLE_INDEX:
                firstItemRadioButton.doClick();
                break;
            case TimeSpanPanel.SPECIFIED_TIME_INDEX:
                secondItemRadioButton.doClick();
                break;
            case TimeSpanPanel.BEFORE_AFTER_INDEX:
                thirdItemRadioButton.doClick();
                break;
        }
    }

    protected void buttonPressed() {
        if (!programmaticButtonSelection) {
            if (firstItemRadioButton.isSelected()) {
                secondItemCalendarPanel.setEnabled(false);
                thirdItemNumUnitsField.setEnabled(false);
                unitsTypeSelector.setEnabled(false);
                buttonSelectionListener.buttonSelectionChanged(this,
                        TimeSpanPanel.SUBPANEL_CONFIGURABLE_INDEX);
            } else if (secondItemRadioButton.isSelected()) {
                secondItemCalendarPanel.setEnabled(true);
                thirdItemNumUnitsField.setEnabled(false);
                unitsTypeSelector.setEnabled(false);
                buttonSelectionListener.buttonSelectionChanged(this,
                        TimeSpanPanel.SPECIFIED_TIME_INDEX);
            } else if (thirdItemRadioButton.isSelected()) {
                secondItemCalendarPanel.setEnabled(false);
                thirdItemNumUnitsField.setEnabled(true);
                unitsTypeSelector.setEnabled(true);
                buttonSelectionListener.buttonSelectionChanged(this,
                        TimeSpanPanel.BEFORE_AFTER_INDEX);
            } else {
                programmaticButtonSelection = true;
                lastButtonSelected.setSelected(true);
                programmaticButtonSelection = false;
            }
        }
    }

    public void setThirdButtonEnabled(boolean flag) {
        thirdItemRadioButton.setEnabled(flag);
    }

    public Integer getThirdItemNumUnits() {
        Integer numUnits = null;
        try {
            numUnits = new Integer(thirdItemNumUnitsField.getText());
        } catch (Exception ex) {
            //Ignore this.  Returning a null means entry is not currently valid.
        }
        return numUnits;
    }

    public boolean unitsChanged() {
        boolean changed = false;
        String unitsText = thirdItemNumUnitsField.getText();
        if (!unitsText.equals(UNITS_FIELD_INITIAL_TEXT)) {
            changed = true;
        } else {
            int unitsIndex = unitsTypeSelector.getSelectedIndex();
            if (unitsIndex != UNITS_TYPE_INITIAL_INDEX) {
                changed = true;
            }
        }
        return changed;
    }
    
    public void addCalendarChangeListener(ChangeListener listener) {
        secondItemCalendarPanel.addChangeListener(listener);
    }
    
    public boolean isMinutesSelected() {
        return unitsTypeSelector.isMinutesSelected();
    }

    public boolean isHoursSelected() {
        return unitsTypeSelector.isHoursSelected();
    }

    public boolean isDaysSelected() {
        return unitsTypeSelector.isDaysSelected();
    }
}//end TimeSpanStartingOrEndingPanel



class UnitsTypeSelector extends JComboBox {
    public final static int MINUTES_LOC = 0;
    public final static int HOURS_LOC = 1;
    public final static int DAYS_LOC = 2;
    private final static String MINUTES = "Minutes"; //$NON-NLS-1$
    private final static String HOURS = "Hours"; //$NON-NLS-1$
    private final static String DAYS = "Days"; //$NON-NLS-1$
    private final static String[] ITEMS = new String[] {MINUTES, HOURS, DAYS};

    public UnitsTypeSelector() {
        super(ITEMS);
        init();
    }

    private void init() {
        this.setSelectedIndex(MINUTES_LOC);
        this.setEditable(false);
    }

    public boolean isMinutesSelected() {
        return (this.getSelectedIndex() == MINUTES_LOC);
    }

    public boolean isHoursSelected() {
        return (this.getSelectedIndex() == HOURS_LOC);
    }

    public boolean isDaysSelected() {
        return (this.getSelectedIndex() == DAYS_LOC);
    }
}//end UnitsTypeSelector




