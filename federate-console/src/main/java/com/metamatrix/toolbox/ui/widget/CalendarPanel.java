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

import java.awt.BorderLayout;
import java.awt.Color;
import java.awt.FlowLayout;
import java.awt.Font;
import java.awt.Frame;
import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.awt.Insets;
import java.awt.Point;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.awt.event.FocusAdapter;
import java.awt.event.FocusEvent;
import java.awt.event.FocusListener;
import java.awt.event.MouseAdapter;
import java.awt.event.MouseEvent;
import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Locale;

import javax.swing.JPanel;
import javax.swing.JWindow;
import javax.swing.SwingConstants;
import javax.swing.SwingUtilities;
import javax.swing.UIManager;
import javax.swing.border.CompoundBorder;
import javax.swing.border.EmptyBorder;
import javax.swing.border.EtchedBorder;
import javax.swing.event.ChangeEvent;
import javax.swing.event.ChangeListener;
import javax.swing.event.EventListenerList;

public final class CalendarPanel 
	extends JPanel 
	implements ActionListener {

    /////////////////////////////////////////////////////////////////
    // FIELDS
    /////////////////////////////////////////////////////////////////

    public static final String ID_TIMESTAMP_BOTH_FMT = "timestamp.both.fmt";
    public static final String ID_TIMESTAMP_NO_DATE_FMT = "timestamp.no_date.fmt";
    public static final String ID_TIMESTAMP_NO_TIME_FMT = "timestamp.no_time.fmt";

    private static final int ARROW_SIZE = 8;
    private SimpleDateFormat _calendarFormatter;
    private boolean calendarShowing = false;
    private Calendar _date = null;
    private SimpleDateFormat _dayFormatter;
    private EventListenerList listeners;
    private SimpleDateFormat _monthFormatter;
    private boolean _showCal = true;
    private boolean _showTime = true;
//    private String _timeFormatId = ID_TIMESTAMP_BOTH_FMT;
//    private boolean valid = false;
    private SimpleDateFormat _specialFormatter = new SimpleDateFormat("MMMddyyyy");
    private Color selectedColor = Color.white;
    private Color originalColor;
    private boolean _readOnly = false;
    private boolean _showToday = true;

    private boolean bSkipFocusLost;
    private ListenerMouse listenerMouse = new ListenerMouse();
    private MyFocusListener focusListener;
    private Frame owner;

    /////////////////////////////////////////////////////////////////
    // CONTROLS
    /////////////////////////////////////////////////////////////////

    private ButtonWidget btnCalendar;
    private ButtonWidget btnClear;
    private CalendarWindow calWindow;
    private ArrowIcon iconUp;
    private ArrowIcon iconDown;
    private TextFieldWidget txfDate;

    /////////////////////////////////////////////////////////////////
    // CONSTRUCTORS
    /////////////////////////////////////////////////////////////////
    public CalendarPanel(Frame owner) {
        this(owner, true, true);
    }
    
    public CalendarPanel(Frame owner, boolean theShowCalFlag, boolean theShowTimeFlag) {
        super(new FlowLayout(FlowLayout.LEFT, 0, 0));
        this.owner = owner;
        
        txfDate = new TextFieldWidget();
        txfDate.setEditable(false);
        txfDate.setBackground(Color.white);
        txfDate.setMinimumSize(txfDate.getPreferredSize());
        add(txfDate);
        iconUp = new ArrowIcon(SwingConstants.NORTH, ARROW_SIZE);
        iconDown = new ArrowIcon(SwingConstants.SOUTH, ARROW_SIZE);
        btnCalendar = new ButtonWidget(iconDown);
        Insets insets = new Insets(2, 2, 2, 2);
        btnCalendar.setMargin(insets);
        btnCalendar.addMouseListener(listenerMouse);
        add(btnCalendar);
        btnClear = new ButtonWidget(new ArrowIcon(-1, ARROW_SIZE));
        btnClear.addActionListener(this);
        btnClear.setMargin(insets);
        add(btnClear);
        setShowCalendar(theShowCalFlag);
        setShowTime(theShowTimeFlag);
        setReadOnly(_readOnly);
        setShowToday(_showToday);

		focusListener = new MyFocusListener();
        btnCalendar.addFocusListener(focusListener);
        btnClear.addFocusListener(focusListener);
        txfDate.addFocusListener(focusListener);
    }

    /////////////////////////////////////////////////////////////////
    // METHODS
    /////////////////////////////////////////////////////////////////

	public void requestFocus() {
	    btnCalendar.requestFocus();
	}
	
    public void addFocusListener(FocusListener theListener) {
        if (listeners == null) {
            listeners = new EventListenerList();
        }
        listeners.add(FocusListener.class, theListener);
    }

    public void removeFocusListener(FocusListener theListener) {
        if (listeners != null) {
        	listeners.remove(FocusListener.class, theListener);
        }
    }
    
    class MyFocusListener implements FocusListener {

        public void focusGained(FocusEvent theEvent) {
            focusLost(theEvent);
        }
    
        public void focusLost(FocusEvent theEvent) {
            Object[] list = listeners.getListenerList();
            for (int i = list.length - 2; i >= 0; i -= 2) {
                Object l = list[i + 1];
                if (l instanceof FocusListener) {
                    if (theEvent.getID() == FocusEvent.FOCUS_GAINED) {
                    	((FocusListener)l).focusGained(theEvent);
                    }
                    else {
                    	((FocusListener)l).focusLost(theEvent);
                    }
                }
            }
        }
    }

    public void reset() {
        actionClear();
    }

    public void setEnabled(boolean bEnabled) {
        super.setEnabled(bEnabled);
        btnCalendar.setEnabled(bEnabled);
        btnCalendar.removeMouseListener(listenerMouse);
        btnClear.setEnabled(bEnabled);
        txfDate.setEnabled(bEnabled);

        if (bEnabled)
            btnCalendar.addMouseListener(listenerMouse);
        else
            btnCalendar.removeMouseListener(listenerMouse);
    }

    // ReadOnly property
    public void setReadOnly(boolean readOnly) {
        setEnabled(!readOnly);
        _readOnly = readOnly;
    }
    public boolean isReadOnly() {
        return _readOnly;
    }

    // ShowCalendar property
    public void setShowCalendar(boolean showCal) {
        _showCal = showCal;
        setFormatId();
        setDateColumns();
    }
    public boolean isShowCalendar() {
        return _showCal;
    }

    // ShowTime property
    public void setShowTime(boolean showTime) {
        _showTime = showTime;
        setFormatId();
        setDateColumns();
    }
    public boolean isShowTime() {
        return _showTime;
    }

    // ShowToday property
    public void setShowToday(boolean showToday) {
        _showToday = showToday;
        /*
            if( _showToday)
            {
              Calendar calendar = Calendar.getInstance();
              calendar.setTime( new java.util.Date());
              _date = calendar;
              txfDate.setText( getCalendarFormatter().format(_date.getTime()));
            }
            else
            {
              _date = null;
              txfDate.setText( "");
            }
        */
    }
    public boolean isShowToday() {
        return _showToday;
    }

    private void actionClear() {
        if (_date != null) {
            _date = null;
            txfDate.setText("");
            fireChangeEvent(new ChangeEvent(this));
        }
        calendarShowing = true;
        processCalendarWindow();
    }

    public void actionPerformed(ActionEvent theEvent) {
        Object source = theEvent.getSource();
        if (source == btnClear) {
            actionClear();
        }
        else if (source == getCalWindow()) {
            if (!bSkipFocusLost) {
                calendarShowing = true;
                processCalendarWindow();

                if (theEvent.getID() == CalendarWindow.SELECTED_DAY) {
                    _date = getCalWindow().getCalendar();
                    txfDate.setText(getCalendarFormatter().format(_date.getTime()));
                    fireChangeEvent(new ChangeEvent(this));
                }
                else if (theEvent.getID() == CalendarWindow.SELECTED_TIME) {
                    _date = getCalWindow().getCalendar();
                    txfDate.setText(getCalendarFormatter().format(_date.getTime()));
                    fireChangeEvent(new ChangeEvent(this));
                }
            }
        }
    }

    public void addChangeListener(ChangeListener theListener) {
        if (listeners == null) {
            listeners = new EventListenerList();
        }
        listeners.add(ChangeListener.class, theListener);
    }

    private void fireChangeEvent(ChangeEvent theEvent) {
        Object[] list = listeners.getListenerList();
        for (int i = list.length - 2; i >= 0; i -= 2) {
            Object l = list[i + 1];
            if (l instanceof ChangeListener) {
                ((ChangeListener) l).stateChanged(theEvent);
            }
        }
    }

    public void setDate(java.util.Date theDate) {
        if (theDate == null) {
            setCalendar(null);
        }
        else {
            Calendar calendar = Calendar.getInstance();
            calendar.setTime(theDate);
            setCalendar(calendar);
        }
    }

    public void setCalendar(Calendar theDate) {
        _date = theDate;
        txfDate.setText((_date == null) ? null : getCalendarFormatter().format(_date.getTime()));
    }

    public Calendar getCalendar() {
        return _date;
    }

    public DateFormat getFormatter() {
        return getCalendarFormatter();
    }

    public Timestamp getTimestamp() {
        if (_date == null) {
            return null;
        }
        return new Timestamp(_date.getTime().getTime());
    }

    private CalendarWindow getCalWindow() {
        if (calWindow == null) {
            calWindow = new CalendarWindow(owner);
            calWindow.setShowCal(_showCal);
            calWindow.setShowTime(_showTime);
            calWindow.addActionListener(this);
            calWindow.pack();
        }
        return calWindow;
    }

    private void initFormatters(Locale theLocale) {
        _dayFormatter = new SimpleDateFormat("EE", theLocale);
        _monthFormatter = new SimpleDateFormat("MMM", theLocale);
        String format = null;
        if (_showCal && _showTime) { // Show both calendar and time
            format = "MMM dd, yyyy HH:mm";
        }
        else if (_showCal && !_showTime) { // Show calendar only
            format = "MMM dd, yyyy";
        }
        else if (!_showCal && _showTime) { // Show time only
            format = "HH:mm";
        }
        _calendarFormatter = new SimpleDateFormat(format, theLocale);
    }

    private SimpleDateFormat getCalendarFormatter() {
        if (_calendarFormatter == null) {
            initFormatters(Locale.getDefault());
        }
        return _calendarFormatter;
    }

    private SimpleDateFormat getDayFormatter() {
        if (_dayFormatter == null) {
            initFormatters(Locale.getDefault());
        }
        return _dayFormatter;
    }

    private SimpleDateFormat getMonthFormatter() {
        if (_monthFormatter == null) {
            initFormatters(Locale.getDefault());
        }
        return _monthFormatter;
    }

    private void setDateColumns() {
        if (_showCal && _showTime)
            txfDate.setColumns(20);
        else if (_showCal && !_showTime)
            txfDate.setColumns(12);
        else if (!_showCal && _showTime)
            txfDate.setColumns(8);
        else
            txfDate.setColumns(0);
    }

    public boolean isDateValid() {
        boolean valid = false;
        try {
            valid = (getCalendarFormatter().parse(txfDate.getText()) != null);
        }
        catch (ParseException pe) {
            valid = false;
        }
        return valid;
    }

    private void processCalendarWindow() {
        if (!calendarShowing) {
            Point p = this.getLocationOnScreen();
            p.y += this.getSize().height;
            getCalWindow().setLocation(p);
            getCalWindow().setCalendar(_date);
            if (!getCalWindow().isVisible()) {
                getCalWindow().setVisible(true);
                getCalWindow().requestFocus();
                getCalWindow().toFront();
            }
        }
        else {
            if (getCalWindow().isVisible()) {
                getCalWindow().setVisible(false);
            }
        }
        calendarShowing = !calendarShowing;
        btnCalendar.setIcon((calendarShowing) ? iconUp : iconDown);
    }

    public void removeChangeListener(ChangeListener theListener) {
        listeners.remove(ChangeListener.class, theListener);
    }

    private void setFormatId() {
        // Determines the Format ID based on _showCal & _showTime
        if (_showCal && !_showTime) {
//            _timeFormatId = ID_TIMESTAMP_NO_TIME_FMT;
        }
        else if (!_showCal && _showTime) {
//            _timeFormatId = ID_TIMESTAMP_NO_DATE_FMT;
        }
    }

    public String getToolTipText() {
        return txfDate.getToolTipText();
    }

    public void setToolTipText(String theText) {
        txfDate.setToolTipText(theText);
    }

    /////////////////////////////////////////////////////////////////
    // INNER CLASSES
    /////////////////////////////////////////////////////////////////

    final class ListenerMouse extends MouseAdapter {
        public void mousePressed(MouseEvent e) {
            if (e.getSource() == btnCalendar)
                bSkipFocusLost = true;
        }
        public void mouseReleased(MouseEvent e) {
            if (e.getSource() == btnCalendar) {
                processCalendarWindow();
                bSkipFocusLost = false;
            }
        }
    }

    private final class CalendarWindow extends JWindow implements ActionListener {
        //
        // constants
        //
        private final Insets INSETS = new Insets(2, 2, 2, 2);
        public static final int SELECTED_DAY = 0;
        public static final int LOST_FOCUS = 1;
        public static final int SELECTED_TIME = 2;

        //
        // fields
        //
        private Calendar _date;
        private Calendar _selectedDate;
        private ButtonWidget btnMonthDec;
        private ButtonWidget btnMonthInc;
        private ButtonWidget btnYearDec;
        private ButtonWidget btnYearInc;
        private IntegerSpinner isHours;
        private IntegerSpinner isMinutes;
        private JPanel pnlDate;
        private JPanel pnlTime;
        private LabelWidget lblMonth;
        private LabelWidget lblYear;
        private JPanel pnlDays;
        private ButtonWidget[] dayButtons = new ButtonWidget[31];
        private ButtonWidget btnSetTime;
        private ListenerFocus fl = new ListenerFocus();

        //
        // constructors
        //
        public CalendarWindow(Frame owner) {
            this(owner, null);
        }
        
        private CalendarWindow(Frame owner, Calendar theDate) {
            super(owner);
            this.addFocusListener(fl);

            // build time panel
            pnlTime = new JPanel();
            pnlTime.setBorder(new EtchedBorder());
            LabelWidget lblHours = new LabelWidget("Hours:");
            lblHours.setName("cp.lblHours");
            pnlTime.add(lblHours);
            isHours = new IntegerSpinner(0, 23, 1);
            isHours.addFocusListener(fl);
            isHours.setEditable(true);
            isHours.setPad(true);
            pnlTime.add(isHours);
            LabelWidget lblMinutes = new LabelWidget("Minutes:");
            lblMinutes.setName("cp.lblMinutes");
            pnlTime.add(lblMinutes);
            isMinutes = new IntegerSpinner(0, 59, 1);
            isMinutes.addFocusListener(fl);
            isMinutes.setEditable(true);
            isMinutes.setPad(true);
            pnlTime.add(isMinutes);
            btnSetTime = new ButtonWidget();
            btnSetTime.setIcon(iconUp);
            btnSetTime.setMargin(new Insets(0, 0, 0, 0));
            btnSetTime.addActionListener(this);
            pnlTime.add(btnSetTime);
            getContentPane().add(pnlTime, BorderLayout.SOUTH);
            // build date panel
            pnlDate = new JPanel(new GridBagLayout());
            pnlDate.setBorder(new EtchedBorder());
            getContentPane().add(pnlDate, BorderLayout.CENTER);
            _date = (theDate != null) ? (Calendar) theDate.clone() : Calendar.getInstance();
            _selectedDate = (theDate == null) ? null : (Calendar) theDate.clone();
            isHours.setValue(_date.get(Calendar.HOUR_OF_DAY));
            isMinutes.setValue(_date.get(Calendar.MINUTE));
            setBackground(pnlDate.getBackground()); // window background was different
            buildUI();
        }
        //
        // methods
        //
        public void actionPerformed(final ActionEvent theEvent) {
            Object source = theEvent.getSource();
            if (source == btnMonthDec) {
                _date.add(Calendar.MONTH, -1);
                redrawPanel();
            }
            else if (source == btnMonthInc) {
                _date.add(Calendar.MONTH, 1);
                redrawPanel();
            }
            else if (source == btnYearDec) {
                _date.add(Calendar.YEAR, -1);
                redrawPanel();
            }
            else if (source == btnYearInc) {
                _date.add(Calendar.YEAR, 1);
                redrawPanel();
            }
            else if (source == btnSetTime) {
                if (_selectedDate == null) {
                    _selectedDate = (Calendar) _date.clone();
                }
                setVisible(false);
                fireActionEvent(new ActionEvent(this, SELECTED_TIME, "time selected"));
            }
            else {
                if (_selectedDate == null) {
                    _selectedDate = (Calendar) _date.clone();
                }
                _selectedDate.set(Calendar.DAY_OF_MONTH, Integer.valueOf(theEvent.getActionCommand()).intValue());
                _selectedDate.set(Calendar.MONTH, _date.get(Calendar.MONTH));
                _selectedDate.set(Calendar.YEAR, _date.get(Calendar.YEAR));
                setVisible(false);
                fireActionEvent(new ActionEvent(this, SELECTED_DAY, "day selected"));
            }
        }

        public void addActionListener(ActionListener theListener) {
            listeners.add(ActionListener.class, theListener);
        }

        public void removeActionListener(ActionListener theListener) {
            listeners.remove(ActionListener.class, theListener);
        }

        private JPanel buildHeaderPanel() {
            lblMonth = new LabelWidget(getMonthFormatter().format(_date.getTime()));
            lblYear = new LabelWidget(Integer.toString(_date.get(Calendar.YEAR)));
            GridBagConstraints gbc = new GridBagConstraints();
            JPanel headerPanel = new JPanel(new GridBagLayout());

            // month label and buttons
            JPanel panel = new JPanel(new GridBagLayout());

            btnMonthDec = new ButtonWidget();
            btnMonthDec.addFocusListener(fl);
            btnMonthDec.setIcon(new ArrowIcon(SwingConstants.WEST, ARROW_SIZE));
            btnMonthDec.setMargin(INSETS);
            btnMonthDec.addActionListener(this);
            gbc.anchor = GridBagConstraints.EAST;
            gbc.insets = INSETS;
            panel.add(btnMonthDec, gbc);

            gbc.anchor = GridBagConstraints.CENTER;
            panel.add(lblMonth, gbc);

            btnMonthInc = new ButtonWidget();
            btnMonthInc.addFocusListener(fl);
            btnMonthInc.setIcon(new ArrowIcon(SwingConstants.EAST, ARROW_SIZE));
            btnMonthInc.setMargin(INSETS);
            btnMonthInc.addActionListener(this);
            gbc.anchor = GridBagConstraints.WEST;
            panel.add(btnMonthInc, gbc);

            gbc.anchor = GridBagConstraints.CENTER;
            gbc.weightx = 1;
            headerPanel.add(panel, gbc);

            // year label and buttons
            panel = new JPanel(new GridBagLayout());

            btnYearDec = new ButtonWidget();
            btnYearDec.addFocusListener(fl);
            btnYearDec.setIcon(new ArrowIcon(SwingConstants.WEST, ARROW_SIZE));
            btnYearDec.setMargin(INSETS);
            btnYearDec.addActionListener(this);
            gbc.anchor = GridBagConstraints.EAST;
            gbc.weightx = 0;
            panel.add(btnYearDec, gbc);

            gbc.anchor = GridBagConstraints.CENTER;
            panel.add(lblYear, gbc);

            btnYearInc = new ButtonWidget();
            btnYearInc.addFocusListener(fl);
            btnYearInc.setIcon(new ArrowIcon(SwingConstants.EAST, ARROW_SIZE));
            btnYearInc.setMargin(INSETS);
            btnYearInc.addActionListener(this);
            gbc.anchor = GridBagConstraints.WEST;
            panel.add(btnYearInc, gbc);

            gbc.anchor = GridBagConstraints.CENTER;
            gbc.weightx = 1;
            headerPanel.add(panel, gbc);

            return headerPanel;
        }

        private void buildUI() {
            for (int i = 0; i < dayButtons.length; i++) {
                dayButtons[i] = new ButtonWidget(Integer.toString(i + 1));
                dayButtons[i].setMargin(INSETS);
                dayButtons[i].addActionListener(this);
            }
            originalColor = dayButtons[0].getBackground();
            GridBagConstraints gbc = new GridBagConstraints();
            gbc.gridx = 0;
            gbc.gridy = 0;
            pnlDate.add(buildHeaderPanel(), gbc);
            pnlDays = new JPanel(new GridBagLayout());
            pnlDays.setBorder(new CompoundBorder(pnlDays.getBorder(), new EmptyBorder(10, 10, 0, 10)));

            // create days
            redrawPanel();
            // add days
            gbc.gridx = 0;
            gbc.gridy++;
            pnlDate.add(pnlDays, gbc);
        }

        private void fireActionEvent(ActionEvent theEvent) {
            Object[] list = listeners.getListenerList();
            for (int i = list.length - 2; i >= 0; i -= 2) {
                Object l = list[i + 1];
                if (l instanceof ActionListener) {
                    ((ActionListener) l).actionPerformed(theEvent);
                }
            }
        }

        private void lostFocus() {
            setHourMinute();
            fireActionEvent(new ActionEvent(this, LOST_FOCUS, "lost focus"));
        }

        private void redrawPanel() {
            pnlDays.removeAll();

            // draw day of week headers
            GridBagConstraints gbc = new GridBagConstraints();
            gbc.fill = GridBagConstraints.BOTH;
            gbc.gridx = 0;
            gbc.gridy = 0;
            gbc.insets = new Insets(1, 1, 1, 1);

            Calendar date = Calendar.getInstance();
            int firstDay = date.getFirstDayOfWeek();
            date.set(Calendar.DAY_OF_WEEK, firstDay);
            for (int i = 0; i < 7; i++, gbc.gridx++) {
                LabelWidget lbl = new LabelWidget(getDayFormatter().format(date.getTime()));
                lbl.setHorizontalAlignment(SwingConstants.CENTER);
                pnlDays.add(lbl, gbc);
                date.set(Calendar.DAY_OF_WEEK, date.get(Calendar.DAY_OF_WEEK) + 1);
            }

            // Draw the days
            lblMonth.setText(getMonthFormatter().format(_date.getTime()));
            lblYear.setText(Integer.toString(_date.get(Calendar.YEAR)));

            firstDay = Calendar.getInstance().getFirstDayOfWeek();
            gbc.gridx = 0;
            gbc.gridy++;

            // days of month
            Calendar today = Calendar.getInstance();
            today.setTime(new java.util.Date());
            Calendar cellDay = (Calendar) today.clone();
            int month = _date.get(Calendar.MONTH);
            int year = _date.get(Calendar.YEAR);

            // start with first of panels month
            cellDay.set(year, month, 1);
            gbc.gridx = cellDay.get(Calendar.DAY_OF_WEEK) - firstDay;

            Font fntBtn = dayButtons[0].getFont();
            Font fntNorm = new Font(fntBtn.getName(), Font.PLAIN, fntBtn.getSize());
            Font fntToday = new Font(fntBtn.getName(), Font.BOLD, fntBtn.getSize());
            while (cellDay.get(Calendar.MONTH) == month) {
                ButtonWidget btn = dayButtons[cellDay.get(Calendar.DAY_OF_MONTH) - 1];
                if (gbc.gridx > 6) {
                    gbc.gridy++;
                    gbc.gridx = 0;
                }
                if (today.equals(cellDay)) {
                    btn.setFont(fntToday);
                }
                else {
                    btn.setFont(fntNorm);
                }

                if ((_selectedDate != null)
                    && (_specialFormatter.format(cellDay.getTime()).equals(_specialFormatter.format(_selectedDate.getTime())))) {
                    btn.setBackground(selectedColor);
                }
                else {
                    if (btn.getBackground().equals(selectedColor)) {
                        btn.setBackground(originalColor);
                    }
                }
                pnlDays.add(btn, gbc);
                gbc.gridx++;
                cellDay.add(Calendar.DAY_OF_MONTH, 1);
            }
            // add space at bottom if needed
            for (; gbc.gridy < 6;) {
                gbc.gridx = 0;
                gbc.gridy++;
                gbc.insets = new Insets(6, 0, 6, 0);
                pnlDays.add(new LabelWidget(" "), gbc);
            }
            repaint();
        }

        private void setCalendar(Calendar theDate) {
            if (theDate == null) {
                if (_selectedDate != null) {
                    dayButtons[_selectedDate.get(Calendar.DAY_OF_MONTH) - 1].setBackground((Color) UIManager.getDefaults().get("Button.background"));
                }
                _selectedDate = null;
            }
            else {
                _date = (Calendar) theDate.clone();
            }
            redrawPanel();
        }
        private Calendar getCalendar() {
            setHourMinute();
            return _selectedDate;
        }

        private boolean setHourMinute() {
            boolean changed = false;
            if (_selectedDate != null) {
                int hour = isHours.getValue();
                int minute = isMinutes.getValue();
                if (_selectedDate.get(Calendar.HOUR_OF_DAY) != hour) {
                    changed = true;
                    _selectedDate.set(Calendar.HOUR_OF_DAY, hour);
                }
                if (_selectedDate.get(Calendar.MINUTE) != minute) {
                    changed = true;
                    _selectedDate.set(Calendar.MINUTE, minute);
                }
                if (changed) {
                }
            }
            return changed;
        }

        private void setShowCal(boolean theShowCalFlag) {
            if (pnlDate.isVisible() != theShowCalFlag) {
                pnlDate.setVisible(theShowCalFlag);
            }
        }

        private void setShowTime(boolean theShowTimeFlag) {
            if (pnlTime.isVisible() != theShowTimeFlag) {
                pnlTime.setVisible(theShowTimeFlag);
            }
        }

        final class ListenerFocus extends FocusAdapter {
            public void focusLost(FocusEvent e) {
                //        if( e.getSource() == CalendarWindow.this && e.isTemporary())
                if (e.isTemporary()) {
                    Runnable doit = new Runnable() {
                        public void run() {
                            lostFocus();
                        }
                    };
                    SwingUtilities.invokeLater(doit);
                }
            }
        }
    }
}
