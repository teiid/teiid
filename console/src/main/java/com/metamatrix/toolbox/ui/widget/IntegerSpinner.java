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

package com.metamatrix.toolbox.ui.widget;

import java.awt.BorderLayout;
import java.awt.Color;
import java.awt.Dimension;
import java.awt.GridLayout;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.awt.event.FocusEvent;
import java.awt.event.FocusListener;
import java.awt.event.KeyAdapter;
import java.awt.event.KeyEvent;
import java.awt.event.MouseAdapter;
import java.awt.event.MouseEvent;
import java.text.ParseException;

import javax.swing.Icon;
import javax.swing.JPanel;
import javax.swing.JTextField;
import javax.swing.SwingConstants;
import javax.swing.Timer;
import javax.swing.event.ChangeEvent;
import javax.swing.event.ChangeListener;
import javax.swing.event.DocumentEvent;
import javax.swing.event.DocumentListener;
import javax.swing.event.EventListenerList;
import javax.swing.text.Document;

import com.metamatrix.toolbox.ui.UIDefaults;
import com.metamatrix.toolbox.ui.UiTextManager;
import com.metamatrix.toolbox.ui.widget.text.DefaultTextFieldModel;

/**
 * The <code>IntegerSpinner</code> is a panel consisting of a
 * <code>JTextField</code> and two buttons. The textfield allows only integer
 * characters to be entered. It defaults to non-editable but can be changed
 * to editable. The two buttons are the "up" arrow and the "down" arrow.
 * The "up" arrow increments the value in the textfield and the "down" arrow
 * decrements the value. An increment value passed in during constructions dictates how 
 * much is incremented or decremented.
 * <p>For an editable textfield, if the value entered is greater than the
 * maximum allowed value, the value will be set to the maximum. And if the
 * value is less than the minimum allowed value, the value will be set to
 * the minimum. If the spinner is wrapping and the next increment would cause
 * the value to be greater than the maximum, the value is set to the minimum.
 * If wrapping is off, the value would be set to the maximum. If wrapping is on
 * and the increment would cause the value to be less than the minimum, the
 * value is set to the maximum. If wrapping is off, the value would be set
 * to the minimum.  IntegerSpinner allows blanks to be set into the spinner
 */
public class IntegerSpinner extends JPanel 
                            implements DocumentListener,
                                       FocusListener {

    ///////////////////////////////////////////////////////////////////////////////////////////////
    // FIELDS
    ///////////////////////////////////////////////////////////////////////////////////////////////

    /** The document used in the textfield. */
    protected Document doc;

    /**
     * Indicates if a document update is in progress. This is needed because a write lock would occur when a setValue was called
     * from outside this class. This causes the document to be updated and a setValue to be called again.
     */
    protected boolean docUpdateInProgress = false;

    /** The maximum allowed value. */
    protected int max;

    /** The minimum allowed value. */
    protected int min;

    /** The amount of each increment or decrement. */
    protected int increment;

    /** Indicates if leading zeroes will be displayed. */
    protected boolean pad;

    /** The current value. */
    protected int value;

    /** The length of a string representation of the maximum value. */
    protected int width;

    /**
     * Indicates if the spinner should wrap from max to min (and vice versa) values.
    */
    protected boolean wrap;

    protected int minWidth = 0;

    private boolean constructed = false;

    /** EventListener list */
    private EventListenerList listeners = new EventListenerList();
    
    protected IntegerSpinnerMouseListener downMouseListener;
    protected IntegerSpinnerMouseListener upMouseListener;

    ///////////////////////////////////////////////////////////////////////////////////////////////
    // CONTROLS
    ///////////////////////////////////////////////////////////////////////////////////////////////

    protected IgnoreButton btnDown;
    protected IgnoreButton btnUp;
    protected JPanel pnlButtons;
    protected TextFieldWidget txf;

    ///////////////////////////////////////////////////////////////////////////////////////////////
    // CONSTRUCTORS
    ///////////////////////////////////////////////////////////////////////////////////////////////

    /**
     * Constructs an <code>IntegerSpinner</code> using the given parameters and sets the initial value to <code>min</code>.
     * @param theMin the minimum value allowed
     * @param theMax the maximum value allowed
     * @param theIncrement the amount of increment
    */
    public IntegerSpinner(int theMin, 
                          int theMax, 
                          int theIncrement) {
        this(theMin, theMax, theIncrement, theMin, true);
    }

    /**
     * Constructs an <code>IntegerSpinner</code> using the given parameters.
     * @param theMin the minimum value allowed
     * @param theMax the maximum value allowed
     * @param theIncrement the amount of increment
     * @param theInitialValue the initial value
     * @param theWrapFlag indicates if wrapping is used
    */
    public IntegerSpinner(int theMin,
                          int theMax, 
                          int theIncrement, 
                          int theInitialValue, 
                          boolean theWrapFlag) {
        super(new BorderLayout());
        min = theMin;
        max = (theMax > theMin) ? theMax : theMin;
        wrap = theWrapFlag;
        increment = (theIncrement > 0) ? theIncrement : 1;
        pad = false;

        setWidth();
        construct();

        setSpinnerToolTipText();
        setValue(theInitialValue);
        txf.setText(format()); //  uses width, pad, min, and value

        upMouseListener = new IntegerSpinnerMouseListener(100, true);
        btnUp.addMouseListener(upMouseListener);
        downMouseListener = new IntegerSpinnerMouseListener(100, false);
        btnDown.addMouseListener(downMouseListener);
        txf.addFocusListener(this);
        constructed = true;
    }

    ///////////////////////////////////////////////////////////////////////////////////////////////
    // METHODS
    ///////////////////////////////////////////////////////////////////////////////////////////////

    public void addChangeListener(ChangeListener theListener) {
        if (listeners == null) {
            listeners = new EventListenerList();
        }

        listeners.add(ChangeListener.class, theListener);
    }

    public void addFocusListener(FocusListener theListener) {
        if (listeners == null) {
            listeners = new EventListenerList();
        }

        listeners.add(FocusListener.class, theListener);
    }

    /**
     * Required by the <code>DocumentListener</code> interface.
     * @param theEvent the event being processed
     * @see javax.swing.event.DocumentListener
     */
    public void changedUpdate(DocumentEvent theEvent) {
        updateValue();
    }

    /**
     * Constructs the GUI.
     */
    private void construct() {
        txf = new TextFieldWidget() {
            public void setToolTipText(String theTip) {
                // the text widget modifies tooltips when it's text changes.
                // don't want this too happen so reset here.
                // want to always show min and max values.
                String tip = UiTextManager.getInstance()
                                          .getText("IntegerSpinner.toolTipText",
                                                   new Object[] {new Integer(min), new Integer(max)});
                super.setToolTipText(tip);
            }
        };
        txf.setMinimumSize(txf.getPreferredSize());
        try {
            txf.setValidCharacters("0-9");
        }
        catch (ParseException theException) {
        }
        txf.setBackground(Color.white);
        setEditable(false);
        txf.setHorizontalAlignment(JTextField.RIGHT);
        txf.addKeyListener(new SpinnerKeyListener());
        txf.setColumns(width + 1); // +1 to eliminate truncation of leftmost digit
        setDocumentWidth();
        add(txf, BorderLayout.CENTER);

        pnlButtons = new JPanel(new GridLayout(2, 1, 0, 1));
        add(pnlButtons, BorderLayout.EAST);

        btnUp = new IgnoreButton(new ArrowIcon(SwingConstants.NORTH, 4));
        btnUp.setPreferredSize(new Dimension(12, 12));
        pnlButtons.add(btnUp);

        btnDown = new IgnoreButton(new ArrowIcon(SwingConstants.SOUTH, 4));
        btnDown.setPreferredSize(new Dimension(12, 12));
        pnlButtons.add(btnDown);
    }

    public boolean decrement() {
        return setValue(value - increment);
    }

    private void fireChangeEvent(ChangeEvent theEvent) {
        Object[] list = listeners.getListenerList();

        for (int i = list.length - 2; i >= 0; i -= 2) {
            Object l = list[i + 1];

            if (l instanceof ChangeListener) {
                ((ChangeListener)l).stateChanged(theEvent);
            }
        }
    }

    private void fireFocusEvent(FocusEvent theEvent) {
        if (!theEvent.isTemporary()) {
            boolean gained = (theEvent.getID() == FocusEvent.FOCUS_GAINED);
            FocusEvent event = new FocusEvent(this, theEvent.getID(), false);
            Object[] list = listeners.getListenerList();

            for (int i = list.length - 2; i >= 0; i -= 2) {
                Object l = list[i + 1];

                if (l instanceof FocusListener) {
                    if (gained) {
                        ((FocusListener)l).focusGained(event);
                    }
                    else {
                        ((FocusListener)l).focusLost(event);
                    }
                }
            }
        }
    }

    public int getMaxWidth() {
        return width;
    }

    /**
     * Increments the current value by the increment value this spinner was constructed with.
     */
    public boolean increment() {
        return setValue(value + increment);
    }

    /**
     * Required by the <code>DocumentListener</code> interface.
     * @param theEvent the event being processed
     * @see javax.swing.event.DocumentListener
     */
    public void insertUpdate(DocumentEvent theEvent) {
        updateValue();
    }

    public boolean isEditable() {
        return txf.isEditable();
    }

    /**
     * Needed by the <code>FocusListener</code> interface.
     */
    public void focusGained(FocusEvent theEvent) {
        fireFocusEvent(theEvent);
    }

    /**
     * Processes the given event by updating the value and the textfield of editable spinners.
     * @param theEvent the event being processed
     */
    public void focusLost(FocusEvent theEvent) {
        if (isEditable()) {
            if (!docUpdateInProgress) {
                String formattedString = format();

                if (!(formattedString.equals(txf.getText()))) {
                    txf.setText(formattedString);
                }
            }
        }

        fireFocusEvent(theEvent);
    }

    /**
    * Gets a formatted string representation if padding is set. Otherwise
    * the <code>toString</code> representation is returned.
    * @return a string representation
    */
    public String format() {
        StringBuffer txt = new StringBuffer(Integer.toString(value));

        if (pad) {
            int offset = (min < 0) ? 1 : 0;

            while (txt.length() < width) {
                txt.insert(offset, "0");
            }
        }

        return txt.toString();
    }

    /**
    * Gets the maximum value.
    * @return the maximum value
    */
    public int getMaxValue() {
        return max;
    }

    /**
    * Gets the minimum value.
    * @return the minimum value
    */
    public int getMinValue() {
        return min;
    }

    /**
    * Gets the preferred size of the spinner. The height is set to the height
    * of the textfield.
    * @return the preferred size of this spinner
    */
    public Dimension getPreferredSize() {
        return new Dimension(txf.getPreferredSize().width + pnlButtons.getPreferredSize().width,
                             txf.getPreferredSize().height);
    }

    public String getTextValue() {
        return txf.getText();
    }
    
    public String getToolTipText() {
        return txf.getToolTipText();
    }

    /**
    * Gets the current value.
    * @return the current value
    */
    public int getValue() {
        return value;
    }

    /**
     * Processes key events where the key pressed is either the up or down arrows. The up arrow increments while the down arrow
     * decrements the value in the textfield.
     * @param theEvent the event being processed
     */
    public void keyListenerKeyPressed(KeyEvent theEvent) {
        if (theEvent.getSource() == txf) {
            switch (theEvent.getKeyCode()) {
                case KeyEvent.VK_DOWN:
                    if (decrement()) {
                        txf.selectAll();
                    }
                    break;
                case KeyEvent.VK_UP:
                    if (increment()) {
                        txf.selectAll();
                    }
                    break;
                default:
                    break;
            }
        }

    }

    public void removeChangeListener(ChangeListener theListener) {
        if (listeners != null) {
            listeners.remove(ChangeListener.class, theListener);
        }
    }

    public void removeFocusListener(FocusListener theListener) {
        if (listeners != null) {
            listeners.remove(FocusListener.class, theListener);
        }
    }

    /**
     * Required by the <code>DocumentListener</code> interface.
     * @param theEvent the event being processed
     * @see javax.swing.event.DocumentListener
     */
    public void removeUpdate(DocumentEvent theEvent) {
        updateValue();
    }

    /**
     * Requests focus to the spinner's textfield.
     */
    public void requestFocus() {
        txf.requestFocus();
    }
    
    public String paramString() {
        return new StringBuffer().append("value=").append(value)
                                 .append(", min=").append(min)
                                 .append(", max=").append(max)
                                 .append(", min width=").append(minWidth)
                                 .append(", width=").append(width)
                                 .append(", increment=").append(increment)
                                 .append(", pad=").append(pad)
                                 .append(", wrap=").append(wrap)
                                 .append(", editable=").append(isEditable())
                                 .append(", docUpdateInProgress=").append(docUpdateInProgress)
                                 .append(", toolTipText=").append(getToolTipText())
                                 .toString();
    }

    private void setDocumentWidth() {
        if (txf != null) {
            DefaultTextFieldModel doc = (DefaultTextFieldModel)txf.getDocument();
            doc.setMaximumLength(width);
        }
    }

    /**
    * Modifies the editable state of the spinner's textfield.
    * @param theEditableFlag sets the editable state of the textfield
    */
    public void setEditable(boolean theEditableFlag) {
        if (txf.isEditable() != theEditableFlag) {
            txf.setEditable(theEditableFlag);

            if (theEditableFlag) {
                txf.addFocusListener(this);
                txf.getDocument().addDocumentListener(this);
                txf.setBackground(UIDefaults.getInstance().getColor("TextField.background"));
            }
            else {
                txf.removeFocusListener(this);
                txf.getDocument().removeDocumentListener(this);
                txf.setBackground(UIDefaults.getInstance().getColor("TextField.uneditableBackgroundColor"));
            }
        }
    }

    /**
     * Sets the enable state.
     * @param theEnableFlag indicates if this spinner should be enabled
     */
    public void setEnabled(boolean theEnableFlag) {
        if (txf.isEnabled() != theEnableFlag) {
            txf.setEnabled(theEnableFlag);
            btnUp.setEnabled(theEnableFlag);
            btnDown.setEnabled(theEnableFlag);
        }
    }

    /**
     * Sets the maximum spinner value to the given value. If the given value is less than the minimum value, the maximum is set
     * equal to the minimum value. If the current spinner value is greater than the new maximum value, the current value is set to
     * the maximum value.
     * @param theNewMax the proposed value for the maximum value
     */
    public void setMaxValue(int theNewMax) {
        max = (theNewMax > min) ? theNewMax : min;

        if (value > max) {
            value = max;
        }

        setWidth();
        setSpinnerToolTipText();
    }

    /**
    * Sets the minimum spinner value to the given value. If the given value
    * is greater than the maximum value, the minimum is set equal to the
    * maximum value. If the current spinner value is less than the new
    * minimum value, the current value is set to the minimum value.
    * @param newMin the proposed value for the minimum value
    */
    public void setMinValue(int newMin) {
        min = (newMin < max) ? newMin : max;

        if (value < min) {
            value = min;
        }

        setWidth();
        setSpinnerToolTipText();
    }

    /**
    * Sets the enable state.
    * @param pad indicates if leading zeros are displayed
    */
    public void setPad(boolean pad) {
        this.pad = pad;
        txf.setText(format());
    }

    /**
    * Set the preferred size of the spinner.
    * @param preferredSize the preferred size
    */
    public void setPreferredSize(Dimension preferredSize) {
        int height = preferredSize.height;
        pnlButtons.setPreferredSize(new Dimension(pnlButtons.getPreferredSize().width, height));
    }

    public void setReadonly(boolean value) {
        txf.setEditable(!value);
        btnUp.setEnabled(!value);
        btnDown.setEnabled(!value);

        if (!value) {
            txf.setBackground(Color.white);
        }
        else {
            txf.setBackground(new Color(204, 204, 204));
        }

    }
    
    private void setSpinnerToolTipText() {
        // see construct method where setToolTipText is overridden in txf
        txf.setToolTipText("");
    }

    /**
     * Sets the current value. If the value is less than the minimum and wrap is on, the value will be set to the maximum. If wrap
     * is off, the value is set to the minimum. If the value is greater than the maximum and wrap is on, the value is set to the
     * minimum. If wrap is off, the value is set to the maximum.
     * @param value the proposed new current value
     * @return <code>true</code> if the value was changed; <code>false</code> otherwise.
     */
    public boolean setValue(int newValue) {
        boolean changed = true;

        if (newValue == value) {
            changed = false;
        }
        else if (newValue > max) {
            if (wrap) {
                if (value != max) {
                    value = max;
                }
                else {
                    changed = false;
                }
            }
            else {
                if (value != max) {
                    value = max;
                }
                else {
                    changed = false;
                }
            }
        }
        else if (newValue < min) {
            if (wrap) {
                if (value != max) {
                    value = max;
                }
                else {
                    changed = false;
                }
            }
            else {
                if (value != min) {
                    value = min;
                }
                else {
                    changed = false;
                }
            }
        }
        else {
            value = newValue;
        }
        
        if (changed) {
            if (!docUpdateInProgress) {
                txf.setText(format());
            }

            if (constructed) {
                fireChangeEvent(new ChangeEvent(this));
            }
        }

        return changed;
    }

    /**
     * Calculates the maximum length of the value based on the max and min values.
     */
    private void setWidth() {
        int newWidth = Math.max((String.valueOf(max)).length(), (String.valueOf(min)).length());

        if (newWidth != width) {
            width = newWidth;
        }

        newWidth = Math.min((String.valueOf(max)).length(), (String.valueOf(min)).length());

        if (newWidth != minWidth) {
            minWidth = newWidth;
        }
        
        setDocumentWidth();
    }

    /**
     * Gets a formatted string representation.
     * @param the string representation
     * @see #format()
     */
    public String toString() {
        return format();
    }

    /**
     * Updates the spinner's current value if needed.
     * @return <code>true</code> if the the current value was updated; <code>false</code> otherwise.
     */
    private boolean updateCurrentValue() {
        boolean update = false;

        try {
            String txtValue = txf.getText();
            int newValue = 0;
            newValue = Integer.parseInt(txtValue);
            update = setValue(newValue);
        }
        catch (NumberFormatException nfe) {
            // should never happen because of document
            // will keep previous value
            update = false;
        }

        return update;
    }

    private void updateValue() {
        if (isEditable()) {
            docUpdateInProgress = true;
            updateCurrentValue();
            docUpdateInProgress = false;
        }
    }

    ///////////////////////////////////////////////////////////////////////////////////////////////
    // IgnoreButton INNER CLASS
    ///////////////////////////////////////////////////////////////////////////////////////////////

    private class IgnoreButton extends ButtonWidget {
        public IgnoreButton(Icon icon) {
            super(icon);
        }
        public boolean isFocusTraversable() {
            return false;
        }
    }

    ///////////////////////////////////////////////////////////////////////////////////////////////
    // IntegerSpinnerMouseListener INNER CLASS
    ///////////////////////////////////////////////////////////////////////////////////////////////

    class IntegerSpinnerMouseListener extends MouseAdapter implements ActionListener {
        protected Timer tm;
        protected boolean directionup;

        public IntegerSpinnerMouseListener(int delay, boolean directionup) {
            tm = new Timer(delay, this);
            tm.setInitialDelay(500);
            this.directionup = directionup;
        }

        public void mousePressed(MouseEvent e) {
            tm.start();
        }

        public void mouseReleased(MouseEvent e) {
            tm.stop();
        }

        public void mouseClicked(MouseEvent e) {
            if (e.getSource() == btnUp) {
                if (btnUp.isEnabled()) {
                    txf.requestFocus();
                    increment();
                }
            }
            else if (e.getSource() == btnDown) {
                if (btnDown.isEnabled()) {
                    txf.requestFocus();
                    decrement();
                }
            }
        }

        public void actionPerformed(ActionEvent e) {
            // only called by Timer
            if (directionup == true) {
                txf.requestFocus();
                increment();
            }
            else {
                txf.requestFocus();
                decrement();
            }
        }
    }

    ///////////////////////////////////////////////////////////////////////////////////////////////
    // SpinnerKeyListener INNER CLASS
    ///////////////////////////////////////////////////////////////////////////////////////////////

    public class SpinnerKeyListener extends KeyAdapter {
        public void keyPressed(KeyEvent event) {
            keyListenerKeyPressed(event);
        }
    }

    ///////////////////////////////////////////////////////////////////////////////////////////////
    // TEST DRIVER
    ///////////////////////////////////////////////////////////////////////////////////////////////

    public static void main(String[] theArgs) {
        javax.swing.JFrame f = new javax.swing.JFrame();
        f.addWindowListener(new java.awt.event.WindowAdapter() {
            public void windowClosing(java.awt.event.WindowEvent theEvent) {
                System.exit(0);
            }
        });

        JPanel p = new JPanel();
        f.getContentPane().add(p);

        IntegerSpinner is = new IntegerSpinner(0, 59, 1);
        is.setEditable(false);
        is.setPad(true);
        p.add(is);

        IntegerSpinner is2 = new IntegerSpinner(0, 10, 1);
        is2.setEditable(true);
        is2.setPad(true);
        p.add(is2);
        
        f.setSize(400, 400);
        f.setVisible(true);
    }

}
