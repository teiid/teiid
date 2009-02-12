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

//################################################################################################################################
package com.metamatrix.toolbox.ui.widget;

import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.awt.event.KeyEvent;

import javax.swing.Action;
import javax.swing.Icon;
import javax.swing.JButton;
import javax.swing.JToolTip;
import javax.swing.KeyStroke;

import com.metamatrix.toolbox.ui.TextUtilities;
import com.metamatrix.toolbox.ui.widget.event.WidgetActionEvent;
import com.metamatrix.toolbox.ui.widget.laf.ButtonLookAndFeel;

/**
 * This class is intended to be used everywhere within the application that a button needs to be displayed.
 * @since 2.0
 * @version 2.0
 * @author <a href="mailto:jverhaeg@metamatrix.com">John P. A. Verhaeg</a>
 */
public class ButtonWidget extends JButton {
    //############################################################################################################################
    //# Constants                                                                                                                #
    //############################################################################################################################
    
    // Property key prefixes
    public static final String PROPERTY_PREFIX = "Button.";

    // Property key suffixes
    public static final String ICON_PROPERTY_SUFFIX     = "Icon";
    public static final String MNEMONIC_PROPERTY_SUFFIX = "Mnemonic";
    
    // Property keys
    public static final String MARGIN_PROPERTY           = PROPERTY_PREFIX + "margin";
    public static final String BORDER_PROPERTY           = PROPERTY_PREFIX + "border";
    public static final String PRESSED_BORDER_PROPERTY   = PROPERTY_PREFIX + "pressedBorder";
    
    //############################################################################################################################
    //# Instance Methods                                                                                                         #
    //############################################################################################################################
    
    private int iconTextGap = 0;
    private int pressedShift = 0;
    private String textFmt; // Initialized by superclass
    private boolean focusTraversable = true;
    
    //############################################################################################################################
    //# Constructors                                                                                                             #
    //############################################################################################################################

    /**
     * Creates a blank button.
     * @since 2.0
     */
    public ButtonWidget() {
        this(null, null);
    }
    
    /**
     * Creates a button with the specified text.
     * @param text The text
     * @since 2.0
     */
    public ButtonWidget(final String text) {
        this(text, null);
    }
    
    /**
     * Creates a button with the specified icon.
     * @param icon The icon
     * @since 2.0
     */
    public ButtonWidget(final Icon icon) {
        this(null, icon);
    }

    /**
     * Creates a button with the specified text and icon.  The button is by default laid out with the icon to the left of the
     * text.
     * @param text The text
     * @param icon The icon
     * @since 2.0
     */
    public ButtonWidget(final String text, final Icon icon) {
        super(text, icon);
        initializeButtonWidget();
    }

    /**
     * Creates a button with the specified text and icon.  The button is by default laid out with the icon to the left of the
     * text.
     * @param text The text
     * @param icon The icon
     * @since 2.0
     */
    public ButtonWidget(Action a) {
        super(a);
        initializeButtonWidget();
    }

    //############################################################################################################################
    //# Instance Methods                                                                                                         #
    //############################################################################################################################

    public JToolTip createToolTip() {
        JToolTip tip = new MultiLineToolTip();
        tip.setComponent(this);
        return tip;
    }

    /**
     * Fires a WidgetActionEvent instead of an ActionEvent to allow it to be marked as processed by listeners.
     * @since 2.0
     */
    protected void fireActionPerformed(final ActionEvent event) {
        final Object[] listeners = listenerList.getListenerList();
        ActionEvent newEvent = null;
        for (int ndx = listeners.length - 2;  ndx >= 0;  ndx -= 2) {
            if (listeners[ndx] == ActionListener.class) {
                if (newEvent == null) {
                      String cmd = event.getActionCommand();
                      if(cmd == null) {
                         cmd = getActionCommand();
                      }
                      newEvent = new WidgetActionEvent(ButtonWidget.this, cmd, event.getModifiers());
                }
                ((ActionListener)listeners[ndx + 1]).actionPerformed(newEvent);
            }          
        }
    }

    /**
     * @since 2.0
     */
    public int getIconTextGap() {
        return iconTextGap;
    }

    /**
     * @since 2.0
     */
    public int getPressedShift() {
        return pressedShift;
    }

    /**
     * @return The text format
     * @since 2.0
     */
    public String getTextFormat() {
        return textFmt;
    }

    /**
     * Registers the <Enter> key as an 'action' key (i.e., it performs the same function as the <Space> key).
     * @since 2.0
     */
    protected void initializeButtonWidget() {
        registerKeyboardAction(getActionForKeyStroke(KeyStroke.getKeyStroke(KeyEvent.VK_SPACE, 0, false)), getActionCommand(), KeyStroke.getKeyStroke(KeyEvent.VK_ENTER, 0, false), WHEN_FOCUSED);
        registerKeyboardAction(getActionForKeyStroke(KeyStroke.getKeyStroke(KeyEvent.VK_SPACE, 0, true)), getActionCommand(), KeyStroke.getKeyStroke(KeyEvent.VK_ENTER, 0, true), WHEN_FOCUSED);
    }
    
    /**
     * @since 2.0
     */
    public void setIconTextGap(final int iconTextGap) {
        this.iconTextGap = iconTextGap;
        ((ButtonLookAndFeel)getUI()).setIconTextGap(iconTextGap);
    }

    /**
     * @since 2.0
     */
    public void setPressedShift(final int pressedShift) {
        this.pressedShift = pressedShift;
        ((ButtonLookAndFeel)getUI()).setPressedShift(pressedShift);
    }

    /**
     * @since 2.0
     */
    public void setText(final String text) {
        textFmt = text;
        super.setText(TextUtilities.getUnformattedText(text));
    }

    /**
     * @return The text
     * @since 2.0
     */
    public String toString() {
        return getText();
    }

    /**
     * @since 2.0
     */
    public void updateUI() {
        setUI(ButtonLookAndFeel.createUI(this));
    }
    
    /**
     * @since 3.1
     */
    public void setFocusTraversable(boolean flag) {
        focusTraversable = flag;
    }

	/**
     * @since 3.1
	 * @see java.awt.Component#isFocusTraversable()
	 */
	public boolean isFocusTraversable() {
		return focusTraversable;
	}

}
