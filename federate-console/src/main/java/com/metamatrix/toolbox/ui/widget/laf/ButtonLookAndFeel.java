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

//################################################################################################################################
package com.metamatrix.toolbox.ui.widget.laf;

// JDK imports
import java.awt.Dimension;
import java.awt.Graphics;
import java.awt.Insets;
import java.awt.Rectangle;

import javax.swing.AbstractButton;
import javax.swing.BorderFactory;
import javax.swing.ButtonModel;
import javax.swing.JComponent;
import javax.swing.UIManager;
import javax.swing.border.Border;
import javax.swing.plaf.ComponentUI;
import javax.swing.plaf.metal.MetalButtonUI;

import com.metamatrix.toolbox.ui.UIDefaults;
import com.metamatrix.toolbox.ui.widget.ButtonWidget;

/**
Sub-classes MetalButtonUI to provide multiple-line text.
@since 2.0
@version 2.0
@author John P. A. Verhaeg
*/
public class ButtonLookAndFeel extends MetalButtonUI {
    //############################################################################################################################
    //# Constants                                                                                                                #
    //############################################################################################################################

    private static final ButtonLookAndFeel INSTANCE = new ButtonLookAndFeel(); 
    
    //############################################################################################################################
    //# Static Variables                                                                                                         #
    //############################################################################################################################
    
    private static boolean areDfltsInstalled = false;
    private static Border border = null;
    private static Border pressedBorder = null;
    
    //############################################################################################################################
    //# Static Methods                                                                                                           #
    //############################################################################################################################
    
    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since 2.0
    */
    public static boolean areDefaultsInstalled() {
        return areDfltsInstalled;
    }
    
    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since 2.0
    */
    public static ComponentUI createUI(final JComponent component) {
        return INSTANCE;
    }
    
    //############################################################################################################################
    //# Instance Methods                                                                                                         #
    //############################################################################################################################
    
    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since 2.0
    */
    public Dimension getPreferredSize(final JComponent component) {
        final ButtonWidget button = (ButtonWidget)component;
        LookAndFeelUtilities.clearViewBounds();
        LookAndFeelUtilities.layoutText(button.getTextFormat(), button.getIcon(), defaultTextIconGap,
                                        button.getVerticalAlignment(), button.getHorizontalAlignment(),
                                        button.getVerticalTextPosition(), button.getHorizontalTextPosition(),
                                        button.getFontMetrics(button.getFont()));
        final Dimension size = LookAndFeelUtilities.getPreferredSize(component);
        // Adjust size by enough room to draw focus rectangle (+ 2)
        size.width += 2;
        size.height += 2;
        // Ensure size is odd to ensure focus rectangle doesn't clip
        if (size.width % 2 == 0) {
            size.width += 1;
        }
        if (size.height % 2 == 0) {
            size.height += 1;
        }
        return size;
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since 2.0
    */
    public void installDefaults(final AbstractButton button) {
        if (!areDfltsInstalled) {
            final UIDefaults dflts = UIDefaults.getInstance();
            final Insets margin = dflts.getInsets(ButtonWidget.MARGIN_PROPERTY);
            final Border marginBorder = BorderFactory.createEmptyBorder(margin.top, margin.left, margin.bottom, margin.right);
            border = BorderFactory.createCompoundBorder(dflts.getBorder(ButtonWidget.BORDER_PROPERTY), marginBorder);
            UIManager.getDefaults().put(ButtonWidget.BORDER_PROPERTY, border);
            pressedBorder = BorderFactory.createCompoundBorder(dflts.getBorder(ButtonWidget.PRESSED_BORDER_PROPERTY),
                                                               marginBorder);
            areDfltsInstalled = true;
        }
        super.installDefaults(button);
        final ButtonWidget buttonWidget = (ButtonWidget)button;
        buttonWidget.setIconTextGap(defaultTextIconGap);
        buttonWidget.setPressedShift(defaultTextShiftOffset);
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since 2.0
    */
    public void paint(final Graphics graphic, final JComponent component) {
        // layout the text and icon
        final ButtonWidget button = (ButtonWidget)component;
        LookAndFeelUtilities.setViewBounds(component);
        String text = LookAndFeelUtilities.layoutText(button.getTextFormat(), button.getIcon(), defaultTextIconGap,
                                                      button.getVerticalAlignment(), button.getHorizontalAlignment(),
                                                      button.getVerticalTextPosition(), button.getHorizontalTextPosition(),
                                                      graphic.getFontMetrics());
        // Paint
        graphic.setFont(component.getFont());
        final ButtonModel model = button.getModel();
        if (model.isArmed()  &&  model.isPressed()) {
            paintButtonPressed(graphic, button); 
        } else {
            clearTextShiftOffset();
            button.setBorder(border);
        }
        if (button.getIcon() != null) { 
            paintIcon(graphic, component, LookAndFeelUtilities.ICON_BOUNDS);
        }
        final int shift = getTextShiftOffset();
        LookAndFeelUtilities.TEXT_BOUNDS.x += shift;
        LookAndFeelUtilities.TEXT_BOUNDS.y += shift;
        LookAndFeelUtilities.ICON_BOUNDS.x += shift;
        LookAndFeelUtilities.ICON_BOUNDS.y += shift;
        if (text.length() > 0) {
            paintText(graphic, component, LookAndFeelUtilities.TEXT_BOUNDS, text);
        }
        if (button.isFocusPainted()  &&  button.hasFocus()) {
            paintFocus(graphic, button, LookAndFeelUtilities.VIEW_BOUNDS, LookAndFeelUtilities.TEXT_BOUNDS,
                       LookAndFeelUtilities.ICON_BOUNDS);
        }
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since 2.0
    */
    protected void paintButtonPressed(final Graphics graphic, final AbstractButton button){
        setTextShiftOffset();
        button.setBorder(pressedBorder);
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since 2.0
    */
    protected void paintText(final Graphics graphic, final JComponent component, final Rectangle TEXT_BOUNDS, final String text) {
        LookAndFeelUtilities.paintText(graphic, component, text, ((AbstractButton)component).getMnemonic());
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since 2.0
    */
    public void setIconTextGap(final int iconTextGap) {
        defaultTextIconGap = iconTextGap;
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since 2.0
    */
    public void setPressedShift(final int pressedShift) {
        defaultTextShiftOffset = pressedShift;
    }
}
