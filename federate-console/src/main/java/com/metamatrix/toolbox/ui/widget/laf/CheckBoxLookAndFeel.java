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

// System imports
import java.awt.Color;
import java.awt.Dimension;
import java.awt.FontMetrics;
import java.awt.Graphics;
import java.awt.Insets;
import java.awt.Rectangle;

import javax.swing.AbstractButton;
import javax.swing.BorderFactory;
import javax.swing.ButtonModel;
import javax.swing.Icon;
import javax.swing.JComponent;
import javax.swing.SwingUtilities;
import javax.swing.UIManager;
import javax.swing.border.Border;
import javax.swing.plaf.ComponentUI;
import javax.swing.plaf.basic.BasicCheckBoxUI;
import javax.swing.plaf.basic.BasicGraphicsUtils;

import com.metamatrix.toolbox.ui.UIDefaults;
import com.metamatrix.toolbox.ui.widget.CheckBox;
import com.metamatrix.toolbox.ui.widget.button.CheckBoxModel;

/**
@since Golden Gate
@version Golden Gate
@author John P. A. Verhaeg
*/
public class CheckBoxLookAndFeel extends BasicCheckBoxUI {
    //############################################################################################################################
    //# Constants                                                                                                                #
    //############################################################################################################################

    private static final CheckBoxLookAndFeel INSTANCE = new CheckBoxLookAndFeel(); 
    private static final Dimension size = new Dimension();
    private static final Rectangle viewRect = new Rectangle();
    private static final Rectangle iconRect = new Rectangle();
    private static final Rectangle textRect = new Rectangle();

    public static final String BOX_BACKGROUND_COLOR_PROPERTY            = CheckBox.PROPERTY_PREFIX + "boxBackgroundColor";
    public static final String BOX_DISABLED_BACKGROUND_COLOR_PROPERTY   = CheckBox.PROPERTY_PREFIX + "boxDisabledBackgroundColor";
    public static final String PARTIALLY_SELECTED_COLOR_PROPERTY        = CheckBox.PROPERTY_PREFIX + "partiallySelectedColor";

    private static final Color DEFAULT_BOX_BACKGROUND_COLOR =
        UIDefaults.getInstance().getColor(BOX_BACKGROUND_COLOR_PROPERTY);
    private static final Color DEFAULT_BOX_DISABLED_BACKGROUND_COLOR =
        UIDefaults.getInstance().getColor(BOX_DISABLED_BACKGROUND_COLOR_PROPERTY);
    
    //############################################################################################################################
    //# Static Variables                                                                                                         #
    //############################################################################################################################

    private static boolean areDfltsInstalled = false;
    private static Color focusColor = null;
    private static Color selectedColor = null;
    private static Color partiallySelectedColor = null;
    
    //############################################################################################################################
    //# Static Methods                                                                                                           #
    //############################################################################################################################
    
    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since Golden Gate
    */
    public static ComponentUI createUI(final JComponent component) {
        return INSTANCE;
    }
    
    //############################################################################################################################
    //# Instance Methods                                                                                                         #
    //############################################################################################################################

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since Golden Gate
    */
    public void installDefaults(final AbstractButton button) {
        super.installDefaults(button);
        if(!areDfltsInstalled) {
            partiallySelectedColor = UIDefaults.getInstance().getColor(PARTIALLY_SELECTED_COLOR_PROPERTY);
            focusColor = UIManager.getColor(getPropertyPrefix() + "focus");
            selectedColor = UIManager.getColor(getPropertyPrefix() + "select");
            areDfltsInstalled = true;
        }
        final CheckBox checkBox = (CheckBox)button;
        checkBox.setBoxBackgroundColor(DEFAULT_BOX_BACKGROUND_COLOR);
        checkBox.setBoxDisabledBackgroundColor(DEFAULT_BOX_DISABLED_BACKGROUND_COLOR);
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since Golden Gate
    */
    public void paint(final Graphics canvas, final JComponent component) {
        canvas.setFont(component.getFont());
        final CheckBox checkBox = (CheckBox)component;
        checkBox.getSize(size);
        viewRect.x = viewRect.y = 0;
        viewRect.width = size.width;
        viewRect.height = size.height;
        iconRect.x = iconRect.y = iconRect.width = iconRect.height = 0;
        textRect.x = textRect.y = textRect.width = textRect.height = 0;
        Icon altIcon = checkBox.getIcon();
        final FontMetrics metrics = canvas.getFontMetrics();
        String text = SwingUtilities.layoutCompoundLabel(component, metrics, checkBox.getText(),
                                                         altIcon != null ? altIcon : getDefaultIcon(),
                                                         checkBox.getVerticalAlignment(), checkBox.getHorizontalAlignment(),
                                                         checkBox.getVerticalTextPosition(), checkBox.getHorizontalTextPosition(),
                                                         viewRect, iconRect, textRect, getDefaultTextIconGap(checkBox));
        // fill background
        canvas.setColor(checkBox.getBackground());
        canvas.fillRect(0, 0, size.width, size.height);
        // Paint the radio checkBox
        final ButtonModel model = checkBox.getModel();
        if (altIcon != null) {
            if (!model.isEnabled()) {
                altIcon = checkBox.getDisabledIcon();
            } else if (model.isPressed()  &&  model.isArmed()) {
                altIcon = checkBox.getPressedIcon();
                if (altIcon == null) {
                    // Use selected icon
                    altIcon = checkBox.getSelectedIcon();
                } 
            } else if (model.isSelected()  ||  (model instanceof CheckBoxModel  &&  
                      ((CheckBoxModel)model).isPartiallySelected())) {
                if (checkBox.isRolloverEnabled()  &&  model.isRollover()) {
                        altIcon = checkBox.getRolloverSelectedIcon();
                        if (altIcon == null) {
                                altIcon = checkBox.getSelectedIcon();
                        }
                } else {
                        altIcon = checkBox.getSelectedIcon();
                }
            } else if (checkBox.isRolloverEnabled()  &&  model.isRollover()) {
                altIcon = checkBox.getRolloverIcon();
            } 
            if (altIcon == null) {
                altIcon = checkBox.getIcon();
            }
            altIcon.paintIcon(component, canvas, iconRect.x, iconRect.y);

        } else {
            if (model.isEnabled()) {
                canvas.setColor(checkBox.getBoxBackgroundColor());
            } else {
                canvas.setColor(checkBox.getBoxDisabledBackgroundColor());
            }
            canvas.fillRect(iconRect.x, iconRect.y, iconRect.width, iconRect.height);
            Border border = checkBox.getBorder();
            if (border == null) {
                border = BorderFactory.createLoweredBevelBorder();
            }
            border.paintBorder(component, canvas, iconRect.x, iconRect.y, iconRect.width, iconRect.height);
            if (checkBox.isPartiallySelected()) {
                // Draw the checkmark
                canvas.translate(iconRect.x, iconRect.y);
                final Insets insets = border.getBorderInsets(component);
                int x1 = iconRect.width / 3;
                int x2 = x1;
                int y1 = iconRect.height / 2;
                int y2 = iconRect.height - insets.bottom - 1;
                if (checkBox.isSelected()) {
                    canvas.setColor(selectedColor);
                } else {
                    canvas.setColor(partiallySelectedColor);
                }
                canvas.drawLine(x1, y1, x2, y2);
                canvas.drawLine(x1 + 1, y1, x2 + 1, y2);
                x1 += 2;
                x2 = iconRect.width - insets.right - 1;
                y1 = y2 - 1;
                y2 = y1 - (x2 - x1);
                canvas.drawLine(x1, y1, x2, y2);
                canvas.drawLine(x1, y1 - 1, x2, y2 - 1);
                canvas.translate(-iconRect.x, -iconRect.y);
            }
        }
        // Draw the Text
        if (text != null) {
            if (model.isEnabled()) {
                canvas.setColor(checkBox.getForeground());
                BasicGraphicsUtils.drawString(canvas,text,model.getMnemonic(), textRect.x, textRect.y + metrics.getAscent());
            } else {
                canvas.setColor(checkBox.getBackground().brighter());
                BasicGraphicsUtils.drawString(canvas,text,model.getMnemonic(), textRect.x + 1,
                                              textRect.y + metrics.getAscent() + 1);
                canvas.setColor(checkBox.getBackground().darker());
                BasicGraphicsUtils.drawString(canvas,text,model.getMnemonic(), textRect.x, textRect.y + metrics.getAscent());
            }
            if (checkBox.hasFocus()  && checkBox.isFocusPainted()  &&  textRect.width > 0  &&  textRect.height > 0) {
                paintFocus(canvas, textRect, size);
            }
        }
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since Golden Gate
    */
    protected void paintFocus(final Graphics canvas, Rectangle textRect, final Dimension size) {
        canvas.setColor(focusColor);
        canvas.drawRect(textRect.x, textRect.y, textRect.width - 1, textRect.height - 1);
    }
}
