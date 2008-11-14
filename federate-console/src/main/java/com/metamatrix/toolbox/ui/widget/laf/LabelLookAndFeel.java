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

import java.awt.Dimension;
import java.awt.Font;
import java.awt.Graphics;

import javax.swing.Icon;
import javax.swing.JComponent;
import javax.swing.JLabel;
import javax.swing.plaf.ComponentUI;
import javax.swing.plaf.metal.MetalLabelUI;

import com.metamatrix.toolbox.ui.widget.LabelWidget;

/**
 * Sub-classes BasicLabelUI to provide multiple-line text.
 * @since 2.0
 * @version 3.0
 * @author <a href="mailto:jverhaeg@metamatrix.com">John P. A. Verhaeg</a>
 */
public class LabelLookAndFeel extends MetalLabelUI {
    //############################################################################################################################
    //# Constants                                                                                                                #
    //############################################################################################################################

    private static final LabelLookAndFeel INSTANCE = new LabelLookAndFeel();
    
    //############################################################################################################################
    //# Static Variables                                                                                                         #
    //############################################################################################################################

    private static boolean areDfltsInstalled = false;
    
    //############################################################################################################################
    //# Static Methods                                                                                                           #
    //############################################################################################################################
    
    /**
     * @since 2.0
     */
    public static ComponentUI createUI(final JComponent component) {
        return INSTANCE;
    }
    
    //############################################################################################################################
    //# Instance Methods                                                                                                         #
    //############################################################################################################################
    
    /**
     * @since 2.0
     */
    public Dimension getPreferredSize(final JComponent component) {
        final LabelWidget label = (LabelWidget)component;
        String text = label.getTextFormat();
        final Icon icon = label.getIcon();
        if ((text == null  ||  text.length() == 0)  &&  icon == null) {
            text = " ";
        }
        LookAndFeelUtilities.clearViewBounds();
        LookAndFeelUtilities.layoutText(text, icon, label.getIconTextGap(), label.getVerticalAlignment(),
                                        label.getHorizontalAlignment(), label.getVerticalTextPosition(),
                                        label.getHorizontalTextPosition(), label.getFontMetrics(label.getFont()));
        final Dimension size = LookAndFeelUtilities.getPreferredSize(component);
        if (label.getFont().getStyle() == Font.ITALIC) {
            size.width += 5;
        }
        return size;
    }

    /**
     * @since 2.0
     */
    protected void installDefaults(final JLabel label) {
        if (!areDfltsInstalled) {
            areDfltsInstalled = true;
        }
        super.installDefaults(label);
    }

    /**
     * @since 2.0
     */
    public void paint(final Graphics graphic, final JComponent component) {
        final LabelWidget label = (LabelWidget)component;
        String text = label.getTextFormat();
        final Icon icon = (label.isEnabled()) ? label.getIcon() : label.getDisabledIcon();
        if ((text == null  ||  text.length() == 0)  &&  icon == null) {
            return;
        }
        LookAndFeelUtilities.setViewBounds(component);
        // layout the text and icon
        text = LookAndFeelUtilities.layoutText(text, icon, label.getIconTextGap(), label.getVerticalAlignment(),
                                               label.getHorizontalAlignment(), label.getVerticalTextPosition(),
                                               label.getHorizontalTextPosition(), graphic.getFontMetrics());
        // Paint
        graphic.setFont(component.getFont());
        if (icon != null) {
            icon.paintIcon(component, graphic, LookAndFeelUtilities.ICON_BOUNDS.x, LookAndFeelUtilities.ICON_BOUNDS.y);
        }
        if (text.length() > 0) {
            LookAndFeelUtilities.paintText(graphic, component, text, ((JLabel)component).getDisplayedMnemonic());
        }
    }
}
