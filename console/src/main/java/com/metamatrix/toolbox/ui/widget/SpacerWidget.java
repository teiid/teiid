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
package com.metamatrix.toolbox.ui.widget;

import java.awt.Dimension;

import javax.swing.JComponent;
import javax.swing.UIManager;

import com.metamatrix.toolbox.ui.UIConstants;
import com.metamatrix.toolbox.ui.UIDefaults;

/**
 * An invisible component intended for use as spacer within layouts.
 * @since 2.0
 * @version 2.0
 * @author <a href="mailto:jverhaeg@metamatrix.com">John P. A. Verhaeg</a>
 */
public class SpacerWidget extends JComponent
implements UIConstants {
    //############################################################################################################################
    //# Constants                                                                                                                #
    //############################################################################################################################

    public static final int DEFAULT_SPACER_WIDTH = UIDefaults.getInstance().getInt(SPACER_HORIZONTAL_LENGTH_PROPERTY);
    public static final int DEFAULT_SPACER_HEIGHT
        = new SpacerWidget().getFontMetrics(UIDefaults.getInstance().getFont("normalFont")).getHeight();
        
    public static final Dimension ZERO_SIZE = new Dimension();
    public static final Dimension INFINITE_SIZE = new Dimension(Short.MAX_VALUE, Short.MAX_VALUE);

    public static final int CHOICE_INDENT = UIManager.getIcon("CheckBox.icon").getIconWidth()
                                               + UIManager.getInt("Button.textIconGap");

    //############################################################################################################################
    //# Static Methods                                                                                                           #
    //############################################################################################################################

    /**
     * Creates an invisible component with no height that can expand horizontally.
     * @return The invisible component
     * @since 2.0
     */
    public static SpacerWidget createHorizontalExpandableSpacer() {
        return new SpacerWidget(ZERO_SIZE, ZERO_SIZE, new Dimension(Short.MAX_VALUE, 0));
    }

    /**
     * Creates an invisible component with no height and a default fixed width.
     * @return The invisible component
     * @since 2.0
     */
    public static SpacerWidget createHorizontalSpacer() {
        return new SpacerWidget(new Dimension(DEFAULT_SPACER_WIDTH, 0));
    }

    /**
     * Creates an invisible component with no height and the specified fixed width.
     * @return The invisible component
     * @since 2.0
     */
    public static SpacerWidget createHorizontalSpacer(final int width) {
        return new SpacerWidget(new Dimension(width, 0));
    }

    /**
     * Creates an invisible component with no width that can expand vertically.
     * @return The invisible component
     * @since 2.0
     */
    public static SpacerWidget createVerticalExpandableSpacer() {
        return new SpacerWidget(ZERO_SIZE, ZERO_SIZE, new Dimension(0, Short.MAX_VALUE));
    }

    /**
     * Creates an invisible component with no width and a default fixed height (The height of the current font).
     * @return The invisible component
     * @since 2.0
     */
    public static SpacerWidget createVerticalSpacer() {
        return new SpacerWidget(new Dimension(0, DEFAULT_SPACER_HEIGHT));
    }

    /**
     * Creates an invisible component with no width and the specified fixed height.
     * @return The invisible component
     * @since 2.0
     */
    public static SpacerWidget createVerticalSpacer(final int height) {
        return new SpacerWidget(new Dimension(0, height));
    }

    //############################################################################################################################
    //# Constructors                                                                                                             #
    //############################################################################################################################

    /**
     * Creates an invisible component with zero size for all constraints.
     * @since 2.0
     */
    public SpacerWidget() {
        this(ZERO_SIZE, ZERO_SIZE, ZERO_SIZE);
    }

    /**
     * Creates an invisible component with the specified size for all constraints.
     * @param size The component's minimum, preferred, and maximum size
     * @since 2.0
     */
    public SpacerWidget(final Dimension size) {
        this(size, size, size);
    }

    /**
     * Creates an invisible component with the specified size constraints.
     * @param minimumSize The component's minimum size
     * @param preferredSize The component's preferred size
     * @param maximumSize The component's maximum size
     * @since 2.0
     */
    public SpacerWidget(final Dimension minimumSize, final Dimension preferredSize, final Dimension maximumSize) {
        setMinimumSize(minimumSize);
        setPreferredSize(preferredSize);
        setMaximumSize(maximumSize);
    }
}
