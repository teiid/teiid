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
package com.metamatrix.toolbox.ui.widget.util;

import java.awt.Component;
import java.awt.Dimension;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import javax.swing.JComponent;

/**
@since 2.0
@version 3.0
@author John P. A. Verhaeg
*/
public final class WidgetUtilities {
    //############################################################################################################################
    //# Static Methods                                                                                                           #
    //############################################################################################################################

    /**
     * Sets the minimum, preferred, and maximum sizes of the specified list of Components to the maximum of their widths and
     * heights for each {@link JComponent} in the list.
     * @param buttons A list of Components
     * @since 2.0
     */
    public static void equalizeSizeConstraints(final Component[] components) {
        equalizeSizeConstraints(Arrays.asList(components));
    }

    /**
     * Sets the minimum, preferred, and maximum sizes of the specified list of {@link Component Components} to the maximum of
     * their widths and heights for each {@link JComponent} in the list.
     * @param buttons A list of Components
     * @since 2.0
     */
    public static void equalizeSizeConstraints(final List components) {
        final Dimension maxSize = getMaximumPreferredSize(components, true);
        Component comp;
        JComponent jComp;
        for (final Iterator iter = components.iterator();  iter.hasNext();) {
            comp = (Component)iter.next();
            if (comp instanceof JComponent) {
                jComp = (JComponent)comp;
	            jComp.setPreferredSize(maxSize);
	            jComp.setMinimumSize(maxSize);
	            jComp.setMaximumSize(maxSize);
            }
        }
    }
    
    /**
     * A convenience method that calls {@link #getMaximumPreferredSize(components, reset)} passing false as the reset parameter.
     * @since 3.0
     */
    public static Dimension getMaximumPreferredSize(final Component[] components) {
        return getMaximumPreferredSize(components, false);
    }
    
    /**
     * Returns a Dimension representing the maximum preferred width and height of the Components within the specified list.
     * @param components The list of Components
     * @param reset      Indicates whether the preferred size of each {@link JComponent} should be unset if currently set
     * @return A Dimension representing the maximum preferred width and height of the Components
     * @since 3.0
     */
    public static Dimension getMaximumPreferredSize(final Component[] components, final boolean reset) {
        return getMaximumPreferredSize(Arrays.asList(components), reset);
    }
    
    /**
     * A convenience method that calls {@link #getMaximumPreferredSize(components, reset)} passing false as the reset parameter.
     * @since 3.0
     */
    public static Dimension getMaximumPreferredSize(final List components) {
        return getMaximumPreferredSize(components, false);
    }
    
    /**
     * Returns a Dimension representing the maximum preferred width and height of the {@link Component Components} within the
     * specified list.
     * @param components The list of Components
     * @param reset      Indicates whether the preferred size of each JComponent should be unset if currently set
     * @return A Dimension representing the maximum preferred width and height of the Components
     * @since 3.0
     */
    public static Dimension getMaximumPreferredSize(final List components, final boolean reset) {
        final Dimension maxSize = new Dimension();
        Component comp;
        JComponent jComp;
        Dimension size;
        for (final Iterator iter = components.iterator();  iter.hasNext();) {
            comp = (Component)iter.next();
            if (comp instanceof JComponent) {
                jComp = (JComponent)comp;
                if (reset  &&  jComp.isPreferredSizeSet()) {
                    jComp.setPreferredSize(null);
                }
            }
            size = comp.getPreferredSize();
            maxSize.width = Math.max(maxSize.width, size.width);
            maxSize.height = Math.max(maxSize.height, size.height);
        }
        return maxSize;
    }
}
