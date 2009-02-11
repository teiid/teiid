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

import javax.swing.AbstractButton;
import javax.swing.JComponent;
import javax.swing.plaf.ComponentUI;

import com.metamatrix.toolbox.ui.UIDefaults;
import com.metamatrix.toolbox.ui.widget.TableWidget;
import com.metamatrix.toolbox.ui.widget.table.EnhancedTableColumn;
import com.metamatrix.toolbox.ui.widget.table.EnhancedTableColumnModel;
import com.metamatrix.toolbox.ui.widget.table.TableHeaderButton;

/**
Sub-classes BasicButtonUI to provide multiple-line text.
@since 2.0
@version 2.0
@author John P. A. Verhaeg
*/
public class TableHeaderButtonLookAndFeel extends ButtonLookAndFeel {
    //############################################################################################################################
    //# Constants                                                                                                                #
    //############################################################################################################################

    private static final TableHeaderButtonLookAndFeel INSTANCE = new TableHeaderButtonLookAndFeel(); 

    public static final String SORT_INDICATOR_COLOR = TableWidget.PROPERTY_PREFIX + "headerButtonSortIndicatorColor";
    public static final String SORT_INDICATOR_GAP   = TableWidget.PROPERTY_PREFIX + "headerButtonSortIndicatorGap";
    
    //############################################################################################################################
    //# Static Variables                                                                                                         #
    //############################################################################################################################
    
    private static Color sortColor = null;
    private static int sortGap = 0;
    private static int sortWth = 0;
    private static FontMetrics metrics = null;
    
    //############################################################################################################################
    //# Static Methods                                                                                                           #
    //############################################################################################################################
    
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
    public Dimension getMinimumSize(final JComponent component) {
        final TableHeaderButton button = (TableHeaderButton)component;
        if (button.getTableWidget().allowsMultipleColumnSorting()) {
            return getPreferredSize(component);
        }
        return super.getPreferredSize(component);
    }
    
    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since 2.0
    */
    public Dimension getPreferredSize(final JComponent component) {
        final Dimension size = super.getPreferredSize(component);
        metrics = component.getFontMetrics(component.getFont());
        sortWth = metrics.charWidth('4') * 2;
        size.width += (sortWth + sortGap) * 2;
        return size;
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since 2.0
    */
    public void installDefaults(final AbstractButton button) {
        if (!areDefaultsInstalled()) {
            final UIDefaults dflts = UIDefaults.getInstance();
            sortColor = dflts.getColor(SORT_INDICATOR_COLOR);
            sortGap = dflts.getInt(SORT_INDICATOR_GAP);
        }
        super.installDefaults(button);
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    Currently assumes icon is to the left of the text.
    @since 2.0
    */
    public void paint(final Graphics canvas, final JComponent component) {
        // Determine preferred width before call to super.paint since both manipulate static BOUNDS variables used later
        final TableHeaderButton button = (TableHeaderButton)component;
        final int prefWth = button.getPreferredSize().width;
        // Paint text
        super.paint(canvas, component);
        // Return if not sorted
        final EnhancedTableColumn col = button.getColumn();
        if (!col.isSorted()  ||  button.getWidth() < prefWth) {
            return;
        }
        // Paint row sort order arrow
        canvas.setColor(sortColor);
        int wth = sortWth;
        // Ensure width is odd
        if (wth % 2 == 0) {
            --wth;
        }
        int hgt = (wth + 1) / 2;
        final int shift = getTextShiftOffset();
        int x = LookAndFeelUtilities.VIEW_BOUNDS.x;
        x += (LookAndFeelUtilities.ICON_BOUNDS.x - shift - x - wth) / 2 + shift;
        int y = LookAndFeelUtilities.VIEW_BOUNDS.y + (LookAndFeelUtilities.VIEW_BOUNDS.height - hgt) / 2 + shift;
        if (!col.isSortedAscending()) {
            y += hgt - 1;
        }
        while (wth >= 0) {
            canvas.drawLine(x, y, x + wth, y);
            if (col.isSortedAscending()) {
                ++y;
            } else {
                --y;
            }
            ++x;
            wth -= 2;
        }
        // Return if only sorted column
        final EnhancedTableColumnModel colModel = button.getColumnModel();
        if (colModel.getSortedColumnCount() == 1) {
            return;
        }
        // Paint column sort priority number
        final int priority = col.getSortPriority();
        if (priority <= 0) {
            return;
        }
        final String text = String.valueOf(priority);
        // TEXT_BOUNDS.x is always set even when no icon is present
        x = LookAndFeelUtilities.TEXT_BOUNDS.x + LookAndFeelUtilities.TEXT_BOUNDS.width + sortGap - shift;
        x += (LookAndFeelUtilities.VIEW_BOUNDS.x + LookAndFeelUtilities.VIEW_BOUNDS.width - x - metrics.stringWidth(text)) / 2 + shift;
        y = LookAndFeelUtilities.VIEW_BOUNDS.y + (LookAndFeelUtilities.VIEW_BOUNDS.height - metrics.getHeight()) / 2 + metrics.getAscent() + shift;
        canvas.drawString(text, x, y);
    }
}
