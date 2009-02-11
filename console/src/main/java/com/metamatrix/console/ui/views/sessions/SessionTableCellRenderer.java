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

package com.metamatrix.console.ui.views.sessions;

import java.awt.Color;
import java.awt.Component;
import java.text.SimpleDateFormat;
import java.util.Date;

import javax.swing.JTable;
import javax.swing.table.*;

import com.metamatrix.console.ui.util.TableCellRendererFactory;
import com.metamatrix.console.util.StaticUtilities;
import com.metamatrix.toolbox.ui.widget.TableWidget;

public class SessionTableCellRenderer extends DefaultTableCellRenderer {
    //Note-- Colors indicated belong were added to the code because original
    //intent was to show sessions from a previous server start in a different
    //background color.  This is no longer an issue, as these sessions are no
    //cleaned up in the Platform.  Leaving code in anyway for possible future
    //use.  BWP 01/31/02
    private final static Color ACCENTUATED_SESSION_UNSELECTED_BACKGROUND;
    private final static Color ACCENTUATED_SESSION_SELECTED_BACKGROUND;

    private final static TableCellRenderer STRING_RENDERER;
    private final static TableCellRenderer LONG_RENDERER;
    private final static TableCellRenderer INTEGER_RENDERER;
    private final static TableCellRenderer DATE_RENDERER;

    static {
        ACCENTUATED_SESSION_UNSELECTED_BACKGROUND =
                //Weight towards white by putting in two occurrences of white.
                StaticUtilities.averageRGBVals(new Color[] {Color.white,
                Color.white, Color.lightGray});
        ACCENTUATED_SESSION_SELECTED_BACKGROUND =
                StaticUtilities.averageRGBVals(new Color[] {Color.lightGray,
                Color.lightGray, Color.white});
    }

    static {
        TableWidget tempTable = new TableWidget();
        STRING_RENDERER = tempTable.getDefaultRenderer(String.class);
        LONG_RENDERER = tempTable.getDefaultRenderer(Long.class);
        INTEGER_RENDERER = tempTable.getDefaultRenderer(Integer.class);

        SimpleDateFormat formatter = StaticUtilities.getDefaultDateFormat();
        DATE_RENDERER = TableCellRendererFactory.createDateRenderer(formatter);
    }

    public SessionTableCellRenderer() {
        super();
    }

    public Component getTableCellRendererComponent(JTable jtable, Object value,
            boolean isSelected, boolean hasFocus, int displayRow, int column) {
        Component comp;
        TableWidget table = (TableWidget)jtable;
        int row = table.convertRowIndexToModel(displayRow);
        if (value instanceof String) {
            comp = STRING_RENDERER.getTableCellRendererComponent(table, value,
                    isSelected, hasFocus, row, column);
        } else if (value instanceof Long) {
            comp = LONG_RENDERER.getTableCellRendererComponent(table, value,
                    isSelected, hasFocus, row, column);
        } else if (value instanceof Integer) {
            comp = INTEGER_RENDERER.getTableCellRendererComponent(table, value,
                    isSelected, hasFocus, row, column);
        } else if (value instanceof Date) {
            comp = DATE_RENDERER.getTableCellRendererComponent(table, value,
                    isSelected, hasFocus, row, column);
        } else {
            comp =  super.getTableCellRendererComponent(table, value,
                    isSelected, hasFocus, row, column);
        }
        if (rowIsForAccentuatedSession(table.getModel(), row)) {
            if (isSelected) {
                comp.setBackground(ACCENTUATED_SESSION_SELECTED_BACKGROUND);
            } else {
                comp.setBackground(ACCENTUATED_SESSION_UNSELECTED_BACKGROUND);
            }
        }
        return comp;
    }

    private boolean rowIsForAccentuatedSession(TableModel model, int row) {
        boolean fromAccentuated = false;
        //For possible future use, insert any useful code here to set fromAccentuated,
        //which will result in different background color used for session.
        return fromAccentuated;
    }
}




