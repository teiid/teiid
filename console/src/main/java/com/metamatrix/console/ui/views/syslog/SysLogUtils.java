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

//#############################################################################
package com.metamatrix.console.ui.views.syslog;

import java.awt.Dimension;
import java.util.ArrayList;

import javax.swing.Icon;
import javax.swing.JTable;
import javax.swing.ListSelectionModel;

import com.metamatrix.console.ui.util.property.GuiComponentFactory;
import com.metamatrix.console.ui.util.property.PropertyProvider;

import com.metamatrix.toolbox.ui.widget.TableWidget;
import com.metamatrix.toolbox.ui.widget.table.DefaultTableModel;

/**
 * @version 1.0
 * @author Dan Florian
 */
public final class SysLogUtils {

    ///////////////////////////////////////////////////////////////////////////
    // CONSTANTS
    ///////////////////////////////////////////////////////////////////////////

    public static final String PROPS =
        "com/metamatrix/console/ui/views/syslog/data/ui";

    ///////////////////////////////////////////////////////////////////////////
    // FIELDS
    ///////////////////////////////////////////////////////////////////////////

    private static PropertyProvider propProvider;

    ///////////////////////////////////////////////////////////////////////////
    // INITIALIZER
    ///////////////////////////////////////////////////////////////////////////

    static {
        ArrayList propFiles = new ArrayList();
        propFiles.add(PROPS);
        propFiles.add(PropertyProvider.COMMON_PROP);
        propFiles.add(GuiComponentFactory.TYPE_DEFS_PROP);
        propProvider = new PropertyProvider(propFiles);
    }

    ///////////////////////////////////////////////////////////////////////////
    // CONSTRUCTORS
    ///////////////////////////////////////////////////////////////////////////

    /** Don't allow no arg construction.*/
    private SysLogUtils() {}

    ///////////////////////////////////////////////////////////////////////////
    // METHODS
    ///////////////////////////////////////////////////////////////////////////

    public static boolean getBoolean(String theKey) {
        return propProvider.getBoolean(theKey);
    }

    public static Icon getIcon(String theKey) {
        return propProvider.getIcon(theKey);
    }

    public static int getInt(
        String theKey,
        int theDefault) {

        return propProvider.getInt(theKey, theDefault);
    }

    public static Object getObject(String theKey) {
        return propProvider.getObject(theKey);
    }

    public static int getMnemonic(String theKey) {
        String key = propProvider.getString(theKey, true);
        return (key == null) ? 0 : (int)key.charAt(0);
    }

    public static String getString(String theKey) {
        return propProvider.getString(theKey);
    }

    public static String getString(
        String theKey,
        boolean theReturnNullFlag) {

        return propProvider.getString(theKey, theReturnNullFlag);
    }

    public static String getString(
        String theKey,
        Object[] theArgs) {

        return propProvider.getString(theKey, theArgs);
    }

    /**
     * Compares to strings with <code>null</code> and the empty string
     * being equal.
     * @param theOne the first string being compared
     * @param theOther the second string being compared
     * @return <code>true</code> if both strings are equivalent
     */
    public static boolean equivalent(
        String theOne,
        String theOther) {

        boolean result = true;
        if ((theOne == null) || (theOne.length() == 0)) {
            if ((theOther != null) && (theOther.length() > 0)) {
                result = false;
            }
        }
        else {
            if ((theOther == null) || (theOther.length() == 0)) {
                result = false;
            }
            else {
                result = theOne.equals(theOther);
            }
        }
        return result;
    }

    public static DefaultTableModel setup(
        TableWidget theTable,
        String[] theHeaders,
        int theVisibleRows,
        final int[] theEditableColumns) {

        DefaultTableModel model = (DefaultTableModel)theTable.getModel();
        model.setColumnIdentifiers(theHeaders);
        theTable.setEditable(false);
        if (theEditableColumns != null) {
            for (int i=0;
                 i<theEditableColumns.length;
                 theTable.setColumnEditable(theEditableColumns[i++], true)) {
                
            }
        }
        theTable.setPreferredScrollableViewportSize(
            new Dimension(theTable.getPreferredSize().width,
                          theVisibleRows*theTable.getRowHeight()));
        theTable.setSelectionMode(ListSelectionModel.SINGLE_SELECTION);
        theTable.setAutoResizeMode(JTable.AUTO_RESIZE_ALL_COLUMNS);
        theTable.setSortable(true);
        return model;
    }

}
