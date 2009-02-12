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
package com.metamatrix.console.ui.util;

import java.awt.Component;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Hashtable;

import javax.swing.JTable;
import javax.swing.table.TableCellRenderer;

import com.metamatrix.toolbox.ui.widget.table.DefaultTableCellRenderer;

/**
 * The <code>TableCellRendererFactory</code> provides table cell renderers.
 * @author  dflorian
 * @since Golden Gate
 * @version 1.0
 */
public class TableCellRendererFactory {

    ///////////////////////////////////////////////////////////////////////////
    // FIELDS
    ///////////////////////////////////////////////////////////////////////////

    /** Caches the renderers. */
    private static Hashtable rendererMap = new Hashtable();

    ///////////////////////////////////////////////////////////////////////////
    // CONSTRUCTORS
    ///////////////////////////////////////////////////////////////////////////

    /** Don't allow no arg construction. */
    private TableCellRendererFactory() {}

    /////////////////////////////////////////////////////////////////
    // METHODS
    /////////////////////////////////////////////////////////////////

    /**
     * Creates a renderer based on the format pattern of the given parameter.
     * @param theFormatter the object used to format dates
     * @return the date renderer
     */
    public static TableCellRenderer createDateRenderer(
        SimpleDateFormat theFormatter) {

        TableCellRenderer renderer =
            (TableCellRenderer)rendererMap.get(theFormatter.toPattern());
        if (renderer == null) {
            renderer = new DateRenderer(theFormatter);
            rendererMap.put(theFormatter.toPattern(), renderer);
        }
        return renderer;
    }

    /////////////////////////////////////////////////////////////////
    // INNER CLASSES
    /////////////////////////////////////////////////////////////////

    /**
     * The <code>DateRenderer</code> class is used to render date objects.
     */
    private static class DateRenderer
        extends DefaultTableCellRenderer {

        private SimpleDateFormat formatter;

        public DateRenderer(SimpleDateFormat theFormatter) {
            formatter = theFormatter;
        }

        public Component getTableCellRendererComponent(
            JTable theTable,
            Object theValue,
            boolean theSelectedFlag,
            boolean theHasFocusFlag,
            int theRow,
            int theColumn) {

            Object value = theValue;
            if (theValue instanceof Date) {
                value = formatter.format((Date)value);
            }
            return super.getTableCellRendererComponent(
                       theTable,
                       value,
                       theSelectedFlag,
                       theHasFocusFlag,
                       theRow,
                       theColumn);
        }
    }

}

