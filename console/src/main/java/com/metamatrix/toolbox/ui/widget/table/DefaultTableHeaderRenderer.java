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
package com.metamatrix.toolbox.ui.widget.table;

// System imports
import java.awt.Component;

import javax.swing.ButtonModel;
import javax.swing.JTable;
import javax.swing.table.JTableHeader;
import javax.swing.table.TableCellRenderer;

import com.metamatrix.toolbox.ui.widget.TableWidget;

/**
@since Golden Gate
@version Golden Gate
@author John P. A. Verhaeg
*/
public class DefaultTableHeaderRenderer extends TableHeaderButton
implements TableCellRenderer {
    //############################################################################################################################
    //# Static Variables                                                                                                         #
    //############################################################################################################################

    private static final DefaultTableHeaderRenderer INSTANCE = new DefaultTableHeaderRenderer();

    //############################################################################################################################
    //# Static Methods                                                                                                           #
    //############################################################################################################################

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since Golden Gate
    */
    public static DefaultTableHeaderRenderer getInstance() {
        return INSTANCE;
    }
    
    //############################################################################################################################
    //# Instance Methods                                                                                                         #
    //############################################################################################################################

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    Overridden for performance reasons to do nothing.
    @since Golden Gate
    */
    public void firePropertyChange(final String propertyName, final boolean oldValue, final boolean newValue) {
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    Overridden for performance reasons to prevent any change notifications other than text changes.
    @since Golden Gate
    */
    protected void firePropertyChange(final String propertyName, final Object oldValue, final Object newValue) {  
        if (propertyName == "text") {
            super.firePropertyChange(propertyName, oldValue, newValue);
        }
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since Golden Gate
    */
    public Component getTableCellRendererComponent(final JTable table, final Object value, final boolean isSelected,
                                                   final boolean hasFocus, final int rowIndex, final int columnIndex) {
        // Set button text
        setText(value.toString());
        // Return if table isn't TableWidget
        if (!(table instanceof TableWidget)) {
            return this;
        }
        final TableWidget widget = (TableWidget)table;
        setTableWidget(widget);
        // Set EnhancedTableColumnModel and EnhancedTableColumn
        final EnhancedTableColumnModel colModel = (EnhancedTableColumnModel)widget.getColumnModel();
        setColumnModel(colModel);
        setColumn((EnhancedTableColumn)colModel.getColumn(columnIndex));
        // Return if table isn't sortable
        if (!widget.isSortable()) {
            return this;
        }
        // Return if header isn't TableHeader
        final JTableHeader hdr = widget.getTableHeader();
        if (!(hdr instanceof TableHeader)) {
          return this;
        }
        final TableHeader widgetHdr = (TableHeader)hdr;
        // Set button's armed and pressed state to match header's state for column
        final ButtonModel buttonModel = getModel();
        if (widgetHdr.getArmedColumnIndex() == columnIndex) {
            buttonModel.setArmed(true);
        } else {
            buttonModel.setArmed(false);
        }
        if (widgetHdr.getPressedColumnIndex() == columnIndex) {
            buttonModel.setPressed(true);
        } else {
            buttonModel.setPressed(false);
        }
        return this;
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    Overridden for performance reasons to do nothing.
    @since Golden Gate
    */
    public void repaint(final java.awt.Rectangle bounds) {
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    Overridden for performance reasons to do nothing.
    @since Golden Gate
    */
    public void repaint(final long time, final int x, final int y, final int width, final int height) {
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    Overridden for performance reasons to do nothing.
    @since Golden Gate
    */
    public void revalidate() {
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    Overridden for performance reasons to do nothing.
    @since Golden Gate
    */
    public void validate() {
    }
}
