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
package com.metamatrix.toolbox.ui.widget.property;

// JDK imports
import java.awt.Component;

import javax.swing.JTable;

import com.metamatrix.toolbox.ui.widget.LabelWidget;
import com.metamatrix.toolbox.ui.widget.table.DefaultTableCellRenderer;

/**
@since 2.1
@version 2.1
@author <a href="mailto:jverhaeg@metamatrix.com">John P. A. Verhaeg</a>
*/
public class PropertiedObjectTableCellRenderer extends DefaultTableCellRenderer {
    //############################################################################################################################
    //# Instance Methods                                                                                                         #
    //############################################################################################################################
    
    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since 2.1
    */
    public Component getTableCellRendererComponent(final JTable table, Object value, final boolean isSelected,
                                                   final boolean hasFocus, final int rowIndex, final int columnIndex) {
        final Component comp = super.getTableCellRendererComponent(table, value, isSelected, hasFocus, rowIndex, columnIndex);
        if (value != null  &&  value.getClass().isArray()) {
            final Object[] vals = (Object[])value;
            if (vals.length > 0) {
                final StringBuffer text = new StringBuffer(vals[0].toString());
                for (int ndx = 1;  ndx < vals.length;  ++ndx) {
                    text.append("; ");
                    text.append(vals[ndx]);
                }
                ((LabelWidget)comp).setText(text.toString());
            } else {
                ((LabelWidget)comp).setText("");
            }
        }
        return comp;
    }
}
