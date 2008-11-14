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
package com.metamatrix.toolbox.ui.widget.list;

// System imports
import java.awt.Component;
import java.awt.Container;
import java.awt.Rectangle;
import javax.swing.BorderFactory;
import javax.swing.Icon;
import javax.swing.JList;
import javax.swing.JViewport;
import javax.swing.ListCellRenderer;
import javax.swing.UIManager;
import javax.swing.border.Border;

// Application imports
import com.metamatrix.toolbox.ui.widget.LabelWidget;
import com.metamatrix.toolbox.ui.widget.text.TextContainer;

/**
This class is intended to be used everywhere within the application that a tree needs to be displayed.
@since Golden Gate
@version Golden Gate
@author John P. A. Verhaeg
*/
public class DefaultListCellRenderer extends LabelWidget
    implements ListCellRenderer {
    //############################################################################################################################
    //# Static Variables                                                                                                         #
    //############################################################################################################################
    
    protected static final Rectangle BOUNDS = new Rectangle();

    //############################################################################################################################
    //# Instance Variables                                                                                                       #
    //############################################################################################################################
    
//    private javax.swing.DefaultListCellRenderer javaRenderer = new javax.swing.DefaultListCellRenderer();

    private Border focusBorder = null;
    private Border noFocusBorder = null;
    
    //############################################################################################################################
    //# Constructors                                                                                                             #
    //############################################################################################################################

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    public DefaultListCellRenderer() {
        initializeDefaultListCellRenderer();
    }
    
    //############################################################################################################################
    //# Instance Methods                                                                                                         #
    //############################################################################################################################
    
    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since Golden Gate
    */
    public Component getListCellRendererComponent(final JList list, final Object value, final int row, final boolean isSelected,
                                                  final boolean hasFocus) {
        if (value == null) {
            return this;
        }
        setEnabled(list.isEnabled());
        setFont(list.getFont());
        if (hasFocus) {
            setBorder(focusBorder);
        } else {
            setBorder(noFocusBorder);
        }
        if (value instanceof Icon) {
            setIcon((Icon)value);
        } else {
            String text = value.toString();
            setText(text);
            if( text.equals("") || text.equals(" ") ) {
                setToolTipText(null);
            } else if (list instanceof TextContainer  &&  ((TextContainer)list).isClipTipEnabled()  &&  hasFocus) {
                final Rectangle rowBounds = list.getCellBounds(row, row);
                rowBounds.width = list.getInsets().left + getInsets().left + getFontMetrics(getFont()).stringWidth(text);
                final Container parent = list.getParent();
                if (!list.getBounds(BOUNDS).contains(rowBounds)
                    ||  (parent instanceof JViewport  &&  parent.getWidth() < rowBounds.x + rowBounds.width)) {
                    setToolTipText(text);
                } else if (getToolTipText() != null) {
                    setToolTipText(null);
                }
            }
        }
        if (isSelected) {
            setBackground(list.getSelectionBackground());
            setForeground(list.getSelectionForeground());
        } else {
            setBackground(list.getBackground());
            setForeground(list.getForeground());
        }
        return this;
    }
    
    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since Golden Gate
    */
    protected void initializeDefaultListCellRenderer() {
        noFocusBorder = BorderFactory.createEmptyBorder(1, 1, 1, 1);
        focusBorder = UIManager.getBorder("List.focusCellHighlightBorder");
        setOpaque(true);
    }
}
