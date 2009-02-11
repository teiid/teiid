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

// System imports
import java.awt.Component;
import java.awt.Point;
import java.awt.Rectangle;
import java.awt.event.MouseAdapter;
import java.awt.event.MouseEvent;
import java.util.Collections;
import java.util.List;
import java.util.Vector;

import javax.swing.JComponent;
import javax.swing.JList;
import javax.swing.JPopupMenu;
import javax.swing.ListCellRenderer;
import javax.swing.ListModel;
import javax.swing.SwingUtilities;
import javax.swing.ToolTipManager;

import com.metamatrix.toolbox.ui.widget.list.DefaultListCellRenderer;
import com.metamatrix.toolbox.ui.widget.menu.DefaultPopupMenuFactory;
import com.metamatrix.toolbox.ui.widget.text.TextContainer;

/**
* This class is intended to be used everywhere within the application that a list needs to be displayed.
* @since 2.0
* @version 2.1
* @author John P. A. Verhaeg
*/
public class ListWidget extends JList
implements TextContainer {
    //############################################################################################################################
    //# Instance Variables                                                                                                       #
    //############################################################################################################################

    private boolean isClipTipEnabled = true;
    private PopupMenuFactory popupMenuFactory;
    
    //############################################################################################################################
    //# Constructs                                                                                                               #
    //############################################################################################################################

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since 2.0
    */
    public ListWidget() {
        this(Collections.EMPTY_LIST);
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since 2.0
    */
    public ListWidget(final List data) {
        super(new Vector(data));
        initializeListWidget();
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since 2.0
    */
    public ListWidget(final ListModel model) {
        super(model);
        initializeListWidget();
    }

    //############################################################################################################################
    //# Instance Methods                                                                                                         #
    //############################################################################################################################

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since 2.0
    */
    protected ListCellRenderer createCellRenderer() {
        return new DefaultListCellRenderer();
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since 2.1
    */
    protected PopupMenuFactory createDefaultPopupMenuFactory() {
        return new DefaultPopupMenuFactory();
    }
    
    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since 2.1
    */
    public PopupMenuFactory getPopupMenuFactory() {
        return popupMenuFactory;
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since 2.0
    */
    public String getToolTipText(final MouseEvent event) {
        if (event != null) {
            final Point mouseLoc = event.getPoint();
            final int row = locationToIndex(mouseLoc);
            final ListCellRenderer renderer = getCellRenderer();
            if(row != -1  &&  renderer != null) {
                final Component comp = renderer.getListCellRendererComponent(this, getModel().getElementAt(row), row,
                                                                             isSelectedIndex(row), true);
                if (comp instanceof JComponent) {
                    final MouseEvent newEvent;
                    final Rectangle cellBounds = getCellBounds(row, row);
                    mouseLoc.translate(-cellBounds.x, -cellBounds.y);
                    newEvent = new MouseEvent(comp, event.getID(), event.getWhen(), event.getModifiers(), mouseLoc.x, mouseLoc.y,
                                              event.getClickCount(), event.isPopupTrigger());
                    
                    return ((JComponent)comp).getToolTipText(newEvent);
                }
            }
        }
        return null;
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since 2.0
    */
    protected void initializeListWidget() {
        ToolTipManager.sharedInstance().registerComponent(this);
        setCellRenderer(createCellRenderer());
        setPopupMenuFactory(createDefaultPopupMenuFactory());
        addMouseListener(new MouseAdapter() {
            public void mousePressed(final MouseEvent event) {
                if (!SwingUtilities.isRightMouseButton(event)) {
                    return;
                }
                final int ndx = locationToIndex(event.getPoint());
                if (!isSelectedIndex(ndx)) {
                    setSelectedIndex(ndx);
                }
                if (popupMenuFactory != null) {
                    final JPopupMenu popup = popupMenuFactory.getPopupMenu(ListWidget.this);
                    if (popup != null) {
                        SwingUtilities.invokeLater(new Runnable() {
                            public void run() {
                                popup.show(ListWidget.this, event.getX(), event.getY());
                            }
                        });
                    }
                }
            }
        });
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since 2.0
    */
    public boolean isClipTipEnabled() {
        return isClipTipEnabled;
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since 2.0
    */
    public void setClipTipEnabled(final boolean isClipTipEnabled) {
        this.isClipTipEnabled = isClipTipEnabled;
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since 2.1
    */
    public void setPopupMenuFactory(final PopupMenuFactory factory) {
        popupMenuFactory = factory;
    }
/**/    
}
