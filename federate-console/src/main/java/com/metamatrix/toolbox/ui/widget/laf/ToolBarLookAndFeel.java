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

// JDK imports
import java.awt.Component;
import java.awt.Container;
import java.awt.event.ContainerListener;
import java.awt.event.FocusListener;

import javax.swing.JComponent;
import javax.swing.plaf.ComponentUI;
import javax.swing.plaf.metal.MetalToolBarUI;

import com.metamatrix.toolbox.ui.widget.ToolBar;

/**
@since 2.1
@version 2.1
@author <a href="mailto:jverhaeg@metamatrix.com">John P. A. Verhaeg</a>
*/
public class ToolBarLookAndFeel extends MetalToolBarUI {
    //############################################################################################################################
    //# Instance Methods                                                                                                         #
    //############################################################################################################################
    
    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since 2.1
    */
    public static ComponentUI createUI(final JComponent component)
    {
        return new ToolBarLookAndFeel();
    }
    
    //############################################################################################################################
    //# Instance Methods                                                                                                         #
    //############################################################################################################################
    
    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since 2.1
    */
    protected FocusListener createButtonFocusListener() {
        return super.createToolBarFocusListener();
    }
    
    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since 2.1
    */
    protected ContainerListener createContainerListener() {
        return super.createToolBarContListener();
    }
    
    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since 2.1
    */
    protected ContainerListener createToolBarContListener() {
        return null;
    }
    
    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since 2.1
    */
    protected FocusListener createToolBarFocusListener() {
        return null;
    }
    
    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since 2.1
    */
    public void installSubclassListeners() {
        final Container buttons = ((ToolBar)toolBar).getButtonContainer();
        toolBarContListener = createContainerListener();
        if (toolBarContListener != null) {
            buttons.addContainerListener(toolBarContListener);
        }
        toolBarFocusListener = createButtonFocusListener();
        if (toolBarFocusListener != null) {
            final Component[] comps = buttons.getComponents();
            for (int ndx = comps.length;  --ndx >= 0;) {
                comps[ndx].addFocusListener(toolBarFocusListener);
            }
        }
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since 2.1
    */
    protected void navigateFocusedComp(final int direction) {
        final Container buttons = ((ToolBar)toolBar).getButtonContainer();
        int nComp = buttons.getComponentCount();
        int j;
        switch (direction) {
            case EAST:
            case SOUTH: {
                if (focusedCompIndex < 0  ||  focusedCompIndex >= nComp) {
                    break;
                }
                j = focusedCompIndex + 1;
                while (j != focusedCompIndex) {
                    if (j >= nComp) {
                        j = 0;
                    }
                    final Component comp = buttons.getComponent(j++);
                    if (comp != null  &&  comp.isFocusTraversable()) {
                        comp.requestFocus();
                        break;
                    }
                }
                break;
            }
            case WEST:
            case NORTH: {
                if (focusedCompIndex < 0  ||  focusedCompIndex >= nComp) {
                    break;
                }
                j = focusedCompIndex - 1;
                while (j != focusedCompIndex) {
                    if (j < 0) {
                        j = nComp - 1;
                    }
                    final Component comp = buttons.getComponent(j--);
                    if (comp != null  &&  comp.isFocusTraversable()) {
                        comp.requestFocus();
                        break;
                    }
                }
                break;
            }
        }
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since 2.1
    */
    protected void uninstallListeners() {
        final Container buttons = ((ToolBar)toolBar).getButtonContainer();
        if (dockingListener != null) {
            toolBar.removeMouseMotionListener(dockingListener);
            toolBar.removeMouseListener(dockingListener);
            dockingListener = null;
        }
        if (propertyListener != null) {
            propertyListener = null;
        }
        if (toolBarContListener != null) {
            buttons.removeContainerListener(toolBarContListener);
            toolBarContListener = null;
        }
        if (toolBarFocusListener != null) {
            final Component[] comps = buttons.getComponents();
            for (int ndx = comps.length;  --ndx >= 0;) {
                comps[ndx].removeFocusListener(toolBarFocusListener);
            }
            toolBarFocusListener = null;
        }
    }
}
