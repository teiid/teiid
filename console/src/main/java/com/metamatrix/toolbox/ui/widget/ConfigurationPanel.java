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
import java.awt.Container;
import java.awt.event.ActionListener;
import java.util.Iterator;
import java.util.List;

import javax.swing.JPanel;
import javax.swing.JTabbedPane;

/**
This class is intended to be used everywhere within the application that a configuration panel needs to be displayed.  It provides
for a navigation button bar at the bottom of the panel, a set of tabbed panels in the center, and a default set of "accept",
"cancel", and "apply" buttons within the navigation bar.
@since 2.0
@version 2.0
@author John P. A. Verhaeg
*/
public class ConfigurationPanel extends DialogPanel
implements ButtonConstants {
    //############################################################################################################################
    //# Instance Variables                                                                                                       #
    //############################################################################################################################

    private transient List tabNames = null;
    private transient List tabContents = null;
    
    private ButtonWidget applyButton = null;

    //############################################################################################################################
    //# Constructors                                                                                                             #
    //############################################################################################################################

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    Creates a ConfigurationPanel with no tabs.
    @since 2.0
    */
    public ConfigurationPanel() {
        this((Component)null);
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    Creates a ConfigurationPanel with no tabs.
    @since 2.0
    */
    public ConfigurationPanel(final Component content) {
        super(content);
        initializeConfigurationPanel();
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    Creates a ConfigurationPanel with one tab for each element in the specified List, named using the element's
    {@link Object#toString() toString} value.
    @since 2.0
    */
    public ConfigurationPanel(final List tabNames) {
        this(tabNames, null);
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since 2.0
    */
    public ConfigurationPanel(final List tabNames, final List tabContents) {
        super(new JTabbedPane());
        this.tabNames = tabNames;
        this.tabContents = tabContents;
        initializeConfigurationPanel();
    }

    //############################################################################################################################
    //# Instance Methods                                                                                                         #
    //############################################################################################################################

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    Adds the specified listener to the lists of ActionListeners registered to both the accept and apply buttons.
    @param listener The ActionListener to be added
    @since 2.0
    */
    public void addApplyActionListener(final ActionListener listener) {
        getAcceptButton().addActionListener(listener);
        applyButton.addActionListener(listener);
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    Creates an apply button with a default label (as determined by the ToolboxStandards class).
    @return The apply button
    @since 2.0
    */
    protected ButtonWidget createApplyButton() {
        return WidgetFactory.createButton(APPLY_BUTTON);
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since 2.0
    */
    protected Container createDefaultTab(final String name) {
        return new JPanel();
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @return The apply button
    @since 2.0
    */
    public ButtonWidget getApplyButton() {
        return applyButton;
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since 2.0
    */
    protected void initializeConfigurationPanel() {
        // Add apply button after the accept button
        applyButton = createApplyButton();
        addNavigationButton(applyButton);
        // If provided, add tabs with the names passed in the constructor
        if (tabNames != null) {
            final JTabbedPane tabs = (JTabbedPane)getContent();
            final Iterator nameIterator = tabNames.iterator();
            Iterator contentIterator = null;
            if (tabContents != null) {
                contentIterator = tabContents.iterator();
            }
            String name;
            while (nameIterator.hasNext()) {
                name = nameIterator.next().toString();
                if (tabContents == null  ||  !contentIterator.hasNext()) {
                    tabs.addTab(name, createDefaultTab(name));
                } else {
                    tabs.addTab(name, (Component)contentIterator.next());
                }
            }
        }
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    Removes the specified listener from the lists of ActionListeners registered to both the accept and apply buttons.
    @param listener The ActionListener to be removed
    @since 2.0
    */
    public void removeApplyActionListener(final ActionListener listener) {
        getAcceptButton().removeActionListener(listener);
        applyButton.removeActionListener(listener);
    }
}
