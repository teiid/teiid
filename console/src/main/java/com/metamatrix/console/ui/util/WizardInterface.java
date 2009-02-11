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

package com.metamatrix.console.ui.util;

import java.awt.Component;

import javax.swing.AbstractButton;

public interface WizardInterface {
    /**
     * Implementer assumes this method will only be called by the current page.
     * Will return the "Finish" button if the current page is the last page,
     * and the "Next button otherwise.
     */
    AbstractButton getForwardButton();

    /**
     * Get the current page index within the wizard.
     */
    int getCurrentPageIndex();
    
    /**
     * Get the page count within the wizard.
     */
    int getPageCount();
    
    /**
     * Get the pages.
     */
    Component[] getPages();
    
    /**
     * For purpose of "owner" argument if creating a dialog, return the owner,
     * i.e. the wizard panel.
     */
    Component getOwner();
}
