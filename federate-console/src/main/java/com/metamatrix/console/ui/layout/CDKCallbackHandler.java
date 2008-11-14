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

package com.metamatrix.console.ui.layout;

import java.awt.Window;
import java.util.Arrays;

import javax.swing.JPanel;

import com.metamatrix.common.callback.Callback;
import com.metamatrix.common.object.PropertiedObject;
import com.metamatrix.common.object.PropertiedObjectEditor;
import com.metamatrix.common.object.PropertyDefinitionGroup;
import com.metamatrix.console.ui.ViewManager;
import com.metamatrix.console.util.ApplicationConstants;
import com.metamatrix.console.util.LogContexts;

import com.metamatrix.toolbox.ui.callback.DialogFactoryCallbackHandler;
import com.metamatrix.toolbox.ui.callback.ParentFrameSupplier;
import com.metamatrix.toolbox.ui.widget.LoggingPanel;

/**
 * An application implements a CallbackHandler and passes it to underlying
 * security services so that they may interact with the application to retrieve
 * specific authentication data, such as usernames and passwords, or to display
 * certain information, such as error and warning messages.
 * <p>
 * CallbackHandlers are implemented in an application-dependent fashion.
 * For example, implementations for an application with a graphical user
 * interface (GUI) may pop up windows to prompt for requested information
 * or to display error messages. An implementation may also choose to
 * obtain requested information from an alternate source without asking
 * the end user.
 */
public class CDKCallbackHandler extends DialogFactoryCallbackHandler implements ParentFrameSupplier {

    public CDKCallbackHandler() {
        super.setParentFrameSupplier(this);
    }

    /**
     * return the parent frame that a callback dialog should display over
     */
    public Window getParentFrameForCallback() {
        return ViewManager.getMainFrame();
    }

    protected JPanel createCallbackPanel(Callback callback,
                                         PropertyDefinitionGroup definitionGroup,
                                         PropertiedObject object,
                                         PropertiedObjectEditor editor) {

        if ( definitionGroup.getName().equals(ApplicationConstants.LOG_CONFIG_DEFN_GROUP.getName()) ) {
            return new LoggingPanel(Arrays.asList(LogContexts.logMessageContexts));
        }
       return super.createCallbackPanel(callback,
                                         definitionGroup,
                                         object,
                                         editor);
    }




}

