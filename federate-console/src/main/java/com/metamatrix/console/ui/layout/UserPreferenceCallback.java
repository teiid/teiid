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

import com.metamatrix.common.callback.CallbackChoices;
import com.metamatrix.common.callback.CallbackImpl;
import com.metamatrix.console.ui.ViewManager;
import com.metamatrix.console.util.ApplicationConstants;

import com.metamatrix.toolbox.event.UserPreferencesEvent;
import com.metamatrix.toolbox.preference.UserPreferences;
import com.metamatrix.toolbox.ui.callback.ParentFrameSupplier;

/**
 * UserPreferenceCallback is a temporary Callback object constructed to generate the UserPreferences
 * dialog.  Use as follows:
 * <pre>
 *      ModelerCallbackHandler handler = new ModelerCallbackHandler();
 *      try {
 *          handler.handle(new UserPreferenceCallback(), this);
 *      } catch (Exception e) {
 *          e.printStackTrace();
 *      }
 * </pre>
 */
public class UserPreferenceCallback extends CallbackImpl implements ParentFrameSupplier {

    private static final String TITLE = "Logging Preferences"; //$NON-NLS-1$
    private static final String NAME = "UserPreferences"; //$NON-NLS-1$
    private static final String PROMPT = "Modify the User Preferences for console"; //$NON-NLS-1$
    private static final CallbackChoices CHOICES =
        new CallbackChoices(PROMPT, CallbackChoices.WARNING_MESSAGE, CallbackChoices.OK_CANCEL_OPTION, CallbackChoices.OK);


    /**
     * Construct a UserPreferenceCallback to be passed to this applicaiton's CallbackHandler.
     */
    public UserPreferenceCallback() {
        super(NAME, TITLE, CHOICES,
                UserPreferences.getInstance().getPropertiedObject(),
                UserPreferences.getInstance().getPropertiedObjectEditor(),
                ApplicationConstants.getUserPreferencesGroupList(),
                false);
    }

    /**
     * return the parent frame that a callback dialog should display over
     */
    public Window getParentFrameForCallback() {
        return ViewManager.getMainFrame();
    }

    /**
     * called when the callback has received a response.
     */
    public void setResponse(int response) {
        if ( response == CallbackChoices.OK ) {
            UserPreferences.getInstance().saveChanges();
            ViewManager.fireApplicationEvent(new UserPreferencesEvent(this, null, null));
        } else {
            UserPreferences.getInstance().clearChanges();
        }
    }
}
