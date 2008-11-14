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

import com.metamatrix.common.properties.TextManager;

import com.metamatrix.toolbox.ui.UIConstants;
import com.metamatrix.toolbox.ui.UIDefaults;

/**
 * @since 2.0
 * @version 2.0
 * @author John P. A. Verhaeg
 */
public class WidgetFactory
implements UIConstants {
    //############################################################################################################################
    //# Static Methods                                                                                                           #
    //############################################################################################################################

    /**
     * Creates a button whose properties are obtained from properties files using the specified key.
     * @param key The properties file key.
     * @return The new button
     * @since 2.0
     */
    public static ButtonWidget createButton(final String key) {
        final TextManager textMgr = TextManager.INSTANCE;
        final UIDefaults dflts = UIDefaults.getInstance();
        final ButtonWidget button = new ButtonWidget(textMgr.getText(ButtonWidget.PROPERTY_PREFIX + key));
        final String iconKey = dflts.getString(ButtonWidget.PROPERTY_PREFIX + key + ButtonWidget.ICON_PROPERTY_SUFFIX);
        if (iconKey != null) {
            button.setIcon(dflts.getIcon(iconKey));
        }
        final String mnemonic = textMgr.getText(ButtonWidget.PROPERTY_PREFIX + key + ButtonWidget.MNEMONIC_PROPERTY_SUFFIX);
        if (mnemonic.length() > 0  &&  mnemonic.charAt(0) != '<') {
            button.setMnemonic(mnemonic.charAt(0));
        }
        return button;
    }
}
