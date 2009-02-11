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

package com.metamatrix.console.ui.dialog;

import java.awt.event.ComponentAdapter;
import java.awt.event.ComponentEvent;
import java.util.ArrayList;
import java.util.Iterator;

import javax.swing.ImageIcon;
import javax.swing.JComponent;
import javax.swing.SwingUtilities;

import com.metamatrix.console.util.StaticProperties;
import com.metamatrix.toolbox.ui.widget.LoginPanel;
import com.metamatrix.toolbox.ui.widget.util.IconFactory;

/**
* This class is overridden from the Login Panel to add URLs into the JComboBox
*/
public class ConsoleLoginPanel extends LoginPanel  {
	private String systemURL;
    // Icon for login dialog
    private static final ImageIcon LOGIN_ICON =
        IconFactory.getIconForImageFile("logoAndName.gif"); //$NON-NLS-1$
	
	public ConsoleLoginPanel(java.util.List urls, boolean insertDefaultUserName) {
		super(true,LOGIN_ICON);
		init(urls, insertDefaultUserName);
	}

	public ConsoleLoginPanel(boolean insertDefaultUserName) {
		this(null, insertDefaultUserName);
	}

	/**
	* All initialization is done in this method
	* This will add all of the action event listeners, setup the urls
	*/
	public void init(java.util.List urls, boolean insertDefaultUserName) {
		initURLs(urls, insertDefaultUserName);
	}

	/**
	* Add the URLS into the combo box
	*/
	private void initURLs(java.util.List urls, boolean insertDefaultUserName) {
		String name = null;
		if (insertDefaultUserName) {
			name = StaticProperties.getProperty(
					StaticProperties.DEFAULT_USERNAME);
		}
       	boolean nameSupplied = ((name != null) && (name.length() > 0));
       	if (nameSupplied) {
	       	getUserNameField().setText(name);
	       	getPasswordField().addComponentListener(new ComponentAdapter() {
	           	public void componentResized(final ComponentEvent event) {
	               	SwingUtilities.invokeLater(new Runnable() {
	                	public void run() {
	                       	((JComponent)event.getSource()).requestFocus();
	                   	}
                   	});
	               	removeComponentListener(this);
	           	}
	       	});
	   	}
	   	if (urls != null) {
            String defaultURL = StaticProperties.getDefaultURL();
			Iterator i_urls = urls.iterator();
			while (i_urls.hasNext()) {
				String url = (String)i_urls.next();
				getSystemField().addItem(url);
			}
            if (defaultURL != null) {
                int index = urls.indexOf(defaultURL);
                if (index >= 0) {
                    getSystemField().setSelectedIndex(index);
                }
            }
		}
    }

    public void saveURLs(boolean replaceCurrent) {
        systemURL = (String)getSystemField().getSelectedItem();
        if (systemURL == null) {
        	if (replaceCurrent) {
        		StaticProperties.setCurrentURL(""); //$NON-NLS-1$
        	}
            StaticProperties.setURLs(new ArrayList(0));
            return;
        }
        java.util.List currentURLs = StaticProperties.getURLsCopy();
        if (replaceCurrent) {
        	StaticProperties.setCurrentURL(systemURL);
        }
        boolean listAlreadyHasURL = ((currentURLs != null) &&
        		currentURLs.contains(systemURL));
		if (!listAlreadyHasURL) {
            java.util.List newList = new ArrayList();
            if (currentURLs != null) {
                newList.addAll(currentURLs);
            }
            newList.add(systemURL);
			StaticProperties.setURLs(newList);
        }
    }
}
