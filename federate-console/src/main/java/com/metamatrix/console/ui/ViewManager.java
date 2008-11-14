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

package com.metamatrix.console.ui;

import java.awt.Image;
import java.awt.event.WindowAdapter;
import java.awt.event.WindowEvent;
import java.util.Collection;
import java.util.EventObject;

import javax.swing.ImageIcon;
import javax.swing.JFrame;
import javax.swing.JWindow;

import com.metamatrix.common.log.LogConfiguration;
import com.metamatrix.common.log.LogManager;
import com.metamatrix.common.log.config.BasicLogConfiguration;

import com.metamatrix.toolbox.event.UserPreferencesEvent;
import com.metamatrix.toolbox.preference.UserPreferences;
import com.metamatrix.toolbox.ui.widget.util.IconFactory;

import com.metamatrix.console.connections.ConnectionInfo;
import com.metamatrix.console.ui.layout.ConsoleMainFrame;
import com.metamatrix.console.util.DialogUtility;


/**
 * ViewManager is a developer's API to the metaMatrix console's
 *framework.  It is a central point of controlling such things as
 *initializing the GUI, refreshing, and retrieving the currently
 *active panel.  (Note that this class consists mostly of static
 *helper methods.)
 * @author Steve Jacobs
 */
public class ViewManager {

    private static ConsoleMainFrame mainFrame;
    private static StatusPanel statusPanel;
    private static JWindow splash;
//    private static Dimension screenSize = Toolkit.getDefaultToolkit().getScreenSize();

    public static final ImageIcon CONSOLE_ICON
        = IconFactory.getIconForImageFile("console_medium.gif"); //$NON-NLS-1$

    public static Image CONSOLE_ICON_IMAGE  = null;
        //= IconFactory.getIconForImageFile("consoleiconsmall.gif").getImage();

	public static void init(ConnectionInfo connection) {
    	//TODO-- change to save a preference for whether or not showing panels
    	//tree (argument to ConsoleMainFrame.createInstance()).  Then make this
    	//call based on saved value.
        ConsoleMainFrame.createInstance(true, connection);
		mainFrame = ConsoleMainFrame.getInstance();
        mainFrame.addWindowListener(new SplashCloser());
        statusPanel = new StatusPanel();
        statusPanel.createComponent();
        DialogUtility.setDefaultComponent(getMainFrame());
        CONSOLE_ICON_IMAGE
            = IconFactory.getIconForImageFile("console_medium.gif").getImage(); //$NON-NLS-1$

        if ( CONSOLE_ICON_IMAGE != null )
        {
            mainFrame.setIconImage( CONSOLE_ICON_IMAGE );
        }

        mainFrame.setInitialView(connection);
		mainFrame.show();
	}

    public static JFrame getMainFrame() {
        return mainFrame;
    }

    public static void setStatus(String statusMessage) {
        statusPanel.setStatusText(statusMessage);
    }

    public static void clearStatus() {
        statusPanel.clearStatusText();
    }

    public static void fireApplicationEvent(EventObject event) {
        if ( event instanceof UserPreferencesEvent ) {
            setLoggingPreferences();
        }
	}

     public static void setLoggingPreferences() {
        UserPreferences preferences = UserPreferences.getInstance();
        LogConfiguration config = LogManager.getLogConfiguration(true);
        int level = Integer.parseInt(preferences.getProperties().getProperty(BasicLogConfiguration.LOG_LEVEL_PROPERTY_NAME, "3")); //$NON-NLS-1$
        config.setMessageLevel(level);
        Collection contexts = UserPreferences.getInstance().getValues(BasicLogConfiguration.LOG_CONTEXT_PROPERTY_NAME, ';');

        config.recordAllContexts();
        config.discardContexts(contexts);
		
        LogManager.setLogConfiguration(config);
    }

    /**
     * Should be called before any operation that will take a long
     *time.  This will make the GUI indicate business.
     */
    public static void startBusy() {
        if (statusPanel != null)
            statusPanel.startBusy();
    }

    public static void startBusySyncronize(){
        if (statusPanel != null)
            statusPanel.startBusySyncronize();
        }

    /**
     * Should be called after the operation that startBusy was
     *called for.  It may be wise to put this method invocation
     *in a finally{} block in case, for example, a remote method
     *invocation fails.
     */
    public static void endBusy() {
        if (statusPanel != null) {
            statusPanel.endBusy();
        }
    }

    public static void endBusySyncronize(){
        if (statusPanel != null) {
            statusPanel.endBusySyncronize();
        }
    }

    private static class SplashCloser extends WindowAdapter {
        public void windowOpened(WindowEvent theEvent) {
            if (splash != null) {
                splash.dispose();
            }
        }
    }
}
