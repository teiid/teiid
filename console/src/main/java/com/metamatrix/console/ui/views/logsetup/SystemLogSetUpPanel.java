/*
 * JBoss, Home of Professional Open Source.
 * See the COPYRIGHT.txt file distributed with this work for information
 * regarding copyright ownership.  Some portions may be licensed
 * to Red Hat, Inc. under one or more contributor license agreements.
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

package com.metamatrix.console.ui.views.logsetup;

import java.awt.GridLayout;
import java.util.ArrayList;
import java.util.Collection;

import javax.swing.*;

import com.metamatrix.common.log.LogManager;
import com.metamatrix.console.connections.ConnectionInfo;
import com.metamatrix.console.models.ServerLogManager;
import com.metamatrix.console.notification.RuntimeUpdateNotification;
import com.metamatrix.console.ui.NotifyOnExitConsole;
import com.metamatrix.console.ui.layout.*;
import com.metamatrix.console.util.*;
import com.metamatrix.toolbox.ui.widget.util.IconFactory;

public class SystemLogSetUpPanel extends JPanel implements WorkspacePanel,
        ConfigurationLogSetUpPanelController, NotifyOnExitConsole, Refreshable {
    public final static int NUM_CONFIGURATIONS = 1;
    public final static int NEXT_STARTUP_INDEX = 0;
    public final static String[] CONFIGURATION_NAMES = new String[] {
            "Next Startup"}; //$NON-NLS-1$
    public final static Icon[] CONFIGURATION_ICONS = new Icon[] {
            IconFactory.getIconForImageFile("NextStartUp_small.gif") //$NON-NLS-1$
            };

    private ServerLogManager manager;
    private boolean canModify;
    private ConnectionInfo connection;
    private boolean isInitialized = false;
    private String[] messageLevelNames;
    private Collection /*<String>*/ allContexts;
    private ConfigurationLogSetUpPanel[] configPanels;
    private JTabbedPane tabbedPane;
    private java.util.List actions = new ArrayList(1);
    
    public SystemLogSetUpPanel(ServerLogManager mgr, boolean canMod,
    		ConnectionInfo conn) {
        super();
        manager = mgr;
        canModify = canMod;
        this.connection = conn;
        isInitialized = init();
    }

    private boolean init() {
        boolean initialized = true;
        try {
            messageLevelNames = manager.getMessageLevelDisplayNames();
        } catch (Exception ex) {
            LogManager.logError(LogContexts.LOG_SETTINGS, ex,
                    "Error retrieving log message levels"); //$NON-NLS-1$
            ExceptionUtility.showMessage("Error retrieving log message levels", //$NON-NLS-1$
                    ex);
            initialized = false;
        }
        if (initialized) {
            configPanels = new ConfigurationLogSetUpPanel[NUM_CONFIGURATIONS];
            for (int i = 0; i < NUM_CONFIGURATIONS; i++) {
                if (initialized) {
                    allContexts = null;
                    try {
                        allContexts = manager.getAllContexts();
                    } catch (Exception ex) {
                        LogManager.logError(LogContexts.LOG_SETTINGS, ex,
                                "Error retrieving logging contexts"); //$NON-NLS-1$
                        ExceptionUtility.showMessage(
                                "Error retrieving logging contexts", ex); //$NON-NLS-1$
                        initialized = false;
                        break;
                    }
                    if (initialized) {
                        Collection /*<String>*/ selectedContexts = null;
                        try {
                            selectedContexts =
                                    manager.getContextsForConfigurationIndex(i);
                        } catch (Exception ex) {
                            LogManager.logError(LogContexts.LOG_SETTINGS, ex,
                                    "Error retrieving logging contexts for configuration"); //$NON-NLS-1$
                            ExceptionUtility.showMessage(
                                    "Error retrieving logging contexts", ex); //$NON-NLS-1$
                            initialized = false;
                            break;
                        }
                        if (initialized) {
                            Collection /*<String>*/ availableContexts = allContexts;
                            int messageLevel = -1;
                            try {
                                messageLevel =
                                        manager.getLoggingLevelForConfigurationIndex(i);
                            } catch (Exception ex) {
                                LogManager.logError(LogContexts.LOG_SETTINGS, ex,
                                        "Error retrieving logging level for configuration"); //$NON-NLS-1$
                                ExceptionUtility.showMessage(
                                        "Error retrieving logging level", ex); //$NON-NLS-1$
                                initialized = false;
                                break;
                            }
                            if (initialized) {
                                String[] otherSourceNames = new String[
                                        CONFIGURATION_NAMES.length - 1];
                                Icon[] otherSourceIcons = new Icon[
                                        CONFIGURATION_NAMES.length - 1];
                                int outputLoc = -1;
                                for (int j = 0; j < CONFIGURATION_NAMES.length; j++) {
                                    if (j != i) {
                                        outputLoc++;
                                        otherSourceNames[outputLoc] =
                                                CONFIGURATION_NAMES[j];
                                        otherSourceIcons[outputLoc] =
                                                CONFIGURATION_ICONS[j];
                                    }
                                }
                                boolean modifiable = canModify;
                                configPanels[i] = new ConfigurationLogSetUpPanel(
                                        CONFIGURATION_NAMES[i], modifiable, this,
                                        otherSourceNames, otherSourceIcons,
                                        messageLevelNames, messageLevel,
                                        new ArrayList(availableContexts),
                                        new ArrayList(selectedContexts));
                            }
                        }
                    }
                }
            }
        }
        if (initialized) {
            tabbedPane = new JTabbedPane();
            for (int i = 0; i < NUM_CONFIGURATIONS; i++) {
                tabbedPane.addTab(CONFIGURATION_NAMES[i],
                        CONFIGURATION_ICONS[i], configPanels[i]);
            }
            this.setLayout(new GridLayout(1, 1));
            this.add(tabbedPane);
		}
        return initialized;
    }

    public String getTitle() {
        return "Log Settings"; //$NON-NLS-1$
    }

	public ConnectionInfo getConnection() {
		return connection;
	}
    
    public void receiveUpdateNotification(RuntimeUpdateNotification notification) {
    	//TODO
    }
	
    public java.util.List /*<Action>*/ resume() {
        if (!isInitialized) {
            isInitialized = init();
        }
        return actions;
    }

    public void refresh() {
        //See if doing a refresh will cause loss of pending changes.  If so,
        //prompt user on proceeding.
        String configNames = ""; //$NON-NLS-1$
        if (configPanels[NEXT_STARTUP_INDEX].havePendingChanges()) {
            configNames += CONFIGURATION_NAMES[NEXT_STARTUP_INDEX] +
                    " Configuration"; //$NON-NLS-1$
        }
        boolean continuing = true;
        if (configNames.length() > 0) {
            String msg = "Refresh will cause loss of pending changes to " + //$NON-NLS-1$
                    configNames + ".\n\nProceed with refresh?"; //$NON-NLS-1$
            int response = DialogUtility.displayYesNoDialog(
                    ConsoleMainFrame.getInstance(), "Changes Pending", msg); //$NON-NLS-1$
            switch (response) {
                case DialogUtility.YES:
                    break;
                case DialogUtility.NO:
                    continuing = false;
                    break;
            }
        }
        if (continuing) {
			int index = NEXT_STARTUP_INDEX;
            Collection /*<String>*/ selectedContexts = null;
            try {
                selectedContexts =
                        manager.getContextsForConfigurationIndex(index);
            } catch (Exception ex) {
                LogManager.logError(LogContexts.LOG_SETTINGS, ex,
                        "Error retrieving logging contexts for configuration"); //$NON-NLS-1$
                ExceptionUtility.showMessage(
                        "Error retrieving logging contexts", ex); //$NON-NLS-1$
                continuing = false;
            }
            if (continuing) {
                Collection /*<String>*/ availableContexts = allContexts;
                int messageLevel = -1;
                try {
                    messageLevel =
                            manager.getLoggingLevelForConfigurationIndex(
                            index);
                } catch (Exception ex) {
                    LogManager.logError(LogContexts.LOG_SETTINGS, ex,
                            "Error retrieving logging level for configuration"); //$NON-NLS-1$
                    ExceptionUtility.showMessage(
                            "Error retrieving logging level", ex); //$NON-NLS-1$
                    continuing = false;
                }
                if (continuing) {
                    configPanels[index].setNewValues(
                            new ArrayList(availableContexts),
                            new ArrayList(selectedContexts),
                            messageLevel);
                }
            }
        }
    }

    private int indexOfConfigurationName(String configName) {
        return StaticQuickSorter.unsortedStringArrayIndex(CONFIGURATION_NAMES,
                configName);
    }

    public void applyButtonStateChanged(String configName, boolean newState) {
//        int index = 
        indexOfConfigurationName(configName);
        for (int i = 0; i < NUM_CONFIGURATIONS; i++) {
            if (!CONFIGURATION_NAMES[i].equals(configName)) {
                if ((configPanels != null) && (configPanels.length >= i + 1) &&
                        (configPanels[i] != null)) {
                    configPanels[i].setCopyButtonState(configName, !newState);
                }
            }
        }
    }

    public void applyButtonPressed(String configName, int messageLevel,
            java.util.List /*<String>*/ messageContexts) {
        int index = indexOfConfigurationName(configName);
        Collection /*<String>*/ allContexts = null;
        Collection /*<String>*/ selectedContexts = null;
        int newMessageLevel = -1;
        boolean continuing = true;
        try {
        	StaticUtilities.startWait();
            manager.setContextsForConfigurationIndex(index, messageContexts);
            manager.setLoggingLevelForConfigurationIndex(index, messageLevel);
            allContexts = manager.getAllContexts();
            selectedContexts = manager.getContextsForConfigurationIndex(index);
            newMessageLevel = manager.getLoggingLevelForConfigurationIndex(index);
        } catch (Exception ex) {
            LogManager.logError(LogContexts.LOG_SETTINGS, ex,
                    "Error applying log settings changes."); //$NON-NLS-1$
            ExceptionUtility.showMessage("Error applying log settings changes", //$NON-NLS-1$
                    ex);
            continuing = false;
        } finally {
        	StaticUtilities.endWait();
        }
        if (continuing) {
            Collection /*<String>*/ availableContexts = allContexts;
            configPanels[index].setNewValues(new ArrayList(availableContexts),
                    new ArrayList(selectedContexts), newMessageLevel);
        }
    }

    public java.util.List /*<String>*/ getContextsFrom(String sourceName) {
        java.util.List messageContexts = null;
        int index = indexOfConfigurationName(sourceName);
        if (index >= 0) {
            messageContexts =
                    configPanels[index].getSelectedMessageContexts();
        } else {
            throw new RuntimeException(
                    "Error in SystemLogSetUpPanel.getContextsFrom(), " + //$NON-NLS-1$
                    "unknown source name of " + sourceName); //$NON-NLS-1$
        }
        return messageContexts;
    }

    public int getMessageLevelFrom(String sourceName) {
        int messageLevel = -1;
        int index = indexOfConfigurationName(sourceName);
        if (index >= 0) {
            messageLevel = configPanels[index].getSelectedMessageLevel();
        } else {
            throw new RuntimeException(
                    "Error in SystemLogSetUpPanel.getMessageLevelFrom(), " + //$NON-NLS-1$
                    "unknown source name of " + sourceName); //$NON-NLS-1$
        }
        return messageLevel;
    }

    public boolean havePendingChanges() {
        boolean havePending = false;
        int i = 0;
        while ((!havePending) && (i < NUM_CONFIGURATIONS)) {
            havePending = configPanels[i].havePendingChanges();
            if (!havePending) {
                i++;
            }
        }
        return havePending;
    }

    public boolean finishUp() {
        boolean stayingHere = false;
        //First check current tab, then any other tabs
        int currentTab = tabbedPane.getSelectedIndex();
        if (configPanels[currentTab].havePendingChanges()) {
            String msg = "Save changes to " + CONFIGURATION_NAMES[currentTab] + //$NON-NLS-1$
                    " configuration log settings?"; //$NON-NLS-1$
            int response = DialogUtility.showPendingChangesDialog(msg,
            		manager.getConnection().getURL(),
            		manager.getConnection().getUser());
            switch (response) {
                case DialogUtility.YES:
                    configPanels[currentTab].doApply();
                    stayingHere = false;
                    break;
                case DialogUtility.NO:
                    configPanels[currentTab].doReset();
                    stayingHere = false;
                    break;
                case DialogUtility.CANCEL:
                    stayingHere = true;
                    break;
            }
        }
        int i = 0;
        while ((!stayingHere) && (i < NUM_CONFIGURATIONS)) {
            if (configPanels[i].havePendingChanges()) {
                tabbedPane.setSelectedIndex(i);
                String msg = "Save changes to " + CONFIGURATION_NAMES[i] + //$NON-NLS-1$
                        " configuration log settings?"; //$NON-NLS-1$
                int response = DialogUtility.showPendingChangesDialog(msg,
                		manager.getConnection().getURL(),
                		manager.getConnection().getUser());
                switch (response) {
                    case DialogUtility.YES:
                        configPanels[i].doApply();
                        stayingHere = false;
                        break;
                    case DialogUtility.NO:
                        configPanels[i].doReset();
                        stayingHere = false;
                        break;
                    case DialogUtility.CANCEL:
                        stayingHere = true;
                        break;
                }
            }
            i++;
        }
        return (!stayingHere);
    }
}//end SystemLogSetUpPanel
