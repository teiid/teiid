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

package com.metamatrix.console.ui.views.properties;

import java.awt.BorderLayout;
import java.util.ArrayList;
import java.util.HashMap;

import javax.swing.Action;
import javax.swing.Icon;
import javax.swing.JPanel;
import javax.swing.JSplitPane;
import javax.swing.JTabbedPane;
import javax.swing.SwingUtilities;
import javax.swing.event.ChangeEvent;
import javax.swing.event.ChangeListener;

import com.metamatrix.common.log.LogManager;
import com.metamatrix.console.connections.ConnectionInfo;
import com.metamatrix.console.models.*;
import com.metamatrix.console.models.ManagerListener;
import com.metamatrix.console.models.ModelChangedEvent;
import com.metamatrix.console.notification.RuntimeUpdateNotification;
import com.metamatrix.console.security.UserCapabilities;
import com.metamatrix.console.ui.NotifyOnExitConsole;
import com.metamatrix.console.ui.layout.BasePanel;
import com.metamatrix.console.ui.layout.MenuEntry;
import com.metamatrix.console.ui.layout.WorkspacePanel;
import com.metamatrix.console.util.*;
import com.metamatrix.console.util.ExceptionUtility;
import com.metamatrix.console.util.LogContexts;

import com.metamatrix.toolbox.ui.widget.Splitter;
import com.metamatrix.toolbox.ui.widget.util.IconFactory;

public class PropertiesMasterPanel extends BasePanel 
		implements WorkspacePanel, ManagerListener, ChangeListener, 
		NotifyOnExitConsole, Refreshable {
    public static final String NEXT_STARTUP = "Next Startup";
    public static final Icon NEXT_STARTUP_ICON =
            IconFactory.getIconForImageFile("NextStartUp_small.gif");
    private NextStartupPanel nextStartupPanel/*, startupPanel*/;
    private JTabbedPane masterTabbedPane;
    private HashMap htSelectors = new HashMap();
    private JSplitPane splMainSplitPane = null;
    private JPanel propDetailPanel,propMasterOuter;
    private PropertyFilterPanel pfPanel;
    private String sSelectedTitle;
    private ArrayList aryActions = new ArrayList();
    private String previousTitle;
    private boolean runStateChange = true;
    private ConsolePropertiedEditor editor;
    private boolean canModifyServerProperties = false;
    private ConnectionInfo connection;

    public PropertiesMasterPanel(ConnectionInfo conn) {
    	super();
    	this.connection = conn;
        init();
    }

    private void init() {
    }

    public void addActionToList(String sId, Action act) {
        aryActions.add(new MenuEntry(sId, act));
    }

    public void createComponent() {
        this.setLayout(new BorderLayout());
        PropertiesManager manager = ModelManager.getPropertiesManager(connection);
        editor = new ConsolePropertiedEditor(manager);
        try{
            canModifyServerProperties = UserCapabilities.getInstance()
                            .canModifyServerProperties(connection);
        } catch (Exception ex) {
            ExceptionUtility.showMessage("NullPointerException", ex);
            LogManager.logError(LogContexts.ROLES, ex,
                "Error setting unmodifiable for user");
        }
        pfPanel = new PropertyFilterPanel(this, editor,canModifyServerProperties);
        masterTabbedPane = new JTabbedPane();
        masterTabbedPane.addChangeListener(this);
        masterTabbedPane.addChangeListener(editor);

        nextStartupPanel = pfPanel.getNextStartupPanel();

        addSelector(pfPanel.getNextStartupPanel());

        propDetailPanel = new JPanel();
        propDetailPanel.setLayout(new BorderLayout());

        propMasterOuter = new JPanel(new BorderLayout());
        propMasterOuter.add(masterTabbedPane,BorderLayout.CENTER);

        splMainSplitPane = new Splitter( JSplitPane.HORIZONTAL_SPLIT, true,
                pfPanel, propMasterOuter);
        add(splMainSplitPane,BorderLayout.CENTER);
    }

    public void modelChanged(ModelChangedEvent e) {
    }

    public void addSelector(NextStartupPanel jc) {
        masterTabbedPane.addTab(jc.getTitle(), jc.getIcon(), jc.getComponent());
        htSelectors.put(jc.getTitle(), jc);
    }

    public void postRealize() {
        splMainSplitPane.setDividerLocation(150);
    }

    public void stateChanged(ChangeEvent e) {
        if (!runStateChange) {
            runStateChange = true;
            return;
        }
        boolean proceeding;
        if (havePendingChanges()) {
            proceeding = finishUp();
            if (proceeding) {
                masterTabChanged();
            } else {
                runStateChange = false;
                masterTabbedPane.setSelectedIndex(
                        masterTabbedPane.indexOfTab(previousTitle));
            }
        } else {
             masterTabChanged();
        }
    }

    private void masterTabChanged() {
        int iSelected = masterTabbedPane.getSelectedIndex();
        sSelectedTitle = masterTabbedPane.getTitleAt( iSelected );
        previousTitle = sSelectedTitle;
        if ( sSelectedTitle == null ) {
            return;
        }
        if (previousTitle.equals(NEXT_STARTUP)) {
            nextStartupPanel.getApplyButton().setEnabled(false);
        }
        if (!canModifyServerProperties) {
            pfPanel.setMRBStatus(true);
        } else {
            pfPanel.setMRBStatus(false);
        }
        SwingUtilities.invokeLater(new Runnable() {
            public void run() {
                pfPanel.refresh(sSelectedTitle);
            }
        });
    }

    public java.util.List resume() {
        return aryActions;
    }

    public  String getTitle() {
        return "System Properties";
    }

	public ConnectionInfo getConnection() {
		return connection;
	}
	
    public boolean havePendingChanges() {
        boolean haveChanges = false;
        if (previousTitle != null) {
            if (previousTitle.equals(NEXT_STARTUP) && nextStartupPanel != null) {
                haveChanges = nextStartupPanel.havePendingChanges();
            }
        }
        return haveChanges;
    }

    public boolean finishUp() {
        if (previousTitle != null) {
            if (previousTitle.equals(NEXT_STARTUP)) {
                return nextStartupPanel.finishUp();
            }
        }
        return true;
    }
    
    public void receiveUpdateNotification(RuntimeUpdateNotification notification) {
    	//TODO
    }
    
    public void refresh() {
        editor.refreshData();
        pfPanel.refresh(sSelectedTitle);
    }
}
