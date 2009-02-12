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

package com.metamatrix.console.connections;

import java.util.ArrayList;

import com.metamatrix.common.log.LogManager;
import com.metamatrix.console.models.ModelManager;
import com.metamatrix.console.security.UserCapabilities;
import com.metamatrix.console.ui.dialog.ConsoleLogin;
import com.metamatrix.console.ui.layout.ConsoleMainFrame;
import com.metamatrix.console.ui.layout.ConsoleMenuBar;
import com.metamatrix.console.ui.layout.PanelsTree;
import com.metamatrix.console.ui.layout.TreeAndControllerCoordinator;
import com.metamatrix.console.ui.layout.Workspace;
import com.metamatrix.console.ui.layout.WorkspaceController;
import com.metamatrix.console.ui.layout.WorkspacePanel;
import com.metamatrix.console.util.ExceptionUtility;
import com.metamatrix.console.util.LogContexts;
import com.metamatrix.console.util.StaticProperties;
import com.metamatrix.console.util.StaticUtilities;

public class ConnectionProcessor {
	private static ConnectionProcessor theInstance = null;
	
	public static ConnectionProcessor getInstance() {
		if (theInstance == null) {
			theInstance = new ConnectionProcessor();
		}
		return theInstance;
	}
	
	public static ConnectionInfo getCurrentConnection() {
		ConnectionInfo currentConnection = null;
		Workspace workspace = Workspace.getInstance();
		WorkspacePanel currentPanel = workspace.getCurrentPanel();
		if (currentPanel != null) {
			currentConnection = currentPanel.getConnection();
		}
		return currentConnection;
	}
	
	private ConnectionProcessor() {
		super();
	}
	
	public void handleAddConnectionRequest() {
		//Get known URLs
        java.util.List /*<string>*/ urlNames = StaticProperties.getURLsCopy();
        if (urlNames == null) {
            urlNames = new ArrayList(0);
        }
        //Now proceed with showing login dialog
        boolean tryableCombinationEntered = false;
        int result = -1;
        ConsoleLogin consoleLogin = null;
		ConnectionInfo[] existingConnections = ModelManager.getConnections();
		String initialPassword = null;
        while (!tryableCombinationEntered) {
        	consoleLogin = new ConsoleLogin(urlNames, false,
        			existingConnections, initialPassword);
        	result = consoleLogin.showDialog();
        	tryableCombinationEntered = 
        			(!consoleLogin.isExistingConnectionURLAndUserEntered());
        	if (!tryableCombinationEntered) {
        		initialPassword = consoleLogin.getPassword();
        	}
        }
        boolean continuing;
        switch (result) {
            case ConsoleLogin.LOGON_SUCCEEDED:
            	continuing = true;
                break;
            case ConsoleLogin.CANCELLED_LOGON:
                continuing = false;
                break;
            default:
                LogManager.logCritical(LogContexts.GENERAL,
                        "ConsoleLogin returned unknown status.  Not continuing."); //$NON-NLS-1$
                continuing = false;
                break;
        }
        if (continuing) {
        	try {
				StaticUtilities.startWait();
        		ConnectionInfo connection = consoleLogin.getConnectionInfo();
        		boolean selectConnection = consoleLogin.selectNewConnection();
        		addConnection(connection, selectConnection);
        	} catch (RuntimeException ex) {
        		throw ex;
        	} finally {
				StaticUtilities.endWait();
        	}
        }
    }
	
	public void handleRemoveConnectionRequest() {
		ConnectionInfo[] connections = ModelManager.getConnections();
		ConnectionInfo currentConnection = 
				ConnectionProcessor.getCurrentConnection();
		RemoveConnectionDialog dialog = new RemoveConnectionDialog(connections,
				currentConnection);
		dialog.show();
		ConnectionInfo selection = dialog.getSelectedURL();
		if (selection != null) {
			int matchLoc = -1;
			int i = 0;
			while (matchLoc < 0) {
				if (connections[i].equals(selection)) {
					matchLoc = i;
				} else {
					i++;
				}
			}
			ConnectionInfo conn = connections[matchLoc];
			boolean continuing = true;
			WorkspaceController wc = WorkspaceController.getInstance();
			if (wc.havePendingChanges(conn)) {
				continuing = wc.finishUp(conn);
			}
			if (continuing) {
				removeConnection(conn);
			}
		}
	}
	
	private void addConnection(ConnectionInfo connection,
			boolean selectConnection) {
		boolean continuing = true;
		boolean initialized = false;
		try {
			ModelManager.init(connection);
		} catch (Exception ex) {
			String msg = "Error processing new connection"; //$NON-NLS-1$
			LogManager.logError(LogContexts.INITIALIZATION, ex, msg);
			ExceptionUtility.showMessage(msg, ex);
			continuing = false;
		}
		if (continuing) {
			try {
				UserCapabilities.getInstance().init(connection);
			} catch (Exception ex) {
				String msg = "Error initializing user capabilities"; //$NON-NLS-1$
				LogManager.logError(LogContexts.INITIALIZATION, ex, msg);
				ExceptionUtility.showMessage(msg, ex);
				continuing = false;
			}
			if (continuing) {
				initialized = ModelManager.initViews(connection, false);
				if (initialized) {
					PanelsTree tree = PanelsTree.createInstance(connection);
					WorkspaceController controller = 
							WorkspaceController.getInstance();
					TreeAndControllerCoordinator.createInstance(tree,
							controller, connection);
					ConsoleMainFrame.getInstance().addConnection(connection,
							tree);
					ConsoleMenuBar.getInstance().setRemoveConnectionEnabled(
							true);
					ConsoleMainFrame.getInstance().setConnectionsPanelVisible(
							true);
					//Even if not changing to new connection, must temporarily 
					//select it to force creation of one panel.  (Fix to defect
					//9458.)
					ConnectionInfo prevConnection = 
							ConnectionProcessor.getCurrentConnection();
					ConsoleMainFrame.getInstance().selectConnection(connection);
					//If not sticking with new connection, switch right back to
					//previous one.
					if (!selectConnection) {
 						ConsoleMainFrame.getInstance().selectConnection(
 								prevConnection);
 					}
				} else {
					ModelManager.removeConnection(connection);
				}
			}
		}
		if (!initialized) {
			connection.close();
		}
	}
	
	public void removeConnection(ConnectionInfo connection) {
		UserCapabilities.getInstance().remove(connection);
		ModelManager.removeConnection(connection);
		WorkspaceController.getInstance().deletePanelsForConnection(connection);
		ConsoleMainFrame.getInstance().removeConnection(connection);
		int connectionCount = ModelManager.getConnections().length;
		if (connectionCount <= 1) {
			ConsoleMenuBar.getInstance().setRemoveConnectionEnabled(false);
			ConsoleMainFrame.getInstance().setConnectionsPanelVisible(false);
		}
        
        if (connection != null) {
            ConnectionCloser closer = new ConnectionCloser(connection);
            new Thread(closer).start();
        }
	}
}//end ConnectionProcessor




class ConnectionCloser implements Runnable {
	private ConnectionInfo connection;
	public ConnectionCloser(ConnectionInfo connection) {
		super();
		this.connection = connection;
	}
	
	public void run() {
		connection.close();
	}
}//end ConnectionCloser			
