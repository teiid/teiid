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

package com.metamatrix.console.ui.views.queries;

import java.awt.Font;
import java.awt.Graphics;
import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.awt.GridLayout;
import java.awt.Insets;
import java.awt.event.ActionEvent;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;

import javax.swing.Action;
import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.JSplitPane;
import javax.swing.JTextArea;

import com.metamatrix.common.config.api.ConnectorBinding;
import com.metamatrix.common.log.LogManager;
import com.metamatrix.console.ConsolePlugin;
import com.metamatrix.console.connections.ConnectionInfo;
import com.metamatrix.console.models.ConnectorManager;
import com.metamatrix.console.models.ManagerListener;
import com.metamatrix.console.models.ModelChangedEvent;
import com.metamatrix.console.models.ModelManager;
import com.metamatrix.console.models.QueryManager;
import com.metamatrix.console.notification.RuntimeUpdateNotification;
import com.metamatrix.console.ui.layout.ConsoleMainFrame;
import com.metamatrix.console.ui.layout.MenuEntry;
import com.metamatrix.console.ui.layout.WorkspacePanel;
import com.metamatrix.console.ui.util.AbstractPanelAction;
import com.metamatrix.console.ui.util.property.PropertyProvider;
import com.metamatrix.console.ui.views.deploy.util.DeployPkgUtils;
import com.metamatrix.console.util.AutoRefreshable;
import com.metamatrix.console.util.AutoRefresher;
import com.metamatrix.console.util.ExceptionUtility;
import com.metamatrix.console.util.ExternalException;
import com.metamatrix.console.util.LogContexts;
import com.metamatrix.console.util.StaticProperties;
import com.metamatrix.console.util.StaticUtilities;
import com.metamatrix.core.util.StringUtil;
import com.metamatrix.platform.security.api.SessionToken;
import com.metamatrix.server.serverapi.RequestInfo;
import com.metamatrix.toolbox.ui.widget.LabelWidget;
import com.metamatrix.toolbox.ui.widget.Splitter;
import com.metamatrix.toolbox.ui.widget.TextFieldWidget;
import com.metamatrix.toolbox.ui.widget.TitledBorder;

public final class QueryPanel
	extends JPanel
	implements ManagerListener, WorkspacePanel, AutoRefreshable {

	///////////////////////////////////////////////////////////////////////////
	// CONSTANTS
	///////////////////////////////////////////////////////////////////////////

	private static final SimpleDateFormat FORMATTER;
	private static final double INITIAL_SPLITTER_LOC = 0.4;

	///////////////////////////////////////////////////////////////////////////
	// INITIALIZER
	///////////////////////////////////////////////////////////////////////////

	static {
		PropertyProvider pp = PropertyProvider.getDefault();
		FORMATTER = (SimpleDateFormat) pp.getObject("date.formatter.default"); //$NON-NLS-1$
	}

	///////////////////////////////////////////////////////////////////////////
	// CONTROLS
	///////////////////////////////////////////////////////////////////////////

	private JTextArea txaCommand;
	private QueryRequestPanel queryPanel;
	private TextFieldWidget txfRequestId;
	private TextFieldWidget txfSessionId;
	private TextFieldWidget txfSubmitted;
	private TextFieldWidget txfTransactionId;
	private TextFieldWidget txfUser;
    private TextFieldWidget txfConnectorBinding;
	private Splitter splitter;

	///////////////////////////////////////////////////////////////////////////
	// FIELDS
	///////////////////////////////////////////////////////////////////////////

	private ConnectionInfo connection;
    private ConnectorManager connectorMgr;
	private ArrayList actions = new ArrayList();
	private Action refreshAction;
    private AutoRefresher arRefresher;
	private boolean hasBeenPainted = false;
	private JPanel pnlDetail;

	///////////////////////////////////////////////////////////////////////////
	// CONSTRUCTORS
	///////////////////////////////////////////////////////////////////////////

	/**
	 * Causes this panel to "create" itself - initialize all of its GUI
	 *components, prepare itself to be displayed, and display itself.
	 */
	public QueryPanel(ConnectionInfo conn) throws Exception {
		super();
		this.connection = conn;
		construct();
		ModelManager.getQueryManager(connection).addManagerListener(this);
		refreshAction = new AbstractPanelAction(0) {
			public void actionImpl(ActionEvent e) {
				refreshImpl();
			}
		};
		refreshAction.putValue(Action.NAME, "Refresh"); //$NON-NLS-1$
		refreshAction.putValue(Action.SMALL_ICON, DeployPkgUtils.getIcon("icon.refresh")); //$NON-NLS-1$
		MenuEntry mnuEntry =
			new MenuEntry(MenuEntry.VIEW_REFRESH_MENUITEM, refreshAction);
		actions.add(mnuEntry);
        
		//now that we have this refresh Action object, we want to associate it with two things:
		// 1.) the refresh menu item
		// 2.) the periodic firing of the timer built in to QueryMgr (a type of TimedMgr)
		//
		// We handle the first in this.resume().
		// The second, we'll do here:
		getQueryManager().setRefreshAction(refreshAction);
        
      
		// Establish AutoRefresher
		arRefresher = new AutoRefresher(this, 15, false, connection);
		arRefresher.init();
		arRefresher.startTimer();
		getQueryManager().setAutoRefresher(arRefresher);
	}

	///////////////////////////////////////////////////////////////////////////
	// METHODS
	///////////////////////////////////////////////////////////////////////////

	private void clearDetailPanel() {
		txfRequestId.setText(StringUtil.Constants.EMPTY_STRING);
		txfSessionId.setText(StringUtil.Constants.EMPTY_STRING);
		txfSubmitted.setText(StringUtil.Constants.EMPTY_STRING);
		txfTransactionId.setText(StringUtil.Constants.EMPTY_STRING);
        txfUser.setText(StringUtil.Constants.EMPTY_STRING);
        txfConnectorBinding.setText(StringUtil.Constants.EMPTY_STRING);
		txaCommand.setText(StringUtil.Constants.EMPTY_STRING);
    }

	private void construct() {
		setLayout(new GridLayout(1, 1));
		splitter = new Splitter(JSplitPane.VERTICAL_SPLIT);
		splitter.setOneTouchExpandable(true);
		add(splitter);

        this.connectorMgr = ModelManager.getConnectorManager(this.connection);
		queryPanel = new QueryRequestPanel(this, connectorMgr);
		splitter.setTopComponent(queryPanel);
        
        
        Action cancelAction = queryPanel.getCancelAction();
        if (cancelAction != null) {
            MenuEntry menuEntry = new MenuEntry(MenuEntry.ACTION_MENUITEM, cancelAction);
            actions.add(menuEntry);
        }
        

		//
		// detail panel
		//
		pnlDetail = new JPanel(new GridBagLayout());
		splitter.setBottomComponent(new JScrollPane(pnlDetail));

        GridBagLayout requestLayout = new GridBagLayout();
		JPanel pnlRequest = new JPanel(requestLayout);
		TitledBorder bdr = new TitledBorder("Request"); //$NON-NLS-1$
		Font bdrFont = bdr.getTitleFont().deriveFont(Font.BOLD);
		bdr.setTitleFont(bdrFont);
		pnlRequest.setBorder(bdr);
		pnlDetail.add(pnlRequest, new GridBagConstraints(0, 0, 1, 1, 1.0, 0.0, 
                GridBagConstraints.CENTER, GridBagConstraints.HORIZONTAL, 
                new Insets(3, 3, 3, 3), 0, 0));

		//
		// row 1
		//
		LabelWidget lblRequestId = new LabelWidget(
                QueryTableModel.COLUMN_NAMES[QueryTableModel.REQUEST_ID_COL] + ':');
        pnlRequest.add(lblRequestId, new GridBagConstraints(0, 0, 1, 1, 0.0, 0.0,
                GridBagConstraints.EAST, GridBagConstraints.NONE, new Insets(2, 2, 2, 2), 0, 0));

		txfRequestId = new TextFieldWidget();
		txfRequestId.setEditable(false);
		pnlRequest.add(txfRequestId, new GridBagConstraints(1, 0, 1, 1, 1.0, 0.0,
                GridBagConstraints.CENTER, GridBagConstraints.HORIZONTAL, new Insets(2, 2, 2, 2),
                0, 0));

		LabelWidget lblUser = new LabelWidget(
                QueryTableModel.COLUMN_NAMES[QueryTableModel.USER_COL] + ':');
        pnlRequest.add(lblUser, new GridBagConstraints(2, 0, 1, 1, 0.0, 0.0,
                GridBagConstraints.EAST, GridBagConstraints.NONE, new Insets(2, 4, 2, 2), 0, 0));

		txfUser = new TextFieldWidget();
		txfUser.setEditable(false);
		pnlRequest.add(txfUser, new GridBagConstraints(3, 0, 1, 1, 1.0, 0.0,
                GridBagConstraints.CENTER, GridBagConstraints.HORIZONTAL, new Insets(2, 2, 2, 2),
                0, 0));

		//
		// row 2
		//
		LabelWidget lblSessionId = new LabelWidget(
                QueryTableModel.COLUMN_NAMES[QueryTableModel.SESSION_ID_COL] + ':');
		pnlRequest.add(lblSessionId, new GridBagConstraints(0, 1, 1, 1, 0.0, 0.0,
                GridBagConstraints.EAST, GridBagConstraints.NONE, new Insets(2, 2, 2, 2), 0, 0));

		txfSessionId = new TextFieldWidget();
		txfSessionId.setEditable(false);
		pnlRequest.add(txfSessionId, new GridBagConstraints(1, 1, 1, 1, 1.0, 0.0,
                GridBagConstraints.CENTER, GridBagConstraints.HORIZONTAL, new Insets(2, 2, 2, 2),
                0, 0));

        LabelWidget lblConnectorBinding = new LabelWidget(
                QueryTableModel.CONNECTOR_BINDING_HDR + ':');
        pnlRequest.add(lblConnectorBinding, new GridBagConstraints(2, 1, 1, 1, 0.0, 0.0,
                GridBagConstraints.EAST, GridBagConstraints.NONE, new Insets(2, 4, 2, 2), 0, 0));                                                                   
        
        txfConnectorBinding = new TextFieldWidget();
        txfConnectorBinding.setEditable(false);
        pnlRequest.add(txfConnectorBinding, new GridBagConstraints(3, 1, 1, 1, 1.0, 0.0,
                GridBagConstraints.CENTER, GridBagConstraints.HORIZONTAL, new Insets(2, 2, 2, 2),
                0, 0));

		//
		// row 3
		//
		LabelWidget lblTransactionId = new LabelWidget(QueryTableModel.TRANSACTION_ID_HDR + ':');
		pnlRequest.add(lblTransactionId, new GridBagConstraints(0, 2, 1, 1, 0.0, 0.0,
                GridBagConstraints.EAST, GridBagConstraints.NONE, new Insets(2, 2, 2, 2), 0, 0));
        
        txfTransactionId = new TextFieldWidget();
		txfTransactionId.setEditable(false);
		pnlRequest.add(txfTransactionId, new GridBagConstraints(1, 2, 1, 1, 1.0, 0.0, 
                GridBagConstraints.CENTER, GridBagConstraints.HORIZONTAL, new Insets(2, 2, 2, 2),
                0, 0));

        LabelWidget lblSubmitted = new LabelWidget(QueryTableModel.TIME_SUBMITTED_HDR + ':');
        pnlRequest.add(lblSubmitted, new GridBagConstraints(2, 2, 1, 1, 0.0, 0.0,
                GridBagConstraints.EAST, GridBagConstraints.NONE, new Insets(2, 4, 2, 2), 0, 0));                                                            

        txfSubmitted = new TextFieldWidget();
        txfSubmitted.setEditable(false);
        pnlRequest.add(txfSubmitted, new GridBagConstraints(3, 2, 1, 1, 1.0, 0.0,
                GridBagConstraints.CENTER, GridBagConstraints.HORIZONTAL, new Insets(2, 2, 2, 2),
                0, 0));
        
        //
		// command panel
		//
		JPanel pnlCommand = new JPanel(new GridLayout(1, 1));
		bdr = new TitledBorder("Command"); //$NON-NLS-1$
		bdr.setTitleFont(bdrFont);
		pnlCommand.setBorder(bdr);
		pnlDetail.add(pnlCommand, new GridBagConstraints(0, 1, 1, 1, 1.0, 1.0,
                GridBagConstraints.CENTER, GridBagConstraints.BOTH, new Insets(3, 3, 3, 3), 0, 0));                                                         

		txaCommand = new JTextArea();
		txaCommand.setLineWrap(true);
		txaCommand.setWrapStyleWord(true);
		txaCommand.setEditable(false);

		JScrollPane spnCommand = new JScrollPane(txaCommand);
		pnlCommand.add(spnCommand);
	}

	/**
	 * Main interface to this panel - supply a Request to be displayed. String.valueOf(request.getRequestID().getID())
	 * @param request Request to be displayed   String.valueOf(request.getSessionToken().getSessionID())
	 */
    public void displayQuery(RequestInfo request) {
        if (request != null) {
            txfRequestId.setText(String.valueOf(request.getRequestID()));
            SessionToken sessionToken = request.getSessionToken();
            String sessionIDStr = StringUtil.Constants.EMPTY_STRING;
            if (sessionToken != null) {
                sessionIDStr = sessionToken.getSessionIDValue();
            }
            txfSessionId.setText(sessionIDStr);
            Date date = request.getProcessingTimestamp();
			txfSubmitted.setText((date == null) ? StringUtil.Constants.EMPTY_STRING : 
                    FORMATTER.format(date));
			String transId = request.getTransactionId();
			txfTransactionId.setText((transId == null) ? StringUtil.Constants.EMPTY_STRING : transId);
            String userStr = StringUtil.Constants.EMPTY_STRING;
            if (sessionToken != null) {
                userStr = sessionToken.getUsername();
            }
			txfUser.setText(userStr);
            
            String bindingUUID = request.getConnectorBindingUUID();
            String bindingName = StringUtil.Constants.EMPTY_STRING;
            if ((bindingUUID != null) && (bindingUUID.trim().length() > 0)) {
                try {
                    ConnectorBinding cb = connectorMgr.getConnectorBindingByUUID(bindingUUID);
                    if (cb != null) {
                        bindingName = cb.getName();
                    }
                } catch (Exception ex) {
                    LogManager.logError(LogContexts.QUERIES, ex, 
                            "Error retrieving connector binding for UUID " + bindingUUID); //$NON-NLS-1$
                }
            }
            txfConnectorBinding.setText(bindingName);
            
			String command = request.getCommand();
			StringBuffer sql = new StringBuffer();
			sql.append(command.toString());
			txaCommand.setText(sql.toString());

		} else {
			clearDetailPanel();
		}
		pnlDetail.validate();
	}

	private QueryManager getQueryManager() {
		return ModelManager.getQueryManager(connection);
	}

	public String getTitle() {
		return "Queries"; //$NON-NLS-1$
	}

	public ConnectionInfo getConnection() {
		return connection;
	}

	public void modelChanged(ModelChangedEvent e) {
		refreshTable();
	}

	public void receiveUpdateNotification(RuntimeUpdateNotification notification) {
		//TODO
	}

	private void refreshImpl() {
		// setting stale causes a ModelChangedEvent to be generated
		// which does the refresh
		getQueryManager().setIsStale(true);
	}

	public void refresh() {
		// Add code here to test a QueryPanel-only busy flag...to prevent
		//  the refresh method from doing anything if the app is already involved
		//  in some time-consuming operation:
		refreshTable();

	}

	public void refreshTable() {
        final String selectedRequestID = txfRequestId.getText();
		try {
			StaticUtilities.startWait(ConsoleMainFrame.getInstance());
            final Collection allQueries = getQueryManager().getAllRequests();
            
            
            //refresh in the Swing thread
            Runnable runnable = new Runnable () {
                public void run() {
                    clearDetailPanel();
                    queryPanel.updateView(allQueries, selectedRequestID);
                }
            };
            StaticUtilities.invokeLaterSafe(runnable);
            
		} catch (ExternalException ee) {
            ExceptionUtility.showUnspecifiedFailureMessage(ConsolePlugin.Util.getString("QueryPanel.Could_not_refresh_query_table_1"), ee); //$NON-NLS-1$
        } catch (Throwable e) {
			ExceptionUtility.showUnspecifiedFailureMessage(ConsolePlugin.Util.getString("QueryPanel.Could_not_refresh_query_table_1"), e); //$NON-NLS-1$
		} finally {
			StaticUtilities.endWait(ConsoleMainFrame.getInstance());
		}
	}

	public java.util.List resume() {
		refresh();
		return actions;
	}

	/**
	 * Name must uniquely identify this Refreshable object.  Useful to support
	 * applying mods to the rate and enabled state by outside agencies.
	 */
	public String getName() {
		return StaticProperties.DATA_QUERY;
	}

	/**
	 * Turns the refresh feature on or off.
	 *
	 */
	public void setAutoRefreshEnabled(boolean b) {
		arRefresher.setAutoRefreshEnabled(b);
	}

	/**
	 * Sets the refresh rate.
	 *
	 */
	public void setRefreshRate(int iRate) {
		arRefresher.setRefreshRate(iRate);
	}

	/**
	 * Set the 'AutoRefresh' agent
	 *
	 */
	public void setAutoRefresher(AutoRefresher ar) {
		arRefresher = ar;
	}

	/**
	 * Get the 'AutoRefresh' agent
	 *
	 */
	public AutoRefresher getAutoRefresher() {
		return arRefresher;
	}

	public void paint(Graphics g) {
		if (!hasBeenPainted) {
			hasBeenPainted = true;
			splitter.setDividerLocation(INITIAL_SPLITTER_LOC);
		}
		super.paint(g);
	}
}
