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

import java.awt.BorderLayout;
import java.awt.GridBagLayout;
import java.awt.event.ActionEvent;
import java.awt.event.MouseAdapter;
import java.awt.event.MouseEvent;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Vector;

import javax.swing.Action;
import javax.swing.BorderFactory;
import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.JPopupMenu;
import javax.swing.JScrollPane;
import javax.swing.SwingUtilities;
import javax.swing.event.ListSelectionEvent;
import javax.swing.event.ListSelectionListener;

import com.metamatrix.common.config.api.ConnectorBinding;
import com.metamatrix.console.ConsolePlugin;
import com.metamatrix.console.connections.ConnectionInfo;
import com.metamatrix.console.models.ConnectorManager;
import com.metamatrix.console.models.ModelManager;
import com.metamatrix.console.security.UserCapabilities;
import com.metamatrix.console.ui.layout.BasePanel;
import com.metamatrix.console.ui.layout.ConsoleMainFrame;
import com.metamatrix.console.ui.util.AbstractPanelAction;
import com.metamatrix.console.ui.util.ColumnSortInfo;
import com.metamatrix.console.ui.util.property.PropertyProvider;
import com.metamatrix.console.util.ExternalException;
import com.metamatrix.console.util.StaticUtilities;
import com.metamatrix.dqp.message.RequestID;
import com.metamatrix.server.serverapi.RequestInfo;
import com.metamatrix.toolbox.ui.widget.DialogPanel;
import com.metamatrix.toolbox.ui.widget.DialogWindow;
import com.metamatrix.toolbox.ui.widget.LabelWidget;
import com.metamatrix.toolbox.ui.widget.TableWidget;
import com.metamatrix.toolbox.ui.widget.table.DefaultTableModel;
import com.metamatrix.toolbox.ui.widget.table.EnhancedTableColumn;

/**
 * Panel which displays a Collection of Request info objects in a
 *JTable.  Also exposes Actions to cancel querie requests or display properties
 *of a query request.
 */
public class QueryRequestPanel extends BasePanel {

	///////////////////////////////////////////////////////////////////////////
	// FIELDS
	///////////////////////////////////////////////////////////////////////////

	private QueryPanel caller;
    private ConnectorManager connectorManager;
	private ConnectionInfo connection = null;
	private TableWidget queryTable;
	private QueryTableModel tableModel;

	private HashMap /*<requestID display string to RequestInfo>*/ requestsMap = new HashMap();

	private CancelQueryRequestsAction actionCancel = null;
	private Vector actionsVec;

	///////////////////////////////////////////////////////////////////////////
	// CONSTRUCTORS
	///////////////////////////////////////////////////////////////////////////

	public QueryRequestPanel(QueryPanel cllr, ConnectorManager connMgr) {
		super();
		caller = cllr;
        this.connectorManager = connMgr;
		connection = caller.getConnection();
		createComponent();

		UserCapabilities cap = null;
		try {
			cap = UserCapabilities.getInstance();
		} catch (Exception ex) {
			//Cannot occur
		}
        
        actionsVec = new Vector(1);
        if (cap.canModifyQueries(connection)) {
            actionCancel = new CancelQueryRequestsAction();
            actionCancel.setEnabled(false);
            actionsVec.add(actionCancel);
        }
	}

	///////////////////////////////////////////////////////////////////////////
	// METHODS
	///////////////////////////////////////////////////////////////////////////

	public void createComponent() {
		// Create a Query TableModel and a JTable
		initializeTable();
		
		queryTable.setRowSelectionAllowed(true);
		queryTable.setColumnSelectionAllowed(false);
		queryTable.setSortable(true);
        EnhancedTableColumn requestIDCol = (EnhancedTableColumn)queryTable.getColumn(
                QueryTableModel.REQUEST_ID_HDR);
        queryTable.setColumnSortedAscending(requestIDCol);

		JScrollPane scrollPane = new JScrollPane(queryTable);

		this.setLayout(new BorderLayout());
		this.add(scrollPane, BorderLayout.CENTER);

		queryTable.getSelectionModel().addListSelectionListener(new ListSelectionListener() {
			public void valueChanged(ListSelectionEvent e) {
				if (!e.getValueIsAdjusting()) {
					int numSelections = getNumSelections();
                    
                    boolean enableCancel = (numSelections > 0);
                    if (actionCancel != null) {
                        actionCancel.setEnabled(enableCancel);
                    }
                    
                    
					if (numSelections == 1) {
						int selectionRow = queryTable.getSelectedRows()[0];
						String requestIDStr = (String)tableModel.getValueAt(
								queryTable.convertRowIndexToModel(selectionRow),
								tableModel.getRequestIdColumn());
						RequestInfo req = (RequestInfo) requestsMap.get(requestIDStr);
						caller.displayQuery(req);
					} else {
						// either 0 or more queries selected
						// clear out detail panel
						caller.displayQuery(null);
					}
				}
			}
		});

		queryTable.addMouseListener(new MouseAdapter() {
			public void mouseClicked(MouseEvent e) {
				maybePopup(e);
			}
			private void maybePopup(MouseEvent e) {
				if (SwingUtilities.isRightMouseButton(e)) {
					handlePopupTrigger(e);
				}
			}
		});
	}

	private JPopupMenu getActionsPopupMenu() {
		JPopupMenu menu = new JPopupMenu();
		Iterator it = actionsVec.iterator();
		while (it.hasNext()) {
			menu.add((Action) it.next());
		}
		return menu;
	}

	private void handlePopupTrigger(MouseEvent e) {
		try {
			if (UserCapabilities.getInstance().canModifyQueries(connection)) {
				getActionsPopupMenu().show(queryTable, e.getX() + 10, e.getY());
			}
		} catch (Exception ex) {
			//Cannot occur
		}
	}

	private int getNumSelections() {
		int[] rows = queryTable.getSelectedRows();
		return rows.length;
	}

	/**
	 * Cancels all selected queries, regardless of owner or session
	 * @see QueryRequestPanel#getCurrentSelections
	 */
	private void cancelSelectedQueries() throws ExternalException {
		ConfirmCancelPanel pnl = new ConfirmCancelPanel();
		DialogWindow.show(ConsoleMainFrame.getInstance(), ConsolePlugin.Util.getString("QueryRequestPanel.Cancel_Confirmation_2"), //$NON-NLS-1$
		pnl);
		if (pnl.getSelectedButton() == pnl.getAcceptButton()) {
			try {
				ModelManager.getQueryManager(connection).cancelQueryRequests(
					getCurrentSelections());
			} catch (Exception theException) {
				throw new ExternalException(theException);
			}
		}
	}

	/**
	 * @param newQueryRequests new Request collection that this
	 * panel should populate its view with.
	 */
	public void updateView(Collection /*<Request>*/ newQueryRequests,
            String selectedRequestID) {
        StaticUtilities.startWait(this);
		updateTheView(newQueryRequests);
		StaticUtilities.endWait(this);
        selectRowOfRequestID(selectedRequestID);
	}

    private void selectRowOfRequestID(String requestID) {
        if ((requestID != null) && (requestID.length() > 0)) {
            boolean found = false;
            int i = 0;
            int numRows = queryTable.getRowCount();
            while ((i < numRows) && (!found)) {
                String curRequestID = (String)tableModel.getValueAt(i, 
                        QueryTableModel.REQUEST_ID_COL);
                if (requestID.equals(curRequestID)) {
                    found = true;
                    int viewRow = queryTable.convertRowIndexToView(i);
                    queryTable.getSelectionModel().setSelectionInterval(viewRow, viewRow);
                } else {
                    i++;
                }
            }
        }
    }
    
	private void initializeTable() {
		tableModel = new QueryTableModel();
		this.queryTable = new TableWidget(tableModel, true);
		this.queryTable.setName("QueryTable"); //$NON-NLS-1$
		queryTable.getTableHeader().setName("QueryTable.header"); //$NON-NLS-1$
	}

	private void updateTheView(Collection /*<RequestInfo>*/ newRequests) {
        ColumnSortInfo[] tableColumnSortInfo = ColumnSortInfo.getTableColumnSortInfo(this.queryTable);
        Vector outerVec = new Vector(newRequests.size());
        Iterator iterator = newRequests.iterator();

		HashMap tempRequestsMap = new HashMap();

		// Populate the table data.
		RequestInfo q;
        while (iterator.hasNext()) {
			q = (RequestInfo) iterator.next();
            Object[] rowData = new Object[QueryTableModel.NUM_COLUMNS];
            String requestStr = getRequestInfoDisplayString(q);
			rowData[QueryTableModel.REQUEST_ID_COL] = requestStr;
			tempRequestsMap.put(requestStr, q);
			rowData[QueryTableModel.USER_COL] = q.getUserName();
			rowData[QueryTableModel.SESSION_ID_COL] = q.getSessionId();
            rowData[QueryTableModel.CONNECTOR_BINDING_COL] =
                    getConnectorBindingForUUID(q.getConnectorBindingUUID());
            Vector innerVec = StaticUtilities.arrayToVector(rowData);
            outerVec.add(innerVec);
        }
        
        //deselect rows, to avoid problems when re-populating rows
        queryTable.clearSelection();
        
        //clear and populate rows
        tableModel.setRows(outerVec);
       
        
		queryTable.sizeColumnsToFitData();
		requestsMap = tempRequestsMap;

		queryTable.requestFocus();
		queryTable.setEditable(false);
        ColumnSortInfo.setColumnSortOrder(tableColumnSortInfo, this.queryTable);
	}
    
       
    
    

    private String getRequestInfoDisplayString(RequestInfo q) {
        RequestID id = q.getRequestID();
        boolean isAtomicQuery = q.isAtomicQuery();
        String requestStr;
        if (isAtomicQuery) {
            requestStr = id.toString() + '.' + q.getNodeID();
        } else {
            requestStr = id.toString();
        }
        return requestStr;
    }
        
    private String getConnectorBindingForUUID(String connectorBindingUUID) {
        ConnectorBinding cb = null;
        String name = null;
        try {
            cb = connectorManager.getConnectorBindingByUUID(connectorBindingUUID);
        } catch (Exception ex) {
        }
        if (cb != null) {
            name = cb.getFullName();
        }
        return name;
    }
    
	/**
	 * Assemble a Collection of Requests that were previously
	 *selected, before an update - do this by getting each index that
	 *is selected from the table selection model, and using each index
	 *to get each Query ID from the table model, and using each ID to
	 *get an actual Request from the Query Manager
	 */
	public Collection /*<RequestInfo>*/ getCurrentSelections() {
		int[] selectedRows = queryTable.getSelectedRows();
		Collection queries = new ArrayList(selectedRows.length);
		for (int i = 0; i < selectedRows.length; i++) {
			String requestIDStr = (String) tableModel.getValueAt(
					queryTable.convertRowIndexToModel(selectedRows[i]),
					tableModel.getRequestIdColumn());
            RequestInfo ri = (RequestInfo)requestsMap.get(requestIDStr);
			queries.add(ri);
		}
		return queries;
	}

	/**
	 * @return the SortableTable instance used by this panel
	 */
	public TableWidget getQueryTable() {
		return queryTable;
	}

	/**
	 * @return the SortableTableModel instance used by the
	 *SortableTable instance used by this panel
	 */
	public DefaultTableModel getTableModel() {
		return tableModel;
	}

    public CancelQueryRequestsAction getCancelAction() {
        return actionCancel;
    }
    
    
	///////////////////////////////////////////////////////////////////////////
	// INNER CLASSES
	///////////////////////////////////////////////////////////////////////////

	public class CancelQueryRequestsAction extends AbstractPanelAction {
		public CancelQueryRequestsAction() {
			super(0);
			putValue(SHORT_DESCRIPTION, ConsolePlugin.Util.getString("QueryRequestPanel.Cancel_the_selected_queries_5")); //$NON-NLS-1$
			putValue(NAME, "Cancel"); //$NON-NLS-1$
		}
		protected void actionImpl(ActionEvent theEvent)
			throws ExternalException {
			cancelSelectedQueries();
		}
		protected void handleError(Exception theException) {
			CancelFailedPanel pnl = new CancelFailedPanel();
			DialogWindow.show(ConsoleMainFrame.getInstance(), ConsolePlugin.Util.getString("QueryRequestPanel.Cancel_Query_Error_7"), //$NON-NLS-1$
			pnl);
		}
	}

	private class CancelFailedPanel extends DialogPanel {

		public CancelFailedPanel() {
			super();
			JPanel pnl = new JPanel(new GridBagLayout());
			pnl.setBorder(BorderFactory.createEmptyBorder(30, 40, 30, 40));

				JLabel lbl = new LabelWidget(ConsolePlugin.Util.getString("QueryRequestPanel.At_least_one_query_failed_to_cancel._n_8") + //$NON-NLS-1$
	ConsolePlugin.Util.getString("QueryRequestPanel.Refresh_to_get_an_updated_list_of_active_queries._9")); //$NON-NLS-1$
			lbl.setIcon(PropertyProvider.getDefault().getIcon("icon.info")); //$NON-NLS-1$
			pnl.add(lbl);
			setContent(pnl);
			removeNavigationButton(getCancelButton());
		}

	}

	private class ConfirmCancelPanel extends DialogPanel {

		public ConfirmCancelPanel() {
			super();
			JPanel pnl = new JPanel(new GridBagLayout());
			pnl.setBorder(BorderFactory.createEmptyBorder(30, 40, 30, 40));

			int rows = getNumSelections();
				JLabel lbl = new LabelWidget((getNumSelections() > 1) ? ConsolePlugin.Util.getString("QueryRequestPanel.Are_you_sure_you_want_to_cancel_the__11") + rows + ConsolePlugin.Util.getString("QueryRequestPanel._selected_queries__12") //$NON-NLS-1$ //$NON-NLS-2$
	: ConsolePlugin.Util.getString("QueryRequestPanel.Are_you_sure_you_want_to_cancel_the_selected_query__13")); //$NON-NLS-1$
			lbl.setIcon(PropertyProvider.getDefault().getIcon("icon.warning")); //$NON-NLS-1$
			pnl.add(lbl);
			setContent(pnl);
		}

	}

}
