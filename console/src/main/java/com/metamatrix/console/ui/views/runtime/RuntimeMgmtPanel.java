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

//#############################################################################
package com.metamatrix.console.ui.views.runtime;

import java.awt.Component;
import java.awt.Dimension;
import java.awt.Graphics;
import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.awt.GridLayout;
import java.awt.Insets;
import java.awt.Point;
import java.awt.event.ActionEvent;
import java.awt.event.MouseAdapter;
import java.awt.event.MouseEvent;
import java.awt.event.MouseListener;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import javax.swing.JPanel;
import javax.swing.JPopupMenu;
import javax.swing.JScrollPane;
import javax.swing.JSplitPane;
import javax.swing.JTabbedPane;
import javax.swing.JTable;
import javax.swing.ListSelectionModel;
import javax.swing.SwingUtilities;
import javax.swing.border.CompoundBorder;
import javax.swing.event.TreeModelEvent;
import javax.swing.event.TreeModelListener;
import javax.swing.event.TreeSelectionEvent;
import javax.swing.event.TreeSelectionListener;
import javax.swing.tree.TreePath;
import javax.swing.tree.TreeSelectionModel;

import com.metamatrix.common.log.LogManager;
import com.metamatrix.console.connections.ConnectionInfo;
import com.metamatrix.console.connections.ConnectionProcessor;
import com.metamatrix.console.models.ModelManager;
import com.metamatrix.console.models.RuntimeMgmtManager;
import com.metamatrix.console.notification.RuntimeUpdateNotification;
import com.metamatrix.console.security.UserCapabilities;
import com.metamatrix.console.ui.layout.ConsoleMainFrame;
import com.metamatrix.console.ui.layout.MenuEntry;
import com.metamatrix.console.ui.layout.WorkspacePanel;
import com.metamatrix.console.ui.util.AbstractPanelAction;
import com.metamatrix.console.ui.views.runtime.model.HostStatistics;
import com.metamatrix.console.ui.views.runtime.model.RuntimeMgmtModel;
import com.metamatrix.console.ui.views.runtime.model.StatisticsConstants;
import com.metamatrix.console.ui.views.runtime.util.RuntimeMgmtUtils;
import com.metamatrix.console.ui.views.runtime.util.ServiceStateConstants;
import com.metamatrix.console.util.AutoRefreshable;
import com.metamatrix.console.util.AutoRefresher;
import com.metamatrix.console.util.ExceptionUtility;
import com.metamatrix.console.util.ExternalException;
import com.metamatrix.console.util.LogContexts;
import com.metamatrix.console.util.StaticTreeUtilities;
import com.metamatrix.console.util.StaticUtilities;
import com.metamatrix.platform.admin.api.runtime.HostData;
import com.metamatrix.platform.admin.api.runtime.PSCData;
import com.metamatrix.platform.admin.api.runtime.ProcessData;
import com.metamatrix.platform.admin.api.runtime.ServiceData;
import com.metamatrix.toolbox.ui.widget.DialogWindow;
import com.metamatrix.toolbox.ui.widget.LabelWidget;
import com.metamatrix.toolbox.ui.widget.Splitter;
import com.metamatrix.toolbox.ui.widget.TableWidget;
import com.metamatrix.toolbox.ui.widget.TextFieldWidget;
import com.metamatrix.toolbox.ui.widget.TitledBorder;
import com.metamatrix.toolbox.ui.widget.TreeWidget;
import com.metamatrix.toolbox.ui.widget.menu.DefaultPopupMenuFactory;
import com.metamatrix.toolbox.ui.widget.table.DefaultTableModel;
import com.metamatrix.toolbox.ui.widget.tree.DefaultTreeNode;

/**
 * @version 1.0
 * @author Dan Florian
 */
public final class RuntimeMgmtPanel

    extends JPanel
    implements OperationsDelegate,
               StatisticsConstants,
               ServiceStateConstants,
               TreeModelListener,
               TreeSelectionListener,
               WorkspacePanel,
               AutoRefreshable {

    ///////////////////////////////////////////////////////////////////////////
    // CONSTANTS
    ///////////////////////////////////////////////////////////////////////////

    private static /*final*/ String[] PROC_STATS_HDRS;
    private static /*final*/ String[] SERV_STATS_HDRS;
    private static final double SPLIT_INIT = RuntimeMgmtUtils.getInt("split.init_location", 70) / 100D; //$NON-NLS-1$
    private TreeMenuFactory servicePopQueuesTree = null;
    private TreeMenuFactory servicePopQueueTree = null;
    private TreeMenuFactory processPopupTree = null;
    private boolean iSSyncing = false;
    private Object mutex = new Object();
    private AutoRefresher arRefresher;
    
    ///////////////////////////////////////////////////////////////////////////
    // INITIALIZER
    ///////////////////////////////////////////////////////////////////////////

    static {
        PROC_STATS_HDRS = new String[NUM_PROCESS_STATS];
        PROC_STATS_HDRS[PROC_HOST_INDEX] = getString("prochost.hdr"); //$NON-NLS-1$
        PROC_STATS_HDRS[TOTAL_PROCS_INDEX] = getString("totalprocs.hdr"); //$NON-NLS-1$
        PROC_STATS_HDRS[SYNCHED_PROCS_INDEX] = getString("synchedprocs.hdr"); //$NON-NLS-1$
        PROC_STATS_HDRS[NOT_REGISTERED_PROCS_INDEX] = getString("notregisteredprocs.hdr"); //$NON-NLS-1$
        PROC_STATS_HDRS[NOT_DEPLOYED_PROCS_INDEX] = getString("notdeployedprocs.hdr"); //$NON-NLS-1$

        SERV_STATS_HDRS = new String[NUM_SERV_STATS];
        SERV_STATS_HDRS[SERV_HOST_INDEX] = getString("servhost.hdr"); //$NON-NLS-1$
        SERV_STATS_HDRS[TOTAL_SERVS_INDEX] = getString("totalservs.hdr"); //$NON-NLS-1$
        SERV_STATS_HDRS[RUNNING_INDEX] = getString("runningservs.hdr"); //$NON-NLS-1$
        SERV_STATS_HDRS[SYNCHED_SERVS_INDEX] = getString("synchedservs.hdr"); //$NON-NLS-1$
        SERV_STATS_HDRS[NOT_REGISTERED_SERVS_INDEX] = getString("notregisteredservs.hdr"); //$NON-NLS-1$
        SERV_STATS_HDRS[NOT_DEPLOYED_SERVS_INDEX] = getString("notdeployedservs.hdr"); //$NON-NLS-1$
        SERV_STATS_HDRS[FAILED_INDEX] = getString("failedservs.hdr"); //$NON-NLS-1$
        SERV_STATS_HDRS[STOPPED_INDEX] = getString("stoppedservs.hdr"); //$NON-NLS-1$
        SERV_STATS_HDRS[INIT_FAILED_INDEX] = getString("initfailedservs.hdr"); //$NON-NLS-1$
        SERV_STATS_HDRS[NOT_INIT_INDEX] = getString("notinitservs.hdr"); //$NON-NLS-1$
        SERV_STATS_HDRS[DATA_SOURCE_UNAVAILABLE_INDEX] = getString("datasourceunavailableservs.hdr"); //$NON-NLS-1$
    }

    ///////////////////////////////////////////////////////////////////////////
    // CONTROLS
    ///////////////////////////////////////////////////////////////////////////

    private TreeWidget tree;
    private TreeMenuFactory popTree;
    private TableWidget tblProcStats;
    private TableWidget tblServStats;
    private TextFieldWidget txfLastChange;
    private OperationsPanel pnlOps;
    private ServiceMgmtPanel pnlServMgmt;
    private ProcessMgmtPanel pnlProcMgmt;

    ///////////////////////////////////////////////////////////////////////////
    // FIELDS
    ///////////////////////////////////////////////////////////////////////////

	private ConnectionInfo connection;
	//private RuntimeMgmtManager manager;
    private ArrayList actions = new ArrayList();
    private PanelAction actionShutdown = new PanelAction(PanelAction.SHUTDOWN);
    private PanelAction actionBounce = new PanelAction(PanelAction.BOUNCE);
    private PanelAction actionSynch = new PanelAction(PanelAction.SYNCH);
    private DefaultTableModel procsTblModel;
    private DefaultTableModel servsTblModel;
    private QueueStatisticsDisplayHandler qsdh;
    private VMStatisticsDisplayHandler vmdh;
    private QueueStatisticsRefreshRequestHandlerImp qsrh;
    private RuntimeMgmtModel treeModel;
    private HashMap serviceHM;
    private ArrayList serviceList;

    private MouseListener mouseListener = new MouseAdapter() {
        public void mouseReleased(MouseEvent theEvent) {
            JTable tbl = ((JTable)theEvent.getSource());
            Point p = new Point(theEvent.getX(), theEvent.getY());
            int row = tbl.rowAtPoint(p);
            int col = tbl.convertColumnIndexToModel(tbl.columnAtPoint(p));
            // host is currently in the same column in both tables
            // but they may not always be the same (but I doubt it)
            if ((col != PROC_HOST_INDEX) && (col != PROC_HOST_INDEX)) {
                if (theEvent.getClickCount() == 2) {
                    if (tbl == tblProcStats) {
                        showProcessDetails(row, col);
                    } else {
                        showServiceDetails(row, col);
                    }
                }
            }
        }
    };
    private Map hostStats;
    private Object lastSelectedObj;
    private boolean disabled = false;

    ///////////////////////////////////////////////////////////////////////////
    // CONSTRUCTORS
    ///////////////////////////////////////////////////////////////////////////

    public RuntimeMgmtPanel(ConnectionInfo conn)
    		throws ExternalException {
    	super();
    	this.connection = conn;
    	setLayout(new GridBagLayout());
        setBorder(RuntimeMgmtUtils.EMPTY_BORDER);
        construct();
		try {
            if (!UserCapabilities.getInstance().canPerformRuntimeOperations(
            		connection)) {
                setActionsDisabled();
                pnlOps.setActionsDisabled();
                disabled = true;
            }
        } catch (Exception theException) {
            throw new ExternalException("RuntimeMgmtPanel:init", theException); //$NON-NLS-1$
        }
    }

    ///////////////////////////////////////////////////////////////////////////
    // METHODS
    ///////////////////////////////////////////////////////////////////////////
    
    private RuntimeMgmtManager getRuntimeMgmtManager() {
        return ModelManager.getRuntimeMgmtManager(this.connection);
    }

    private void bounce()
        throws ExternalException {

        ConfirmationPanel pnlConfirm = new ConfirmationPanel("dlg.bounceserver.msg"); //$NON-NLS-1$
        DialogWindow.show(this, getString("dlg.bounceserver.title"), pnlConfirm); //$NON-NLS-1$
        if (pnlConfirm.isConfirmed()) {
            StaticUtilities.startWait(this);
            getRuntimeMgmtManager().bounceServer();
            
            StaticUtilities.endWait(this);
            // let the configuration manager know they need to refresh
            ModelManager.getConfigurationManager(connection).setRefreshNeeded();
        }
    }

    private void construct()
        throws ExternalException {

        JPanel pnlLastChange = new JPanel();
        GridBagConstraints gbc = new GridBagConstraints();
        gbc.gridx = 0;
        gbc.gridy = 0;
        gbc.insets = new Insets(3, 3, 3, 3);
        gbc.anchor = GridBagConstraints.EAST;
        add(pnlLastChange, gbc);

        LabelWidget lblLastChange = new LabelWidget(getString("lblLastChange")); //$NON-NLS-1$
        pnlLastChange.add(lblLastChange);

        txfLastChange = RuntimeMgmtUtils.createTextField("timestamp"); //$NON-NLS-1$
        txfLastChange.setEditable(false);
        pnlLastChange.add(txfLastChange);

        Splitter split = new Splitter(JSplitPane.VERTICAL_SPLIT) {
            boolean init = false;
            public void paint(Graphics theGraphics) {
                if (!init) {
                    setDividerLocation(SPLIT_INIT);
                }
                init = true;
                super.paint(theGraphics);
            }
        };
        split.setOneTouchExpandable(true);
        gbc.gridx = 0;
        gbc.gridy = 1;
        gbc.fill = GridBagConstraints.BOTH;
        gbc.weightx = 1.0;
        gbc.weighty = 1.0;
        add(split, gbc);

        JPanel pnlTree = new JPanel(new GridBagLayout());
        GridBagConstraints gbcTreePanel = new GridBagConstraints();
        pnlTree.setMinimumSize(new Dimension(0, 0));
        TitledBorder tBorder = new TitledBorder(getString("pnlTree.title")); //$NON-NLS-1$
        pnlTree.setBorder(new CompoundBorder(tBorder , 
        		RuntimeMgmtUtils.EMPTY_BORDER));
        split.setTopComponent(pnlTree);

        tree = new TreeWidget() {
            public String getToolTipText(MouseEvent theEvent) {
                TreePath path =
                    tree.getPathForLocation(theEvent.getX(), theEvent.getY());
                if (path == null) return null;
                DefaultTreeNode node =
                    (DefaultTreeNode)path.getLastPathComponent();
                Object userObj = node.getContent();
                if (userObj instanceof ServiceData) {
                    return RuntimeMgmtUtils.getServiceStateToolTip(
                    		(ServiceData)userObj);
                }
                return null;
            }
        };
        tree.setVisibleRowCount(RuntimeMgmtUtils.getInt("tree.rows", 15)); //$NON-NLS-1$
        tree.setRootVisible(false);
        tree.setShowsRootHandles(true);
        tree.setMinimumSize(tree.getPreferredSize());
        tree.setCellRenderer(new RuntimeStateCellRenderer());
        tree.getSelectionModel().setSelectionMode(
            	TreeSelectionModel.SINGLE_TREE_SELECTION);
        tree.getSelectionModel().addTreeSelectionListener(this);
        tree.setPopupMenuFactory(popTree);
        treeModel = new RuntimeMgmtModel(connection);
        tree.setModel(treeModel);
        treeModel.addTreeModelListener(this);

        JScrollPane spnTree = new JScrollPane(tree);
        gbcTreePanel.gridx = 0;
        gbcTreePanel.gridy = 0;
        gbcTreePanel.anchor = GridBagConstraints.WEST;
        gbcTreePanel.fill = GridBagConstraints.BOTH;
        gbcTreePanel.weightx = 0.9;
        gbcTreePanel.weighty = 0.9;
        pnlTree.add(spnTree, gbcTreePanel);

        pnlOps = new OperationsPanel(this);
        gbcTreePanel.gridx = 1;
        gbcTreePanel.fill = GridBagConstraints.NONE;
        gbcTreePanel.weightx = 0.1;
        gbcTreePanel.weighty = 0.1;
        gbcTreePanel.insets = new Insets(5, 5, 5, 5);
        pnlTree.add(pnlOps, gbcTreePanel);

        JPanel pnlStats = new JPanel(new GridLayout(1, 1));
        pnlStats.setMinimumSize(new Dimension(0, 0));
        tBorder = new TitledBorder(getString("pnlStats.title")); //$NON-NLS-1$
        pnlStats.setBorder(
            	new CompoundBorder(tBorder, RuntimeMgmtUtils.EMPTY_BORDER));


        split.setBottomComponent(pnlStats);

        JTabbedPane tpnStats = new JTabbedPane();
        pnlStats.add(tpnStats);

        tblProcStats = new TableWidget();
        procsTblModel = (DefaultTableModel)tblProcStats.getModel();
        procsTblModel.setColumnIdentifiers(PROC_STATS_HDRS);
        tblProcStats.setEditable(false);
        tblProcStats.setPreferredScrollableViewportSize(
            	new Dimension(tblProcStats.getPreferredSize().width,
                getInt("statstblrows", 10) * tblProcStats.getRowHeight())); //$NON-NLS-1$
        tblProcStats.setCellSelectionEnabled(true);
        tblProcStats.setAutoResizeMode(JTable.AUTO_RESIZE_ALL_COLUMNS);
        tblProcStats.setSortable(true);
        tblProcStats.getSelectionModel().setSelectionMode(
            	ListSelectionModel.SINGLE_SELECTION);
        tblProcStats.addMouseListener(mouseListener);

        JScrollPane spnProcStats = new JScrollPane(tblProcStats);
        tpnStats.addTab(getString("processstats.tab"), spnProcStats); //$NON-NLS-1$

        tblServStats = new TableWidget();
        servsTblModel = (DefaultTableModel)tblServStats.getModel();
        servsTblModel.setColumnIdentifiers(SERV_STATS_HDRS);
        tblServStats.setEditable(false);
        tblServStats.setPreferredScrollableViewportSize(
            	new Dimension(tblServStats.getPreferredSize().width,
                getInt("statstblrows", 10) * tblServStats.getRowHeight())); //$NON-NLS-1$
        tblServStats.setCellSelectionEnabled(true);
        tblServStats.setAutoResizeMode(JTable.AUTO_RESIZE_ALL_COLUMNS);
        tblServStats.setSortable(true);
        tblServStats.getSelectionModel().setSelectionMode(
            	ListSelectionModel.SINGLE_SELECTION);
        tblServStats.addMouseListener(mouseListener);

        JScrollPane spnServStats = new JScrollPane(tblServStats);
        tpnStats.addTab(getString("servicestats.tab"), spnServStats); //$NON-NLS-1$
        qsrh = new QueueStatisticsRefreshRequestHandlerImp(connection);
        qsdh = new QueueStatisticsDisplayHandler(qsrh);
	    vmdh = new VMStatisticsDisplayHandler(qsrh);
	    qsrh.setDisplayVMHandler(vmdh); 
        qsrh.setDisplayHandler(qsdh);
        qsdh.setOperationsPnl(pnlOps);
        vmdh.setOperationsPnl(pnlOps);
        // add actions not associated with buttons (only menuitems)
        actions.add(new MenuEntry(MenuEntry.ACTION_MENUITEM, actionBounce));
        actions.add(new MenuEntry(MenuEntry.ACTION_MENUITEM, actionShutdown));
        actions.add(new MenuEntry(MenuEntry.ACTION_MENUITEM, actionSynch));
        pnlOps.setVisibleService(false,0, false);
        popTree = new TreeMenuFactory(tree, pnlOps.getActions());
        tree.setPopupMenuFactory(popTree);

        arRefresher = new AutoRefresher(this, 15, true, connection);
        arRefresher.init();
        arRefresher.startTimer();
    }

    public static int getInt(String theKey, int theDefault) {
        return RuntimeMgmtUtils.getInt(theKey, theDefault);
    }

    private DefaultTreeNode getSelectedNode() {
        TreePath path = tree.getSelectionPath();
        return (path == null) ? null : (DefaultTreeNode)path.getLastPathComponent();
    }

    private static String getString(String theKey) {
        return RuntimeMgmtUtils.getString(theKey);
    }
    
    private static String getString(String theKey, Object[] value) {
        return RuntimeMgmtUtils.getString(theKey, value);
    }    

    public String getTitle() {
        return getString("systemstatetitle"); //$NON-NLS-1$

    }

	public ConnectionInfo getConnection() {
		return connection;
	}
	
    public void refresh() {
        treeModel.refresh();
    }

    public List resume() {
        DefaultTreeNode node = getSelectedNode();
        Object userObj;
        if (node != null) {
            userObj = node.getContent();
            lastSelectedObj = userObj;
        }
        refresh();

        List clone = (List)actions.clone();
        List opsActions = pnlOps.getActions();
        if (!opsActions.isEmpty()) {
            clone.add(MenuEntry.DEFAULT_SEPARATOR);
            clone.addAll(opsActions);
        }
        return clone;
    }

  
    private void selectNode(DefaultTreeNode theNode) {
        ArrayList pathNodes = new ArrayList();
        pathNodes.add(theNode);
        DefaultTreeNode parent = theNode.getParent();
        while (parent != null) {
            pathNodes.add(parent);
            parent = parent.getParent();
        }
        Collections.reverse(pathNodes);
        TreePath path = new TreePath(pathNodes.toArray());
        tree.setSelectionPath(path);
        tree.scrollRowToVisible(tree.getRowForPath(path));
    }

    private void setActionsDisabled() {
        actionShutdown.setEnabled(false);
        actionBounce.setEnabled(false);
        actionSynch.setEnabled(false);
    }

    private void setLastChangeDate(Map theStats) {
        SimpleDateFormat formatter = StaticUtilities.getDefaultDateFormat();
        txfLastChange.setText( formatter.format(treeModel.getLastChangeDate()) );
    }

    private void showProcessDetails(
        int theRow,
        int theColumn) {

        HostData host =
            (HostData)tblProcStats.getValueAt(theRow, PROC_HOST_INDEX);
        HostStatistics stats = (HostStatistics)hostStats.get(host);
        List procs = null;
        String title = ""; //$NON-NLS-1$
        if (theColumn == SYNCHED_PROCS_INDEX) {
            procs = stats.getSynchedProcesses();
            title = getString("sm.title.synched"); //$NON-NLS-1$
        } else if (theColumn == NOT_REGISTERED_PROCS_INDEX) {
            procs = stats.getNotRegisteredProcesses();
            title = getString("sm.title.notregistered"); //$NON-NLS-1$
        } else if (theColumn == NOT_DEPLOYED_PROCS_INDEX) {
            procs = stats.getNotDeployedProcesses();
            title = getString("sm.title.notdeployed"); //$NON-NLS-1$
        } else if (theColumn == TOTAL_PROCS_INDEX) {
            procs = stats.getProcesses();
            title = getString("sm.title.all"); //$NON-NLS-1$
        }
        if (!procs.isEmpty()) {
            try {
                if (pnlProcMgmt == null) {
                	RuntimeMgmtManager manager = 
                			ModelManager.getRuntimeMgmtManager(connection);
                    pnlProcMgmt = new ProcessMgmtPanel(manager);
                    if (disabled) {
                        pnlProcMgmt.setActionsDisabled();
                    }
                }
                pnlProcMgmt.load(procs);
                DialogWindow.show(
                    this,
                    RuntimeMgmtUtils.getString("pm.title", //$NON-NLS-1$
                    		new Object[] {title, host}),
                    pnlProcMgmt);
            } catch (Exception theException) {
                ExceptionUtility.showMessage(RuntimeMgmtUtils.getString(
                		"msg.configmgrproblem", //$NON-NLS-1$
                        new Object[] {getClass().getName(),
                        "showProcessDetails"}), //$NON-NLS-1$
                    	theException.getMessage(),
                    	theException);
                LogManager.logError(LogContexts.RUNTIME,
                		theException,
                        getClass().getName() + "showProcessDetails"); //$NON-NLS-1$
            }
        }
    }



    private void showServiceDetails(int theRow, int theColumn) {
		HostData host =
            	(HostData)tblProcStats.getValueAt(theRow, PROC_HOST_INDEX);
        HostStatistics stats = (HostStatistics)hostStats.get(host);
        List servs = null;
        String title = ""; //$NON-NLS-1$
        if (theColumn == RUNNING_INDEX) {
            servs = stats.getSynchedServices();
            title = getString("sm.title.running"); //$NON-NLS-1$
        } else if (theColumn == SYNCHED_SERVS_INDEX) {
            servs = stats.getSynchedServices();
            title = getString("sm.title.synched"); //$NON-NLS-1$
        } else if (theColumn == NOT_REGISTERED_SERVS_INDEX) {
            servs = stats.getNotRegisteredServices();
            title = getString("sm.title.notregistered"); //$NON-NLS-1$
        } else if (theColumn == NOT_DEPLOYED_SERVS_INDEX) {
            servs = stats.getNotDeployedServices();
            title = getString("sm.title.notdeployed"); //$NON-NLS-1$
        } else if (theColumn == FAILED_INDEX) {
            servs = stats.getFailedServices();
            title = getString("sm.title.failed"); //$NON-NLS-1$
        } else if (theColumn == STOPPED_INDEX) {
            servs = stats.getStoppedServices();
            title = getString("sm.title.stopped"); //$NON-NLS-1$
        } else if (theColumn == INIT_FAILED_INDEX) {
            servs = stats.getInitFailedServices();
            title = getString("sm.title.initfailed"); //$NON-NLS-1$
        } else if (theColumn == NOT_INIT_INDEX) {
            servs = stats.getNotInitServices();
            title = getString("sm.title.notstarted"); //$NON-NLS-1$
        } else if (theColumn == TOTAL_SERVS_INDEX) {
            servs = stats.getServices();
            title = getString("sm.title.all"); //$NON-NLS-1$
        }
        if (!servs.isEmpty()) {
            if (pnlServMgmt == null) {
                pnlServMgmt = new ServiceMgmtPanel(getRuntimeMgmtManager());
                if (disabled) {
                    pnlServMgmt.setActionsDisabled();
                }
            }
            pnlServMgmt.load(servs);
            DialogWindow.show(
                this,
                RuntimeMgmtUtils.getString("sm.title", //$NON-NLS-1$
						new Object[] {title, host}),
                pnlServMgmt);
        }
    }

    private void shutdown()
        throws ExternalException {

        ConfirmationPanel pnlConfirm = new ConfirmationPanel("dlg.shutdown.msg"); //$NON-NLS-1$
        DialogWindow.show(this, getString("dlg.shutdown.title"), pnlConfirm); //$NON-NLS-1$
        if (pnlConfirm.isConfirmed()) {
            ConnectionInfo connection = getConnection();
            getRuntimeMgmtManager().shutdownServer();
            ModelManager.removeConnectionLo(connection);
            if (ModelManager.getNumberofConnections() < 1) {
                ConsoleMainFrame.getInstance().exitConsole();
            } else {
                ConnectionProcessor cp = ConnectionProcessor.getInstance();
                cp.removeConnection(connection);
            }
        }
    }

    public void startOperation()
        throws ExternalException {
		DefaultTreeNode node = getSelectedNode();
        Object userObj = node.getContent();
        lastSelectedObj = userObj;

        if (userObj instanceof ServiceData) {
            getRuntimeMgmtManager().startService((ServiceData)userObj);
        } else if (userObj instanceof ProcessData) {
            getRuntimeMgmtManager().startProcess((ProcessData)userObj);
        } else if (userObj instanceof PSCData) {
            getRuntimeMgmtManager().startPsc((PSCData)userObj);
        } else if (userObj instanceof HostData) {
            getRuntimeMgmtManager().startHost((HostData)userObj);
        }
    }

    public void stopOperation() throws ExternalException {

        String msgId = null;
        String titleId = null;
        DefaultTreeNode node = getSelectedNode();
        Object userObj = node.getContent();
        lastSelectedObj = userObj;

        if (userObj instanceof ServiceData) {
            msgId = "dlg.stopservice.msg"; //$NON-NLS-1$
            titleId = "dlg.stopservice.title"; //$NON-NLS-1$
        } else if (userObj instanceof ProcessData) {
            msgId = "dlg.stopprocess.msg"; //$NON-NLS-1$
            if (procsTblModel.getRowCount() == 1) {
                Number number = (Number) procsTblModel.getValueAt(0, TOTAL_PROCS_INDEX);
                if (number.intValue() == 1) {
                    msgId = "dlg.stoponlyprocess.msg"; //$NON-NLS-1$
                }
            }
            titleId = "dlg.stopprocess.title"; //$NON-NLS-1$
        } else if (userObj instanceof PSCData) {
            msgId = "dlg.stoppsc.msg"; //$NON-NLS-1$
            titleId = "dlg.stoppsc.title"; //$NON-NLS-1$
        } else if (userObj instanceof HostData) {
            if (procsTblModel.getRowCount() == 1) {
                msgId = "dlg.stoponlyhost.msg"; //$NON-NLS-1$
            } else {           
                msgId = "dlg.stophost.msg"; //$NON-NLS-1$
            }           
            titleId = "dlg.stophost.title"; //$NON-NLS-1$
        }
        
        

        ConfirmationPanel pnlConfirm = new ConfirmationPanel(msgId, new Object[] {userObj});
        DialogWindow.show(this, getString(titleId, new Object[] {userObj} ), pnlConfirm);
        if (pnlConfirm.isConfirmed()) {
            try {
                StaticUtilities.startWait(this);
                if (userObj instanceof ServiceData) {
                    getRuntimeMgmtManager().stopService((ServiceData)userObj);
                } else if (userObj instanceof ProcessData) {
                    getRuntimeMgmtManager().stopProcess((ProcessData)userObj);
                } else if (userObj instanceof PSCData) {
                    getRuntimeMgmtManager().stopPsc((PSCData)userObj);
                } else if (userObj instanceof HostData) {
                    getRuntimeMgmtManager().stopHost((HostData)userObj);
                }
            } finally {
                StaticUtilities.endWait(this);
            }
        }
    }

    public void stopNowOperation() throws ExternalException {
        String msgId = null;
        String titleId = null;
        DefaultTreeNode node = getSelectedNode();
        Object userObj = node.getContent();
        lastSelectedObj = userObj;

        if (userObj instanceof ServiceData) {
            msgId = "dlg.stopnowservice.msg"; //$NON-NLS-1$
            titleId = "dlg.stopnowservice.title"; //$NON-NLS-1$
        } else if (userObj instanceof ProcessData) {
            msgId = "dlg.stopnowprocess.msg"; //$NON-NLS-1$
            if (procsTblModel.getRowCount() == 1) {
                Number number = (Number) procsTblModel.getValueAt(0, TOTAL_PROCS_INDEX);
                if (number.intValue() == 1) {
                    msgId = "dlg.stopnowonlyprocess.msg"; //$NON-NLS-1$
                }
            }
            titleId = "dlg.stopnowprocess.title"; //$NON-NLS-1$
        } else if (userObj instanceof PSCData) {
            msgId = "dlg.stopnowpsc.msg"; //$NON-NLS-1$
            titleId = "dlg.stopnowpsc.title"; //$NON-NLS-1$
        } else if (userObj instanceof HostData) {
            if (procsTblModel.getRowCount() == 1) {
                msgId = "dlg.stopnowonlyhost.msg"; //$NON-NLS-1$
            } else {           
                msgId = "dlg.stopnowhost.msg"; //$NON-NLS-1$
            }           
            titleId = "dlg.stopnowhost.title"; //$NON-NLS-1$
        }
        
        

        ConfirmationPanel pnlConfirm =
            new ConfirmationPanel(msgId, new Object[] {userObj});
        DialogWindow.show(this, getString(titleId, new Object[] {userObj}), pnlConfirm);
        if (pnlConfirm.isConfirmed()) {
            try {
                StaticUtilities.startWait(this);
                if (userObj instanceof ServiceData) {
                    getRuntimeMgmtManager().stopServiceNow((ServiceData)userObj);
                } else if (userObj instanceof ProcessData) {
                    getRuntimeMgmtManager().stopProcessNow((ProcessData)userObj);
                } else if (userObj instanceof PSCData) {
                    getRuntimeMgmtManager().stopPscNow((PSCData)userObj);
                } else if (userObj instanceof HostData) {
                    getRuntimeMgmtManager().stopHostNow((HostData)userObj);
                }
            } finally {
                StaticUtilities.endWait(this);
            }
        }
    }
    
    /**
     *  
     * @see com.metamatrix.console.ui.views.runtime.OperationsDelegate#showServcieError()
     * @since 4.4
     */
    public void showServcieError() throws ExternalException {
        Throwable theError = null;
        String titleId = null;
        DefaultTreeNode node = getSelectedNode();
        Object userObj = node.getContent();
        lastSelectedObj = userObj;

        if (userObj != null && userObj instanceof ServiceData) {
            ServiceData data = (ServiceData) userObj;
            theError = data.getInitError();
            if (theError != null) {
                titleId = "dlg.showserviceError.title"; //$NON-NLS-1$
                String errorMsg = theError.getMessage();
                if (errorMsg == null || errorMsg.length() == 0) {
                    errorMsg = "Error message was null."; //$NON-NLS-1$
                }

                ExceptionUtility.showMessage(getString(titleId, new Object[] {userObj}), errorMsg, theError);
            }
        }
    }

    
    private void synch()
        throws ExternalException {

        ConfirmationPanel pnlConfirm = new ConfirmationPanel("dlg.synch.msg"); //$NON-NLS-1$
        DialogWindow.show(this, getString("dlg.synch.title"), pnlConfirm); //$NON-NLS-1$
        paintImmediately(getBounds());
        if (!iSSyncing && pnlConfirm.isConfirmed()) {
            Thread thread = new Thread("console-synchronize") { //$NON-NLS-1$
                public void run() {
                    try {
                        StaticUtilities.startWait(RuntimeMgmtPanel.this);
                        synchronized (mutex) {
                            iSSyncing = true;
                            getRuntimeMgmtManager().synchronizeServer();
                        // let the configuration manager know they need to refresh
                           ModelManager.getConfigurationManager(connection).setRefreshNeeded();
                        }
                    } catch (ExternalException e) {
                        ExceptionUtility.showMessage(RuntimeMgmtUtils.getString("msg.configmgrproblem", //$NON-NLS-1$
                            new Object[] {getClass().getName(), "showProcessDetails"}), //$NON-NLS-1$
                            e.getMessage(), e);
                        LogManager.logError(LogContexts.RUNTIME, e, getClass().getName() + "showProcessDetails");                         //$NON-NLS-1$
                    } finally {
                        iSSyncing = false;
                        StaticUtilities.endWait(RuntimeMgmtPanel.this);
                    }
                }
            };
            thread.start();
        }
    }

    public void treeNodesChanged(TreeModelEvent theEvent) {}
    public void treeNodesInserted(TreeModelEvent theEvent) {}
    public void treeNodesRemoved(TreeModelEvent theEvent) {}

    public void treeStructureChanged(TreeModelEvent theEvent) {
        Runnable runnable = new Runnable() {
            public void run() {
                hostStats = treeModel.getStatistics();
                serviceList = treeModel.getServiceList();
                procsTblModel.setNumRows(0);
                servsTblModel.setNumRows(0);
                if (hostStats != null) {
                    Iterator itr = hostStats.values().iterator();
                    while (itr.hasNext()) {
                        HostStatistics stats = (HostStatistics)itr.next();
                        procsTblModel.addRow(stats.getProcessStats());
                        servsTblModel.addRow(stats.getServiceStats());
                    }
                }
                tblProcStats.sizeColumnsToFitData();
                tblServStats.sizeColumnsToFitData();
                setLastChangeDate(hostStats);
                DefaultTreeNode node = treeModel.getUserObjectNode(lastSelectedObj);
                StaticTreeUtilities.expandAll(tree);
                //reset ServiceData when runtime State Changed. Using in QueueStatisticsDialog when
                // refresh button clicked.
                if (node != null) {
                    if (node.getContent() != null) {
                        Object userObj = node.getContent();
                        if (userObj instanceof ServiceData) {
                            pnlOps.setServiceData((ServiceData)userObj);
                        }
                    }
                }
                if (tree.getRowCount() > 0) {
                    if (node == null) {
                        tree.setSelectionRow(0);
                    } else {
                        // select same node that was selected prior to this state change
                        selectNode(node);
                    }
                }
            }
        };
        
        SwingUtilities.invokeLater(runnable);
    }
    

   public ArrayList getRunningList() {
        ArrayList serviceRunningList = new ArrayList();
        Iterator iter = serviceList.iterator();
        while (iter.hasNext()) {
            ServiceData sd = (ServiceData)iter.next();
            int state = sd.getCurrentState();
            if (state == OPEN || state == 3) {
                serviceRunningList.add(sd);
            }
        }
        return serviceRunningList;
    }

    public QueueStatisticsFrame startShowQueue(ServiceData sd) throws ExternalException {
    	try {
    		return qsdh.startDisplayForService(sd.getName(), sd,
    				qsrh.getQueueStatistics(sd));
    	} catch (Exception ex) {
    		throw new ExternalException(ex);
    	}
    }

    public VMStatisticsFrame startShowProcess(ProcessData pd) {
        if (vmdh != null && (qsrh.getProcessStatistics(pd) != null)) {
            return vmdh.startDisplayForProcess(pd.getName(), pd,
                qsrh.getProcessStatistics(pd));
        } 
        
        return null;
    }
    
    public void receiveUpdateNotification(RuntimeUpdateNotification notification) {
    	//TODO
    }

    public void refreshService(ServiceData sd) {
        qsrh.refreshRequested(sd);
    }

    public void refreshProcess(ProcessData pd) {
        qsrh.refreshProcessRequested(pd);
    }

    public boolean isProcessDisplayed(ProcessData pd) {
        return vmdh.isProcessDisplayed(pd);
    }

    public boolean isServiceDisplayed(ServiceData sd) {
        return qsdh.isServiceDisplayed(sd);
    }

    public void valueChanged(TreeSelectionEvent theEvent) {
        if (serviceHM == null ) {
            serviceHM = qsrh.getServiceMap(getRunningList());
        }
        if (tree.isSelectionEmpty()) {
            pnlOps.setEnabled(false);
        } else {
            TreePath path = theEvent.getNewLeadSelectionPath();
            DefaultTreeNode node = (DefaultTreeNode)path.getLastPathComponent();
            Object userObj = node.getContent();
            boolean[] enablements = new boolean[TOTAL_OPERATIONS];
            if (userObj instanceof ServiceData) {
                enablements = nodeSelected((ServiceData)userObj);
            } else if (userObj instanceof PSCData) {
                ProcessData process = (ProcessData)node.getParent().getContent();
                enablements = nodeSelected((PSCData)userObj, process);
            } else if (userObj instanceof ProcessData) {
                enablements = nodeSelected((ProcessData)userObj);
            } else if (userObj instanceof HostData) {
                enablements = nodeSelected((HostData)userObj);
            }
            pnlOps.setEnabled(enablements);
        }
    }

    /** 
     * @param hostData
     * @return
     * @since 4.4
     */
    private boolean[] nodeSelected(HostData hostData) {
        pnlOps.setVisibleService(false,0, false);
        tree.setPopupMenuFactory(popTree);
        return RuntimeMgmtUtils.getOperationsEnablements(hostData);
    }

    /** 
     * @param processData
     * @return
     * @since 4.4
     */
    private boolean[] nodeSelected(ProcessData processData) {
        pnlOps.setVisibleProcess(true);
        
        //Inserted to get right-click on tree node to enable/disable
        //"Show Process" correctly.  BWP 04/22/02
        pnlOps.setEnabledShowProcess(processData);             
        
        pnlOps.setProcessDate(processData);
        if (processPopupTree == null) {
            processPopupTree = new TreeMenuFactory(tree, pnlOps.getActions());
        }
        tree.setPopupMenuFactory(processPopupTree);
        return RuntimeMgmtUtils.getOperationsEnablements(processData);
    }

    /** 
     * @param pscData
     * @param process
     * @return
     * @since 4.4
     */
    private boolean[] nodeSelected(PSCData pscData, ProcessData process) {
        pnlOps.setVisibleService(false, 0, false);
        tree.setPopupMenuFactory(popTree);
        return RuntimeMgmtUtils.getOperationsEnablements(pscData, process);
    }

    /** 
     * @param serviceData
     * @since 4.4
     */
    private boolean[] nodeSelected(ServiceData serviceData) {
        if (serviceData.getCurrentState() == OPEN) {
            if (!serviceHM.containsKey(serviceData)) {
                serviceHM = qsrh.getServiceMap(getRunningList());
            }
        }
        
        if (serviceData.getInitError() != null) {
            //
            // Service has init error
            //
            pnlOps.setVisibleService(true, 0, true);
            tree.setPopupMenuFactory(new TreeMenuFactory(tree, pnlOps.getActions()));
        } else {
            //
            // Check for queues in service
            //
            Integer queueNumber = (Integer)serviceHM.get(serviceData);
            int numberOfQueues = 0;
            //BWP 02/20/02  Inserting non-null check here in response to
            //defect #4302.  Seeing NPE here in case where service was not
            //successfully initialized.
            if (queueNumber != null) {
                numberOfQueues = queueNumber.intValue();
            }
            if (numberOfQueues != 0) {
                pnlOps.setServiceData(serviceData);
                if (numberOfQueues == 1) {
                    pnlOps.setVisibleService(true, numberOfQueues, false);
                    if (servicePopQueueTree == null) {
                        servicePopQueueTree = new TreeMenuFactory(tree, pnlOps.getActions());
                    }
                    tree.setPopupMenuFactory(servicePopQueueTree);
                } else {
                    pnlOps.setVisibleService(true, numberOfQueues, false);
                    if (servicePopQueuesTree == null) {
                        servicePopQueuesTree = new TreeMenuFactory(tree, pnlOps.getActions());
                    }
                    tree.setPopupMenuFactory(servicePopQueuesTree);
                }
                // if (((ServiceData)userObj).getCurrentState() == OPEN || ((ServiceData)userObj).getCurrentState() == FAILED) {
                // JPC 12/19/05 - Any reason to show queues for failed svcs???
                if (serviceData.getCurrentState() == OPEN) {
                    pnlOps.setShowQueue(true);
                } else {
                    pnlOps.setShowQueue(false);
                }
            } else {
                pnlOps.setVisibleService(false, 0, false);
                tree.setPopupMenuFactory(popTree);
            }
        }
        qsrh.setServiceData(serviceData);
        return RuntimeMgmtUtils.getOperationsEnablements(serviceData);
   }  
    
    
    
    ///////////////////////////////////////////////////////////////////////////
    // INNER CLASSES
    ///////////////////////////////////////////////////////////////////////////

    private class PanelAction extends AbstractPanelAction {
        public static final int SHUTDOWN = 0;
        public static final int BOUNCE = 1;
        public static final int REFRESH = 2;
        public static final int SYNCH = 3;

        public PanelAction(int theType) {
            super(theType);
            if (theType == SHUTDOWN) {
                putValue(SHORT_DESCRIPTION, getString("actionShutdown.tip")); //$NON-NLS-1$
                putValue(MENU_ITEM_NAME, getString("actionShutdown.menu")); //$NON-NLS-1$
            } else if (theType == BOUNCE) {
                putValue(SHORT_DESCRIPTION, getString("actionBounce.tip")); //$NON-NLS-1$
                putValue(MENU_ITEM_NAME, getString("actionBounce.menu")); //$NON-NLS-1$
            } else if (theType == SYNCH) {
                putValue(SHORT_DESCRIPTION, getString("actionSynch.tip")); //$NON-NLS-1$
                putValue(MENU_ITEM_NAME, getString("actionSynch.menu")); //$NON-NLS-1$
            } else {
                throw new IllegalArgumentException(
                    "The action type <" + theType + "> is invalid."); //$NON-NLS-1$ //$NON-NLS-2$
            }
        }
        public void actionImpl(ActionEvent theEvent)
            throws ExternalException {

            if (type == SHUTDOWN) {
                shutdown();
            } else if (type == BOUNCE) {
                bounce();
                refresh();
            } else if (type == SYNCH) {
                synch();
            }
        }
        protected void handleError(Exception theException) {
            String emsg = null;
            if (type == SHUTDOWN) {
                emsg = getString("msg.shutdownerror"); //$NON-NLS-1$
            } else if (type == BOUNCE) {
                emsg = getString("msg.bounceerror"); //$NON-NLS-1$
            } else if (type == SYNCH) {
                emsg = getString("msg.syncherror"); //$NON-NLS-1$
            }
            if (emsg != null) {
                ExceptionUtility.showMessage(emsg,
                                             theException.getMessage(),
                                             theException);
                LogManager.logError(LogContexts.RUNTIME,
                                    theException,
                                    paramString());
            } else {
                super.handleError(theException);
            }
        }
    }

    private static class TreeMenuFactory extends DefaultPopupMenuFactory {
        JPopupMenu pop = new JPopupMenu();
        public TreeMenuFactory(TreeWidget theTree, List popActions) {
            for (int size=popActions.size(), i=0; i<size; i++) {
                MenuEntry me = (MenuEntry)popActions.get(i);
                pop.add(me.getAction());
            }
        }
        protected JPopupMenu createTreePopupMenu(final TreeWidget tree) {
            return pop;
        }
        public JPopupMenu getPopupMenu(final Component context) {
            if (context instanceof TreeWidget) {
                if (((TreeWidget)context).getSelectionCount() != 0) {
                    return pop;
                }
            }
            return null;
        }
    }

	public AutoRefresher getAutoRefresher() {
		return arRefresher;
	}

	public void setAutoRefreshEnabled(boolean b) {
		this.arRefresher.setAutoRefreshEnabled(b);
	}

	public void setAutoRefresher(AutoRefresher ar) {
		this.arRefresher = ar;
	}

	public void setRefreshRate(int rate) {
		this.arRefresher.setRefreshRate(rate);
	}
}