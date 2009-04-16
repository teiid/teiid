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
package com.metamatrix.console.ui.views.deploy;

import java.awt.BorderLayout;
import java.awt.Color;
import java.awt.Component;
import java.awt.Dimension;
import java.awt.Font;
import java.awt.Graphics;
import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.awt.GridLayout;
import java.awt.Insets;
import java.awt.event.ActionEvent;
import java.awt.event.ItemEvent;
import java.awt.event.ItemListener;
import java.awt.event.KeyEvent;
import java.awt.event.MouseEvent;
import java.io.File;
import java.io.FileOutputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Properties;

import javax.swing.AbstractButton;
import javax.swing.Action;
import javax.swing.JComboBox;
import javax.swing.JComponent;
import javax.swing.JPanel;
import javax.swing.JPopupMenu;
import javax.swing.JScrollPane;
import javax.swing.JSplitPane;
import javax.swing.KeyStroke;
import javax.swing.SwingUtilities;
import javax.swing.border.CompoundBorder;
import javax.swing.border.EtchedBorder;
import javax.swing.event.TreeSelectionEvent;
import javax.swing.event.TreeSelectionListener;
import javax.swing.tree.TreePath;
import javax.swing.tree.TreeSelectionModel;

import com.metamatrix.common.config.api.Configuration;
import com.metamatrix.common.config.api.ConfigurationID;
import com.metamatrix.common.config.api.DeployedComponent;
import com.metamatrix.common.config.api.ServiceComponentDefn;
import com.metamatrix.common.config.api.VMComponentDefn;
import com.metamatrix.common.config.util.ConfigurationPropertyNames;
import com.metamatrix.common.config.xml.XMLConfigurationImportExportUtility;
import com.metamatrix.common.log.LogManager;
import com.metamatrix.common.properties.ObjectPropertyManager;
import com.metamatrix.common.tree.directory.DirectoryEntry;
import com.metamatrix.common.tree.directory.FileSystemFilter;
import com.metamatrix.common.tree.directory.FileSystemView;
import com.metamatrix.console.connections.ConnectionInfo;
import com.metamatrix.console.models.ConfigurationManager;
import com.metamatrix.console.models.ModelManager;
import com.metamatrix.console.models.RuntimeMgmtManager;
import com.metamatrix.console.notification.RuntimeUpdateNotification;
import com.metamatrix.console.security.UserCapabilities;
import com.metamatrix.console.ui.NotifyOnExitConsole;
import com.metamatrix.console.ui.layout.MenuEntry;
import com.metamatrix.console.ui.layout.WorkspaceController;
import com.metamatrix.console.ui.layout.WorkspacePanel;
import com.metamatrix.console.ui.util.AbstractPanelAction;
import com.metamatrix.console.ui.util.ConsoleConstants;
import com.metamatrix.console.ui.views.deploy.event.ConfigurationModifier;
import com.metamatrix.console.ui.views.deploy.event.ConfigurationTreeModelEvent;
import com.metamatrix.console.ui.views.deploy.event.ConfigurationTreeModelListener;
import com.metamatrix.console.ui.views.deploy.model.ConfigurationTreeModel;
import com.metamatrix.console.ui.views.deploy.model.ConfigurationTreeModel.HostWrapper;
import com.metamatrix.console.ui.views.deploy.util.DeployPkgUtils;
import com.metamatrix.console.ui.views.runtime.util.RuntimeMgmtUtils;
import com.metamatrix.console.util.DialogUtility;
import com.metamatrix.console.util.ExceptionUtility;
import com.metamatrix.console.util.ExternalException;
import com.metamatrix.console.util.LogContexts;
import com.metamatrix.console.util.Refreshable;
import com.metamatrix.console.util.StaticProperties;
import com.metamatrix.console.util.StaticUtilities;
import com.metamatrix.toolbox.preference.UserPreferences;
import com.metamatrix.toolbox.ui.widget.ButtonWidget;
import com.metamatrix.toolbox.ui.widget.DialogWindow;
import com.metamatrix.toolbox.ui.widget.DirectoryChooserPanel;
import com.metamatrix.toolbox.ui.widget.LabelWidget;
import com.metamatrix.toolbox.ui.widget.Splitter;
import com.metamatrix.toolbox.ui.widget.TreeWidget;
import com.metamatrix.toolbox.ui.widget.menu.DefaultPopupMenuFactory;
import com.metamatrix.toolbox.ui.widget.tree.DefaultTreeNode;

/**
 * @version 1.0
 * @author Dan Florian
 */
public final class DeployMainPanel
    extends JPanel
    implements ItemListener,
               ConfigurationTreeModelListener,
               NotifyOnExitConsole,
               TreeSelectionListener,
               WorkspacePanel,
               Refreshable {

/************ TO DO & ISSUES **********

- maybe load tree model lazily or in the background

***************************************/

    ///////////////////////////////////////////////////////////////////////////
    // CONSTANTS
    ///////////////////////////////////////////////////////////////////////////

    private static int HISTORY_LIMIT =
        DeployPkgUtils.getInt("dmp.historylimit", 50); //$NON-NLS-1$
    
    ///////////////////////////////////////////////////////////////////////////
    // CONTROLS
    ///////////////////////////////////////////////////////////////////////////

    private JComboBox cbxLocation;
    private TreeWidget tree;

    private JSplitPane splitter;
    private ConfigSummaryPanel pnlConfigSummary;
    private DeployedHostPanel pnlDeployedHost;
    private DeployedProcessPanel pnlDeployedProcess;
    private DeployedServicePanel pnlDeployedService;
    private ServiceDefinitionPanel pnlServiceDefn;
    private DeploymentsSummaryPanel pnlDeploymentsSummary;
    private JPanel pnlTreeOpsSizer;
    private JPanel pnlTree;

    ///////////////////////////////////////////////////////////////////////////
    // FIELDS
    ///////////////////////////////////////////////////////////////////////////

    private ConfigurationTreeCellRenderer renderer;
    
	private ConnectionInfo connection;
    private ArrayList actions = new ArrayList();
    private PanelAction actionBack = new PanelAction(PanelAction.BACK);
    private PanelAction actionNext = new PanelAction(PanelAction.NEXT);
    private PanelAction actionForward = new PanelAction(PanelAction.FORWARD);
    private PanelAction actionUp = new PanelAction(PanelAction.UP);
    private PanelAction actionClear = new PanelAction(PanelAction.CLEAR);
    private PanelAction actionImport = new PanelAction(PanelAction.IMPORT);
    private PanelAction actionExport = new PanelAction(PanelAction.EXPORT);
    private PanelAction actionBounce = new PanelAction(PanelAction.BOUNCE);
    private PanelAction actionSynch = new PanelAction(PanelAction.SYNCH);
	private List treeActions;

    // key=navigation object, value=node
    private HashMap navNodeMap = new HashMap();
    // key=node, value=navigation object
    private HashMap nodeNavMap = new HashMap();
    private DetailPanel pnlDetail;
    private boolean eventProcessing = false;
    private ConfigurationTreeModel treeModel;
    private boolean editMode = true;
    private DefaultTreeNode saveNode; // previous selected node
    private ConfigurationID configId; // config Id of the selected node
    private ObjectPropertyManager dflts;
    private boolean programmaticTreeSelection = false;
    private TreePath currentTreePath = null;

    ///////////////////////////////////////////////////////////////////////////
    // CONSTRUCTORS
    ///////////////////////////////////////////////////////////////////////////

    public DeployMainPanel(ConnectionInfo conn) throws ExternalException {
    	super();
    	this.connection = conn;
        if (dflts == null) {
            dflts = new ObjectPropertyManager(RuntimeMgmtUtils.PROPS);
            //Code in ObjectPropertyManager changed, addNamespace() call no
            //longer needed.  BWP 04/03/03
            //dflts.addNamespace(RuntimeMgmtUtils.PROPS);
        }
        setLayout(new BorderLayout(3, 3));
        setBorder(DeployPkgUtils.EMPTY_BORDER);
        construct();
        try {
            editMode = UserCapabilities.getInstance().canUpdateConfiguration(
            		this.connection);
            if (!editMode) {
                setActionsDisabled();
            }
        } catch (Exception theException) {
            throw new ExternalException("DeployMainPanel:init", theException); //$NON-NLS-1$
        }
    }

    ///////////////////////////////////////////////////////////////////////////
    // METHODS
    ///////////////////////////////////////////////////////////////////////////

    private void back() {
        cbxLocation.setSelectedIndex(cbxLocation.getSelectedIndex()-1);
    }

    private void bounce() throws ExternalException {

        // need to see if user modified anything on current panel since
        // bounce does a refresh

        boolean continuing = switchingPanels();
        if (continuing) {
            ConfirmationPanel pnlConfirm = new ConfirmationPanel(
                    "dlg.bounceserver.msg"); //$NON-NLS-1$
            DialogWindow.show(this, dflts.getString("dlg.bounceserver.title"), //$NON-NLS-1$
                    pnlConfirm);
            if (pnlConfirm.isConfirmed()) {
                StaticUtilities.startWait(this);
                try {
                    getRuntimeMgmtManager().bounceServer();
                
                //BWP 02/19/04  Commenting out refresh() call here as solution
                //to part of defect 11948.  A refresh() here will fail because
                //we've just done a bounce.  Therefore, a session service is
                //temporarily unavailable.
                
                //refresh();
                } finally {
                    StaticUtilities.endWait(this);
                }
            }
        }
    }

    private void clearHistory() {
        Object currentItem = cbxLocation.getSelectedItem();
        cbxLocation.removeAllItems();
        cbxLocation.addItem(currentItem);
        actionClear.setEnabled(false);
    }

    private void clearTreeActionsPanel() {
        if (treeActions != null) {
            for (int size=treeActions.size(), i=0;
                 i<size;
                 i++) {
                AbstractPanelAction action =
                    (AbstractPanelAction)treeActions.get(i);
                action.removeComponent(
                    (JComponent)pnlTreeOpsSizer.getComponent(i));
            }
            pnlTreeOpsSizer.removeAll();
            treeActions = null;
        }
    }

    private void construct()
        throws ExternalException {

        // add actions that don't have buttons
        actions.add(new MenuEntry(MenuEntry.ACTION_MENUITEM, actionClear));
        actions.add(MenuEntry.DEFAULT_SEPARATOR);
        actions.add(new MenuEntry(MenuEntry.ACTION_MENUITEM, actionImport));
        actions.add(new MenuEntry(MenuEntry.ACTION_MENUITEM, actionExport));
        actions.add(MenuEntry.DEFAULT_SEPARATOR);
        actions.add(new MenuEntry(MenuEntry.ACTION_MENUITEM, actionBounce));
        actions.add(new MenuEntry(MenuEntry.ACTION_MENUITEM, actionSynch));
        actions.add(MenuEntry.DEFAULT_SEPARATOR);

		JPanel pnlNavigation = new JPanel(new GridBagLayout());
        pnlNavigation.setBorder(
            new CompoundBorder(new EtchedBorder(), DeployPkgUtils.EMPTY_BORDER));
        add(pnlNavigation, BorderLayout.NORTH);

        LabelWidget lblLocation =
            new LabelWidget(DeployPkgUtils.getString("dmp.lblLocation")); //$NON-NLS-1$
        GridBagConstraints gbc = new GridBagConstraints();
        gbc.insets = new Insets(0, 0, 0, 2);
        pnlNavigation.add(lblLocation, gbc);

        cbxLocation = new JComboBox();
        cbxLocation.addItemListener(this);
        cbxLocation.setBackground(Color.white);
        Font font = cbxLocation.getFont();
        String fontName = DeployPkgUtils.getString("dmp.nav.font.name", true); //$NON-NLS-1$
        cbxLocation.setFont(
            new Font((fontName == null) ? font.getName() : fontName,
                     Font.PLAIN,
                     font.getSize()-DeployPkgUtils.getInt("dmp.nav.font.size", 0))); //$NON-NLS-1$
        gbc = new GridBagConstraints();
        gbc.fill = GridBagConstraints.HORIZONTAL;
        gbc.weightx = 1.0;
        pnlNavigation.add(cbxLocation, gbc);

        ButtonWidget btnUp = new ButtonWidget();
        setup(MenuEntry.ACTION_MENUITEM, btnUp, actionUp);
        btnUp.setPreferredSize(
            new Dimension(
                btnUp.getPreferredSize().width,
                cbxLocation.getPreferredSize().height));
        btnUp.setMargin(new Insets(2, 2, 2, 2));
        gbc = new GridBagConstraints();
        gbc.gridx = 4;
        gbc.gridy = 0;
        pnlNavigation.add(btnUp, gbc);

        ButtonWidget btnDown = new ButtonWidget();
        setup(MenuEntry.ACTION_MENUITEM, btnDown, actionNext);
        btnDown.setPreferredSize(
            new Dimension(btnUp.getPreferredSize().width,
                          cbxLocation.getPreferredSize().height));
        btnDown.setMargin(new Insets(2, 2, 2, 2));
        gbc = new GridBagConstraints();
        gbc.gridx = 5;
        gbc.gridy = 0;
        pnlNavigation.add(btnDown, gbc);

        ButtonWidget btnBack = new ButtonWidget();
        setup(MenuEntry.ACTION_MENUITEM, btnBack, actionBack);
        btnBack.setPreferredSize(
            new Dimension(btnUp.getPreferredSize().width,
                          cbxLocation.getPreferredSize().height));
        btnBack.setMargin(new Insets(2, 2, 2, 2));
        gbc = new GridBagConstraints();
        gbc.gridx = 2;
        gbc.gridy = 0;
        pnlNavigation.add(btnBack, gbc);

        ButtonWidget btnForward = new ButtonWidget();
        setup(MenuEntry.ACTION_MENUITEM, btnForward, actionForward);
        btnForward.setPreferredSize(
            new Dimension(btnUp.getPreferredSize().width,
                          cbxLocation.getPreferredSize().height));
        btnForward.setMargin(new Insets(2, 2, 2, 2));
        gbc = new GridBagConstraints();
        gbc.gridx = 3;
        gbc.gridy = 0;
        pnlNavigation.add(btnForward, gbc);

        splitter = new Splitter() {
            boolean init = false;
            public void paint(Graphics theGraphics) {
                super.paint(theGraphics);
                // position the splitter the first time only
                if (!init) {
                    Double temp =
                        (Double)DeployPkgUtils.getObject("dmp.splitter.pos"); //$NON-NLS-1$
                    double initPos = (temp == null) ? 0.35 : temp.doubleValue();
                    init = true;
                    int width = getSize().width;
                    int treeWidth = pnlTree.getPreferredSize().width;
                    if (((double)treeWidth / (double)width) < initPos) {
                        splitter.setDividerLocation(treeWidth);
                    } else {
                        splitter.setDividerLocation(initPos);
                    }
                }
            }
        };
        splitter.setOneTouchExpandable(true);
        add(splitter, BorderLayout.CENTER);

        pnlTree = new JPanel(new GridBagLayout());
        pnlTree.setMinimumSize(pnlTree.getMinimumSize());
        splitter.setLeftComponent(pnlTree);

        tree = new TreeWidget() {
            public String getToolTipText(MouseEvent theEvent) {
                TreePath path =
                    tree.getPathForLocation(theEvent.getX(), theEvent.getY());
                if (path == null) return null;
                DefaultTreeNode node =
                    (DefaultTreeNode)path.getLastPathComponent();
                return getLocationObject(node).toString();
            }
        };
        tree.setRootVisible(false);
        tree.setShowsRootHandles(true);
        tree.setVisibleRowCount(10);
        tree.getSelectionModel().setSelectionMode(
            TreeSelectionModel.SINGLE_TREE_SELECTION);
        treeModel = new ConfigurationTreeModel();
        treeModel.init(getConfigurationManager().getConfig(Configuration.NEXT_STARTUP_ID));

        tree.setModel(treeModel);
        renderer = new ConfigurationTreeCellRenderer(connection);
        tree.setCellRenderer(renderer);
        tree.getSelectionModel().addTreeSelectionListener(this);
        tree.setPopupMenuFactory(new TreeMenuFactory(tree, this));
        // override default tree key bindings so they match the navigation bar
        tree.registerKeyboardAction(actionBack,
                                    (String)actionBack.getValue(Action.NAME),
                                    KeyStroke.getKeyStroke(KeyEvent.VK_LEFT, 0),
                                    WHEN_FOCUSED);
        tree.registerKeyboardAction(actionForward,
                                    (String)actionForward.getValue(Action.NAME),
                                    KeyStroke.getKeyStroke(KeyEvent.VK_RIGHT, 0),
                                    WHEN_FOCUSED);
        tree.registerKeyboardAction(actionNext,
                                    (String)actionNext.getValue(Action.NAME),
                                    KeyStroke.getKeyStroke(KeyEvent.VK_DOWN, 0),
                                    WHEN_FOCUSED);
        tree.registerKeyboardAction(actionUp,
                                    (String)actionUp.getValue(Action.NAME),
                                    KeyStroke.getKeyStroke(KeyEvent.VK_UP, 0),
                                    WHEN_FOCUSED);

        JScrollPane spnTree = new JScrollPane(tree);
        gbc = new GridBagConstraints();
        gbc.gridwidth = GridBagConstraints.REMAINDER;
        gbc.fill = GridBagConstraints.BOTH;
        gbc.insets = new Insets(3, 3, 3, 3);
        gbc.weightx = 1.0;
        gbc.weighty = 1.0;
        pnlTree.add(spnTree, gbc);

        JPanel pnlTreeOps = new JPanel();
        gbc = new GridBagConstraints();
        gbc.gridx = 0;
        gbc.gridy = 1;
        pnlTree.add(pnlTreeOps, gbc);

        pnlTreeOpsSizer = new JPanel(new GridLayout(1, 0));
        pnlTreeOps.add(pnlTreeOpsSizer);

        getConfigurationManager().addConfigurationChangeListener(treeModel);
        getConfigurationManager().refreshImpl();
        setInitialTreeState(true);
        treeModel.addConfigurationTreeModelListener(this);
    }

    /**
     * Deletes all references in the navigation bar history for this
     * node and all its child nodes.
     * @param theNode the node being removed from navigation history
     */
    private void deleteFromHistory(DefaultTreeNode theNode) {
        eventProcessing = true; // make sure combobox listener doesn't react
        List kids = theNode.getChildren();
        if (kids != null) {
            for (int size=kids.size(), i=0; i<size; i++) {
                deleteFromHistory((DefaultTreeNode)kids.get(i));
            }
        }
        Object navigation = nodeNavMap.get(theNode);
        if (navigation != null) {
            // node has been visited before
            // have to walk all the items unfortunately
            for (int i=cbxLocation.getItemCount()-1; i>=0; i--) {
                Object item = cbxLocation.getItemAt(i);
                if (item.toString().equals(navigation.toString())) {
                    cbxLocation.removeItemAt(i);
                    // see if two of the same locations are back-to-back
                    if (((i-1) >=0) && (i <= (cbxLocation.getItemCount()-1))) {
                        if (cbxLocation.getItemAt(i).toString().equals(
                            cbxLocation.getItemAt(i-1).toString())) {
                            cbxLocation.removeItemAt(i);
                        }
                    }
                    navNodeMap.remove(item);
                    nodeNavMap.remove(theNode);
                }
            }
        }
        eventProcessing = false;
    }

    private void exportConfig()
        throws ExternalException {

        String[] extensions =
            (String[])DeployPkgUtils.getObject("dmp.importexport.extensions"); //$NON-NLS-1$
        FileSystemView view = new FileSystemView();
        String dirTxt =
//            (String)UserPreferences.getInstance()
//                                   .getValue(DeployPkgUtils.LAST_DIR);                                   
             (String) UserPreferences.getInstance().getValue(ConsoleConstants.CONSOLE_DIRECTORY_LOCATION_KEY);
          
        if (dirTxt != null) {
            try {
                view.setHome(view.lookup(dirTxt));
            } catch (Exception ex) {
                //Any exception that may occur on setting the initial view is
                //inconsequential.  This is merely a convenience to the user.
            }
        }
        FileSystemFilter[] filters = null;
        if (extensions != null) {
            FileSystemFilter filter =
                new FileSystemFilter(
                    view,
                    extensions,
                    DeployPkgUtils.getString("dmp.importexport.description")); //$NON-NLS-1$
            filters = new FileSystemFilter[] {filter};
        }

        DirectoryChooserPanel pnlChooser = (filters == null)
            ? new DirectoryChooserPanel(view, DirectoryChooserPanel.TYPE_SAVE)
            : new DirectoryChooserPanel(view,
                                        DirectoryChooserPanel.TYPE_SAVE,
                                        filters);
        pnlChooser.setAcceptButtonLabel(
            DeployPkgUtils.getString("dmp.export.open")); //$NON-NLS-1$
        pnlChooser.setShowPassThruFilter(false);

        DialogWindow.show(
            this,
            DeployPkgUtils.getString("dmp.export.title", //$NON-NLS-1$
                                     new Object[] {configId}),
            pnlChooser);
        if (pnlChooser.getSelectedButton() == pnlChooser.getAcceptButton()) {
            DirectoryEntry result =
                (DirectoryEntry)pnlChooser.getSelectedTreeNode();
            String filename = result.getNamespace();
            //
            // first save directory to preferences file
            //
            int index = filename.lastIndexOf(File.separatorChar);
            String path = filename.substring(0, index);
            UserPreferences.getInstance()
                           .setValue(ConsoleConstants.CONSOLE_DIRECTORY_LOCATION_KEY, path);
            UserPreferences.getInstance().saveChanges();
            //
            // now do the export
            //
            try {
                Collection exportObjs = getConfigurationManager().getConfigObjects(configId);
                FileOutputStream fileOutput = new FileOutputStream(filename);
                XMLConfigurationImportExportUtility xmlUtil =
                    new XMLConfigurationImportExportUtility();
                String userName =
                    UserCapabilities.getLoggedInUser(connection).getName();
                String version = StaticProperties.getVersion();
                Properties props = new Properties();
                props.put(ConfigurationPropertyNames.APPLICATION_CREATED_BY,
                          DeployPkgUtils.getString("dmp.console.name")); //$NON-NLS-1$
                props.put(
                    ConfigurationPropertyNames.APPLICATION_VERSION_CREATED_BY,
                    version);
                props.put(ConfigurationPropertyNames.USER_CREATED_BY, userName);
                xmlUtil.exportConfiguration(fileOutput, exportObjs, props);
            } catch (Exception theException) {
                String detail =
                    DeployPkgUtils.getString("dmp.msg.exporterrordetail", //$NON-NLS-1$
                                             new Object[] {configId, filename});
                throw new ExternalException(
                    detail + " " + theException.getMessage(), //$NON-NLS-1$
                    theException);
            }

        }
    }

    public boolean finishUp() {
        // required by the NotifyOnExitConsole interface
        // Finish up any pending work.  Returns true if exit still okay, false if exiting should
        // be cancelled.
        return switchingPanels();
    }

    private String formatNavigationTip(String theLocation) {
        return
            theLocation.substring(theLocation.lastIndexOf(File.separatorChar)+1);
    }

    private void forward() {
        cbxLocation.setSelectedIndex(cbxLocation.getSelectedIndex()+1);
    }

    private Object[] getAncestors(TreePath thePath) {
        TreePath path = thePath.getParentPath();
        ArrayList ancestors = new ArrayList();
        int count = path.getPathCount();
        if (count != 0) {
            // don't put header nodes in ancestor collection
            // load starting with immediate parent
            for (int i=count-1; i>=0; i--) {
                DefaultTreeNode node = (DefaultTreeNode)path.getPathComponent(i);
                if (!treeModel.isHeaderNode(node) &&
                    (node != treeModel.getRoot())) {
                    Object content = node.getContent();
//                    if (content instanceof PscWrapper) {
//                        ancestors.add(((PscWrapper)content).getPsc());
//                    } else 
                    if (content instanceof HostWrapper) {
                        ancestors.add(((HostWrapper)content).getHost());
                    } else {
                        ancestors.add(content);
                    }
                }
            }
        }
        return ancestors.toArray();
    }

    private ConfigurationID getConfigId(DefaultTreeNode theNode) {
        if (theNode == null) {
            // no configuration nodes found
            throw new IllegalStateException(
                DeployPkgUtils.getString("dmp.msg.confignotfound")); //$NON-NLS-1$
        }
        ConfigurationID id;
        Object domainObj = theNode.getContent();
        if (domainObj instanceof Configuration) {
            id = (ConfigurationID)((Configuration)domainObj).getID();
        } else {
            return getConfigId(theNode.getParent());
        }
        return id;
    }

    private DefaultTreeNode getDownNode() {
        DefaultTreeNode node = getSelectedNode();
        if (node == null) return null;
        DefaultTreeNode parent = node.getParent();
        int numKids = parent.getChildCount();
        int index = parent.getChildIndex(node);
        int downIndex = ((index+1) == numKids) ? 0 : (index+1);
        return parent.getChild(downIndex);
    }

    private Object getLocationObject(DefaultTreeNode theNode) {
//!!!!dan
        if (theNode == null) {
            return ""; //$NON-NLS-1$
        }
//!!!dan
        ArrayList path = new ArrayList();
        DefaultTreeNode node = theNode;
        while (node.getParent() != null) {
            path.add(node.getName());
            node = node.getParent();
        }
        StringBuffer location = new StringBuffer(File.separatorChar);
        for (int size=path.size(), i=size-1; i>=0; i--) {
            location.append(File.separatorChar).append(path.get(i));
        }
        return location;
    }

    private DefaultTreeNode getSelectedNode() {
        TreePath path = tree.getSelectionPath();
        if( path == null ) return null;
        return (DefaultTreeNode)path.getLastPathComponent();
    }

    public String getTitle() {
        return DeployPkgUtils.getString("dmp.title"); //$NON-NLS-1$
    }

	public ConnectionInfo getConnection() {
		return connection;
	}
    
    public RuntimeMgmtManager getRuntimeMgmtManager() {
        return ModelManager.getRuntimeMgmtManager(getConnection());
    }
    
    public ConfigurationManager getConfigurationManager() {
        return ModelManager.getConfigurationManager(getConnection());
    }
	
    private List getTreeActions() {
        return treeActions;
    }
    
    public void receiveUpdateNotification(RuntimeUpdateNotification notification) {
    	//TODO
    }

    public boolean havePendingChanges() {
        // required by the NotifyOnExitConsole interface
        // Does tab have pending changes?  If so, focus will be given to the tab.  The tab can
        // then put up a dialog asking the user whether to save or complete the changes, or
        // whatever.
        boolean pending = false;
        if (pnlDetail instanceof ConfigurationModifier) {
        	pending = (!((ConfigurationModifier)pnlDetail).isPersisted());
        }
        return pending;
    }

    private void importConfig()
        throws ExternalException {

        // check to see if current panel is saved first because a refresh is done
        boolean continuing = switchingPanels();
        if (continuing) {
            ConfigurationImportWizard wizard = new ConfigurationImportWizard(connection);
            if (wizard.run()) {
                Collection configObjs = wizard.getImportedObjects();
                getConfigurationManager().commitImportedObjects(configObjs);
                refresh();
            }
        }
    }

    public void itemStateChanged(ItemEvent theEvent) {
        if (theEvent.getStateChange() == ItemEvent.SELECTED) {
            if (!eventProcessing) {
                eventProcessing = true;
                Object item = theEvent.getItem();
                DefaultTreeNode node = (DefaultTreeNode)navNodeMap.get(item);
                selectNode(node);
                updateNavigationState();
                eventProcessing = false;
            }
        }
    }

//    private void manageHosts()
//        throws ExternalException {
//
//        DialogWindow.show(this,
//                          DeployPkgUtils.getString("hmp.title"),
//                          new HostMgmtPanel());
//    }

    private void next() {
        selectNode(getDownNode());
    }

    public void refresh() {
        boolean continuing = switchingPanels();
        if (continuing) {
            
            eventProcessing = true;
            navNodeMap.clear();
            nodeNavMap.clear();
            pnlDetail = null;

            // need to select same node as before refresh, if possible
 //           boolean deploymentHdr = false;
 //           boolean pscDefHdr = false;
            Object userObj = null;
            if( saveNode != null ) {
                userObj = saveNode.getContent();
            }
  //          if (treeModel.isDeploymentsHeaderNode(saveNode)) {
  //              deploymentHdr = true;
   //         } 
//            else if (treeModel.isPscDefinitionsHeaderNode(saveNode)) {
//                pscDefHdr = true;
//            }

			DefaultTreeNode tempSaveNode = saveNode;
            saveNode = null;
            clearTreeActionsPanel();
            tree.getSelectionModel().removeTreeSelectionListener(DeployMainPanel.this);
            treeModel.removeConfigurationTreeModelListener(DeployMainPanel.this);
            cbxLocation.removeItemListener(DeployMainPanel.this);
            cbxLocation.removeAllItems();

            treeModel.refresh();    
            
            ConfigurationManager configManager = getConfigurationManager();
            if( configManager.getListeners().size() == 0) {
                configManager.addConfigurationChangeListener(treeModel);
                //tree.setCellRenderer(renderer);
            }
            try {
                configManager.refreshImpl();
            } catch (ExternalException theException) {
                ExceptionUtility.showMessage(
                        DeployPkgUtils.getString("dmp.msg.refresherror"), //$NON-NLS-1$
                        ""+theException.getMessage(), //$NON-NLS-1$
                        theException);
                LogManager.logError(LogContexts.PSCDEPLOY, theException,
                        DeployPkgUtils.getString("dmp.msg.refresherror")); //$NON-NLS-1$
            }
            //If problem, saveNode may not have been set, so reset to previous
            //node.  Fix to defect 9183.
            if (saveNode == null) {
            	saveNode = tempSaveNode;
            }
            	
            // go ahead and hook back all the wiring even if exception occurred
            // maybe things will work on subsequent tries
            tree.getSelectionModel().addTreeSelectionListener(DeployMainPanel.this);
            treeModel.addConfigurationTreeModelListener(DeployMainPanel.this);
            //treeModel.refresh();
            cbxLocation.addItemListener(DeployMainPanel.this);
            setInitialTreeState(false);

            //select same node as before refresh if it still exists
            DefaultTreeNode node = null;
///            if (deploymentHdr) {
//                node = treeModel.getDeploymentsHeaderNode((Configuration)userObj);
//            } else if (pscDefHdr) {
//                node = treeModel.getPscDefinitionsHeaderNode(
//                        (Configuration)userObj);
 //           } else {
                node = treeModel.getUserObjectNode(userObj);
  //          }
            eventProcessing = false;
            
            
            if (node == null) {
                if (tree.getRowCount() > 0) {
                    //update tree in the Swing Thread
                    Runnable runnable = new Runnable() {
                        public void run() {
                            tree.setSelectionRow(0);
                        }
                    };
                    SwingUtilities.invokeLater(runnable);                    
                } else {
                    selectNode(node);
                }
            } else {
                selectNode(node);
            }
        }
    }

    
    
    
    public List resume() {
        // check to see if a refresh is needed since the last time
        // this panel was displayed. this would occur if a bounce
        // was done from the runtime management/system state panel
        if (getConfigurationManager().isRefreshNeeded()) {
        	refresh();
        }
        
        // when this panel is first displayed there is no detail panel
        if (pnlDetail == null) {
            return null;
        }
        List clone = (List)actions.clone();
        List detailActions = pnlDetail.getActions();
        if (!detailActions.isEmpty()) {
            clone.add(MenuEntry.DEFAULT_SEPARATOR);
            clone.addAll(detailActions);
        }
        return clone;
    }

    private void selectNode(final DefaultTreeNode theNode) {
        //update tree in the Swing Thread
    	if (theNode == null) {
    		new Exception("Null Node").printStackTrace();
    		return;
    	}
        Runnable runnable = new Runnable() {
            public void run() {
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
        };        
        SwingUtilities.invokeLater(runnable);
    }

    private void setActionsDisabled() {
        actionImport.setEnabled(false);
        actionBounce.setEnabled(false);
        actionSynch.setEnabled(false);
        // Can export if user has System.READ role
    }

    private void setInitialTreeState(final boolean theSelectFirstNode) {
        //update tree in the Swing Thread
        Runnable runnable = new Runnable() {
            public void run() {
                // expand entirely the first configuration node
                DefaultTreeNode root = (DefaultTreeNode)treeModel.getRoot();
                int numOtherKids = root.getChildren().size() - 1;
                int row = 0;
                while (row < (tree.getRowCount() - numOtherKids)) {
                    tree.expandRow(row);
                    row++;
                }
                if (theSelectFirstNode && (tree.getRowCount() > 0)) {
                    tree.setSelectionRow(0);
                }
            }
        };
        SwingUtilities.invokeLater(runnable);
    }

    private void setup(
        String theMenuEntryType,
        AbstractButton theButton,
        AbstractPanelAction theAction) {

        theAction.addComponent(theButton);
        actions.add(new MenuEntry(theMenuEntryType, theAction));
    }

    private boolean switchingPanels() {
        boolean switching = true;
        if (havePendingChanges()) {
            if (pnlDetail instanceof NotifyOnExitConsole) {
                switching = ((NotifyOnExitConsole)pnlDetail).finishUp();
            } else {
                if ((pnlDetail instanceof DeployedHostPanel) || (pnlDetail
                        instanceof DeployedProcessPanel)) {
                    String msg;
                    String panelObjectString;
                    if (pnlDetail instanceof DeployedHostPanel) {
                        msg = "Save changes to deployed host " + //$NON-NLS-1$
                                ((DeployedHostPanel)pnlDetail).getHostName() +
                                "?"; //$NON-NLS-1$
                        panelObjectString = "deployed host"; //$NON-NLS-1$
                    } else {
                        msg = "Save changes to deployed process " + //$NON-NLS-1$
                                ((DeployedProcessPanel)pnlDetail).getProcessName()
                                + "?"; //$NON-NLS-1$
                        panelObjectString = "deployed process"; //$NON-NLS-1$
                    }
                    int response = DialogUtility.showPendingChangesDialog(
                    		msg, getConnection().getURL(),
                    		getConnection().getUser());
                    switch (response) {
                        case DialogUtility.YES:
                            try {
                                ((ConfigurationModifier)pnlDetail).persist();
                            } catch (ExternalException ex) {
                                String errMsg = "Error saving changes to " + //$NON-NLS-1$
                                        panelObjectString;
                                LogManager.logError(LogContexts.PSCDEPLOY, ex,
                                        errMsg);
                                ExceptionUtility.showMessage(errMsg, ex);
                            }
                            break;
                        case DialogUtility.NO:
                            break;
                        case DialogUtility.CANCEL:
                            switching = false;
                            break;
                    }
                } else {
                    // made changes to current detail panel and have not saved
                    // show dialog to save/abort changes
                    ConfirmationPanel pnlConfirm = new ConfirmationPanel(
                            "dmp.msg.confirmsave", "icon.warning", "dmp.btnOk", //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
                            "dmp.btnCancel"); //$NON-NLS-1$
                    try{
                        DialogWindow.show(
                                this,
                                DeployPkgUtils.getString("dmp.confirmsavedlg.title"), //$NON-NLS-1$
                                pnlConfirm);
                    }catch (Exception ex) {
                        ExceptionUtility.showMessage(
                        ex.getMessage(), ex);
                        return true;                        
                    }

                    if (pnlConfirm.isConfirmed()) {
                        // save changes
                        try {
                            ((ConfigurationModifier)pnlDetail).persist();
                        } catch (ExternalException theException) {
                            ExceptionUtility.showMessage(
                                    DeployPkgUtils.getString("msg.persistproblem"), //$NON-NLS-1$
                                    "" + theException.getMessage(), theException); //$NON-NLS-1$

                            LogManager.logError(LogContexts.PSCDEPLOY, theException,
                                    DeployPkgUtils.getString("msg.persistproblem")); //$NON-NLS-1$
                        }
                    } else {
                        ((ConfigurationModifier)pnlDetail).reset();
                    }
                }
            }
        }
        return switching;
    }

    private void synch()
        throws ExternalException {

        ConfirmationPanel pnlConfirm = new ConfirmationPanel("dlg.synch.msg"); //$NON-NLS-1$
        DialogWindow.show(this,
                          dflts.getString("dlg.synch.title"), //$NON-NLS-1$
                          pnlConfirm);
        paintImmediately(getBounds());
        if (pnlConfirm.isConfirmed()) {
            try {
                StaticUtilities.startWait(this);
                getRuntimeMgmtManager().synchronizeServer();
            } finally {
                StaticUtilities.endWait(this);
            }
        }
    }

    /**
     * Invoked when a <code>ConfigurationTreeModelEvent</code> occurs.
     * @param theEvent the event to process
     */
    public void treeNodesChanged(final ConfigurationTreeModelEvent theEvent) {
        if (theEvent.isNew()) {
            // if new host event, only select if the currently selected
            // node has the same configuration. this is because when
            // a host is created, events fire to add into all configurations.
            if (theEvent.isHostChange()) {
                Configuration config = theEvent.getConfiguration();
                if (config.getID().equals(pnlDetail.getConfigId())) {
                    selectNode(theEvent.getSourceNode());
                }
            } else if (!(pnlDetail instanceof DeployedProcessPanel) &&
                     !theEvent.isServiceDefinitionChange()) {
                // if a new object is created, don't select it if
                // current detail panel is the DeployedProcessPane.
                // focus should stay on the panel, not the new one.
                // also don't select new service definitions. that event is
                // fired only when new psc definitions are created
                selectNode(theEvent.getSourceNode());
            }
        } else if (theEvent.isDeleted()) {
            deleteFromHistory(theEvent.getSourceNode());
            // host is deleted from every config. need to select the
            // node in the config that is currently selected
            if (theEvent.isHostChange()) {
                Configuration config = theEvent.getConfiguration();
                if (config.getID().equals(pnlDetail.getConfigId())) {
                    // select the appropriate deployment summary node
                    selectNode(treeModel.getUserObjectNode(config));
                }
            } else if (!(pnlDetail instanceof DeployedProcessPanel) ||
                     ((pnlDetail instanceof DeployedProcessPanel) &&
                     (theEvent.isProcessChange()))) {
                // if on the DeployedProcessPanel, select parent node
                // only if the process itself is being deleted
                selectNode(theEvent.getSourceNodeParent());
            }

        } else if (theEvent.isModified()) {
            // not doing anything now
        } else if (theEvent.isRefreshStart()) {
            refresh();
        }
    }

    /** Action take when the up button/menu-item is selected. */
    private void up() {
        DefaultTreeNode child = getSelectedNode();
        selectNode(child.getParent());
    }

    private void updateNavigationState() {
        int index = cbxLocation.getSelectedIndex();
        int lastIndex = cbxLocation.getItemCount()-1;
        // enable/disable back button
        if (index-1 >= 0) {
            String tip =
                formatNavigationTip(cbxLocation.getItemAt(index-1).toString());
            actionBack.putValue(Action.SHORT_DESCRIPTION,
                                DeployPkgUtils.getString("dmp.actionBack.tip") + tip); //$NON-NLS-1$
            if (!actionBack.isEnabled()) {
                actionBack.setEnabled(true);
            }
        } else {
            actionBack.putValue(Action.SHORT_DESCRIPTION, null);
            if (actionBack.isEnabled()) {
                actionBack.setEnabled(false);
            }
        }
        // enable/disable forward button
        if (index+1 <= lastIndex) {
            String tip =
                formatNavigationTip(cbxLocation.getItemAt(index+1).toString());
            actionForward.putValue(Action.SHORT_DESCRIPTION,
                                   DeployPkgUtils.getString("dmp.actionForward.tip") + tip); //$NON-NLS-1$
            if (!actionForward.isEnabled()) {
                actionForward.setEnabled(true);
            }
        } else {
            actionForward.putValue(Action.SHORT_DESCRIPTION, null);
            if (actionForward.isEnabled()) {
                actionForward.setEnabled(false);
            }
        }

        DefaultTreeNode child = getSelectedNode();
        // parent will never be null since root is not shown
        DefaultTreeNode parent = child.getParent();

        // enable/disable up button
        if (parent == tree.getModel().getRoot()) {
            actionUp.putValue(Action.SHORT_DESCRIPTION, null);
            if (actionUp.isEnabled()) {
                actionUp.setEnabled(false);
            }
        } else {
            String tip =
                formatNavigationTip(getLocationObject(parent).toString());
            actionUp.putValue(Action.SHORT_DESCRIPTION,
                              DeployPkgUtils.getString("dmp.actionUp.tip") + tip); //$NON-NLS-1$
            if (!actionUp.isEnabled()) {
                actionUp.setEnabled(true);
            }
        }
        // enable/disable down button
        if (parent.getChildCount() > 1) {
            String tip =
                formatNavigationTip(getLocationObject(getDownNode()).toString());
            actionNext.putValue(Action.SHORT_DESCRIPTION,
                                DeployPkgUtils.getString("dmp.actionNext.tip") + tip); //$NON-NLS-1$
            if (!actionNext.isEnabled()) {
                actionNext.setEnabled(true);
            }
        } else {
            actionNext.putValue(Action.SHORT_DESCRIPTION, null);
            if (actionNext.isEnabled()) {
                actionNext.setEnabled(false);
            }
        }
    }

    public void valueChanged(TreeSelectionEvent theEvent) {
        if (!programmaticTreeSelection) {
            DetailPanel pnl = null;
            TreePath path = theEvent.getNewLeadSelectionPath();
            if (tree.isSelectionEmpty()) {
                // deselection by user or deleting a node caused deselection
                // if not a deleted node, reselect last node
                if (treeModel.contains(saveNode)) {
                    eventProcessing = true;
                    selectNode(saveNode);
                    eventProcessing = false;
                }
            } else {
                DefaultTreeNode node = (DefaultTreeNode)path.getLastPathComponent();
                if (!eventProcessing) {
                    eventProcessing = true;
                    // update navigation bar
                    Object location = getLocationObject(node);
                    navNodeMap.put(location, node);
                    nodeNavMap.put(node, location);
                    cbxLocation.addItem(location);
                    if (cbxLocation.getItemCount() > 1) {
                        // action checks to see if enabled state is different
                        actionClear.setEnabled(true);
                    }
                    cbxLocation.setSelectedIndex(cbxLocation.getItemCount()-1);
                    if (cbxLocation.getItemCount() > HISTORY_LIMIT) {
                        cbxLocation.removeItemAt(0);
                    }
                    updateNavigationState();
                    eventProcessing = false;
                }
                // switch detail panel
                boolean switching = false;
                try {
                    // before switching see if changes were made
                    // switching also means keeping the same panel but changing the data
                    switching = switchingPanels();
                    if (switching) {
                        // TreeWidget switches to the wait cursor automatically
                        configId = getConfigId(node);
                        Object domainObj = node.getContent();
//                        if (treeModel.isDeploymentsHeaderNode(node)) {
//                            if (pnlDeploymentsSummary == null) {
//                                pnlDeploymentsSummary =
//                                        new DeploymentsSummaryPanel(configId,
//                                        connection);
//                            }
//                            pnl = pnlDeploymentsSummary;
//                        } else if (treeModel.isPscDefinitionsHeaderNode(node)) {
//                            if (pnlPscSummary == null) {
//                                pnlPscSummary = new PscSummaryPanel(configId,
//                                		connection);
//                            }
//                            pnl = pnlPscSummary;
//                        } else if (domainObj instanceof PscWrapper) {
//                            PscWrapper wrapper = (PscWrapper)domainObj;
//                            if (wrapper.isDefinition()) {
//                                if (pnlPscDefn == null) {
//                                    pnlPscDefn = new PscDefinitionPanel(  
//                                            configId,connection, this);
//                                }
//                                pnl = pnlPscDefn;
//                            } else {
//                                if (pnlDeployedPsc == null) {
//                                    pnlDeployedPsc = new DeployedPscPanel(connection,
//                                            configId);
//                                }
//                                pnl = pnlDeployedPsc;
//                            }
//                            domainObj = wrapper.getPsc();
//                        } else 
//                        if (domainObj instanceof ServiceComponentDefn) {
//                            if (pnlServiceDefn == null) {
//                                pnlServiceDefn = new ServiceDefinitionPanel(
//                                		true, null, configId, connection);
//                            }
//                            pnl = pnlServiceDefn;
//                        } else 
                        if (domainObj instanceof Configuration) {
                            if (pnlConfigSummary == null) {
                                pnlConfigSummary = new ConfigSummaryPanel(
                                        configId, connection);
                            }
                            pnl = pnlConfigSummary;
                        } else if (domainObj instanceof HostWrapper) {
                            if (pnlDeployedHost == null) {
                                pnlDeployedHost = new DeployedHostPanel(
                                        this, configId, connection);
                            }
                            pnl = pnlDeployedHost;
                            domainObj = ((HostWrapper)domainObj).getHost();
//                        } else if (domainObj instanceof ProductType) {
//                            if (pnlProductDefn == null) {
//                                pnlProductDefn = new ProductDefinitionPanel(
//                                        configId, connection);
//                            }
//                            pnl = pnlProductDefn;
                        } else if (domainObj instanceof VMComponentDefn) {
                            if (pnlDeployedProcess == null) {
                                pnlDeployedProcess = new DeployedProcessPanel(
                                       configId, connection);
                            }
                            pnl = pnlDeployedProcess;
                        } else if (domainObj instanceof DeployedComponent) {
                            if (pnlDeployedService == null) {
                                pnlDeployedService = new DeployedServicePanel(
                                        configId, connection);
                            }
                            pnl = pnlDeployedService;
                        } else {
                            
                            if (pnlDeploymentsSummary == null) {
                                pnlDeploymentsSummary =
                                        new DeploymentsSummaryPanel(configId,
                                        connection);
                            }
                            pnl = pnlDeploymentsSummary;
                       	
//                        	
//                            String msg = DeployPkgUtils.getString("dmp.msg.unknowntype") + //$NON-NLS-1$
//                            domainObj.getClass();
//                            
//                            IllegalStateException ise = new IllegalStateException(msg);
//                            ExceptionUtility.showMessage(
//                                                         DeployPkgUtils.getString("msg.configmgrproblem", //$NON-NLS-1$
//                                                         new Object[] {getClass(), "valueChanged"}), //$NON-NLS-1$
//                                                         ""+msg, ise); //$NON-NLS-1$
//                                                 LogManager.logError(LogContexts.PSCDEPLOY, ise,
//                                                         "valueChanged"); //$NON-NLS-1$
//                            return;
                        }

                        // set config type if different than last selected
                        if (!pnl.getConfigId().equals(configId)) {
                            pnl.setConfigId(configId);
                        }

                        // set domain object in panel before changing
                        pnl.setDomainObject(domainObj, getAncestors(path));

                        // set panel if different panel type than last selected
                        if (pnl != pnlDetail) {
                            // get rid of current tree buttons and
                            // remove btn from action
                            clearTreeActionsPanel();
                            treeActions = pnl.getTreeActions();
                            // install new tree actions if necessary
                            if (treeActions != null) {
                                for (int size=treeActions.size(), i=0; i<size; i++) {
                                    ButtonWidget btn = new ButtonWidget();
                                    AbstractPanelAction action =
                                            (AbstractPanelAction)treeActions.get(i);
                                    action.addComponent(btn);
                                    pnlTreeOpsSizer.add(btn);
                                }
                            }
                            // put new panel in splitpane
                            pnlDetail = pnl;
                            // update actions menu
                            WorkspaceController.getInstance().updateActions(this);
                            splitter.setRightComponent(pnlDetail);
                        }

                        saveNode = node;
                        // check to see if configuration of node is editable
//                        boolean enable = editMode &&
//                                getConfigurationManager().isEditable(configId);
                        
                        pnl.setEnabled(true);

                        currentTreePath = theEvent.getNewLeadSelectionPath();
                    } else {
                        programmaticTreeSelection = true;
                        
                        //update tree in the Swing Thread
                        Runnable runnable = new Runnable() {
                            public void run() {
                                tree.setSelectionPath(currentTreePath);
                            }
                        };
                        SwingUtilities.invokeLater(runnable);
                        
                        programmaticTreeSelection = false;
                    }
                } catch (ExternalException theException) {
                    ExceptionUtility.showMessage(
                            DeployPkgUtils.getString("msg.configmgrproblem", //$NON-NLS-1$
                            new Object[] {getClass(), "valueChanged"}), //$NON-NLS-1$
                            ""+theException.getMessage(), theException); //$NON-NLS-1$
                    LogManager.logError(LogContexts.PSCDEPLOY, theException,
                            "valueChanged"); //$NON-NLS-1$
                }
            }
        }
    }

    ///////////////////////////////////////////////////////////////////////////
    // INNER CLASSES
    ///////////////////////////////////////////////////////////////////////////

    private class PanelAction extends AbstractPanelAction {
        public static final int BACK = 0;
        public static final int FORWARD = 1;
        public static final int UP = 2;
        public static final int NEXT = 3;
        public static final int CLEAR = 4;
//        public static final int REFRESH = 5;
        public static final int IMPORT = 6;
        public static final int EXPORT = 7;
        public static final int BOUNCE = 9;
        public static final int SYNCH = 10;

		public PanelAction(int theType) {
            super(theType);
            String iconId = null;
            if (dflts == null) {
                dflts = new ObjectPropertyManager(RuntimeMgmtUtils.PROPS);
                //Code in ObjectPropertyManager changed, addNamespace() call
                //no longer needed.  BWP 04/03/03
                //dflts.addNamespace(RuntimeMgmtUtils.PROPS);
            }
            if (theType == BACK) {
                iconId = "icon.back"; //$NON-NLS-1$
                putValue(MENU_ITEM_NAME,
                         DeployPkgUtils.getString("dmp.actionBack.menu")); //$NON-NLS-1$
                setMnemonic(
                    DeployPkgUtils.getMnemonic("dmp.actionBack.mnemonic")); //$NON-NLS-1$
            } else if (theType == FORWARD) {
                iconId = "icon.forward"; //$NON-NLS-1$
                putValue(MENU_ITEM_NAME,
                         DeployPkgUtils.getString("dmp.actionForward.menu")); //$NON-NLS-1$
                setMnemonic(
                    DeployPkgUtils.getMnemonic("dmp.actionForward.mnemonic")); //$NON-NLS-1$
            } else if (theType == UP) {
                iconId = "icon.up"; //$NON-NLS-1$
                putValue(MENU_ITEM_NAME,
                         DeployPkgUtils.getString("dmp.actionUp.menu")); //$NON-NLS-1$
            } else if (theType == NEXT) {
                iconId = "icon.next"; //$NON-NLS-1$
                putValue(MENU_ITEM_NAME,
                         DeployPkgUtils.getString("dmp.actionNext.menu")); //$NON-NLS-1$
            } else if (theType == CLEAR) {
                iconId = "icon.clear"; //$NON-NLS-1$
                putValue(SHORT_DESCRIPTION,
                         DeployPkgUtils.getString("dmp.actionClear.tip")); //$NON-NLS-1$
                putValue(MENU_ITEM_NAME,
                         DeployPkgUtils.getString("dmp.actionClear.menu")); //$NON-NLS-1$
                setEnabled(false);
			} else if (theType == IMPORT) {
                iconId = "icon.import"; //$NON-NLS-1$
                putValue(SHORT_DESCRIPTION,
                         DeployPkgUtils.getString("dmp.actionImport.tip")); //$NON-NLS-1$
                putValue(MENU_ITEM_NAME,
                         DeployPkgUtils.getString("dmp.actionImport.menu")); //$NON-NLS-1$
            } else if (theType == EXPORT) {
                iconId = "icon.export"; //$NON-NLS-1$
                putValue(SHORT_DESCRIPTION,
                         DeployPkgUtils.getString("dmp.actionExport.tip")); //$NON-NLS-1$
                putValue(MENU_ITEM_NAME,
                         DeployPkgUtils.getString("dmp.actionExport.menu")); //$NON-NLS-1$
            } else if (theType == BOUNCE) {
                iconId = "icon.blank"; //$NON-NLS-1$
                putValue(SHORT_DESCRIPTION,
                         dflts.getString("actionBounce.tip")); //$NON-NLS-1$
                putValue(MENU_ITEM_NAME,
                         dflts.getString("actionBounce.menu")); //$NON-NLS-1$
            } else if (theType == SYNCH) {
                iconId = "icon.blank"; //$NON-NLS-1$
                putValue(SHORT_DESCRIPTION,
                         dflts.getString("actionSynch.tip")); //$NON-NLS-1$
                putValue(MENU_ITEM_NAME,
                         dflts.getString("actionSynch.menu")); //$NON-NLS-1$
			} else {
                throw new IllegalArgumentException(
                    DeployPkgUtils.getString("msg.invalidactiontype") + theType + "."); //$NON-NLS-1$ //$NON-NLS-2$
            }
            if (iconId != null) {
                putValue(Action.SMALL_ICON, DeployPkgUtils.getIcon(iconId));
            }
        }
        public void actionImpl(ActionEvent theEvent)
            throws ExternalException {

            if (type == BACK) {
                back();
            } else if (type == FORWARD) {
                forward();
            } else if (type == UP) {
                up();
            } else if (type == NEXT) {
                next();
            } else if (type == CLEAR) {
                clearHistory();
//            } else if (type == REFRESH) {
//                refresh();
            } else if (type == IMPORT) {
                importConfig();
            } else if (type == EXPORT) {
                exportConfig();
            } else if (type == BOUNCE) {
                bounce();
            } else if (type == SYNCH) {
                synch();
			}
        }
        protected void handleError(Exception theException) {
            String emsg = null;
//            if (type == REFRESH) {
//                emsg = DeployPkgUtils.getString("dmp.msg.refresherror");
//            } else if (type == IMPORT) {
if (type == IMPORT) {
                emsg = DeployPkgUtils.getString("dmp.msg.importerror"); //$NON-NLS-1$
            } else if (type == EXPORT) {
                emsg = DeployPkgUtils.getString("dmp.msg.exporterror"); //$NON-NLS-1$
            } else if (type == BOUNCE) {
                emsg = DeployPkgUtils.getString("dmp.msg.bounceerror"); //$NON-NLS-1$
            } else if (type == SYNCH) {
                emsg = DeployPkgUtils.getString("dmp.msg.syncherror"); //$NON-NLS-1$
			}
            if (emsg != null) {
                ExceptionUtility.showMessage(emsg,
                                             theException.getMessage(),
                                             theException);
                LogManager.logError(LogContexts.PSCDEPLOY,
                                    theException,
                                    paramString());
            } else {
                super.handleError(theException);
            }
        }
    }

    private static class TreeMenuFactory extends DefaultPopupMenuFactory {
        JPopupMenu pop = new JPopupMenu();
        DeployMainPanel pnl;
        public TreeMenuFactory(TreeWidget theTree, DeployMainPanel thePanel) {
            pnl = thePanel;
        }
        protected JPopupMenu createTreePopupMenu(final TreeWidget tree) {
            return pop;
        }
        public JPopupMenu getPopupMenu(final Component context) {
            if (context instanceof TreeWidget) {
                pop.removeAll();
                List actions = pnl.getTreeActions();
                if (actions != null) {
                    for (int size=actions.size(), i=0;
                         i<size;
                         pop.add((Action)actions.get(i++))) {
                        
                    }
                    return pop;
                }
            }
            return null;
        }
    }
    
}
