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

package com.metamatrix.console.ui.layout;

import java.awt.Color;
import java.awt.Component;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.awt.event.ItemEvent;
import java.awt.event.ItemListener;
import java.awt.event.MouseEvent;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import javax.swing.Icon;
import javax.swing.ImageIcon;
import javax.swing.JCheckBoxMenuItem;
import javax.swing.JMenu;
import javax.swing.JMenuItem;
import javax.swing.JPopupMenu;
import javax.swing.JTree;
import javax.swing.tree.*;
import javax.swing.tree.TreePath;

import com.metamatrix.console.connections.*;
import com.metamatrix.console.connections.ConnectionAndPanel;
import com.metamatrix.console.ui.util.IconComponent;
import com.metamatrix.console.ui.util.ItemsBlockedCallback;
import com.metamatrix.console.ui.util.ItemsBlockedTreeSelectionListener;
import com.metamatrix.console.util.StaticTreeUtilities;

import com.metamatrix.toolbox.ui.widget.PopupMenuFactory;
import com.metamatrix.toolbox.ui.widget.TreeWidget;
import com.metamatrix.toolbox.ui.widget.tree.DefaultTreeCellRenderer;
import com.metamatrix.toolbox.ui.widget.util.IconFactory;

public class PanelsTree
    extends TreeWidget
    implements ItemsBlockedCallback, PopupMenuFactory {

    public final static Color UNSELECTED_BACKGROUND_COLOR = new Color(240, 240, 255);
    public final static Color SELECTED_BACKGROUND_COLOR = new Color(204, 204, 255);

    ///////////////////////////////////////////////////////////////////////////
    // CONSTANTS
    ///////////////////////////////////////////////////////////////////////////

    // icons for renderer
    private static final ImageIcon AUTH_HDR_ICON =
        IconFactory.getIconForImageFile("authorization_medium.gif"); //$NON-NLS-1$
    private static final ImageIcon AUTH_ICON =
        IconFactory.getIconForImageFile("authorization_small.gif"); //$NON-NLS-1$
    private static final ImageIcon CONFIG_HDR_ICON =
        IconFactory.getIconForImageFile("configuration_medium.gif"); //$NON-NLS-1$
    private static final ImageIcon CONFIG_ICON =
        IconFactory.getIconForImageFile("configuration_small.gif"); //$NON-NLS-1$
    private static final ImageIcon MM_SERVER_ICON =
        IconFactory.getIconForImageFile("mm_server_small.jpg"); //$NON-NLS-1$
    private static final ImageIcon MB_SERVER_ICON = 
    	IconFactory.getIconForImageFile("md_server_small.jpg"); //$NON-NLS-1$
    private static final ImageIcon RUNTIME_HDR_ICON =
        IconFactory.getIconForImageFile("runtime_medium.gif"); //$NON-NLS-1$
    private static final ImageIcon RUNTIME_ICON =
        IconFactory.getIconForImageFile("runtime_small.gif"); //$NON-NLS-1$

	private static Map /*<ConnectionInfo to PanelsTree>*/ treeMap = new HashMap();
	
    ///////////////////////////////////////////////////////////////////////////
    // FIELDS
    ///////////////////////////////////////////////////////////////////////////

    private TreePath selectedTreePath = null;
    private boolean selectionCancellation = false;
    private JCheckBoxMenuItem showPanelsTreeMenuItem;
    private Map /*<leaf node display text to TreePath>*/ treePathMap;
    private ConnectionInfo connection;

    ///////////////////////////////////////////////////////////////////////////
    // CONSTRUCTORS
    ///////////////////////////////////////////////////////////////////////////

    private PanelsTree(PanelsTreeModel model, ConnectionInfo connection) {
        super(model);
        this.connection = connection;
		init();
	}

    ///////////////////////////////////////////////////////////////////////////
    // METHODS
    ///////////////////////////////////////////////////////////////////////////

    public static PanelsTree getInstance(ConnectionInfo connection) {
    	PanelsTree tree = (PanelsTree)treeMap.get(connection);
    	return tree;
    }

	public static void removeInstance(ConnectionInfo connection) {
		treeMap.remove(connection);
	}
	
	public static PanelsTree createInstance(ConnectionInfo connection) {
		PanelsTreeModel model = PanelsTreeModel.createInstance(connection);
		PanelsTree tree = new PanelsTree(model, connection);
		StaticTreeUtilities.expandAll(tree);
		treeMap.put(connection, tree);
		return tree;
	}
	
	public ConnectionInfo getConnection() {
		return connection;
	}
		
    public String getToolTipText(MouseEvent theEvent) {
        TreePath path =
            this.getPathForLocation(theEvent.getX(), theEvent.getY());
        if (path == null) return null;
        PanelsTreeNode node =
            (PanelsTreeNode)path.getLastPathComponent();
        return node.getToolTipText();
    }

    private void init() {
        this.setRootVisible(false);
        this.setShowsRootHandles(true);
        this.setBackground(UNSELECTED_BACKGROUND_COLOR);
        this.setCellRenderer(new PanelsTreeCellRenderer());
        this.addTreeSelectionListener(new ItemsBlockedTreeSelectionListener(
                this, this));
		this.setPopupMenuFactory(this);
		this.setTreePathMap();
    }

	private void setTreePathMap() {
		treePathMap = new HashMap();
		TreePath[] leafNodePaths = StaticTreeUtilities.allTreePathsToLeafNodes(
				this.getModel());
		for (int i = 0; i < leafNodePaths.length; i++) {
			String text = leafNodePaths[i].getLastPathComponent().toString();
			treePathMap.put(text, leafNodePaths[i]);
		}
	}
	
	public boolean isSelectionCancellation() {
	    return selectionCancellation;
	}
	
    public void itemSelectionBlocked() {
        if (selectedTreePath != null) {
            //Set flag to cause ignore handling of this change.  This is
            //because we are merely rejecting the selection of a non-leaf tree
            //node which has no panel to display.  So the previous panel is
            //already being displayed, hence there is no processing to do on
            //the selection path change.
            selectionCancellation = true;
            this.getSelectionModel().setSelectionPath(selectedTreePath);
            //Now unset flag.
            selectionCancellation = false;
        }
    }

    public void itemSelectionChanged(TreePath newPath) {
        selectedTreePath = newPath;
    }

	public Object[] getBlockedItems() {
		Object[] alwaysBlockedItems = PanelsTreeModel.BLOCKED_ITEMS;
		java.util.List blockedItems = new ArrayList();
		for (int i = 0; i < alwaysBlockedItems.length; i++) {
			blockedItems.add(alwaysBlockedItems[i]);
		}
		java.util.List /*<PanelsTreeNode>*/ nodesList = 
				StaticTreeUtilities.descendantsOfNode(
				(DefaultMutableTreeNode)this.getModel().getRoot(), false);
		Iterator it = nodesList.iterator();
		while (it.hasNext()) {
			PanelsTreeNode node = (PanelsTreeNode)it.next();
			if (node.isConnectionNode()) {
				blockedItems.add(node);
			}
		}
		Object[] blockedItemsArray = new Object[blockedItems.size()];
		it = blockedItems.iterator();
		for (int i = 0; it.hasNext(); i++) {
			blockedItemsArray[i] = it.next();
		}
		return blockedItemsArray;
	}
	
    public void selectNodeForPanel(ConnectionAndPanel thePanel) {
        PanelsTreeModel model = (PanelsTreeModel)getModel();
        TreePath path = model.getPathForPanel(thePanel);
        setSelectionPath(path);
        scrollRowToVisible(getRowForPath(path));
    }

    public JPopupMenu getPopupMenu(Component c) {
        JPopupMenu menu = null;
        if (c == this) {
            menu = new JPopupMenu();
        }
        return menu;
    }
    
    public JMenu createPanelsMenu() {
    	//This method traverses the tree and creates a JMenu based on the tree's
    	//contents.  The JMenu will have a JMenuEntry for each displayable panel.
    	//Also, added on the end will be a JCheckBoxMenuEntry for whether or not
    	//to show the tree at all.  This JMenu will then be added to the 
    	//Console's menu bar and will serve as an alternate 
    	//means for selecting which panel to display.  The selection of any item
    	//in the menu will cause its corresponding tree node to be selected
    	//(whether or not the tree is even being displayed),
    	//which will in turn cause the correct panel to be displayed.
    	
    	JMenu menu = new JMenu(ConsoleMenuBar.PANELS_MENU_HEADER);
    	PanelsTreeModel model = (PanelsTreeModel)this.getModel();
    	TreePath[] leafNodePaths = 
    			StaticTreeUtilities.allTreePathsToLeafNodes(model);
    	Icon prevFirstIcon = null;
    	for (int i = 0; i < leafNodePaths.length; i++) {
    		java.util.List /*<Icon>*/ icons = new ArrayList(2);
    		PanelsTreeNode firstNode = 
    				(PanelsTreeNode)leafNodePaths[i].getPathComponent(0);
    		PanelsTreeNode secondNode = null;
    		if (firstNode.isRoot()) {
    			firstNode = 
    					(PanelsTreeNode)leafNodePaths[i].getPathComponent(1);
    			secondNode = 
    					(PanelsTreeNode)leafNodePaths[i].getPathComponent(2);
    		} else {
    			secondNode = 
    					(PanelsTreeNode)leafNodePaths[i].getPathComponent(1);
    		}
    		String hdrText = model.getHeaderNodeText(firstNode);
    		if (hdrText.equals(PanelsTreeModel.RUNTIME)) {
    			icons.add(RUNTIME_ICON);
    		} else if (hdrText.equals(PanelsTreeModel.CONFIGURATION)) {
    			icons.add(CONFIG_ICON);
    		} else if (hdrText.equals(PanelsTreeModel.AUTHORIZATION)) {
    			icons.add(AUTH_ICON);
    		}
    		if (secondNode.getChildCount() > 0) {
    			String secondNodeName = secondNode.getName();
    			if (secondNodeName.equals(PanelsTreeModel.MM_SERVER)) {
    				icons.add(MM_SERVER_ICON);
    			} else if (secondNodeName.equals(PanelsTreeModel.MB_SERVER)) {
    				icons.add(MB_SERVER_ICON);
    			}
    		}
    		Icon[] iconsArray = new Icon[icons.size()];
    		Iterator it = icons.iterator();
    		for (int j = 0; it.hasNext(); j++) {
    			iconsArray[j] = (Icon)it.next();
    		}
    		if (prevFirstIcon != null) {
    			if (iconsArray[0] != prevFirstIcon) {
    				menu.addSeparator();
    			}
    		}
    		prevFirstIcon = iconsArray[0];
    		IconComponent menuEntryIcon = new IconComponent(iconsArray);
    		final String text = leafNodePaths[i].getLastPathComponent().toString();
    		JMenuItem menuItem = new JMenuItem(text, menuEntryIcon);
    		menuItem.addActionListener(new ActionListener() {
    			public void actionPerformed(ActionEvent ev) {
    				menuItemSelected(text);
    			}
    		});
    		menu.add(menuItem);
    	}
    	menu.addSeparator();
    	//TODO-- if initially not displaying panels tree, flag should be changed
    	//to false.
    	showPanelsTreeMenuItem = new JCheckBoxMenuItem("Show panels tree",  //$NON-NLS-1$
    			true);
    	showPanelsTreeMenuItem.addItemListener(new ItemListener() {
    		public void itemStateChanged(ItemEvent ev) {
    			showPanelsTreeStateChanged();
    		}
    	});
    	menu.add(showPanelsTreeMenuItem);
    	return menu;
    }
    
    private void menuItemSelected(String menuItemText) {
    	TreePath tp = (TreePath)treePathMap.get(menuItemText);
    	if (tp != null) {
    		this.setSelectionPath(tp);
    	}
    }

	private void showPanelsTreeStateChanged() {
		boolean curState = showPanelsTreeMenuItem.isSelected();
		ConsoleMainFrame.getInstance().setShowingTree(curState);
	}
	    
    ///////////////////////////////////////////////////////////////////////////
    // INNER CLASSES
    ///////////////////////////////////////////////////////////////////////////

    private class PanelsTreeCellRenderer extends DefaultTreeCellRenderer {
        public PanelsTreeCellRenderer() {
            super();
		}

        public Color getBackgroundNonSelectionColor() {
            return PanelsTree.UNSELECTED_BACKGROUND_COLOR;
        }

        public Color getBackgroundSelectionColor() {
            return PanelsTree.SELECTED_BACKGROUND_COLOR;
        }

        public Component getTreeCellRendererComponent(
            final JTree tree,
            final Object value,
            boolean isSelected,
            final boolean isExpanded,
            final boolean isLeaf,
            final int row,
            final boolean hasFocus) {

            if (value == null) {
                return this;
            }

            // call super to set all background/foreground colors for
            // isSelected, hasFocus
            super.getTreeCellRendererComponent(
                tree, value, isSelected, isExpanded, isLeaf, row, hasFocus);

            // set the icon
            PanelsTreeModel model = (PanelsTreeModel)tree.getModel();
            PanelsTreeNode node = (PanelsTreeNode)value;
            String hdrTxt = model.getHeaderNodeText(node);

            if (hdrTxt != null) {
                if (node.getName().equals(PanelsTreeModel.MM_SERVER)) {
                    setIcon(MM_SERVER_ICON);
                } else if (node.getName().equals(PanelsTreeModel.MB_SERVER)) {
                	setIcon(MB_SERVER_ICON);
                } else if (hdrTxt.equals(PanelsTreeModel.RUNTIME)) {
                    setIcon((node.getName().equals(PanelsTreeModel.RUNTIME))
                        ? RUNTIME_HDR_ICON
                        : RUNTIME_ICON);
                } else if (hdrTxt.equals(PanelsTreeModel.CONFIGURATION)) {
                    setIcon((node.getName().equals(PanelsTreeModel.CONFIGURATION))
                        ? CONFIG_HDR_ICON
                        : CONFIG_ICON);
                } else if (hdrTxt.equals(PanelsTreeModel.AUTHORIZATION)) {
                    setIcon((node.getName().equals(PanelsTreeModel.AUTHORIZATION))
                        ? AUTH_HDR_ICON
                        : AUTH_ICON);
                }
            }

            return this;
        }
    }
}//end PanelsTree
