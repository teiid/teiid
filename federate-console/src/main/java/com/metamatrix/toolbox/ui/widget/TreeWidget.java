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

//################################################################################################################################
package com.metamatrix.toolbox.ui.widget;

import java.awt.Component;
import java.awt.Dimension;
import java.awt.Font;
import java.awt.Rectangle;
import java.awt.event.MouseAdapter;
import java.awt.event.MouseEvent;
import java.util.List;

import javax.swing.Icon;
import javax.swing.JPopupMenu;
import javax.swing.JToolTip;
import javax.swing.JTree;
import javax.swing.SwingUtilities;
import javax.swing.ToolTipManager;
import javax.swing.event.TreeModelEvent;
import javax.swing.event.TreeModelListener;
import javax.swing.tree.TreeCellEditor;
import javax.swing.tree.TreeCellRenderer;
import javax.swing.tree.TreeModel;
import javax.swing.tree.TreePath;

import com.metamatrix.common.tree.TreeNode;
import com.metamatrix.common.tree.TreeView;
import com.metamatrix.toolbox.property.VetoedChangeEvent;
import com.metamatrix.toolbox.property.VetoedChangeListener;
import com.metamatrix.toolbox.ui.UIDefaults;
import com.metamatrix.toolbox.ui.widget.laf.TreeLookAndFeel;
import com.metamatrix.toolbox.ui.widget.menu.DefaultPopupMenuFactory;
import com.metamatrix.toolbox.ui.widget.text.TextContainer;
import com.metamatrix.toolbox.ui.widget.transfer.TreeNodeDragAndDropController;
import com.metamatrix.toolbox.ui.widget.tree.DefaultTreeCellEditor;
import com.metamatrix.toolbox.ui.widget.tree.DefaultTreeCellRenderer;
import com.metamatrix.toolbox.ui.widget.tree.DefaultTreeModel;

/**
 * This class is intended to be used everywhere within the application that a tree needs to be displayed.
 * @since 2.0
 * @version 2.1
 * @author <a href="mailto:jverhaeg@metamatrix.com">John P. A. Verhaeg</a>
 */
public class TreeWidget extends JTree
implements TextContainer {
	
	/**
	 * Converts the child index in the unfiltered view into the child index in the filtered view.
	 * @param theParent the parent node of the child
	 * @param theStartIndex the index of the child in the unfiltered view
	 * @param theFilteredView the view whose child index is being requested
	 * @param theUnfilteredView the view pertaining to the given index
	 * @return the index of the child node in the filtered tree or <code>-1</code> if the start index is invalid
	 */
	public static int convertIndexToModel(TreeNode theParent,
	                                      int theStartIndex,
	                                      TreeView theFilteredView,
	                                      TreeView theUnfilteredView) {
	    if (theParent == null) {
	    	throw new IllegalArgumentException("TreeWidget.convertIndexToModel:Parent is null.");
	    }
	    
	    if (theFilteredView == null) {
	    	throw new IllegalArgumentException("TreeWidget.convertIndexToModel:Filtered view is null.");
	    }
	    
	    if (theUnfilteredView == null) {
	    	throw new IllegalArgumentException("TreeWidget.convertIndexToModel:Unfiltered view is null.");
	    }
	    
	    int index = theStartIndex;
		List kids = theUnfilteredView.getChildren(theParent); // won't be null per TreeView contract

		if (!kids.isEmpty()) {
			if ((index > 0) && (index-1 < kids.size())) {
       			for (int i=index-1; i>=0; i--) {
    				TreeNode kid = (TreeNode)kids.get(i);
    				if (theFilteredView.isHidden(kid)) {
    					--index;
    				}
    			}
			}
			else {
				if (theStartIndex != 0) {
					// invalid start index
					index = -1;
				}
			}
		}
		else {
			if (theStartIndex != 0) {
				// invalid start index
				index = -1;
			}
		}
	    
	    return index;
	}
	
    //############################################################################################################################
    //# Constants                                                                                                                #
    //############################################################################################################################

    public static final String PROPERTY_PREFIX = "Tree.";
    
    private static final String LINE_STYLE_PROPERTY = "JTree.lineStyle";
     
    //############################################################################################################################
    //# Instance Variables                                                                                                       #
    //############################################################################################################################

    private TreeModel model;
    private transient Object value;
    private boolean isClipTipEnabled;
    private PopupMenuFactory popupMenuFactory;
    private TreeNodeDragAndDropController dndCtrlr;
    private TreeModelListener modelListener;
    private VetoedChangeListener vetoListener;

    //############################################################################################################################
    //# Constructors                                                                                                             #
    //############################################################################################################################

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since 2.0
    */
    public TreeWidget() {
        this(null);
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since 2.0
    */
    public TreeWidget(final Object value) {
        super((TreeModel)null);
        this.value = value;
        initializeTreeWidget();
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since 2.0
    */
    public TreeWidget(final TreeModel model) {
        super((TreeModel)null);
        this.model = model;
        initializeTreeWidget();
    }

    //############################################################################################################################
    //# Instance Methods                                                                                                         #
    //############################################################################################################################

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    Called whenever a name change is vetoed by a node, usually because either the node is read-only or the name is invalid.  Does
    nothing by default.
    @param event The VetoedChangeEvent fired by the node that vetoed the change
    @since 2.0
    */
    protected void changeVetoed(final VetoedChangeEvent event) {
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since 2.0
    */
    public String convertValueToText(final Object value, final boolean isSelected, final boolean isExpanded, final boolean isLeaf,
                                     final int row, final boolean hasFocus) {
        if (value == null) {
            return "";
        }
        if (value instanceof TreeNode) {
            return ((TreeNode)value).getName();
        }
        return value.toString();
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since 2.0
    */
    protected PopupMenuFactory createDefaultPopupMenuFactory() {
        return new DefaultPopupMenuFactory();
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since 2.0
    */
    protected TreeCellEditor createDefaultTreeCellEditor() {
        final javax.swing.tree.DefaultTreeCellRenderer adapter = new javax.swing.tree.DefaultTreeCellRenderer() {
            public Icon getClosedIcon() {
                final TreeCellRenderer renderer = getCellRenderer();
                if (renderer != null  &&  renderer instanceof DefaultTreeCellRenderer) {
                    return ((DefaultTreeCellRenderer)renderer).getClosedIcon();
                }
                return super.getClosedIcon();
            }
            public Font getFont() {
                if (TreeWidget.this == null) {
                    return super.getFont();
                }
                final TreeCellRenderer renderer = getCellRenderer();
                if (renderer != null  &&  renderer instanceof DefaultTreeCellRenderer) {
                    return ((DefaultTreeCellRenderer)renderer).getFont();
                }
                return super.getFont();
            }
            public int getIconTextGap() {
                final TreeCellRenderer renderer = getCellRenderer();
                if (renderer != null  &&  renderer instanceof DefaultTreeCellRenderer) {
                    return ((DefaultTreeCellRenderer)renderer).getIconTextGap();
                }
                return super.getIconTextGap();
            }
            public Icon getLeafIcon() {
                final TreeCellRenderer renderer = getCellRenderer();
                if (renderer != null  &&  renderer instanceof DefaultTreeCellRenderer) {
                    return ((DefaultTreeCellRenderer)renderer).getLeafIcon();
                }
                return super.getLeafIcon();
            }
            public Icon getOpenIcon() {
                final TreeCellRenderer renderer = getCellRenderer();
                if (renderer != null  &&  renderer instanceof DefaultTreeCellRenderer) {
                    return ((DefaultTreeCellRenderer)renderer).getOpenIcon();
                }
                return super.getOpenIcon();
            }
            public Dimension getPreferredSize() {
                final TreeCellRenderer renderer = getCellRenderer();
                if (renderer != null  &&  renderer instanceof DefaultTreeCellRenderer) {
                    return ((DefaultTreeCellRenderer)renderer).getPreferredSize();
                }
                return super.getPreferredSize();
            }
            public Component getTreeCellRendererComponent(final JTree tree, final Object value, final boolean selected,
                                                  		  final boolean expanded, final boolean leaf, final int row,
														  final boolean focused) {
                final TreeCellRenderer renderer = getCellRenderer();
                if (renderer != null  &&  renderer instanceof DefaultTreeCellRenderer) {
                    return renderer.getTreeCellRendererComponent(tree, value, selected, expanded, leaf, row, focused);
                }
                return super.getTreeCellRendererComponent(tree, value, selected, expanded, leaf, row, focused);
            }
        };
        return new DefaultTreeCellEditor(this, adapter);
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since 2.0
    */
    protected TreeCellRenderer createDefaultTreeCellRenderer() {
        return new DefaultTreeCellRenderer();
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since 2.0
    */
    protected TreeModel createDefaultTreeModel(final Object value) {
        return new DefaultTreeModel(value);
    }
    
    public JToolTip createToolTip() {
        JToolTip tip = new MultiLineToolTip();
        tip.setComponent(this);
        return tip;
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since 2.0
    */
    public TreeNodeDragAndDropController getDragAndDropController() {
        return dndCtrlr;
    }
    
    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since 2.0
    */
    public PopupMenuFactory getPopupMenuFactory() {
        return popupMenuFactory;
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    Returns the row of the specified node as if it were visible, regardless of its actual visibility, but only if it's been made
    visible at least once.  If the node has never been made visible, then -1 is returned.
    @return The row of the specified node if it's ever been visible, -1 otherwise
    @since 2.0
    */
    public int getRowForNode(final TreePath parentPath, final Object node) {
        // Just return row for path if parent path is null
        if (parentPath == null) {
            return getRowForPath(new TreePath(new Object[] {node}));
        }
        // Return if parent has never been expanded
        if (!hasBeenExpanded(parentPath)) {
            return -1;
        }
        // Ensure parent is expanded, storing the path to the highest collapsed ancestor in the parent path, if any, along the way
        TreePath collapsedPath = null;
        if (isCollapsed(parentPath)) {
            collapsedPath = parentPath;
            while (collapsedPath.getPathCount() > 1  &&  isCollapsed(collapsedPath.getParentPath())) {
                collapsedPath = collapsedPath.getParentPath();
            }
            setExpandedState(parentPath, true);
        }
        // Extend path to include node
        final int count = parentPath.getPathCount();
        final Object[] nodePath = new Object[count + 1];
        System.arraycopy(parentPath.getPath(), 0, nodePath, 0, count);
        nodePath[count] = node;
        // Get node's expanded row
        final int parentRow = getRowForPath(parentPath);
        int row = getRowForPath(new TreePath(nodePath));
        for (int ndx = row - 1;  ndx > parentRow;  --ndx) {
            if (!getPathForRow(ndx).getParentPath().equals(parentPath)) {
                --row;
            }
        }
        // Restore tree original expanded/collapsed state
        if (collapsedPath != null) {
            setExpandedState(collapsedPath, false);
        }
        
        return row - parentRow - 1;
    }
    
    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since 2.0
    */
    protected void initializeTreeWidget() {
        modelListener = new TreeModelListener() {
            public void treeNodesChanged(final TreeModelEvent event) {
                if (getUI() instanceof TreeLookAndFeel) {
                    ((TreeLookAndFeel)getUI()).invalidateLayoutCache();
                }
            }
            public void treeNodesInserted(final TreeModelEvent event) {
                final TreePath rootPath = new TreePath(model.getRoot());
                if (!isRootVisible()  &&  !isExpanded(rootPath)) {
                    expandPath(rootPath);
                }
            }
            public void treeNodesRemoved(final TreeModelEvent event) {
            }
            public void treeStructureChanged(final TreeModelEvent event) {
            }
        };
        vetoListener = new VetoedChangeListener() {
            public void changeVetoed(final VetoedChangeEvent event) {
                TreeWidget.this.changeVetoed(event);
            }
        };
        if (model == null) {
            setModel(createDefaultTreeModel(value));
        } else {
            setModel(model);
        }
        isClipTipEnabled = true;
        ToolTipManager.sharedInstance().registerComponent(this);
        final TreeCellRenderer renderer = createDefaultTreeCellRenderer();
        setCellRenderer(renderer);
        if (renderer instanceof DefaultTreeCellRenderer) {
            setCellEditor(createDefaultTreeCellEditor());
        }
        setPopupMenuFactory(createDefaultPopupMenuFactory());
        addMouseListener(new MouseAdapter() {
            public void mousePressed(final MouseEvent event) {
                if (!SwingUtilities.isRightMouseButton(event)) {
                    return;
                }
                final TreePath path = getPathForLocation(event.getX(), event.getY());
                if (!isPathSelected(path)) {
                    setSelectionPath(path);
                }
                if (popupMenuFactory != null) {
                    final JPopupMenu popup = popupMenuFactory.getPopupMenu(TreeWidget.this);
                    if (popup != null) {
                        SwingUtilities.invokeLater(new Runnable() {
                            public void run() {
                                popup.show(TreeWidget.this, event.getX(), event.getY());
                            }
                        });
                    }
                }
            }
        });
        putClientProperty(LINE_STYLE_PROPERTY, UIDefaults.getInstance().getString(LINE_STYLE_PROPERTY));
        setInvokesStopCellEditing(true);
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since 2.0
    */
    public boolean isClipTipEnabled() {
        return isClipTipEnabled;
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since 2.0
    */
    public void setClipTipEnabled(final boolean isClipTipEnabled) {
        this.isClipTipEnabled = isClipTipEnabled;
    }
    
    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    Creates a controller to handle all drap and drop operations.
    @since 2.0
    */
    public void setDragAndDropController(final TreeNodeDragAndDropController controller) {
        // Remove reference to this TreeWidget from old controller
        if (dndCtrlr != null) {
            dndCtrlr.setComponent(null);
        }
        // Initialize new controller
        dndCtrlr = controller;
        if (dndCtrlr != null) {
            dndCtrlr.setComponent(this);
        }
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since 2.0
    */
    public void setModel(final TreeModel model) {
        if (model == null) {
            return;
        }
        model.addTreeModelListener(modelListener);
        if (model instanceof DefaultTreeModel) {
            final DefaultTreeModel dfltModel = (DefaultTreeModel)model;
            dfltModel.setTreeWidget(this);
            dfltModel.addVetoedChangeListener(vetoListener);
        }
        final TreeModel oldModel = getModel();
        if (oldModel != null) {
            oldModel.removeTreeModelListener(modelListener);
            if (oldModel instanceof DefaultTreeModel) {
                ((DefaultTreeModel)oldModel).removeVetoedChangeListener(vetoListener);
            }
        }
        super.setModel(model);
        this.model = model;
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since 2.0
    */
    public void setPopupMenuFactory(final PopupMenuFactory popupMenuFactory) {
        this.popupMenuFactory = popupMenuFactory;
    }
    
    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since 2.0
    */
    public void updateUI() {
        setUI(TreeLookAndFeel.createUI(this));
    }

	/**
     * Overridden to work around Java Bug Parade ID# 4292529
	 * @see javax.swing.JTree#getPathForLocation(int, int)
	 */
	public TreePath getPathForLocation(int x, int y) {
        TreePath closestPath = getClosestPathForLocation(x, y);
        if(closestPath != null) {
            Rectangle pathBounds = getPathBounds(closestPath);
            if ( pathBounds != null ) {
                return super.getPathForLocation(x,y);
            }
        }
        return null;
	}

}
