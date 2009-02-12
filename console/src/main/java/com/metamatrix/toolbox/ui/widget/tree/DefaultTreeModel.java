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

package com.metamatrix.toolbox.ui.widget.tree;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import javax.swing.event.TreeModelEvent;
import javax.swing.event.TreeModelListener;
import javax.swing.tree.TreeModel;
import javax.swing.tree.TreePath;
import javax.swing.SwingUtilities;

import com.metamatrix.api.exception.MultipleRuntimeException;

import com.metamatrix.common.log.LogManager;
import com.metamatrix.common.transaction.TransactionException;
import com.metamatrix.common.transaction.UserTransaction;
import com.metamatrix.common.tree.TreeNode;
import com.metamatrix.common.tree.TreeNodeEditor;
import com.metamatrix.common.tree.TreeView;
import com.metamatrix.core.MetaMatrixRuntimeException;
import com.metamatrix.core.util.Assertion;

import com.metamatrix.toolbox.property.VetoedChangeEvent;
import com.metamatrix.toolbox.property.VetoedChangeListener;
import com.metamatrix.toolbox.ui.widget.TreeWidget;

/**
 * This is the default model used by {@link com.metamatrix.toolbox.ui.widget.TreeWidget TreeWidgets}.  It acts as an wrapper for
 * {@link com.metamatrix.common.tree.TreeView TreeView}.
 * @since 2.0
 */
public class DefaultTreeModel
implements TreeConstants, TreeModel {
    //############################################################################################################################
    //# Constants                                                                                                                #
    //############################################################################################################################
    
    public static final int MODEL_CHANGED   = 0;
    public static final int NODE_CHANGED    = 1;
    public static final int NODE_ADDED      = 2;
    public static final int NODE_REMOVED    = 3;
    public static final int NODES_CHANGED   = 4;

    private static final String HIDDEN_ROOT_NAME = "Root";
    
    //############################################################################################################################
    //# Variables                                                                                                                #
    //############################################################################################################################
    
    private TreeView view;
    private List modelListeners; 
    private List vetoListeners;
    private transient Object value;
    private Object xactionSrc;
    private DefaultTreeNode hiddenRoot;
    private boolean forceHiddenRoot;
    
    private transient TreeNode node;
    
    
    //############################################################################################################################
    //# Constructors                                                                                                             #
    //############################################################################################################################

    /**
     * Creates an empty model.
     * @since 2.0
     */
    public DefaultTreeModel() {
        this(null, false);
    }

    /**
     * Creates a model from the specified TreeView.
     * @param view The TreeView
     * @since 2.0
     */
    public DefaultTreeModel(final TreeView view) {
        this(view, false);
    }

    /**
     * Creates a model from the specified TreeView.  If specified via the second parameter, the TreeView will be added under a
     * "hidden" root.
     * @param view             The TreeView
     * @param forceHiddenRoot  Indicates to add a hidden root to the model as a parent to the TreeView's root(s)
     * @since 2.0
     */
    public DefaultTreeModel(final TreeView view, final boolean forceHiddenRoot) {
        this.view = view;
        this.forceHiddenRoot = forceHiddenRoot;
        initializeDefaultTreeModel();
    }

    /**
     * Creates a model with a default TreeView containing a default root TreeNode created using the specified value.
     * @param value The value used to create the default TreeNode
     * @since 2.0
     */
    public DefaultTreeModel(final Object value) {
        this.value = value;
        initializeDefaultTreeModel();
    }
    
    //############################################################################################################################
    //# Methods                                                                                                                  #
    //############################################################################################################################

    /**
     * Remember to add support for view.allowsChild method
     * @return The newly created child node
     * @since 2.0
     */
    public TreeNode addNode(final TreeNode parent, final String childName) {
        Assertion.assertTrue(hiddenRoot == null  ||  parent != hiddenRoot, "Nodes may not be added to the temporary root");
        final boolean succeeded = executeTransaction(new EditorTask() {
            public boolean execute(final TreeNodeEditor editor) {
        		Assertion.assertTrue(!editor.isReadOnly(parent), "Parent node is read-only");
	            node = editor.create(parent, childName, null);
	            fireEvent(NODE_ADDED, getTransactionSource(), node);
                return true;
            }
        }, "addNode(TreeNode, String)");
        if (!succeeded) {
            return null;
        }
        return node;
    }

    /**
     * @since 2.0
     */
    public synchronized void addTreeModelListener(final TreeModelListener listener) {
        if (modelListeners == null) {
            modelListeners = Collections.synchronizedList(new ArrayList());
        }
        modelListeners.add(listener);
    }

    /**
     * Adds the specified listener to the list of listeners registered to receive notifications of VetoedChangeEvents, which can
     * occur if a TreeNode's name change is rejected because it is either read-only or the new name is invalid.
     * @param listener The VetoedChangeListener to be registered
     * @since 2.0
     */
    public void addVetoedChangeListener(final VetoedChangeListener listener) {
        if (vetoListeners == null) {
            vetoListeners = Collections.synchronizedList(new ArrayList());
        }
        vetoListeners.add(listener);
    }
    
    /**
     * @since 2.0
     */
    protected TreeView createDefaultTreeView(final Object value) {
        return new DefaultTreeView(value);
    }
    
    /**
     * @since 2.1
     */
    protected boolean executeTransaction(final EditorTask task, final String context) {
        Throwable delayedErr = null;
        final TreeNodeEditor editor = getEditor();
   		Assertion.isNotNull(editor);
        final UserTransaction xaction = editor.createWriteTransaction(getTransactionSource());
        boolean succeeded = false;
        try {
            xaction.begin();
            succeeded = task.execute(editor);
            if (succeeded) {
		        succeeded = false;
            	xaction.commit();
		        succeeded = true;
            }
        } catch (final Throwable err) {
            delayedErr = err;
        } finally {
            if (!succeeded) {
	            try {
                    xaction.rollback();
	            } catch (final TransactionException err) {
					if (delayedErr == null) {
		            	delayedErr = err;
					} else {
					    delayedErr = new MultipleRuntimeException(new Throwable[] {delayedErr, err});
					}
                } finally {
    	            if (delayedErr != null) {
                        if (!(delayedErr instanceof RuntimeException)) {
                            delayedErr = new MetaMatrixRuntimeException(delayedErr);
                        }
    		            LogManager.logCritical(getClass().getName() + '.' + context, delayedErr, (String)null);
    	            	throw (RuntimeException)delayedErr;
    	            }
	            }
            }
        }
    	return succeeded;
    }

    /**
     * Fires a TreeModelEvent to the treeNodesChanged method of all registered listeners indicating that all of the children of
     * the specified parent have changed in some way.
     * @param source The source of the event
     * @param parent The parent of the child nodes that have changed
     * @since 2.1
     */
    public void fireChildrenChangedEvent(final Object source, final TreeNode parent) {
        final List childList = view.getChildren(parent);
        final int count = childList.size();
        final int[] ndxs = new int[count];
        final Object[] children = new Object[count];
        final Iterator iter = childList.iterator();
        for (int ndx = 0;  iter.hasNext();  ++ndx) {
            ndxs[ndx] = ndx;
            children[ndx] = iter.next();
        }
        fireEvent(MODEL_CHANGED, source, getPath(parent, 0), ndxs, children);
    }

    /**
     * @since 2.0
     */
    protected void fireEvent(final int type, final Object source, final TreeNode node) {
        final TreeNode parent = view.getParent(node);
        if (parent == null) {
            if (hiddenRoot != null) {
                fireEvent(type, source, getPath(hiddenRoot, 0), new int[] {view.getRoots().indexOf(node)}, new Object[] {node});
            } else {
                fireEvent(type, source, getPath(node, 0), null, null);
            }
        } else {
            int index = getChildIndex(parent, node);
            if(index != -1) {
                fireEvent(type, source, getPath(parent, 0), new int[] {index}, new Object[] {node});
            }
        }
    }

    /**
     * @since 2.0
     */
    protected void fireEvent(final Object source, final TreeNode parent, final TreeNode child, final int childIndex) {
        if (parent == null  &&  hiddenRoot != null) {
            fireEvent(NODE_REMOVED, source, getPath(hiddenRoot, 0), new int[] {childIndex}, new TreeNode[] {child});
        } else {
            fireEvent(NODE_REMOVED, source, getPath(parent, 0), new int[] {childIndex}, new TreeNode[] {child});
        }
    }


    
    protected void fireEvent(final int type, final Object source, final Object[] path, final int[] childIndices,
                             final Object[] children) {

        if (SwingUtilities.isEventDispatchThread()) {
            fireEventInner(type, source, path, childIndices, children);
        } else {
            //update tree in the Swing Thread
            Runnable runnable = new Runnable() {
                public void run() {
                    fireEventInner(type, source, path, childIndices, children);
                }
            };        
            SwingUtilities.invokeLater(runnable);            
        }
    }
    
    private synchronized void fireEventInner(final int type, final Object source, final Object[] path, final int[] childIndices,
                             final Object[] children) {
        
        if (modelListeners == null) {
            return;
        }
        List listeners = new ArrayList(modelListeners);
        final Iterator iterator = listeners.iterator();
        TreeModelListener listener;
        TreeModelEvent event = null;
        while (iterator.hasNext()) {
            if (event == null) {
                event = new TreeModelEvent(source, path, childIndices, children);
            }
            listener = (TreeModelListener)iterator.next();
            switch (type) {
                case NODE_CHANGED:
                case NODES_CHANGED: {
                    listener.treeNodesChanged(event);
                    break;
                }
                case NODE_ADDED: {
                    listener.treeNodesInserted(event);
                    break;
                }
                case NODE_REMOVED: {
                    listener.treeNodesRemoved(event);
                    break;
                }
                default: {
                    listener.treeStructureChanged(event);
                }
            }
        }
    }

    /**
     * @since 2.0
     */
    public void fireModelChangedEvent(final Object source, final TreeNode node) {
        fireEvent(MODEL_CHANGED, source, getPath(node, 0), null, null);
    }

    /**
     * @since 2.0
     */
    public void fireNodeAddedEvent(final Object source, final TreeNode node) {
        fireEvent(NODE_ADDED, source, node);
    }

    /**
     * @since 2.0
     */
    public void fireNodeChangedEvent(final Object source, final TreeNode node) {
        fireEvent(NODE_CHANGED, source, node);
    }

    /**
     * @since 2.0
     */
    public void fireNodeRemovedEvent(final Object source, final TreeNode parent, final TreeNode child, final int childIndex) {
        fireEvent(source, parent, child, childIndex);
    }

    /**
     * Fires a VetoedChangeEvent containing the specified TreeNode's TreePath and vetoed name to all registered listeners.
     * @param path     The TreePath of the TreeNode upon which the name change was attempted
     * @param property The key name of the name property.
     * @param name     The name that was vetoed
     * @since 2.0
     */
    protected void fireVetoedChangeEvent(final TreePath path, final String property, final Object name) {
        if (vetoListeners == null) {
            return;
        }
        final VetoedChangeEvent event = new VetoedChangeEvent(path, property, name);
        
        synchronized (vetoListeners) {
            final Iterator iterator = vetoListeners.iterator();
            while (iterator.hasNext()) {
                ((VetoedChangeListener)iterator.next()).changeVetoed(event);
            }
        }
    }

    /**
     * @since 2.0
     */
    public Object getChild(final Object parent, final int index) {
        if (hiddenRoot != null  &&  parent == hiddenRoot) {
            return view.getRoots().get(index);
        }
        final List children = view.getChildren((TreeNode)parent);
        if (children == null || 
            children.isEmpty() || 
            index >= children.size()) {
            return null;
        }
        return children.get(index);
    }

    /**
     * @since 2.0
     */
    public int getChildCount(final Object parent) {
        if (hiddenRoot != null  &&  parent == hiddenRoot) {
            return view.getRoots().size();
        }
        final List children = view.getChildren((TreeNode)parent);
        if (children == null) {
            return 0;
        }
        return children.size();
    }

    /**
     * @since 2.0
     */
    protected int getChildIndex(final Object parent, final Object child) {
        if (parent == null) {
            return 0;
        }
        if (hiddenRoot != null  &&  parent == hiddenRoot) {
            return view.getRoots().indexOf(child);
        }
        final List children = view.getChildren((TreeNode)parent);
        if (children == null || children.isEmpty()) {
            return -1;
        }
        return children.indexOf(child);
    }

    /**
     * @since 2.0
     */
    public TreeNodeEditor getEditor() {
        return view.getTreeNodeEditor();
    }

    /**
     * @since 2.0
     */
    public int getIndexOfChild(final Object parent, final Object child) {
        return getChildIndex(parent, child);
    }

    /**
     * @since 2.0
     */
    public String getName(final TreeNode node) {
        return node.getName();
    }

    /**
     * @since 2.0
     */
    public TreeNode[] getPath(final TreeNode node) {
        return getPath(node, 0);
    }

    /**
     * @since 2.0
     */
    protected TreeNode[] getPath(final TreeNode node, int size) {
        if (node == null  ||  node == hiddenRoot) {
            if (hiddenRoot != null) {
                final TreeNode[] path = new TreeNode[size + 1];
                path[0] = hiddenRoot;
                return path;
            }
            return new TreeNode[size];
        }
        final TreeNode[] path = getPath(view.getParent(node), ++size);
        path[path.length - size] = node;
        return path;
    }

    /**
     * @since 2.0
     */
    public Object getRoot() {
        if (hiddenRoot != null) {
            return hiddenRoot;
        }
        return view.getRoots().get(0);
    }

    /**
     * @return The object that is currently acting as the source of all write transactions.
     * @since 2.0
     */
    public Object getTransactionSource() {
        return xactionSrc;
    }

    /**
     * @return The TreeView to which this model applies.
     * @since 2.0
     */
    public TreeView getTreeView() {
        return view;
    }

    /**
     * @since 2.0
     */
    protected void initializeDefaultTreeModel() {
        xactionSrc = this;
        if (view == null) {
            view = createDefaultTreeView(value);
        }
        setTreeView(view);
    }

    /**
     * @since 2.0
     */
    public boolean isLeaf(final Object node) {
        if (hiddenRoot != null  &&  node == hiddenRoot) {
            return false;
        }
        return !view.allowsChildren((TreeNode)node);
    }

    /**
     * @since 2.0
     */
    public boolean isRootHidden() {
        return (hiddenRoot != null);
    }
    
    /**
     * @since 2.0
     */
    public boolean moveNode(final TreeNode node, final TreeNode parent) {
        return moveNode(node, parent, getChildCount(parent));
    }
    
    /**
     * @since 2.0
     */
    public boolean moveNode(final TreeNode node, final TreeNode parent, final int index) {
        Assertion.assertTrue(hiddenRoot == null  ||  (parent != hiddenRoot  &&  !view.getRoots().contains(parent)),
        				 "Nodes may not be moved within the temporary root");
        return executeTransaction(new EditorTask() {
            public boolean execute(final TreeNodeEditor editor) {
	            final TreeNode oldParent = view.getParent(node);
	            Assertion.assertTrue(!editor.isReadOnly(oldParent), "Node's original parent is read-only");
	            Assertion.assertTrue(!editor.isReadOnly(parent), "Node's new parent is read-only");
	            final int ndx = getChildIndex(oldParent, node);
	            if (editor.move(node, parent, index)) {
		            fireEvent(getTransactionSource(), oldParent, node, ndx);
		            fireNodeAddedEvent(getTransactionSource(), node);
	                return true;
	            }
	            return false;
            }
        }, "moveNode(TreeNode, TreeNode, int)");
    }
    
    /**
     * @since 2.0
     */
    public boolean removeNode(final TreeNode node) {
        Assertion.assertTrue(hiddenRoot == null  ||  (node != hiddenRoot  &&  !view.getRoots().contains(node)),
        				 "Nodes may not be removed from the temporary root");
        return executeTransaction(new EditorTask() {
            public boolean execute(final TreeNodeEditor editor) {
	            final TreeNode parent = view.getParent(node);
	            Assertion.assertTrue(!editor.isReadOnly(parent), "Node's parent is read-only");
	            final int ndx = getChildIndex(parent, node);
	            if (editor.delete(node)) {
		            fireEvent(getTransactionSource(), parent, node, ndx);
	                return true;
	            }
	            return false;
            }
        }, "removeNode(TreeNode)");
    }

    /**
     * @since 2.0
     */
    public synchronized void removeTreeModelListener(final TreeModelListener listener) {
        if (modelListeners == null) {
            return;
        }
        modelListeners.remove(listener);
        if (modelListeners.size() == 0) {
            modelListeners = null;
        }
    }

    /**
     * Removes the specified listener from the list of listeners registered to receive notifications of VetoedChangeEvents.
     * @param listener The VetoedChangeListener to be unregistered
     * @since 2.0
     */
    public void removeVetoedChangeListener(final VetoedChangeListener listener) {
        synchronized (vetoListeners) {
            if (vetoListeners == null) {
                return;
            }
            vetoListeners.remove(listener);
            if (vetoListeners.size() == 0) {
                vetoListeners = null;
            }
        }
    }

    /**
     * @since 2.0
     */
    public boolean setName(final TreeNode node, final String name) {
        if (hiddenRoot != null  &&  node == hiddenRoot) {
            hiddenRoot.setName(name);
            return true;
        }
        return executeTransaction(new EditorTask() {
            public boolean execute(final TreeNodeEditor editor) {
	            if (name != null  &&  name.length() > 0  &&  !editor.isReadOnly(node)
	            	&&  editor.isValidValue(node, editor.getNamePropertyDefinition(node), name)  &&  editor.rename(node, name)) {
	                fireEvent(NODE_CHANGED, getTransactionSource(), node);
	                return true;
	            }
	            return false;
            }
        }, "setName(TreeNode, String)");
    }

    /**
     * @since 2.0
     */
    public void setTreeWidget(final TreeWidget treeWidget) {
//        this.treeWidget = treeWidget;
    }

    /**
     * Sets the object that will act as the source of all write transactions.
     * @param source The source object
     * @since 2.0
     */
    public void setTransactionSource(final Object source) {
        if (source == null) {
            xactionSrc = this;
        } else {
            xactionSrc = source;
        }
    }

    /**
     * Sets the TreeView to which this model applies.
     * @param view The TreeView
     * @since 2.0
     */
    public void setTreeView(final TreeView view) {
        this.view = view;
        final List roots = view.getRoots();
        if (forceHiddenRoot  ||  roots.size() > 1) {
            hiddenRoot = new DefaultTreeNode(HIDDEN_ROOT_NAME);
            hiddenRoot.addChildren(roots);
            fireModelChangedEvent(xactionSrc, hiddenRoot);
        } else {
            hiddenRoot = null;
            fireModelChangedEvent(xactionSrc, (TreeNode)roots.get(0));
        }
    }

    /**
     * @param path The TreePath of the TreeNode upon which the name change was attempted
     * @param name The new name
     * @since 2.0
     */
    public void valueForPathChanged(final TreePath path, final Object name) {
        if ( path != null ) {
            final TreeNode node = (TreeNode)path.getLastPathComponent();
            if ( name != null ) {
                if ( name.equals(node.getName()) ) {
                    // don't set name if the name didn't change.
                    fireVetoedChangeEvent(path, NAME_PROPERTY, name);
                } else if ( !setName(node, name.toString()) ) {
                    fireVetoedChangeEvent(path, NAME_PROPERTY, name);
                }
            }
        }
    }

    //############################################################################################################################
    //# Inner Interface: EditorTask                                                                                              #
    //############################################################################################################################
    
    /**
     * @since 2.1
     */
    private interface EditorTask {
	    //# EditorTask ###########################################################################################################
	    //# Instance Methods                                                                                                     #
	    //########################################################################################################################
	
	    /// EditorTask
        /**
         * @since 2.1
         */
        boolean execute(TreeNodeEditor editor)
        	throws Exception;
    }
}
