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

//################################################################################################################################
package com.metamatrix.toolbox.ui.widget;

// JDK imports
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Vector;

import com.metamatrix.common.object.PropertiedObject;
import com.metamatrix.common.object.PropertyDefinition;
import com.metamatrix.common.tree.TreeNode;
import com.metamatrix.common.tree.TreeNodeEditor;
import com.metamatrix.common.tree.TreeView;
import com.metamatrix.core.util.Assertion;

import com.metamatrix.toolbox.ui.widget.table.DefaultTableModel;
import com.metamatrix.toolbox.ui.widget.transfer.DragAndDropController;

/**
@since 2.1
@version 2.1
@author <a href="mailto:jverhaeg@metamatrix.com">John P. A. Verhaeg</a>
*/
public class TreeNodeTableWidget extends TableWidget {
    //############################################################################################################################
    //# Instance Variables                                                                                                       #
    //############################################################################################################################
 
    private TreeNodeEditor editor;   
    private TreeView view;
    private List nodes;
    private List defs;
    private String emptyTableColName;
    private DragAndDropController dndCtrlr;
     
    //############################################################################################################################
    //# Constructors                                                                                                             #
    //############################################################################################################################
    
    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since 2.1
    */
    public TreeNodeTableWidget() {
        this(null, null, null, null);
    }
    
    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since 2.1
    */
    public TreeNodeTableWidget(final TreeNodeEditor editor) {
        this(editor, null, null, null);
    }
    
    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since 2.1
    */
    public TreeNodeTableWidget(final TreeNodeEditor editor, final TreeView view) {
        this(editor, view, null, null);
    }
    
    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since 2.1
    */
    public TreeNodeTableWidget(final TreeNodeEditor editor, final List nodes, final List definitions) {
        this(editor, null, nodes, definitions);
    }
    
    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since 2.1
    */
    public TreeNodeTableWidget(final TreeNodeEditor editor, final TreeView view, final List nodes) {
        this(editor, view, nodes, null);
    }
    
    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since 2.1
    */
    public TreeNodeTableWidget(final TreeNodeEditor editor, final TreeView view, final List nodes, final List definitions) {
        super(true);
        initializeTreeNodeTable(editor, view, nodes, definitions);
    }
    
    //############################################################################################################################
    //# Instance Methods                                                                                                         #
    //############################################################################################################################
    
    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since 2.1
    */
    public List getPropertyDefinitionsShown() {
        return defs;
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since 2.1
    */
    public List getTreeNodes() {
        return nodes;
    }
    
    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since 2.1
    */
    public TreeNode getTreeNode(final int index) {
        if (nodes == null) {
            return null;
        }
        return (TreeNode)nodes.get(index);
    }
    
    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since 2.1
    */
    public TreeView getTreeView() {
        return view;
    }
    
    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since 2.1
    */
    protected void initializeTreeNodeTable(final TreeNodeEditor editor, final TreeView view, final List nodes,
                                           final List definitions) {
        this.editor = editor;
        this.view = view;
        this.nodes = nodes;
        updateModel(editor, nodes, definitions);
    }
    
    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    Creates a controller to handle all drap and drop operations.
    @since 2.1
    */
    public void setDragAndDropController(final DragAndDropController controller) {
        // Initialize new controller
        dndCtrlr = controller;
        if (dndCtrlr != null) {
            dndCtrlr.setComponent(this);
        }
    }
    
    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since 2.1
    */
    public void setEmptyTableColumnName(final String name) {
        emptyTableColName = name;
        if (nodes == null  ||  nodes.size() == 0) {
            final DefaultTableModel model =
                new DefaultTableModel(new Vector(), new Vector(Arrays.asList(new String[] {emptyTableColName})));
            model.setEditable(false);
            setModel(model);
        }
    }
    
    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since 2.1
    */
    public void setPropertyDefinitionsShown(List definitions) {
        updateModel(editor, nodes, definitions);
    }
    
    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since 2.1
    */
    public void setTreeNodeEditor(final TreeNodeEditor editor) {
        this.editor = editor;
        updateModel(editor, nodes, defs);
    }
    
    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since 2.1
    */
    public void setTreeNodes(final List nodes) {
        this.nodes = nodes;
        updateModel(editor, nodes, defs);
    }
    
    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since 2.1
    */
    public void setTreeView(final TreeView view) {
        this.view = view;
    }
    
    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since 2.1
    */
    protected void updateModel(final TreeNodeEditor editor, List nodes, List definitions) {
        if (nodes == null  &&  view != null) {
            this.nodes = nodes = view.getRoots();
        }
        if ((definitions == null  ||  definitions.size() == 0)  &&  editor != null  &&  nodes != null  &&  nodes.size() > 0) {
            definitions = editor.getPropertyDefinitions((PropertiedObject)nodes.get(0));
        }
        defs = new ArrayList();
        if (definitions != null) {
            // Remove hidden properties
            final Iterator defIter = definitions.iterator();
            Object def;
            while (defIter.hasNext()) {
                def = defIter.next();
                // Ensure list contains only PropertyDefinitions along the way
                Assertion.assertTrue(def instanceof PropertyDefinition, "All elements must be instances of PropertyDefinition: "
                                 + def.getClass().getName());
                if (((PropertyDefinition)def).isModifiable()) {
                    defs.add(def);
                }
            }
        }
        if (editor == null) {
            return;
        }
        // Re-build model
        final Vector rowData = new Vector();
        final Vector colNames = new Vector();
        if (nodes == null  ||  nodes.size() == 0) {
            if (emptyTableColName != null) {
                colNames.add(emptyTableColName);
            }
        } else {
            final Iterator nodeIter = nodes.iterator();
            PropertiedObject node;
            final int colCount = defs.size();
            Vector colData;
            Iterator defIter;
            while (nodeIter.hasNext()) {
                node = (PropertiedObject)nodeIter.next();
                colData = new Vector(colCount);
                defIter = defs.iterator();
                while (defIter.hasNext()) {
                    colData.add(editor.getValue(node, (PropertyDefinition)defIter.next()));
                }
                rowData.add(colData);
            }
            defIter = defs.iterator();
            while (defIter.hasNext()) {
                colNames.add(((PropertyDefinition)defIter.next()).getDisplayName());
            }
        }
        final DefaultTableModel model = new DefaultTableModel(rowData, colNames);
        model.setEditable(false);
        setModel(model);
    }
}
