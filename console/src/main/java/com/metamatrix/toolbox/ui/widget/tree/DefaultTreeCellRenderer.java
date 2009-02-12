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
package com.metamatrix.toolbox.ui.widget.tree;

// System imports
import java.awt.BasicStroke;
import java.awt.Color;
import java.awt.Component;
import java.awt.Container;
import java.awt.Graphics;
import java.awt.Graphics2D;
import java.awt.Rectangle;

import javax.swing.Icon;
import javax.swing.JTree;
import javax.swing.JViewport;
import javax.swing.UIManager;
import javax.swing.tree.TreeCellRenderer;

import com.metamatrix.toolbox.ui.widget.LabelWidget;
import com.metamatrix.toolbox.ui.widget.TreeWidget;
import com.metamatrix.toolbox.ui.widget.text.TextContainer;
import com.metamatrix.toolbox.ui.widget.transfer.TreeNodeDragAndDropController;

/**
This class is intended to be used everywhere within the application that a tree needs to be displayed.
@since 2.0
@version 2.0
@author John P. A. Verhaeg
*/
public class DefaultTreeCellRenderer extends LabelWidget
    implements TreeCellRenderer {
    //############################################################################################################################
    //# Static Variables                                                                                                         #
    //############################################################################################################################

    static final Rectangle BOUNDS = new Rectangle();
    
    private static final String DROP_TARGET_SELECTION_COLOR         = "DefaultTreeCellRenderer.dropTargetSelectionColor"; //$NON-NLS-1$
    private static final String DROP_TARGET_SELECTION_STROKE_WIDTH  = "DefaultTreeCellRenderer.dropTargetSelectionStrokeWidth"; //$NON-NLS-1$

    //############################################################################################################################
    //# Instance Variables                                                                                                       #
    //############################################################################################################################
    
    private javax.swing.tree.DefaultTreeCellRenderer javaRenderer = new javax.swing.tree.DefaultTreeCellRenderer();
    private transient boolean isSelected;
    private transient boolean hasFocus;
    private boolean isFocusBorderDrawnAroundIcon;
    private boolean willDropAboveTarget, willDropBelowTarget, willDropOnTarget;
    
    //############################################################################################################################
    //# Constructs                                                                                                               #
    //############################################################################################################################

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    public DefaultTreeCellRenderer() {
        initializeDefaultTreeCellRenderer();
    }
    
    //############################################################################################################################
    //# Instance Methods                                                                                                         #
    //############################################################################################################################

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since 2.0
    */
    public Color getBackgroundNonSelectionColor() {
        return javaRenderer.getBackgroundNonSelectionColor();
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since 2.0
    */
    public Color getBackgroundSelectionColor() {
        return javaRenderer.getBackgroundSelectionColor();
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since 2.0
    */
    public Color getBorderSelectionColor() {
        return javaRenderer.getBorderSelectionColor();
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since 2.0
    */
    public Icon getClosedIcon() {
        return javaRenderer.getClosedIcon();
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since 2.0
    */
    public Component getComponent() {
        return this;
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since 2.0
    */
    public Icon getDisabledClosedIcon() {
        return javaRenderer.getClosedIcon();
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since 2.0
    */
    public Icon getDisabledLeafIcon() {
        return javaRenderer.getLeafIcon();
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since 2.0
    */
    public Icon getDisabledOpenIcon() {
        return javaRenderer.getOpenIcon();
    }


    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since 2.0
    */
    private int getLabelStart() {
        final Icon icon = getIcon();
        if(icon != null  &&  getText() != null) {
            return icon.getIconWidth() + Math.max(0, getIconTextGap() - 1);
        }
        return 0;
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since 2.0
    */
    public Icon getLeafIcon() {
        return javaRenderer.getLeafIcon();
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since 2.0
    */
    public Icon getOpenIcon() {
        return javaRenderer.getOpenIcon();
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since 2.0
    */
    public Color getTextNonSelectionColor() {
        return javaRenderer.getTextNonSelectionColor();
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since 2.0
    */
    public Color getTextSelectionColor() {
        return javaRenderer.getTextSelectionColor();
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since 2.0
    */
    public Component getTreeCellRendererComponent(final JTree tree, final Object value, boolean isSelected,
                                                  final boolean isExpanded, final boolean isLeaf, final int row,
                                                  final boolean hasFocus) {
        if (value == null) {
            return this;
        }
        this.hasFocus = hasFocus;
        this.isSelected = isSelected;
        final String text = tree.convertValueToText(value, isSelected, isExpanded, isLeaf, row, hasFocus);
        if (((TextContainer)tree).isClipTipEnabled()  &&  hasFocus) {
            final Rectangle rowBounds = tree.getRowBounds(row);
            final Container parent = tree.getParent();
            if (!tree.getBounds(BOUNDS).contains(rowBounds)
                ||  (parent instanceof JViewport  &&  parent.getWidth() < rowBounds.x + rowBounds.width)) {
                setToolTipText(text);
            } else if (getToolTipText() != null) {
                setToolTipText(null);
            }
        }
        setText(text);
        setBackground(tree.getBackground());
        if (isSelected) {
            setForeground(getTextSelectionColor());
        } else {
            setForeground(getTextNonSelectionColor());
        }
        if (!tree.isEnabled()) {
            setEnabled(false);
            if (isLeaf) {
                setDisabledIcon(getDisabledLeafIcon());
            } else if (isExpanded) {
                setDisabledIcon(getDisabledOpenIcon());
            } else {
                setDisabledIcon(getDisabledClosedIcon());
            }
        } else {
            setEnabled(true);
            if (isLeaf) {
                setIcon(getLeafIcon());
            } else if (isExpanded) {
                setIcon(getOpenIcon());
            } else {
                setIcon(getClosedIcon());
            }
        }
        willDropAboveTarget = willDropBelowTarget = willDropOnTarget = false;
        final TreeWidget treeWidget = (TreeWidget)tree;
        final TreeNodeDragAndDropController dndCtrlr = treeWidget.getDragAndDropController();
        if (dndCtrlr != null  &&  dndCtrlr.isDragging()) {
            final Object target = dndCtrlr.getDropTarget();
            if (target == value) {
                willDropAboveTarget = dndCtrlr.willDropAboveTarget();
                willDropBelowTarget = dndCtrlr.willDropBelowTarget();
                willDropOnTarget = dndCtrlr.willDropOnTarget();
//            } else if (tree.getModel() instanceof DefaultTreeModel) {
//                final DefaultTreeModel model = (DefaultTreeModel)tree.getModel();
//                final TreeView view = model.getTreeView();
//                final TreeNode parent = view.getParent((TreeNode)value);
//                if (parent != null  &&  parent == target  &&
//                    model.getIndexOfChild(parent, value) == model.getChildCount(parent) - 1  &&  dndCtrlr.willDropOnTarget()) {
//                    willDropBelowTarget = true;
//                }
            }
        }
        return this;
    }
    
    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since 2.0
    */
    protected void initializeDefaultTreeCellRenderer() {
        final Object value = UIManager.get("Tree.drawsFocusBorderAroundIcon"); //$NON-NLS-1$
        isFocusBorderDrawnAroundIcon = (value != null  &&  value instanceof Boolean  &&  ((Boolean)value).booleanValue());
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since 2.0
    */
    public boolean isFocusBorderDrawnAroundIcon() {
        return isFocusBorderDrawnAroundIcon;
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since 2.0
    */
    public void paint(final Graphics canvas) {
        Color color;
        if (isSelected) {
            color = getBackgroundSelectionColor();
        } else {
            color = getBackgroundNonSelectionColor();
            if (color == null) {
                color = getBackground();
            }
        }
        int imageOffset = -1;
        if (color != null) {
            imageOffset = getLabelStart();
            canvas.setColor(color);
            canvas.fillRect(imageOffset, 0, getWidth() - 1 - imageOffset, getHeight());
        }
        if (hasFocus) {
            color = getBorderSelectionColor();
            if (color != null) {
                if (isFocusBorderDrawnAroundIcon) {
                    imageOffset = 0;
                } else if (imageOffset == -1) {
                    imageOffset = getLabelStart();
                }
                canvas.setColor(color);
                canvas.drawRect(imageOffset, 0, getWidth() - 1 - imageOffset, getHeight() - 1);
            }
        }
        super.paint(canvas);
        if (willDropOnTarget  ||  willDropAboveTarget  ||  willDropBelowTarget) {
            canvas.setColor(UIManager.getColor(DROP_TARGET_SELECTION_COLOR));
            int strokeWth = UIManager.getInt(DROP_TARGET_SELECTION_STROKE_WIDTH);
            ((Graphics2D)canvas).setStroke(new BasicStroke(strokeWth));
            strokeWth /= 2;
            if (willDropOnTarget) {
                canvas.drawRect(strokeWth, strokeWth, getWidth() - 1 - strokeWth, getHeight() - 1 - strokeWth);
            } else if (willDropAboveTarget) {
                canvas.drawLine(strokeWth, strokeWth, getWidth() - 1 - strokeWth, strokeWth);
            } else if (willDropBelowTarget) {
                canvas.drawLine(strokeWth, getHeight() - 1 - strokeWth, getWidth() - 1 - strokeWth, getHeight() - 1 - strokeWth);
            }
        }
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since 2.0
    */
    public void setBackgroundNonSelectionColor(final Color color) {
        javaRenderer.setBackgroundNonSelectionColor(color);
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since 2.0
    */
    public void setBackgroundSelectionColor(final Color color) {
        javaRenderer.setBackgroundSelectionColor(color);
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since 2.0
    */
    public void setBorderSelectionColor(final Color color) {
        javaRenderer.setBorderSelectionColor(color);
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since 2.0
    */
    public void setClosedIcon(final Icon icon) {
        javaRenderer.setClosedIcon(icon);
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since 2.0
    */
    public void setDisabledClosedIcon(final Icon icon) {
        javaRenderer.setClosedIcon(icon);
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since 2.0
    */
    public void setDisabledLeafIcon(final Icon icon) {
        javaRenderer.setLeafIcon(icon);
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since 2.0
    */
    public void setDisabledOpenIcon(final Icon icon) {
        javaRenderer.setOpenIcon(icon);
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since 2.0
    */
    public void setFocusBorderDrawnAroundIcon(final boolean isFocusBorderDrawnAroundIcon) {
        this.isFocusBorderDrawnAroundIcon = isFocusBorderDrawnAroundIcon;
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since 2.0
    */
    public void setLeafIcon(final Icon icon) {
        javaRenderer.setLeafIcon(icon);
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since 2.0
    */
    public void setOpenIcon(final Icon icon) {
        javaRenderer.setOpenIcon(icon);
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since 2.0
    */
    public void setTextNonSelectionColor(final Color color) {
        javaRenderer.setTextNonSelectionColor(color);
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since 2.0
    */
    public void setTextSelectionColor(final Color color) {
        javaRenderer.setTextSelectionColor(color);
    }
}
