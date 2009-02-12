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

import java.awt.Component;
import java.awt.Container;
import java.awt.Dimension;
import java.awt.Graphics;
import java.awt.Point;
import java.awt.Rectangle;
import java.awt.event.ComponentAdapter;
import java.awt.event.ComponentEvent;
import java.awt.event.MouseAdapter;
import java.awt.event.MouseEvent;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Enumeration;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.Vector;

import javax.swing.JComponent;
import javax.swing.JPopupMenu;
import javax.swing.JScrollBar;
import javax.swing.JScrollPane;
import javax.swing.JTable;
import javax.swing.JToolTip;
import javax.swing.JViewport;
import javax.swing.SwingUtilities;
import javax.swing.event.AncestorEvent;
import javax.swing.event.AncestorListener;
import javax.swing.event.ChangeEvent;
import javax.swing.event.TableModelEvent;
import javax.swing.plaf.basic.BasicTableHeaderUI;
import javax.swing.table.JTableHeader;
import javax.swing.table.TableCellRenderer;
import javax.swing.table.TableColumn;
import javax.swing.table.TableColumnModel;
import javax.swing.table.TableModel;

import com.metamatrix.toolbox.ToolboxPlugin;
import com.metamatrix.toolbox.ui.TextUtilities;
import com.metamatrix.toolbox.ui.UIConstants;
import com.metamatrix.toolbox.ui.UIDefaults;
import com.metamatrix.toolbox.ui.widget.menu.DefaultPopupMenuFactory;
import com.metamatrix.toolbox.ui.widget.table.DefaultTableCellEditor;
import com.metamatrix.toolbox.ui.widget.table.DefaultTableCellRenderer;
import com.metamatrix.toolbox.ui.widget.table.DefaultTableColumnModel;
import com.metamatrix.toolbox.ui.widget.table.DefaultTableComparator;
import com.metamatrix.toolbox.ui.widget.table.DefaultTableHeader;
import com.metamatrix.toolbox.ui.widget.table.DefaultTableHeaderRenderer;
import com.metamatrix.toolbox.ui.widget.table.DefaultTableModel;
import com.metamatrix.toolbox.ui.widget.table.DefaultTableSorter;
import com.metamatrix.toolbox.ui.widget.table.EnhancedTableColumn;
import com.metamatrix.toolbox.ui.widget.table.EnhancedTableColumnModel;
import com.metamatrix.toolbox.ui.widget.table.EnhancedTableModel;
import com.metamatrix.toolbox.ui.widget.table.TableColumnSortListener;
import com.metamatrix.toolbox.ui.widget.table.TableComparator;
import com.metamatrix.toolbox.ui.widget.table.TableFilter;
import com.metamatrix.toolbox.ui.widget.table.TableHeader;
import com.metamatrix.toolbox.ui.widget.table.TableSorter;

/**
This class is intended to be used everywhere within the application that a table needs to be displayed.
Changing a cell's value will not automatically cause sorted columns to be re-sorted nor cause the row to be removed from the view
if it would now be filtered by the currently set filters.
@since 2.0
@version 2.1
@author John P. A. Verhaeg
*/
public class TableWidget extends JTable
implements TableColumnSortListener, UIConstants {
    //############################################################################################################################
    //# Static Constants and Variables                                                                                           #
    //############################################################################################################################
    
    public static final String PROPERTY_PREFIX = "Table."; //$NON-NLS-1$

    public static final String CHECKBOX_BORDER_PROPERTY     = PROPERTY_PREFIX + "checkBoxBorder"; //$NON-NLS-1$
    public static final String FOCUS_BORDER_PROPERTY        = PROPERTY_PREFIX + "focusCellHighlightBorder"; //$NON-NLS-1$
    public static final String FOCUS_BACKGROUND_PROPERTY    = PROPERTY_PREFIX + "focusCellBackground"; //$NON-NLS-1$
    public static final String NO_FOCUS_BORDER_PROPERTY     = PROPERTY_PREFIX + "noFocusBorder"; //$NON-NLS-1$
    
    private static final boolean NEED_MODIFIED_HEADER_UI;
    
    //############################################################################################################################
    //# Static Methods                                                                                                           #
    //############################################################################################################################
    
    //If the java version is 1.4 or greater, until proven otherwise we need an 
    //extension to the ComponentUI for the JTableHeader.  This is due to some
    //strangeness introduced in 1.4 which shrinks each column header by the value of
    //the column margin, which looks odd.  1.3 did not do this and we do not want it,
    //so we are kludging around it.  BWP 02/03/04
    static {
		boolean isGreater = false;
		String versStr = System.getProperty("java.version");//$NON-NLS-1$
		//To determine if the version is 1.4 or greater:
		//
		//We will find the second period in the version string, is there is one, and 
		//consider just those characters to the left of the second period.  If there 
		//are less than two periods we will look at the whole string.
		//We will try to form a Float from the portion of the string we are looking
		//at.  Then we will return whether this number > 1.3.
		int lastPosit = versStr.length() - 1;
		int numPeriods = 0;
		int posit = 0;
		String substring = null;
		while ((posit <= lastPosit) && (numPeriods < 2)) {
			char curChar = versStr.charAt(posit);
			if (curChar == '.') {
				numPeriods += 1;
				if (numPeriods == 2) {
					substring = versStr.substring(0, posit);
				}
			}
			posit += 1;
		}
		if (substring == null) {
			substring = versStr;
		}
		substring = substring.trim();
		Float versFloat = null;
		try {
			versFloat = new Float(substring);
		} catch (Exception ex) {
		}
		if (versFloat != null) {
			float vers = versFloat.floatValue();
			isGreater = (vers > 1.3);
		}
		NEED_MODIFIED_HEADER_UI = isGreater;
	}
	
    	
    
    //############################################################################################################################
    //# Instance Variables                                                                                                       #
    //############################################################################################################################

    private transient List colNames;    // Used by superclass

    private EnhancedTableModel model;   // Set by superclass
    private EnhancedTableColumnModel colModel;    // Set by superclass
    
    private List filters = null;
    
    private boolean isSortable = false;
    private TableSorter sorter = null;
    private TableComparator comparator = null;
    private boolean allowsMultipleColumnSorting;

    private List modelRowMap = null;

    private boolean sizingColumns = false;
    private int remainingDeltaColNdx = 0;

    private PopupMenuFactory popupMenuFactory = null;
        
    //############################################################################################################################
    //# Constructors                                                                                                             #
    //############################################################################################################################

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since 2.0
    */
    public TableWidget() {
        this((List)null, false);
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since 2.0
    */
    public TableWidget(final List columnNames) {
        this(columnNames, false);
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since 2.0
    */
    public TableWidget(final TableModel model) {
        this(model, false);
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since 2.0
    */
    public TableWidget(final boolean isSortable) {
        this((List)null, isSortable);
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since 2.0
    */
    public TableWidget(final List columnNames, final boolean isSortable) {
        colNames = columnNames;
        this.isSortable = isSortable;
        initializeTableWidget();
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since 2.0
    */
    public TableWidget(final TableModel model, final boolean isSortable) {
        super(model);
        this.isSortable = isSortable;
        initializeTableWidget();
    }

    //############################################################################################################################
    //# Instance Methods                                                                                                         #
    //############################################################################################################################

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    Overridden to set the header renderer, minimum width, header value, and ID of the specified column.
    @since 2.0
    */
    public void addColumn(final TableColumn column) {
        // Set the column's header renderer
        TableCellRenderer renderer = column.getHeaderRenderer();
        if (renderer == null) {
            renderer = createDefaultHeaderRenderer();
            column.setHeaderRenderer(renderer);
        }
        if (column.getHeaderValue() == null) {
            final int ndx = column.getModelIndex();
            // Set column ID to model column name
            final String name = getModel().getColumnName(ndx);
            column.setIdentifier(TextUtilities.getUnformattedText(name));
            // Set column header text to name passed in constructor if available, model column name otherwise
            if (colNames != null  &&  ndx < colNames.size()) {
                column.setHeaderValue(colNames.get(ndx));
            } else {
                column.setHeaderValue(name);
            }
        }
        // Add column to model
        colModel.addColumn(column);
        // Set the column's minimum width
        int someMinWidth = renderer.getTableCellRendererComponent(this, column.getHeaderValue(), false, false, -1, getColumnCount() - 1).getMinimumSize().width;
        column.setMinWidth(Math.min(someMinWidth, Short.MAX_VALUE ));
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since 2.0
    */
    public void addFilter(final TableFilter filter) {
        // Prevent null values from getting in filter list
        if (filter == null) {
            throw new NullPointerException(ToolboxPlugin.Util.getString("TableWidget.Filter_parameter_cannot_be_null_6")); //$NON-NLS-1$
        }
        if (filters == null) {
            filters = new ArrayList();
        }
        filters.add(filter);
        if (modelRowMap == null) {
            createModelRowMap();
        } else {
            final List rows = model.getDataVector();
            final Iterator iterator = modelRowMap.iterator();
            int rowNdx;
            while (iterator.hasNext()) {
                rowNdx = ((Integer)iterator.next()).intValue();
                if (filter.isFiltered(rowNdx, (List)rows.get(rowNdx))) {
                    iterator.remove();
                }
            }
        }
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    Returns whether multiple column sorting is allowed.  This value does not reflect whether the table is sortable at all, and is
    therefore meaningless unless the table is actually sortable.
    @return True if multiple column sorting is allowed
    @since 2.0
    */
    public boolean allowsMultipleColumnSorting() {
        return allowsMultipleColumnSorting;
    }
    
    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since 2.0
    */
    public void clearFilters() {
        filters = null;
        if (!isSorted()) {
            modelRowMap = null;
        } else {
            createModelRowMap();
        }
    }
    
    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since 2.0
    */
    public void columnMarginChanged(final ChangeEvent event) {
        int colNdx = -1;
        final JTableHeader hdr = getTableHeader();
        if (hdr != null) {
            final TableColumn col = hdr.getResizingColumn();
            if (col != null) {
                colNdx = colModel.getColumnIndex(col);
            }
        }
        sizeColumnsToFitContainer(colNdx);
    }
    
    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since 2.0
    */
    public void columnSorted() {
        sort();
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since 2.0
    */
    public int convertRowIndexToModel(final int row) {
        if (modelRowMap == null) {
            return row;
        }
        return ((Integer)modelRowMap.get(row)).intValue();
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since 2.1
    */
    public int convertRowIndexToView(final int row) {
        if (modelRowMap == null) {
            return row;
        }
        return modelRowMap.indexOf(new Integer(row));
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since 2.0
    */
    protected final TableColumnModel createDefaultColumnModel() {
        return createEnhancedColumnModel();
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since 2.0
    */
    public void createDefaultColumnsFromModel() {
        // Remove any current columns
        colModel.removeColumnModelListener(this);
        while (colModel.getColumnCount() > 0)
            colModel.removeColumn(colModel.getColumn(0));
        // Create new columns from the data model info
        for (int ndx = 0;  ndx < model.getColumnCount();  ++ndx) {
            addColumn(new EnhancedTableColumn(ndx));
        }
        colModel.addColumnModelListener(this);
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since 2.0
    */
    protected TableComparator createDefaultComparator() {
        return DefaultTableComparator.getInstance();
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since 2.0
    */
    protected final TableModel createDefaultDataModel() {
        return createEnhancedModel();
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since 2.0
    */
    protected TableCellRenderer createDefaultHeaderRenderer() {
        return DefaultTableHeaderRenderer.getInstance();
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
    Overridden for performance reasons.
    @since 2.0
    */
    protected void createDefaultRenderers() {
        defaultRenderersByColumnClass = new Hashtable();
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since 2.0
    */
    protected TableSorter createDefaultSorter() {
        return DefaultTableSorter.getInstance();
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since 2.0
    */
    protected final JTableHeader createDefaultTableHeader() {
        return new DefaultTableHeader(colModel);
    }
    
    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since 2.0
    */
    protected EnhancedTableColumnModel createEnhancedColumnModel() {
        colModel = new DefaultTableColumnModel();
        return colModel;
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since 2.0
    */
    protected EnhancedTableModel createEnhancedModel() {
        model = new DefaultTableModel();
        return model;
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since 2.0
    *  Changing from protected to public.  Part of fix to defect 14083.  BWP 10/20/04
    */
    public void createModelRowMap() {    
        modelRowMap = new ArrayList();
        mapModelRows(0, model.getRowCount());
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
    public TableComparator getComparator() {
        return comparator;
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since 2.0
    */
    public EnhancedTableColumnModel getEnhancedColumnModel() {
        return colModel;
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since 2.0
    */
    public EnhancedTableModel getEnhancedModel() {
        return model;
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since 2.0
    */
    public List getFilters() {
        return Collections.unmodifiableList(filters);
    }
    
    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since 2.0
    */
    public int getHiddenColumnCount() {
        return colModel.getHiddenColumnCount();
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since 2.0
    */
    public List getHiddenColumns() {
        return colModel.getHiddenColumns();
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
    Overridden to return the current width as the preferred width.
    @since 2.0
    */
    public Dimension getPreferredSize() {
        if (getWidth() == 0) {
            return super.getPreferredSize();
        }
        return new Dimension(getWidth(), super.getPreferredSize().height);
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since 2.0
    */
    public int getRowCount() {
        if (modelRowMap == null) {
            return super.getRowCount();
        }
        return modelRowMap.size();
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since 2.0
    */
    public boolean getScrollableTracksViewportWidth() {
        return false;
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since 2.0
    */
    public int getSortedColumnCount() {
        return colModel.getSortedColumnCount();
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since 2.0
    */
    public List getSortedColumns() {
        return colModel.getSortedColumns();
    }
    
    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since 2.0
    */
    public TableSorter getSorter() {
        return sorter;
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since 2.0
    */
    public Object getValueAt(final int rowIndex, final int columnIndex) {
        return model.getValueAt(convertRowIndexToModel(rowIndex), convertColumnIndexToModel(columnIndex));
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since 2.0
    */
    protected void increaseColumnSize(final int index, final int width) {
        final TableColumn col = colModel.getColumn(index);
        col.setWidth(col.getWidth() + width);
    }
    
    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since 2.0
    */
    protected void initializeTableWidget() {
        allowsMultipleColumnSorting = true;
        if (colNames != null) {
            final Vector colIDs = new Vector(colNames.size());
            final Iterator nameIterator = colNames.iterator();
            while (nameIterator.hasNext()) {
                colIDs.add(TextUtilities.getUnformattedText((String)nameIterator.next()));
            }
            model.setColumnIdentifiers(colIDs);
        }
        // Set default header that handles pressed header buttons when column is sortable
        setTableHeader(createDefaultTableHeader());
        // Create default sorting environment
        sorter = createDefaultSorter();
        comparator = createDefaultComparator();
        setSortable(isSortable);
        // Setup popup context menu capability with default popup menu
        setPopupMenuFactory(createDefaultPopupMenuFactory());
        addMouseListener(new MouseAdapter() {
            public void mousePressed(final MouseEvent event) {
                if (!SwingUtilities.isRightMouseButton(event)) {
                    return;
                }
                final int colNdx = columnAtPoint(event.getPoint());
                final int rowNdx = rowAtPoint(event.getPoint());
                if (!isCellSelected(rowNdx, colNdx)) {
                    setColumnSelectionInterval(colNdx, colNdx);
                    setRowSelectionInterval(rowNdx, rowNdx);
                }
                if (popupMenuFactory != null) {
                    final JPopupMenu popup = popupMenuFactory.getPopupMenu(TableWidget.this);
                    if (popup != null) {
                        SwingUtilities.invokeLater(new Runnable() {
                            public void run() {
                                popup.show(TableWidget.this, event.getX(), event.getY());
                            }
                        });
                    }
                }
            }
        });
        // Fix JDK keyboard action bug
//        patchKeyboardActions();
        // Add listeners to auto-resize table when JViewport parent resizes
        addAncestorListener(new AncestorListener() {
            private Container parent;
            private ComponentAdapter compListener;
            public void ancestorAdded(final AncestorEvent event) {
                parent = getParent();
                sizeColumnsToFitContainer(-1);
                compListener = new ComponentAdapter() {
                    public void componentResized(final ComponentEvent event) {
                        sizeColumnsToFitContainer(-1);
                    }
                };
                parent.addComponentListener(compListener);
                if (parent instanceof JViewport) {
                    final JScrollPane scroller = (JScrollPane)parent.getParent();   // Assume viewport parent is a JScrollPane
                    if (parent.getParent() == null) {
                        addAncestorListener(new AncestorListener() {
                            public void ancestorAdded(final AncestorEvent event) {
                                sizeColumnsToFitViewport(scroller);
                            }
                            public void ancestorMoved(final AncestorEvent event) {
                            }
                            public void ancestorRemoved(final AncestorEvent event) {
                            }
                        });
                    } else {
                        sizeColumnsToFitViewport(scroller);
                    }
                }
            }
            public void ancestorMoved(final AncestorEvent event) {
            }
            public void ancestorRemoved(final AncestorEvent event) {
                parent.removeComponentListener(compListener);
            }
        });
        // Set default cell renderer and editor for cells rendered as strings
        setDefaultEditor(Object.class, new DefaultTableCellEditor());
        setDefaultRenderer(Object.class, new DefaultTableCellRenderer());
        colModel.setColumnMargin(UIDefaults.getInstance().getInt(SPACER_HORIZONTAL_LENGTH_PROPERTY) / 2);
        if (NEED_MODIFIED_HEADER_UI) {
        	getTableHeader().setUI(new ExtendedBasicTableHeaderUI());
        }
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since 2.0
    */
    public boolean isCellEditable(final int rowIndex, final int columnIndex) {
        return model.isCellEditable(convertRowIndexToModel(rowIndex), convertColumnIndexToModel(columnIndex));
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since 2.0
    */
    public boolean isColumnEditable(final int columnIndex) {
        return model.isColumnEditable(convertColumnIndexToModel(columnIndex));
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since 2.0
    */
    public boolean isEditable() {
        return model.isEditable();
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since 2.0
    */
    protected boolean isFiltered(final int rowIndex, final List row) {
        if (filters == null) {
            return false;
        }
        final Iterator iterator = filters.iterator();
        while (iterator.hasNext()) {
            if (((TableFilter)iterator.next()).isFiltered(rowIndex, row)) {
                return true;
            }
        }
        return false;
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since 2.0
    */
    public boolean isFilteredBy(final TableFilter filter) {
        return filters.contains(filter);
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since 2.0
    */
    public boolean isRowEditable(final int rowIndex) {
        return model.isRowEditable(convertRowIndexToModel(rowIndex));
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since 2.0
    */
    public boolean isSorted() {
        return (colModel.getSortedColumnCount() > 0);
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since 2.0
    */
    public boolean isSortable() {
        return isSortable;
    }
    
    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since 2.0
    */
    protected void mapModelRows(final int firstRowIndex, final int rowCount) {
        final List sortedCols = colModel.getSortedColumns();
        final List rows = model.getDataVector();
        final ListIterator iterator = rows.listIterator(firstRowIndex);
        List row;
        for (int rowNdx = firstRowIndex;  rowNdx < firstRowIndex + rowCount;  ++rowNdx) {
            row = (List)iterator.next();
            if (!isFiltered(rowNdx, row)) {
                if (isSortable) {
                    modelRowMap.add(sorter.getInsertionIndex(row, /*rowNdx,*/ rows, modelRowMap, sortedCols, comparator, modelRowMap.size()),
                                    new Integer(rowNdx));
                } else {
                    modelRowMap.add(new Integer(rowNdx));
                }
            }
        }
    }
    
    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    Fixes a JDK bug where certain characters can't be entered in a cell unless in edit mode
    @since 2.0
    */
//    private void patchKeyboardActions() {
//        registerKeyboardAction('q');
//        registerKeyboardAction('"');
//        registerKeyboardAction('(');
//        registerKeyboardAction('$');
//        registerKeyboardAction('!');
//        registerKeyboardAction('%');
//        registerKeyboardAction('&');
//        registerKeyboardAction('#');
//        registerKeyboardAction('\'');
//    }
    
    /**
    @since 2.0
    */
//    private void registerKeyboardAction(final char chr) {
//        registerKeyboardAction(KeyStroke.getKeyStroke(chr), chr);
//    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since 2.0
    */
//    private void registerKeyboardAction(final KeyStroke keyStroke, final char chr) {
//        final ActionListener listener = new ActionListener() {
//            public void actionPerformed(final ActionEvent event) {
//                Component comp;
//                if (isEditing()) {
//                    comp = getEditorComponent();
//                    if (comp != null  &&  comp.isVisible()  &&  SwingUtilities.findFocusOwner(TableWidget.this) == comp) {
//                        return;
//                    }
//                } else {
//                    int rowNdx = getSelectionModel().getAnchorSelectionIndex();
//                    int colNdx = colModel.getSelectionModel().getAnchorSelectionIndex();
//                    if (rowNdx >= 0  &&  colNdx >= 0  &&  !editCellAt(rowNdx, colNdx)) {
//                        return;
//                    }
//                    comp = getEditorComponent();
//                }
//                if (!isEditing()  ||  comp == null  ||  !(comp instanceof JTextField)) {
//                    return;
//                }
//                final Keymap map = ((JTextField)comp).getKeymap();
//                Action action = map.getAction(keyStroke);
//                if (action == null) {
//                    action = map.getDefaultAction();
//                }
//                if (action != null) {
//                    action.actionPerformed(new ActionEvent(comp, ActionEvent.ACTION_PERFORMED, String.valueOf(chr)));
//                }
//            }
//        };
//        registerKeyboardAction(listener, keyStroke, WHEN_ANCESTOR_OF_FOCUSED_COMPONENT);
//    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since 2.0
    */
    public void removeFilter(final TableFilter filter) {
        filters.remove(filter);
        if (filters.size() == 0) {
            filters = null;
            if (!isSorted()) {
                modelRowMap = null;
            } else {
                createModelRowMap();
            }
        } else {
            createModelRowMap();
        }
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    Sets whether multiple column sorting is allowed.  This value does not reflect whether the table is sortable at all, and is
    therefore meaningless unless the table is actually sortable.
    @param allowsMultipleColumnSorting Indicates if multiple column sorting is allowed
    @since 2.0
    */
    public void setAllowsMultipleColumnSorting(final boolean allowsMultipleColumnSorting) {
        this.allowsMultipleColumnSorting = allowsMultipleColumnSorting;
    }
    
    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since 2.0
    */
    public void setCellEditable(final int rowIndex, final int columnIndex, final boolean isEditable) {
        model.setCellEditable(convertRowIndexToModel(rowIndex), convertColumnIndexToModel(columnIndex), isEditable);
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since 2.0
    */
    public void setColumnEditable(final int columnIndex, final boolean isEditable) {
        model.setColumnEditable(convertColumnIndexToModel(columnIndex), isEditable);
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since 2.0
    */
    public void setColumnHidden(final EnhancedTableColumn column, final boolean isHidden) {
        colModel.setColumnHidden(column, isHidden);
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since 2.0
    */
    public void setColumnModel(final TableColumnModel model) {
        if (!(model instanceof EnhancedTableColumnModel)) {
            throw new IllegalArgumentException(ToolboxPlugin.Util.getString("TableWidget.Model_parameter_must_be_an_instance_of__7") + EnhancedTableColumnModel.class); //$NON-NLS-1$
        }
        colModel = (EnhancedTableColumnModel)model;
        colModel.addColumnSortListener(this);
        super.setColumnModel(model);
    }
    
    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since 2.0
    */
    public void setColumnNotSorted(final EnhancedTableColumn column) {
        colModel.setColumnNotSorted(column, false);
    }
    
    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since 2.0
    */
    public void setColumnNotSorted(final EnhancedTableColumn column, final boolean isCurrentColumnSortOrderKept) {
        colModel.setColumnNotSorted(column, isCurrentColumnSortOrderKept);
    }
    
    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since 2.0
    */
    public void setColumnSortedAscending(final EnhancedTableColumn column) {
        colModel.setColumnSortedAscending(column, false);
    }
    
    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since 2.0
    */
    public void setColumnSortedAscending(final EnhancedTableColumn column, final boolean isCurrentColumnSortOrderKept) {
        colModel.setColumnSortedAscending(column, isCurrentColumnSortOrderKept);
    }
    
    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since 2.0
    */
    public void setColumnSortedDescending(final EnhancedTableColumn column) {
        colModel.setColumnSortedDescending(column, false);
    }
    
    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since 2.0
    */
    public void setColumnSortedDescending(final EnhancedTableColumn column, final boolean isCurrentColumnSortOrderKept) {
        colModel.setColumnSortedDescending(column, isCurrentColumnSortOrderKept);
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since 2.0
    */
    public void setColumnsHidden(final boolean isHidden) {
        colModel.setColumnsHidden(isHidden);
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since 2.0
    */
    public void setColumnsNotSorted() {
        colModel.setColumnsNotSorted();
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since 2.0
    */
    public void setComparator(final TableComparator comparator) {
        if (comparator == null) {
            this.comparator = createDefaultComparator();
        } else {
            this.comparator = comparator;
        }
        if (isSorted()) {
            sort();
        }
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since 2.0
    */
    public void setEditable(final boolean isEditable) {
        model.setEditable(isEditable);
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    Overridden to enforce models to be instances of EnhancedTableModel and to reapply sorting to columns with the same identifiers
    in both the new and old models.
    @since 2.0
    */
    public void setModel(final TableModel model) {
        if (!(model instanceof EnhancedTableModel)) {
            throw new IllegalArgumentException(ToolboxPlugin.Util.getString("TableWidget.Model_parameter_must_be_an_instance_of__8") + EnhancedTableModel.class); //$NON-NLS-1$
        }
        // Save columns to reapply sorting and hiding after replacing model
        final List cols = new ArrayList(colModel.getColumnCount());
        Enumeration iter = colModel.getColumns();
        while (iter.hasMoreElements()) {
            cols.add(iter.nextElement());
        }
        final List sortedCols = colModel.getSortedColumns();
        final List hiddenCols = colModel.getHiddenColumns();
        // Unhide columns so that later call to this method doesn't re-add the old hidden columns to the new hidden list
        colModel.setColumnsHidden(false);
        // Replace model
        this.model = (EnhancedTableModel)model;
        super.setModel(model);
        modelRowMap = null;
        // Reapply sorting to columns with same id
        if (sortedCols != null) {
            colModel.setColumnsNotSorted();
            EnhancedTableColumn col, sortedCol;
            final Iterator iterator = sortedCols.iterator();
            Enumeration iterator2;
            while (iterator.hasNext()) {
                sortedCol = (EnhancedTableColumn)iterator.next();
                iterator2 = colModel.getColumns();
                while (iterator2.hasMoreElements()) {
                    col = (EnhancedTableColumn)iterator2.nextElement();
                    if (col.getIdentifier().equals(sortedCol.getIdentifier())) {
                        if (sortedCol.isSortedAscending()) {
                            colModel.setColumnSortedAscending(col, true);
                        } else {
                            colModel.setColumnSortedDescending(col, true);
                        }
                        break;
                    }
                }
            }
        }
        // Recreate model row map if table was previously filtered
        if (modelRowMap == null  &&  filters != null  &&  filters.size() > 0) {
            createModelRowMap();
        }
        // Reapply hiding to columns with same id
        if (hiddenCols != null) {
            colModel.setColumnsHidden(false);
            EnhancedTableColumn col, hiddenCol;
            final Iterator iterator = hiddenCols.iterator();
            Enumeration iterator2;
            while (iterator.hasNext()) {
                hiddenCol = (EnhancedTableColumn)iterator.next();
                iterator2 = colModel.getColumns();
                while (iterator2.hasMoreElements()) {
                    col = (EnhancedTableColumn)iterator2.nextElement();
                    if (col.getIdentifier().equals(hiddenCol.getIdentifier())) {
                        colModel.setColumnHidden(col, true);
                        break;
                    }
                }
            }
        }
        // Reapply column sizes
        iter = colModel.getColumns();
        EnhancedTableColumn col, oldCol;
        Iterator iter2;
        while (iter.hasMoreElements()) {
            col = (EnhancedTableColumn)iter.nextElement();
            iter2 = cols.iterator();
            while (iter2.hasNext()) {
                oldCol = (EnhancedTableColumn)iter2.next();
                if (col.getIdentifier().equals(oldCol.getIdentifier())) {
                    col.setWidth(oldCol.getWidth());
                    break;
                }
            }
        }
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
    public void setRowEditable(final int rowIndex, final boolean isEditable) {
        model.setRowEditable(convertRowIndexToModel(rowIndex), isEditable);
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since 2.0
    */
    public void setSortable(final boolean isSortable) {
        this.isSortable = isSortable;
        if (isSortable) {
            if (filters == null) {
                modelRowMap = null;
            }
        } else if (isSorted()) {
            sort();
        }
        final JTableHeader hdr = getTableHeader();
        if (hdr != null) {
            hdr.repaint();
        }
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since 2.0
    */
    public void setSorter(final TableSorter sorter) {
        if (sorter == null) {
            this.sorter = createDefaultSorter();
        } else {
            this.sorter = sorter;
        }
        if (isSorted()) {
            sort();
        }
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
     * Unsort the entire table.  This method explicitly ignores the current sort state of the table
     * to clear all sort columns, run the sort algorithm, and resizeAndRepaint the table.
     * @since 3.1
     */
    public void unsortAll() {
        ((EnhancedTableColumnModel) getColumnModel()).setColumnsNotSorted();
        sort();
        resizeAndRepaint();
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since 2.0
    */
    public void setTableHeader(final JTableHeader header) {
        if (!(header instanceof TableHeader)) {
            throw new IllegalArgumentException(ToolboxPlugin.Util.getString("TableWidget.Model_parameter_must_be_an_instance_of__9") + TableHeader.class); //$NON-NLS-1$
        }
        super.setTableHeader(header);
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since 2.0
    */
    public void setValueAt(final Object value, final int rowIndex, final int columnIndex) {
        model.setValueAt(value, convertRowIndexToModel(rowIndex), convertColumnIndexToModel(columnIndex));
    }
    
    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    Changes the size of the specified column to match the size of its longest cell contents (or the header value if its longer
    than the longest cell contents).  Only the first number of data cells equal to the specified count are used to determine the
    longest cell.
    @param column   The column to be sized
    @param count    The number of data cells to use when determining the longest cell
    @return The column's view index
    @since 2.0
    */
    protected int sizeColumnToFitDataInternal(final EnhancedTableColumn column, int count) {
        final int modelColNdx = column.getModelIndex();
        final int colNdx = convertColumnIndexToView(modelColNdx);
        TableCellRenderer renderer = column.getCellRenderer();
        if (renderer == null) {
            renderer = getDefaultRenderer(getModel().getColumnClass(modelColNdx));
        }
        int maxWth = column.getMinWidth();
        count = Math.min(count, getRowCount());
        
        for (int rowNdx = 0;  rowNdx < count;  ++rowNdx) {
            Component component = renderer.getTableCellRendererComponent(this, getValueAt(rowNdx, colNdx), false, false, rowNdx, colNdx);
            int preferredWidth = component.getPreferredSize().width;
            if (preferredWidth < Short.MAX_VALUE) {
                maxWth = Math.max(maxWth, preferredWidth);
            }
        }
                
        
        maxWth += colModel.getColumnMargin();                
        column.setPreferredWidth(maxWth);
        return colNdx;
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    Changes the size of the specified column to match the size of its longest cell contents (or the header value if its longer
    than the longest cell contents).
    @param column The column to be sized
    @since 2.0
    */
    public void sizeColumnToFitData(final EnhancedTableColumn column) {
        sizeColumnToFitData(column, getRowCount());
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    Changes the size of the specified column to match the size of its longest cell contents (or the header value if its longer
    than the longest cell contents).  Only the first number of data cells equal to the specified count are used to determine the
    longest cell.
    @param column   The column to be sized
    @param count    The number of data cells to use when determining the longest cell
    @since 2.0
    */
    public void sizeColumnToFitData(final EnhancedTableColumn column, final int count) {
        if (isShowing()) {
            sizeColumnsToFitContainer(sizeColumnToFitDataInternal(column, count));
        } else {
            addComponentListener(new ComponentAdapter() {
                public void componentResized(final ComponentEvent event) {
                    sizeColumnToFitData(column, count);
                    removeComponentListener(this);
                }
            });
        }
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @deprecated
    @since 2.0
    */
    public void sizeColumnsToFit(final int resizedColumnIndex) {
        sizeColumnsToFitContainer(resizedColumnIndex);
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    Expands the columns with indexes after the specified column to fit within the table's container when necessary.  This method
    is called internally whenever a column or the table's container is resized.
    @param resizedColumnIndex The index of the column just resized or -1 if the table's container was resized
    @since 2.0
    */
    public void sizeColumnsToFitContainer(final int resizedColumnIndex) {
        final int count = colModel.getColumnCount();
        if (sizingColumns  ||  count == 0) {
            return;
        }
        sizingColumns = true;
        try {
            int wth = colModel.getTotalColumnWidth();
            // Adjust width to account for bug in JDK versions prior to 1.3
            final boolean oldJDK = (System.getProperty("java.version").compareTo("1.3") < 0); //$NON-NLS-1$ //$NON-NLS-2$
            if (oldJDK) {
                wth -= colModel.getColumnMargin() * count;
            }
            // Check if columns must be expanded to fit container
            final Container parent = getParent();
            if (parent != null  &&  wth < parent.getWidth()) {
                // Cache values used frequently below
                final int parentWth = parent.getWidth();
                int delta = parentWth - wth;
                // Sizes columns according to current resize mode
                final int mode = getAutoResizeMode();
                if (mode == AUTO_RESIZE_LAST_COLUMN) {
                    if (resizedColumnIndex == count - 1) {
                        increaseColumnSize(0, delta);
                    } else {
                        increaseColumnSize(count - 1, delta);
                    }
                } else if (mode == AUTO_RESIZE_NEXT_COLUMN  &&  resizedColumnIndex >= 0) {
                    if (count == 1) {
                        increaseColumnSize(0, delta);
                    } else if (resizedColumnIndex == count - 1) {
                        increaseColumnSize(resizedColumnIndex - 1, delta);
                    } else {
                        increaseColumnSize(resizedColumnIndex + 1, delta);
                    }
                } else {
                    // Get index of first column to size
                    int firstNdx, lastNdx;
                    if (count == 1) {
                        firstNdx = 0;
                        lastNdx = 1;
                    } else if (resizedColumnIndex == count - 1) {
                        firstNdx = 0;
                        lastNdx = count - 1;
                    } else {
                        firstNdx = resizedColumnIndex + 1;
                        lastNdx = count;
                    }
                    // Calc max width of columns eligible for resizing
                    // Don't need to check for overflow below since EnhancedTableColumn ensures max width <= Short.MAX_VALUE
                    int maxWth = 0;
                    for (int ndx = firstNdx;  ndx < lastNdx;  ++ndx) {
                        maxWth += colModel.getColumn(ndx).getMaxWidth();
                    }
                    // Calc percentage of change between max widths and current widths for these columns
                    final double pctDelta = (double)(delta) / (maxWth - wth);
                    // Adjust column sizes by this percentage of the difference of each column's max width and current width
                    TableColumn col;
                    wth = 0;
                    int expandableCols = 0;
                    for (int ndx = 0;  ndx < count;  ++ndx) {
                        col = colModel.getColumn(ndx);
                        if (ndx >= firstNdx  &&  ndx < lastNdx) {
                            col.setWidth(col.getWidth() + (int)((col.getMaxWidth() - col.getWidth()) * pctDelta));
                            // Keep track of how many columns still have available expansion room along the way
                            if (col.getWidth() < col.getMaxWidth()) {
                                ++expandableCols;
                            }
                        }
                        wth += col.getWidth();
                    }
                    // Distribute any remaining difference between the updated widths and the parent's width across the expandable
                    // columns
                    delta = parentWth - wth;
                    for (remainingDeltaColNdx %= count;  expandableCols > 0  &&  delta > 0;
                         remainingDeltaColNdx = (remainingDeltaColNdx + 1) % count) {
                        if (remainingDeltaColNdx < firstNdx  ||  remainingDeltaColNdx >= lastNdx) {
                            continue;
                        }
                        col = colModel.getColumn(remainingDeltaColNdx);
                        if (col.getWidth() < col.getMaxWidth()) {
                            --delta;
                            col.setWidth(col.getWidth() + 1);
                            if (col.getWidth() == col.getMaxWidth()) {
                                --expandableCols;
                            }
                        }
                    }
                    // As a last resort, distribute any remaining difference across columns ignoring max widths
                    for (remainingDeltaColNdx %= count;  delta > 0;  remainingDeltaColNdx = (remainingDeltaColNdx + 1) % count) {
                        if (remainingDeltaColNdx < firstNdx  ||  remainingDeltaColNdx >= lastNdx) {
                            continue;
                        }
                        col = colModel.getColumn(remainingDeltaColNdx);
                        --delta;
                        col.setWidth(col.getWidth() + 1);
                    }
                }
                // Update width to match parent's width
                wth = parentWth;
            }
            int hgt = getHeight();
            if (hgt == 0) {
                hgt = getPreferredSize().height;
            }
            // Set table's size to current column width
            setSize(wth, hgt);
            // Force repaint to see width changes
            resizeAndRepaint();
            // Update header similarly
            final JTableHeader hdr = getTableHeader();
            if (hdr != null) {
                hdr.setPreferredSize(null);
                hdr.setPreferredSize(new Dimension(wth, hdr.getPreferredSize().height));
                hdr.repaint();
            }
        } finally {
            sizingColumns = false;
        }
    }
    
    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    Changes the sizes of all columns to match the size of their longest cell contents (or the header value if they are longer
    than the longest cell contents).
    @since 2.0
    */
    public void sizeColumnsToFitData() {
        sizeColumnsToFitData(getRowCount());
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    Changes the sizes of all columns to match the size of their longest cell contents (or the header value if they are longer
    than the longest cell contents).  Only the first number of data cells equal to the specified count are used to determine the
    longest cell.
    @param count The number of data cells to use when determining the longest cell
    @since 2.0
    */
    public void sizeColumnsToFitData(final int count) {
        if (isShowing()) {
            final Enumeration iterator = colModel.getColumns();
            while (iterator.hasMoreElements()) {
                sizeColumnToFitDataInternal((EnhancedTableColumn)iterator.nextElement(), count);
            }
            sizeColumnsToFitContainer(-1);
        } else {
            addComponentListener(new ComponentAdapter() {
                public void componentResized(final ComponentEvent event) {
                    sizeColumnsToFitData(count);
                    removeComponentListener(this);
                }               
            });
        }
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    Attempts to resize the columns when both the table is a view within a JScrollPane and the vertical scroll bar first appears,
    in order to eliminate the horizontal scroll bar.
    @since 2.0
    */
    protected void sizeColumnsToFitViewport(final JScrollPane scroller) {
        final JScrollBar bar = scroller.getVerticalScrollBar();
        bar.addComponentListener(new ComponentAdapter() {
            public void componentShown(final ComponentEvent event) {
                final int wth = colModel.getTotalColumnWidth();
                final Container parent = getParent();
                if (parent == null) {
                    return;
                }
                final int parentWth = parent.getWidth();
                int delta = wth - parentWth;
                if (delta != bar.getWidth()) {
                    return;
                }
                // Calc min column width of all columns
                int minWth = 0;
                Enumeration iter = colModel.getColumns();
                while (iter.hasMoreElements()) {
                    minWth += ((TableColumn)iter.nextElement()).getMinWidth();
                }
                // Return if can't get rid of horizontal scroll bar anyway
                if (wth - delta < minWth) {
                    return;
                }
                // Shorten columns, starting with the last, until the delta is used up
                TableColumn col;
                int colDelta;
                for (int ndx = colModel.getColumnCount();  --ndx >= 0  &&  delta > 0;  delta -= colDelta) {
                    col = colModel.getColumn(ndx);
                    colDelta = Math.max(col.getMinWidth(), col.getWidth() - delta);
                    col.setWidth(colDelta);
                }
                // Set table's size to current column width
                int hgt = getHeight();
                if (hgt == 0) {
                    hgt = getPreferredSize().height;
                }
                setSize(parentWth, hgt);
                // Force repaint to see width changes
                resizeAndRepaint();
                // Update header similarly
                final JTableHeader hdr = getTableHeader();
                if (hdr != null) {
                    hdr.setPreferredSize(null);
                    hdr.setPreferredSize(new Dimension(parentWth, hdr.getPreferredSize().height));
                    hdr.repaint();
                }
            }
        });
    }
    
    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since 2.0
    */
    public void sort() {
        if (isEditing()  &&  !getCellEditor().stopCellEditing()) {
            return;
        }
        // Save model indexes of any selected rows
        final int[] tmpSelectedRows = getSelectedRows();
        clearSelection();
        final Object[] selectedRows = new Object[tmpSelectedRows.length];
        if (modelRowMap == null) {
            for (int ndx = selectedRows.length;  --ndx >= 0;) {
                selectedRows[ndx] = new Integer(tmpSelectedRows[ndx]);
            }
            createModelRowMap();
        } else {
            for (int ndx = selectedRows.length;  --ndx >= 0;) {
                selectedRows[ndx] = modelRowMap.get(tmpSelectedRows[ndx]);
            }
            final List rows = model.getDataVector();
            final List sortedCols = colModel.getSortedColumns();
            int rowNdx;
            for (int ndx = 0;  ndx < modelRowMap.size();  ++ndx) {
                rowNdx = ((Integer)modelRowMap.remove(ndx)).intValue();
                modelRowMap.add(sorter.getInsertionIndex((List)rows.get(rowNdx), /*rowNdx,*/ rows, modelRowMap, sortedCols, comparator, ndx),
                                new Integer(rowNdx));
            }
        }
        // Reselect any previously selected data
        int rowNdx;
        for (int ndx = selectedRows.length;  --ndx >= 0;) {
            rowNdx = modelRowMap.indexOf(selectedRows[ndx]);
            addRowSelectionInterval(rowNdx, rowNdx);
        }
        
        // make sure first selected row is visible
        if (selectedRows.length > 0) {
            scrollRectToVisible(getCellRect(modelRowMap.indexOf(selectedRows[0]), 0, true));
        }
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since 2.0
    */
    public void tableChanged(final TableModelEvent event) {
        super.tableChanged(event);
        // Resize columns if model changed
        if (event.getType() == TableModelEvent.UPDATE  &&  event.getFirstRow() == TableModelEvent.HEADER_ROW) {
            sizeColumnsToFitContainer(-1);
        }
        if (modelRowMap == null) {
            return;
        }
        final int type = event.getType();
        if (type == TableModelEvent.INSERT) {
            // Increment row indexes in map after added rows in model by number of added rows
            final int firstRowNdx = event.getFirstRow();
            final int rowCount = event.getLastRow() - firstRowNdx + 1;
            final ListIterator iterator = modelRowMap.listIterator();
            int rowNdx;
            while (iterator.hasNext()) {
                rowNdx = ((Integer)iterator.next()).intValue();
                if (rowNdx >= firstRowNdx) {
                    iterator.set(new Integer(rowNdx + rowCount));
                }
            }
            // Add added rows to map if not filtered
            mapModelRows(firstRowNdx, rowCount);
        } else if (type == TableModelEvent.DELETE) {
            // Decrement row indexes in map after deleted rows in model by number of deleted rows, and
            // Remove deleted rows from map if present (not filtered)
            final int firstRowNdx = event.getFirstRow();
            final int rowCount = event.getLastRow() - firstRowNdx + 1;
            final ListIterator iterator = modelRowMap.listIterator();
            int rowNdx;
            while (iterator.hasNext()) {
                rowNdx = ((Integer)iterator.next()).intValue();
                if (rowNdx >= firstRowNdx) {
                    if (rowNdx >= firstRowNdx + rowCount) {
                        iterator.set(new Integer(rowNdx - rowCount));
                    } else {
                        iterator.remove();
                    }
                }
            }
        }
    }
}//end TableWidget




/**
 * Extension to BasicTableHeaderUI to undo some strangeness introduced in v1.4, 
 * wherein each column header was being shrunk by the value of the column margin.
 */
class ExtendedBasicTableHeaderUI extends BasicTableHeaderUI {
	public ExtendedBasicTableHeaderUI() {
		super();
	}
	
	/**
	 * paint() method as in BasicTableHeaderUI(), with offending lines corrected.
	 */
	public void paint(Graphics g, JComponent c) {
		if (header.getColumnModel().getColumnCount() <= 0) { 
	    	return; 
		}
        boolean ltr = header.getComponentOrientation().isLeftToRight();

		Rectangle clip = g.getClipBounds(); 
        Point left = clip.getLocation();
        Point right = new Point( clip.x + clip.width - 1, clip.y );
		TableColumnModel cm = header.getColumnModel(); 
        int cMin = header.columnAtPoint( ltr ? left : right );
        int cMax = header.columnAtPoint( ltr ? right : left );
        // This should never happen. 
        if (cMin == -1) {
	    	cMin =  0;
        }
        // If the table does not have enough columns to fill the view we'll get -1.
        // Replace this with the index of the last column.
        if (cMax == -1) {
	    	cMax = cm.getColumnCount()-1;  
        }

		TableColumn draggedColumn = header.getDraggedColumn(); 
		int columnWidth;
		int columnMargin = cm.getColumnMargin();
        Rectangle cellRect = header.getHeaderRect(cMin); 
		TableColumn aColumn;
		if (ltr) {
	    	for(int column = cMin; column <= cMax ; column++) { 
				aColumn = cm.getColumn(column); 
				columnWidth = aColumn.getWidth();
//				cellRect.width = columnWidth - columnMargin;
cellRect.width = columnWidth;
				if (aColumn != draggedColumn) {
		    		paintCell(g, cellRect, column);
				} 
				cellRect.x += columnWidth;
	    	}
		} else {
	    	aColumn = cm.getColumn(cMin);
	    	if (aColumn != draggedColumn) {
				columnWidth = aColumn.getWidth();
//				cellRect.width = columnWidth - columnMargin;
cellRect.width = columnWidth;
				cellRect.x += columnMargin;
				paintCell(g, cellRect, cMin);
	    	}
	    	for(int column = cMin+1; column <= cMax; column++) {
				aColumn = cm.getColumn(column);
				columnWidth = aColumn.getWidth();
//				cellRect.width = columnWidth - columnMargin;
cellRect.width = columnWidth;
				cellRect.x -= columnWidth;
				if (aColumn != draggedColumn) {
		    		paintCell(g, cellRect, column);
				}
	    	}
		} 

        // Paint the dragged column if we are dragging. 
        if (draggedColumn != null) { 
            int draggedColumnIndex = viewIndexForColumn(draggedColumn); 
	    	Rectangle draggedCellRect = header.getHeaderRect(draggedColumnIndex); 
            
            // Draw a gray well in place of the moving column. 
            g.setColor(header.getParent().getBackground());
            g.fillRect(draggedCellRect.x, draggedCellRect.y,
            		draggedCellRect.width, draggedCellRect.height);

            draggedCellRect.x += header.getDraggedDistance();

	    	// Fill the background. 
	    	g.setColor(header.getBackground());
	    	g.fillRect(draggedCellRect.x, draggedCellRect.y,
		    		draggedCellRect.width, draggedCellRect.height);
 
            paintCell(g, draggedCellRect, draggedColumnIndex);
        }

		// Remove all components in the rendererPane. 
		rendererPane.removeAll(); 
    }

    private Component getHeaderRenderer(int columnIndex) { 
        TableColumn aColumn = header.getColumnModel().getColumn(columnIndex); 
		TableCellRenderer renderer = aColumn.getHeaderRenderer(); 
        if (renderer == null) { 
	    	renderer = header.getDefaultRenderer(); 
		}
		return renderer.getTableCellRendererComponent(header.getTable(), 
				aColumn.getHeaderValue(), false, false, -1, columnIndex);
    }

    private void paintCell(Graphics g, Rectangle cellRect, int columnIndex) {
        Component component = getHeaderRenderer(columnIndex); 
        rendererPane.paintComponent(g, component, header, cellRect.x, cellRect.y,
                            cellRect.width, cellRect.height, true);
    }

    private int viewIndexForColumn(TableColumn aColumn) {
        TableColumnModel cm = header.getColumnModel();
        for (int column = 0; column < cm.getColumnCount(); column++) {
            if (cm.getColumn(column) == aColumn) {
                return column;
            }
        }
        return -1;
    }
}//end ExtendedBasicTableHeaderUI
