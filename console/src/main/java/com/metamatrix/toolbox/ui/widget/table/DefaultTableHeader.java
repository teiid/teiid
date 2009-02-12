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
package com.metamatrix.toolbox.ui.widget.table;

// System imports
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.awt.event.MouseEvent;

import javax.swing.JMenuItem;
import javax.swing.SwingUtilities;
import javax.swing.event.MouseInputAdapter;
import javax.swing.table.JTableHeader;
import javax.swing.table.TableColumnModel;

import com.metamatrix.toolbox.ui.widget.ConfigurationPanel;
import com.metamatrix.toolbox.ui.widget.DialogWindow;
import com.metamatrix.toolbox.ui.widget.PopupMenu;
import com.metamatrix.toolbox.ui.widget.TableWidget;

/**
@since 2.0
@version 2.0
@author John P. A. Verhaeg
*/
public class DefaultTableHeader extends JTableHeader
implements TableConstants, TableHeader {
    //############################################################################################################################
    //# Instance Methods                                                                                                         #
    //############################################################################################################################

    private int armedColNdx = -1;
    private int pressedColNdx = -1;
    
    private AbstractTableFilterOptionPanel filterPanel;
    private TableOptionPanel optionPanel;

    private boolean allOptionsEnabled;
    private boolean popupMenuEnabled;

    //############################################################################################################################
    //# Constructors                                                                                                             #
    //############################################################################################################################

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since 2.0
    */
    public DefaultTableHeader(final TableColumnModel model) {
        super(model);
        initializeDefaultTableHeader();
    }

    //############################################################################################################################
    //# Instance Methods                                                                                                         #
    //############################################################################################################################

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since 2.0
    */
    protected TableOptionPanel createOptionPanel() {
        return new TableOptionPanel(this);
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since 2.0
    */
    public int getArmedColumnIndex() {
        return armedColNdx;
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since 2.0
    */
    public AbstractTableFilterOptionPanel getFilterOptionPanel() {
        return filterPanel;
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since 2.0
    */
    public TableOptionPanel getOptionPanel() {
        return optionPanel;
    }
    
    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since 2.0
    */
    public int getPressedColumnIndex() {
        return pressedColNdx;
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since 2.0
    */
    protected void initializeDefaultTableHeader() {
        popupMenuEnabled = allOptionsEnabled = true;
        final MouseInputAdapter listener = new MouseInputAdapter() {
            EnhancedTableColumn resizingCol = null;
            public void mouseClicked(final MouseEvent event) {
                final TableWidget table = (TableWidget)getTable();
                if (resizingCol != null) {
                    if (SwingUtilities.isLeftMouseButton(event)  &&  event.getClickCount() == 2  &&  table.getRowCount() > 0) {
                        table.sizeColumnToFitData(resizingCol);
                    }
                    return;
                }
                final int colNdx = table.columnAtPoint(event.getPoint());
                if (colNdx < 0) {
                    return;
                }
                final EnhancedTableColumnModel colModel = table.getEnhancedColumnModel();
                final EnhancedTableColumn col = (EnhancedTableColumn)colModel.getColumn(colNdx);
                // Display popup menu with sort/hide/filter options
                if (SwingUtilities.isRightMouseButton(event)  &&  popupMenuEnabled) {
                    final PopupMenu menu = new PopupMenu();
                    JMenuItem item;
                    if (table.isSortable()) {
                        final int count = colModel.getSortedColumnCount();
                        // The sorting options below should later be converted to display and handle column sort order if the
                        // [Ctrl] or [Shift] keys were pressed when the popup menu was displayed
                        item = new JMenuItem("Sort Ascending");
                        item.addActionListener(new ActionListener() {
                            public void actionPerformed(final ActionEvent event) {
                                if (shouldCurrentColumnSortOrderBeCleared(count, col)) {
                                    colModel.setColumnSortedAscending(col, !KEEP_CURRENT_COLUMN_SORT_ORDER);
                                } else if (!col.isSortedAscending()) {
                                    colModel.setColumnSortedAscending(col, KEEP_CURRENT_COLUMN_SORT_ORDER);
                                } else {
                                    return;
                                }
                                repaint();
                                table.repaint();
                            }
                        });
                        menu.add(item);
                        item = new JMenuItem("Sort Descending");
                        item.addActionListener(new ActionListener() {
                            public void actionPerformed(final ActionEvent event) {
                                if (shouldCurrentColumnSortOrderBeCleared(count, col)) {
                                    colModel.setColumnSortedDescending(col, !KEEP_CURRENT_COLUMN_SORT_ORDER);
                                } else if (!col.isSortedDescending()) {
                                    colModel.setColumnSortedDescending(col, KEEP_CURRENT_COLUMN_SORT_ORDER);
                                } else {
                                    return;
                                }
                                repaint();
                                table.repaint();
                            }
                        });
                        menu.add(item);
                        item = new JMenuItem("Do Not Sort");
                        item.addActionListener(new ActionListener() {
                            public void actionPerformed(final ActionEvent event) {
                                if (shouldCurrentColumnSortOrderBeCleared(count, col)) {
                                    colModel.setColumnNotSorted(col, !KEEP_CURRENT_COLUMN_SORT_ORDER);
                                } else if (col.isSorted()) {
                                    colModel.setColumnNotSorted(col, KEEP_CURRENT_COLUMN_SORT_ORDER);
                                } else {
                                    return;
                                }
                                repaint();
                                table.repaint();
                            }
                        });
                        menu.add(item);
                        menu.addSeparator();
                    }
                    if (colModel.getColumnCount() > 1) {
                        item = new JMenuItem("Hide");
                        item.addActionListener(new ActionListener() {
                            public void actionPerformed(final ActionEvent event) {
                                colModel.setColumnHidden(col, true);
                                table.sizeColumnsToFitContainer(-1);
                                repaint();
                                table.repaint();
                            }
                        });
                        menu.add(item);
                        menu.addSeparator();
                    }
                    if (getReorderingAllowed()) {
                        item = new JMenuItem("Reorder");
                        item.addActionListener(new ActionListener() {
                            public void actionPerformed(final ActionEvent event) {
                                final TableReorderOptionPanel reorderPanel = new TableReorderOptionPanel(table);
                                reorderPanel.setSelectedColumn(col);
                                final ConfigurationPanel configPanel = new ConfigurationPanel(reorderPanel);
                                configPanel.addApplyActionListener(new ActionListener() {
                                    public void actionPerformed(final ActionEvent event) {
                                        reorderColumns(reorderPanel);
                                    }
                                });
                                DialogWindow.show(table, "Reorder Columns", configPanel);
                                repaint();
                                table.repaint();
                            }
                        });
                        menu.add(item);
                        menu.addSeparator();
                    }
                    if (getResizingAllowed()) {
                        item = new JMenuItem("Fit To Contents");
                        item.addActionListener(new ActionListener() {
                            public void actionPerformed(final ActionEvent event) {
                                table.sizeColumnToFitData(col);
                                repaint();
                                table.repaint();
                            }
                        });
                        menu.add(item);
                        menu.addSeparator();
                    }
                    if (allOptionsEnabled) {
                        item = new JMenuItem("All Options...");
                        item.addActionListener(new ActionListener() {
                            public void actionPerformed(final ActionEvent event) {
                                optionPanel = createOptionPanel();
                                DialogWindow.show(table.getParent(), "Table Options", optionPanel);
                                // Repaint to erase menu in case it's behind dialog
                                SwingUtilities.windowForComponent(DefaultTableHeader.this).repaint();
                            }
                        });
                        menu.add(item);
                    }
                    menu.show(DefaultTableHeader.this, event.getX(), event.getY());
                    return;
                }
                // Sort column
                if (!table.isSortable()) {
                    return;
                }
                final int count = colModel.getSortedColumnCount();
                if (event.isAltDown()) {
                    if (!event.isShiftDown()  &&  !event.isControlDown()  &&  shouldCurrentColumnSortOrderBeCleared(count, col)) {
                        colModel.setColumnSortedDescending(col, !KEEP_CURRENT_COLUMN_SORT_ORDER);
                    } else if (!col.isSorted()  ||  (col.isSortedAscending()  &&  !event.isControlDown())) {
                        colModel.setColumnSortedDescending(col, table.allowsMultipleColumnSorting());
                    } else if (col.isSortedDescending()) {
                        colModel.setColumnSortedAscending(col, table.allowsMultipleColumnSorting());
                    } else {
                        colModel.setColumnNotSorted(col, table.allowsMultipleColumnSorting());
                    }
                } else {
                    if (!event.isShiftDown()  &&  !event.isControlDown()  &&  shouldCurrentColumnSortOrderBeCleared(count, col)) {
                        colModel.setColumnSortedAscending(col, !KEEP_CURRENT_COLUMN_SORT_ORDER);
                    } else if (!col.isSorted()  ||  (col.isSortedDescending()  &&  !event.isControlDown())) {
                        colModel.setColumnSortedAscending(col, table.allowsMultipleColumnSorting());
                    } else if (col.isSortedAscending()) {
                        colModel.setColumnSortedDescending(col, table.allowsMultipleColumnSorting());
                    } else {
                        colModel.setColumnNotSorted(col, table.allowsMultipleColumnSorting());
                    }
                }
                repaint();
                table.repaint();
            }
            public void mouseEntered(final MouseEvent event) {
                armedColNdx = getTable().columnAtPoint(event.getPoint());
            }
            public void mouseExited(final MouseEvent event) {
                armedColNdx = -1;
            }
            public void mouseMoved(final MouseEvent event) {
                armedColNdx = getTable().columnAtPoint(event.getPoint());
            }
            public void mousePressed(final MouseEvent event) {
                resizingCol = (EnhancedTableColumn)getResizingColumn();
                if (resizingCol != null  ||  SwingUtilities.isRightMouseButton(event)) {
                    return;
                }
                pressedColNdx = getTable().columnAtPoint(event.getPoint());
                repaint();
            }
            public void mouseDragged(final MouseEvent event) {
                pressedColNdx = -1;
            }
            public void mouseReleased(final MouseEvent event) {
                pressedColNdx = -1;
                repaint();
            }
        };
        addMouseListener(listener);
        addMouseMotionListener(listener);
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since 2.0
    */
    protected boolean shouldCurrentColumnSortOrderBeCleared(final int count, final EnhancedTableColumn column) {
        return (count > 0  &&  (count > 1  ||  !column.isSorted()));
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since 2.0
    */
    protected void reorderColumns(final TableReorderOptionPanel reorderPanel) {
        final EnhancedTableColumnModel model = ((TableWidget)getTable()).getEnhancedColumnModel();
        final Object[] cols = reorderPanel.getColumns();
        for (int ndx = 0;  ndx < cols.length;  ++ndx) {
            model.moveHiddenOrShownColumn(model.getHiddenOrShownColumnIndex(cols[ndx]), ndx);
        }
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    Sets whether the "All Options" option is displayed in the header's popup menu.
    @param enabled True if the "All Options" option should appear in the header's popup menu
    @since 2.0
    */
    public void setPopupAllOptionsEnabled(final boolean enabled) {
        allOptionsEnabled = enabled;
    }
    
    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since 2.0
    */
    public void setFilterOptionPanel(final AbstractTableFilterOptionPanel filterPanel) {
        this.filterPanel = filterPanel;
    }
    
    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since 2.1
    */
    public void setPopupMenuEnabled(final boolean enabled) {
        popupMenuEnabled = enabled;
    }
}
