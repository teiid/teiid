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
import java.awt.Component;
import java.awt.Rectangle;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

import javax.swing.Icon;
import javax.swing.JTable;
import javax.swing.SwingConstants;
import javax.swing.border.Border;
import javax.swing.table.TableCellRenderer;

import com.metamatrix.toolbox.ui.UIDefaults;
import com.metamatrix.toolbox.ui.widget.CheckBox;
import com.metamatrix.toolbox.ui.widget.LabelWidget;
import com.metamatrix.toolbox.ui.widget.TableWidget;

/**
This is the default cell renderer for TableWidgets.  It renders boolean values as CheckBoxes and right-justifies numeric values.
@since Golden Gate
@version Golden Gate
@author John P. A. Verhaeg
*/
public class DefaultTableCellRenderer
implements SwingConstants, TableCellRenderer {
    //############################################################################################################################
    //# Instance Variables                                                                                                       #
    //############################################################################################################################
    
    private LabelWidget label = null;
    private CheckBox checkBox = null;

    private final Border focusBorder = UIDefaults.getInstance().getBorder(TableWidget.FOCUS_BORDER_PROPERTY);
    public final Border noFocusBorder = UIDefaults.getInstance().getBorder(TableWidget.NO_FOCUS_BORDER_PROPERTY);
    public final Border checkBoxBorder = UIDefaults.getInstance().getBorder(TableWidget.CHECKBOX_BORDER_PROPERTY);

    private DateFormat dateFmtr = null;
    
    //############################################################################################################################
    //# Instance Methods                                                                                                         #
    //############################################################################################################################

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    Returns the appropriate Component to render the value in the specified table at the specified row and column indexes.
    @param table        The table containing the value
    @param value        The value
    @param isSelected   True if the cell containing the value is currently selected
    @param hasFocus     True if the cell containing the value currently has focus
    @param rowIndex     The row index of the cell containing the value
    @param columnIndex  The column index of the cell containing the value
    @return A CheckBox for boolean values, a LabelWidget with right-justified text for numeric values, and a LabelWidget with
            left-justified text for all other values.
    @since Golden Gate
    */
    public Component getTableCellRendererComponent(final JTable table, final Object value, final boolean isSelected,
                                                   final boolean hasFocus, final int rowIndex, final int columnIndex) {
        Component comp;
        if (value instanceof Boolean) {
            // Create if necessary
            if (checkBox == null) {
                checkBox = new DefaultBooleanComponent();
                // Make box disabled background color match table's
                checkBox.setBoxDisabledBackgroundColor(table.getBackground());
            }
            // Set value
            checkBox.setSelected(((Boolean)value).booleanValue());
            // Set enabled status
            checkBox.setEnabled(table.isCellEditable(rowIndex, columnIndex));
            // Set border
            checkBox.setBorder((hasFocus) ? focusBorder : checkBoxBorder);

            comp = checkBox;
        } else {
            // Create if necessary
            if (label == null) {
                label = new DefaultTextComponent();
                // Make foreground match table's
                label.setForeground(table.getForeground());
            }
            label.setIcon(null);
            // Set value and alignment
            if (value == null) {
                label.setText(""); //$NON-NLS-1$
            } else if (value instanceof Number) {
                label.setText(value.toString());
                label.setHorizontalAlignment(RIGHT);
            } else if (value instanceof Icon) {
                label.setIcon((Icon)value);
                label.setHorizontalAlignment(CENTER);
                label.setText(""); //$NON-NLS-1$
            } else if (value instanceof Date) {
                // Initialize date formatter
                if (dateFmtr == null) {
                    dateFmtr = DateFormat.getInstance();
                    ((SimpleDateFormat)dateFmtr).applyPattern("MM/dd/yyyy hh:mm:ss a");  // Change later to locale-specific property  //$NON-NLS-1$
                }
                label.setText(dateFmtr.format(value));
                label.setHorizontalAlignment(LEFT);
            } else {
                label.setText(value.toString());
                label.setHorizontalAlignment(LEFT);
            }
            // Set tooltip if necessary
            final Rectangle bounds = table.getCellRect(rowIndex, columnIndex, false);
            if (bounds.width < label.getPreferredSize().width) {
                label.setToolTipText(label.getText());
            } else {
                label.setToolTipText(null);
            }
            // Set border
            label.setBorder((hasFocus) ? focusBorder : noFocusBorder);

            comp = label;
        }
        initializeComponent(comp, table, isSelected, hasFocus, rowIndex, columnIndex);
        return comp;
    }
    
    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since 2.1
    */
    protected void initializeComponent(final Component component, final JTable table, final boolean isSelected,
                                       final boolean hasFocus, final int rowIndex, final int columnIndex) {
        // Make font match table's
        component.setFont(table.getFont());
        // Set background color
//        if (table.isCellEditable(rowIndex, columnIndex)) {
            if (hasFocus  &&  table.isCellEditable(rowIndex, columnIndex)) {
                component.setBackground(UIDefaults.getInstance().getColor(TableWidget.FOCUS_BACKGROUND_PROPERTY));
            } else if (isSelected) {
                component.setBackground(table.getSelectionBackground());
            } else {
                component.setBackground(table.getBackground());
            }
/*            
        } else {
            
            if (isSelected) {
                comp.setBackground(table.getSelectionBackground().darker());
            } else {
                comp.setBackground(ToolboxStandards.getColor(ToolboxStandards.TEXT_FIELD_UNEDITABLE_BACKGROUND_COLOR));
            }
        }
*/        
    }

    public void setBooleanComponent(CheckBox theCheckBox) {
        checkBox = theCheckBox;
    }
        
    public void setTextComponent(LabelWidget theLabel) {
        label = theLabel;
    }
        
    //############################################################################################################################
    //# DefaultBooleanComponent Inner Class                                                                                                       #
    //############################################################################################################################

    public class DefaultBooleanComponent extends CheckBox {
        public DefaultBooleanComponent() {
            // Center within cell
            setHorizontalAlignment(CENTER);
            // Change border to something visible on white background
            setBorder(checkBoxBorder);
        }
        
        // The following methods are overridden for performance reasons
        public void firePropertyChange(final String propertyName, final boolean oldValue, final boolean newValue) {
        }
        protected void firePropertyChange(final String propertyName, final Object oldValue, final Object newValue) {  
            if ("text".equals(propertyName)) { //$NON-NLS-1$
                super.firePropertyChange(propertyName, oldValue, newValue);
            }
        }
        public void repaint(final java.awt.Rectangle bounds) {
        }
        public void repaint(final long time, final int x, final int y, final int width, final int height) {
        }
        public void revalidate() {
        }
        public void validate() {
        }
    }
    
    //############################################################################################################################
    //# DefaultTextComponent Inner Class                                                                                                       #
    //############################################################################################################################

    public class DefaultTextComponent extends LabelWidget {
        public DefaultTextComponent() {
            // Make opaque so background color settings can be seen
            setOpaque(true);
        }

        // The following methods are overridden for performance reasons
        public void firePropertyChange(final String propertyName, final boolean oldValue, final boolean newValue) {
        }
        protected void firePropertyChange(final String propertyName, final Object oldValue, final Object newValue) {  
            if ("text".equals(propertyName)) { //$NON-NLS-1$
                super.firePropertyChange(propertyName, oldValue, newValue);
            }
        }
        public void repaint(final java.awt.Rectangle bounds) {
        }
        public void repaint(final long time, final int x, final int y, final int width, final int height) {
        }
        public void revalidate() {
        }
        public void validate() {
        }
    }
    
}
