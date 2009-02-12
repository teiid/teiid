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

import java.awt.Component;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.awt.event.FocusAdapter;
import java.awt.event.FocusEvent;
import java.awt.event.MouseEvent;
import java.text.DateFormat;
import java.text.DecimalFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.EventObject;
import java.util.List;

import javax.swing.JTable;
import javax.swing.SwingConstants;
import javax.swing.event.CellEditorListener;
import javax.swing.event.ChangeEvent;
import javax.swing.table.TableCellEditor;

import com.metamatrix.toolbox.ui.UIDefaults;
import com.metamatrix.toolbox.ui.widget.CheckBox;
import com.metamatrix.toolbox.ui.widget.LabelWidget;
import com.metamatrix.toolbox.ui.widget.TableWidget;
import com.metamatrix.toolbox.ui.widget.TextFieldWidget;

/**
 * This is the default cell editor for TableWidgets.  It edits boolean values using CheckBoxes and numeric values using right-
 * justified TextFields.
 * @since 2.0
 * @version 2.0
 * @author <a href="mailto:jverhaeg@metamatrix.com">John P. A. Verhaeg</a>
 */
public class DefaultTableCellEditor
implements SwingConstants, TableCellEditor {
    //############################################################################################################################
    //# Instance Variables                                                                                                       #
    //############################################################################################################################

    public static final String CLICK_COUNT_TO_START_PROPERTY = TableWidget.PROPERTY_PREFIX + "clickCountToStart";

    private TextFieldWidget fld = null;
    private CheckBox checkBox = null;

    private Delegate fldDelegate;
    private Delegate checkBoxDelegate;
    private Delegate delegate;
    
    private List listeners = null;
    private int clickCountToStart = UIDefaults.getInstance().getInt(CLICK_COUNT_TO_START_PROPERTY);

    //############################################################################################################################
    //# Instance Methods                                                                                                         #
    //############################################################################################################################

    /**
    Registers the specified listener to be notified of changes to the editor.
    @param listener A CellEditorListener
    @since 2.0
    */
    public void addCellEditorListener(final CellEditorListener listener) {
        if (listeners == null) {
            listeners = new ArrayList();
        }
        listeners.add(listener);
    }

    /**
    Fires a ChangeEvent to all registered CellEditorListeners via the editingCanceled method.
    @since 2.0
    */
    public void cancelCellEditing() {
        if (listeners == null) {
            return;
        }
        // Fire event, notifying listeners from last to first
        ChangeEvent event = null;
        for (int ndx = listeners.size();  --ndx >= 0;) {
            if (event == null) {
                event = new ChangeEvent(this);
            }          
            ((CellEditorListener)listeners.get(ndx)).editingCanceled(event);
        }
    }

    /**
    @return The editor component's value
    @since 2.0
    */
    public Object getCellEditorValue() {
        return delegate.getCellEditorValue();
    }

    /**
	 * @since 3.0
	 */
    protected CheckBox getCheckBox() {
        return checkBox;
    }

    /**
    @return The number of mouse clicks required before the cell begins editing.
    @since 2.0
    */
    public int getClickCountToStart() {
        return clickCountToStart;
    }

    /**
    @since 2.0
    */
    public Component getTableCellEditorComponent(final JTable table, final Object value, final boolean isSelected,
                                                 final int rowIndex, final int columnIndex) {
        Component comp;
        final Component rendererComp =
            table.getCellRenderer(rowIndex, columnIndex).getTableCellRendererComponent(table, value, false, false, rowIndex,
                                                                                       columnIndex);
        if (value instanceof Boolean) { // Editing component should be a CheckBox
            // Create if necessary
            if (checkBox == null) {
                checkBox = new CheckBox();
                // Set properties to match renderer's
                final CheckBox rendererCheckBox = (CheckBox)rendererComp;
                checkBox.setHorizontalAlignment(rendererCheckBox.getHorizontalAlignment());
                checkBox.setBorder(rendererCheckBox.getBorder());
                // Create delegate
                checkBoxDelegate = new Delegate() {
                    public Object getCellEditorValue() {
                        return new Boolean(checkBox.isSelected());
                    }
                    public void setCellEditorValue(final Object value) {
                        if (value == null) {
                            checkBox.setSelected(false);
                        } else {
                            checkBox.setSelected(((Boolean)value).booleanValue());
                        }
                    }
                };
            }
            comp = checkBox;
            delegate = checkBoxDelegate;
        } else {    // Editing component should be a TextFieldWidget
            // Create if necessary
            if (fld == null) {
                fld = new TextFieldWidget();
                fld.setAutoscrolls(false);  // Fixes a JDK bug
                // Make hitting [Enter] key stop editing
                fld.addActionListener(new ActionListener() {
                    public void actionPerformed(final ActionEvent event) {
                        stopCellEditing();
                    }
                });
                // Set properties to match renderer's
                fld.setBorder(((LabelWidget)rendererComp).getBorder());
                // Create delegate
                fldDelegate = new Delegate() {
                    private DateFormat dateFmtr = null;
                    public Object getCellEditorValue() {
                        final String text = fld.getText();
                        final Object val = getOriginalCellEditorValue();
                        if (val instanceof Number) {
                            try {
                                return DecimalFormat.getInstance().parse(text);
                            } catch (final ParseException ignored) {
                                // So text will just be returned as a String
                            }
                        } else if (val instanceof Date) {
                            try {
                                return dateFmtr.parse(text);
                            } catch (final ParseException ignored) {
                                // So text will just be returned as a String
                            }
                        }
                        return text;
                    }
                    public void setCellEditorValue(final Object value) {
                        super.setCellEditorValue(value);
                        if (value == null) {
                            fld.setText("");
                        } else if (value instanceof Date) {
                            if (dateFmtr == null) {
                                dateFmtr = DateFormat.getInstance();
                                ((SimpleDateFormat)dateFmtr).applyPattern("MM/dd/yyyy hh:mm:ss a");  // Change later to locale-specific property
                            }
                            fld.setText(dateFmtr.format(value));
                        } else {
                            fld.setText(value.toString());
                        }
                    }
                };
            }
            if (value instanceof Number) {
                fld.setHorizontalAlignment(RIGHT);
            } else {
                fld.setHorizontalAlignment(LEFT);
            }
            comp = fld;
            delegate = fldDelegate;
        }
        initializeComponent(comp, table, value, isSelected, rowIndex, columnIndex);
        return comp;
    }

    /**
	 * @since 3.0
	 */
	protected TextFieldWidget getTextField() {
		return fld;
	}

    /**
    @since 2.1
    */
    protected void initializeComponent(final Component component, final JTable table, final Object value,
    								   final boolean isSelected, final int rowIndex, final int columnIndex) {
        final Component rendererComp =
            table.getCellRenderer(rowIndex, columnIndex).getTableCellRendererComponent(table, value, false, false, rowIndex,
                                                                                       columnIndex);
        // Set value
        delegate.setCellEditorValue(value);
        // Set properties to match renderer's
        component.setFont(rendererComp.getFont());
        component.setBackground(rendererComp.getBackground());
        // Add listener to stop editing when focus lost
        component.addFocusListener(new FocusAdapter() {
            public void focusLost(final FocusEvent event) {
                if (table.isEditing()) {
                    stopCellEditing();
                }
            }
        });
    }

    /**
    @param event An event object (usually a MouseEvent)
    @return True if the cell should start editing
    @since 2.0
    */
    public boolean isCellEditable(final EventObject event) {
        if (event instanceof MouseEvent) { 
            return ((MouseEvent)event).getClickCount() >= clickCountToStart;
        }
        return true;
    }

    /**
    Unregisters the specified listener.
    @param listener A CellEditorListener
    @since 2.0
    */
    public void removeCellEditorListener(final CellEditorListener listener) {
        listeners.remove(listener);
        if (listeners.size() == 0) {
            listeners = null;
        }
    }

    /**
     * @return The editor component's value
     * @since 3.0
     */
    public void setCellEditorValue(final Object value) {
        delegate.setCellEditorValue(value);
    }

    /**
    Sets the number of mouse clicks required before the cell begins editing.
    @param clickCountToStart The number of mouse clicks
    @since 2.0
    */
    public void setClickCountToStart(final int clickCountToStart) {
        this.clickCountToStart = clickCountToStart;
    }

    /**
    Sets the delegate property to the specified delegate and initializes its value to the specified value.  The delegate is used
    by the getCellEditorValue method.
    @since 2.0
    */
    protected void setDelegate(final Delegate delegate, final Object value) {
        this.delegate = delegate;
        delegate.setCellEditorValue(value);
    }
    
    /**
    @param event An event object (usually a MouseEvent)
    @return True
    @since 2.0
    */
    public boolean shouldSelectCell(final EventObject event) {
        return true;
    }

    /**
    Fires a ChangeEvent to all registered CellEditorListeners via the editingStopped method.
    @return True
    @since 2.0
    */
    public boolean stopCellEditing() {
        if (listeners == null) {
            return true;
        }
        // Fire event, notifying listeners from last to first
        ChangeEvent event = null;
        for (int ndx = listeners.size();  --ndx >= 0;) {
            if (event == null) {
                event = new ChangeEvent(this);
            }          
            ((CellEditorListener)listeners.get(ndx)).editingStopped(event);
        }
        return true;
    }

    //############################################################################################################################
    //# Inner Class: Delegate                                                                                                    #
    //############################################################################################################################
    
    /**
    @since 2.0
    */
    protected abstract class Delegate {
        //# Delegate #############################################################################################################
        //# Instance Variables                                                                                                   #
        //########################################################################################################################
        
        private Object origVal;
        
        //# Delegate #############################################################################################################
        //# Instance Methods                                                                                                     #
        //########################################################################################################################

        /// Delegate
        /**
        @since 2.0
        */
        public abstract Object getCellEditorValue();

        /// Delegate
        /**
        @since 2.0
        */
        public Object getOriginalCellEditorValue() {
            return origVal;
        }

        /// Delegate
        /**
        @since 2.0
        */
        public void setCellEditorValue(Object value) {
            origVal = value;
        }
    }
}
