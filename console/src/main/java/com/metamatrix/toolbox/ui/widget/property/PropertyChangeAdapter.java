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
package com.metamatrix.toolbox.ui.widget.property;

import java.awt.Color;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.awt.event.FocusEvent;
import java.awt.event.KeyAdapter;
import java.awt.event.KeyEvent;
import java.beans.PropertyChangeEvent;
import java.beans.PropertyChangeListener;
import java.text.DateFormat;
import java.text.ParseException;

import javax.swing.DefaultComboBoxModel;
import javax.swing.JCheckBox;
import javax.swing.JComboBox;
import javax.swing.JComponent;
import javax.swing.JPasswordField;
import javax.swing.event.EventListenerList;
import javax.swing.text.JTextComponent;

import com.metamatrix.common.log.LogManager;
import com.metamatrix.common.object.PropertiedObject;
import com.metamatrix.common.object.PropertiedObjectEditor;
import com.metamatrix.common.object.PropertyDefinition;
import com.metamatrix.common.transaction.TransactionException;
import com.metamatrix.common.transaction.UserTransaction;
import com.metamatrix.toolbox.ui.widget.PasswordButton;

/**
 * PropertyChangeAdapter is a class for obtaining, validating, and setting property values for
 * all editable properties in the PropertiesPanel.  The class listens to key and selection
 * events from the various types of JComponent TableCellEditor implementations for the various
 * PropertyDefinition types and adapts them to use the PropertiedObject methods isValidValue()
 * and setValue().
 */
public class PropertyChangeAdapter extends KeyAdapter implements PropertyValidationListener {

    private Object originalValue;
    private PropertiedObject entity;
    private PropertiedObjectEditor objectEditor;
    private PropertyDefinition def;
    private Object transactionSource;
    private JComponent currentEditorComponent;
    private boolean isEditing = false;
    private EventListenerList listenerList;
    private boolean matchedOrigVal;
    private boolean transactionPending = false;

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since 2.0
    */
    public PropertyChangeAdapter(PropertiedObjectEditor editor, Object transactionSource) {
        this.objectEditor = editor;
        this.transactionSource = transactionSource;
        listenerList = new EventListenerList();
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    Registers the specified listener to be notified whenever the value of one of the displayed components changes.  An event will
    be fired to the listener regardless of whether the new value if valid.  Since the property whose value the component is
    displaying is only changed if the value if valid, the PropertiedObjectEditor's getValue may return the either the new or old
    value depending on the validity of the new value.
    @param listener The PropertyChangeListener
    @since 2.0
    */
    public void addPropertyChangeListener(final PropertyChangeListener listener) {
        listenerList.add(PropertyChangeListener.class, listener);
    }

    /**
     * sets or clears the transactionPending flag
     */
    public void setTransactionPending(boolean isPending) {
        transactionPending = isPending;
    }

    /**
     * sets or clears the transactionPending flag
     */
    public boolean isTransactionPending() {
        return transactionPending;
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    Fires a PropertyChangeEvent to all registered listeners.
    @since 2.0
    */
    protected void firePropertyChangeEvent(final Object source, final Object value) {
        final Object[] listeners = listenerList.getListenerList();
        PropertyChangeEvent event = null;
        for (int ndx = listeners.length - 2;  ndx >= 0;  ndx -= 2) {
            if (listeners[ndx] == PropertyChangeListener.class) {
                if (event == null) {
                    event = new PropertyChangeEvent(source, def.getName(), originalValue, value);
                }
                ((PropertyChangeListener)listeners[ndx + 1]).propertyChange(event);
            }          
        }
    }

    /**
     * set the PropertiedObject that this adapter will be using for validation and setting
     * of new property values.
     * @param entity the PropertiedObject for setting this property value.
     */
    public void setPropertiedObject(PropertiedObject entity) {
        this.entity = entity;
        transactionPending = false;
    }

    /**
     * set the PropertyDefinition that this adapter will be controlling the value of for
     * the PropertiedObject.
     * @param propertyDefinition the PropertyDefinition of this property.
     */
    public void setPropertyDefinition(PropertyDefinition propertyDefinition) {
        this.def = propertyDefinition;
        transactionPending = false;
    }

    /**
     * determine if this Adapter is in the middle of a user edit.
     * @return true if the property value has edit focus and has not yet been set on the Propertied
     * Object.  false if it is safe to use the PropertiedObject.
     */
    public boolean isEditing() {
        return this.isEditing;
    }

    /**
     * set this property's TableCellEditor just before editing of the value begins.
     * The method obtains and saves the original value from the JComponent, and
     * wires up to listen for the appropriate events to determine when the user
     * has modified the value.
     * @param editor the JComponent responsible for editing this property value.
     */
    public void setEditorComponentBeforeEdit(JComponent editor) {
        originalValue = getValueFromJComponent(editor, false);
        matchedOrigVal = true;
        setValidityListenerForJComponent(editor);
        //editor.addFocusListener(this); NOW BEING DONE BY PropertyFocusListener
        currentEditorComponent = editor;
        isEditing = true;
    }

    /**
     * set this property's TableCellEditor just after editing of the value has
     * completed. The method obtains new value from the JComponent and compares it
     * to the original value.  If the values differ, and the new value is valid,
     * this method will set the property on the PropertiedObject.
     * @param editor the JComponent responsible for editing this property value.
     */
    protected void setEditorComponentAfterEdit(JComponent editor) {
        if ( this.def != null && this.entity != null ) {

            if ( ! objectEditor.isReadOnly(entity, def) ) {

                isEditing = true;
                boolean setValue = true;

	            Object value = getValueFromJComponent(editor, true);
	            if (editor instanceof PasswordButton  &&  value instanceof char[]) {
	                value = new String((char[])value);
	            }
	            Object nullValue = PropertyComponent.EMPTY_STRING;
	
	            // if we are working with an editor that was previously passed via setEditorComponentBeforeEdit, we can
	            // check to see if the value has changed.  
	            if ( editor == currentEditorComponent ) {
	                LogManager.logDetail(PropertiedObjectPanel.LOG_CONTEXT, "[PropertyChangeAdapter] examining change for type " + def.getPropertyType().getClassName());
	
	                if (value == null) {
	                    value = def.getDefaultValue();
	                    if (value != null) {
	                        setJComponentValue(editor, value);
	                    }
	                }
	                if (value != null  &&  value.equals(def.getDefaultValue())) {
	                    editor.setForeground(Color.blue);
	                }
	                // detect whether or not the value has been changed
	                if ( editor instanceof PropertyComponent ) {
	                    PropertyComponent c = (PropertyComponent)editor;
	                    nullValue = c.getNullValue();
	                    // ask the PropertyComponent if the value is the same
	                    if(c.isCurrentValueEqualTo(originalValue)) {
	                        setValue = false;
	                    }
	                    c.editingStopped();
	                } else {
	                    if ( value == null && originalValue == null ) {
	                        // No change - Do not set the value on the PropertiedObject
	                        LogManager.logDetail(PropertiedObjectPanel.LOG_CONTEXT, "[PropertyChangeAdapter] detected no change, value = null");
	                        setValue = false;
	                    } else if ( value != null && value.equals(originalValue) ) {
	                        if ( def.isMasked() ) {
	                            LogManager.logDetail(PropertiedObjectPanel.LOG_CONTEXT, "[PropertyChangeAdapter] detected no change to masked field");
	                        } else {
	                            LogManager.logDetail(PropertiedObjectPanel.LOG_CONTEXT, "[PropertyChangeAdapter] detected no change, value = " + value);
	                        }
	                        // No change - Do not set the value on the PropertiedObject
	                        setValue = false;
	                    }
	                }
	            } else {
                    // The editor is not the same editor as this adapter was set to for editing, so it is not safe to change the value
                    setValue = false;
                }
	
	            if ( setValue ) {
	                firePropertyChangeEvent(editor, value);
	                // we've determined that the value needs to be saved on the PropertiedObject
	                if ( def.isMasked() ) {
	                    LogManager.logDetail(PropertiedObjectPanel.LOG_CONTEXT, "[PropertyChangeAdapter] detected masked value change");
	                } else {
	                    LogManager.logDetail(PropertiedObjectPanel.LOG_CONTEXT, "[PropertyChangeAdapter] detected value change, value = " + value);
	                }
	                if ( isValid(value, nullValue) ) {
                        
                        transactionPending = true;                        
	                    UserTransaction txn = null;
	                    boolean wasErr = true;
	                    // set the value on the PropertiedObject
	                    try {
	                        txn = objectEditor.createWriteTransaction( transactionSource );
	                        txn.begin();
	                        if (value == null  ||  value.equals(def.getDefaultValue())) {
	                            LogManager.logDetail(PropertiedObjectPanel.LOG_CONTEXT, "[PropertyChangeAdapter] nulling value");
	                            objectEditor.setValue(entity, def, null);
	                        } else {
	                            if ( def.isMasked() ) {
	                                LogManager.logDetail(PropertiedObjectPanel.LOG_CONTEXT, "[PropertyChangeAdapter] setting masked value");
	                            } else {
	                                LogManager.logDetail(PropertiedObjectPanel.LOG_CONTEXT, "[PropertyChangeAdapter] setting value to " + value);
	                            }
	                            objectEditor.setValue(entity, def, value);
	                        }
	                        originalValue = value;
	                        wasErr = false;
	                    } catch (TransactionException e) {
	                        LogManager.logCritical(PropertiedObjectPanel.LOG_CONTEXT, e, "[PropertyChangeAdapter] caught exception");
	                    } finally {
	                        try {
	                            if ( txn != null ) {
	                                if (wasErr) {
	                                    txn.rollback();
	                                } else {
	                                    txn.commit();
	                                }
	                            }
	                        } catch (final TransactionException err) {
	                            LogManager.logCritical(PropertiedObjectPanel.LOG_CONTEXT, err, "Failed to " + (wasErr ? "rollback." : "commit."));
	                        }
	                    }
	                    txn = null;
                        transactionPending = false;
	                } else {
	                    if ( def.isMasked() ) {
	                        LogManager.logDetail(PropertiedObjectPanel.LOG_CONTEXT, "[PropertyChangeAdapter] detected invalid masked value");
	                    } else {
	                        LogManager.logDetail(PropertiedObjectPanel.LOG_CONTEXT, "[PropertyChangeAdapter] detected invalid value = " + value);
	                    }
	                }
	                
	                if ( editor instanceof JComboBox ) {
	                    
						JComboBox comboBox = (JComboBox) editor;
						if (value != null && ! this.def.isRequired() ) {
	                        // see if the NULL_OBJECT is already in the combo box
	                        int count = comboBox.getItemCount();
	                        boolean foundNullObject = false;
	                        for ( int i=0 ; i<count ; ++i ) {
	                            if ( comboBox.getItemAt(i) == PropertyComponentFactory.NULL_OBJECT ) {
	                                foundNullObject = true;
	                                break;
	                            }
	                        }
	                        
	                        if ( ! foundNullObject ) {
	                            DefaultComboBoxModel model = (DefaultComboBoxModel) comboBox.getModel();
	                            model.addElement(PropertyComponentFactory.NULL_OBJECT);
	                        }
	                    }
	                }
                }    
            }
        }
        isEditing = false;
    }

    /**
     * Set this adapter as a listener to validate partial entries of non-constrained
     * properties.  For example, this adapter listens to every keystroke within a
     * JTextComponent and validates the entry, turning the entry red if a keystroke
     * has caused the entire entry to become in valid -- like when an alpha character
     * is entered into an integer field.
     */
    protected void setValidityListenerForJComponent(JComponent comp) {
        if ( comp instanceof PropertyComponent ) {
            ((PropertyComponent)comp).setPropertyValidationListener(this);
        } else if (comp instanceof JTextComponent  &&  !(comp instanceof JPasswordField)) {
            ((JTextComponent)comp).addKeyListener(this);
        } else if (comp instanceof JComboBox) {
            JComboBox comboBox = (JComboBox) comp;
            ((JTextComponent)(comboBox).getEditor().getEditorComponent()).addKeyListener(this);
            //SWJ 9/27/02: THIS IS NOT WORKING - IT DOES NOT SEEM THAT THIS BLOCK IS EVER EXECUTED
            comboBox.addActionListener(new ActionListener() {
                public void actionPerformed(ActionEvent e) {
                    checkComboBoxValidity(e.getSource());       
                }
            });
        }
    }

    /**
     * Remove this adapter from the specified component's listener list.
     */
    protected void removeValidityListenerForJComponent(JComponent comp) {
        if ( comp instanceof PropertyComponent ) {
            ((PropertyComponent)comp).removePropertyValidationListener(this);
	    } else if (comp instanceof JTextComponent  &&  !(comp instanceof JPasswordField)) {
            ((JTextComponent)comp).removeKeyListener(this);
        } else if (comp instanceof JComboBox) {
            ((JTextComponent)((JComboBox)comp).getEditor().getEditorComponent()).removeKeyListener(this);
        }
    }

    // **********************************
    // PropertyValidationListener Methods
    // **********************************

    /**
     * Callback from a custom component to check the validity of a partial entry.
     * @param source the custom PropertyComponent requesting validation.
     * @param value the property value that should be validated.
     */
    public void isValueValid(PropertyComponent source, Object value) {
        boolean result = isValid(value, source.getNullValue());
        source.setValidity(result);
    }

    public boolean isValid(final Object value, final Object nullObject) {
        boolean result = false;
        if (def.isMasked()) {
            result = true;
        } else if ( value != null && nullObject != null && value.equals(nullObject) ) {
            // detected that the PropertyComponent returned it's null object
            result = objectEditor.isValidValue(entity, def, null);
        } else {
            result = objectEditor.isValidValue(entity, def, value);
        }
        return result;
    }

    // ******************
    // KeyAdapter Methods
    // ******************

    /**
     * Invoked when a key has been released.  This KeyAdapter method will cause the
     * full text entry of the source component to be validated by the PropertiedObject.
     * If the entry is not valid, the foreground text color turns to red.
     */
    public void keyReleased(KeyEvent e) {
        if ( e.getSource() instanceof JTextComponent ) {
            JTextComponent c = (JTextComponent) e.getSource();
            String value = c.getText();
            final boolean matchesOrigVal = value.equals(originalValue)  ||  (value.length() == 0  &&  originalValue == null);
            if ((matchedOrigVal  &&  !matchesOrigVal)  ||  (!matchedOrigVal  &&  matchesOrigVal)) {
                firePropertyChangeEvent(c, value);
                matchedOrigVal = !matchedOrigVal;
            }
            if (c instanceof JPasswordField) {
                return;
            }
            if (isValid(value, PropertyComponent.EMPTY_STRING) ) {
                //TODO: obtain the default foreground color from the JTextComponent UI.
                c.setForeground(Color.black);
            } else {
                c.setForeground(Color.red);
            }
        }
    }


    /**
     * Invoked when a component loses the keyboard focus.
     */
    public void focusLost(final FocusEvent event) {
        final JComponent comp = (JComponent)event.getSource();
        setEditorComponentAfterEdit(comp);
        removeValidityListenerForJComponent(comp);
    }

    /**
     * Invoked when a component loses the keyboard focus.
     */
    public void actionPerformed(ActionEvent e) {
        JComponent c = (JComponent) e.getSource();
        setEditorComponentAfterEdit(c);
    }


    // **************
    // Static Methods
    // **************

    /**
     * Return the value object controlled by the specified JComponent TreeCellEditor.
     * This method must support all JComponent types used by MetadataPropertiesPanel
     * and MetadataPropertyTableEntry.
     * @param comp the JComponent that this method will extract the value from.
     * @return the object controlled by the JComponent editor.  If a JComponent type
     * is passed in that this method cannot support, the method will return a String
     * containing a message that the PropertyChangeAdapter does not support this
     * component.
     */
    public static Object getValueFromJComponent(JComponent comp, boolean finished) {
        Object result = new String("PropertyChangeAdapter cannot support " + comp.getClass().getName());
        if (comp instanceof PropertyComponent) {
            PropertyComponent c = (PropertyComponent)comp;
            result = c.getValue();
            if ( finished ) {
                c.editingStopped();
            } else {
                c.editingStarted();
            }
            if (result != null  &&  result.equals(c.getNullValue())) {
                result = null;
            }
        } else if (comp instanceof JTextComponent) {
            final JTextComponent c = (JTextComponent)comp;
            result = c.getText();
            if (result != null) {
                if (((String)result).trim().length() == 0) {
                    result = null;
                } else {
                    final DateFormat dateFmt = (DateFormat)c.getClientProperty(PropertyComponentFactory.DATE_FORMAT_PROPERTY);
                    if (dateFmt != null) {
                        try {
                            result = dateFmt.parse((String)result);
                        } catch (final ParseException err) {
                            comp.setForeground(Color.red);
                            result = null;  // This really should be original value, but this is a static method,
                                            // so good enough for now
                        }
                    }
                }
            }
        } else if (comp instanceof JCheckBox) {
            if (((JCheckBox) comp).isSelected()) {
                result = Boolean.TRUE;
            } else {
                result = Boolean.FALSE;
            }
        } else if (comp instanceof JComboBox) {
            JComboBox comboBox = (JComboBox) comp;
            result = comboBox.getSelectedItem();
            if (result != null ) {
                if ( result instanceof String  &&  ((String)result).trim().length() == 0) {
                    result = null;
                } else if ( result == PropertyComponentFactory.NULL_OBJECT ) {
                    result = null;
                }
            }
        } else if (comp instanceof PasswordButton) {
            result = ((PasswordButton)comp).getPassword();
        } else {
            LogManager.logCritical(PropertiedObjectPanel.LOG_CONTEXT, "[PropertyChangeAdapter] " + result);
        }

        return result;
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    Unregisters the specified listener from getting notifications of value changes within the component displaying values for one
    of the PropertiedObject's properties.
    @param listener The PropertyChangeListener currently registered to receive events
    @since 2.0
    */
    public void removePropertyChangeListener(final PropertyChangeListener listener) {
        listenerList.remove(PropertyChangeListener.class, listener);
    }
    
    private void checkComboBoxValidity(Object sourceComponent) {
        if ( sourceComponent instanceof JComboBox ) {
            JComboBox comboBox = (JComboBox) sourceComponent;
            if ( comboBox.isEditable() ) {
                if ( isValid(comboBox.getSelectedItem(), PropertyComponent.EMPTY_STRING) ) {
                    comboBox.getEditor().getEditorComponent().setForeground(Color.black);
                } else {
                    comboBox.getEditor().getEditorComponent().setForeground(Color.red);
                }
            }
        }
    }

    public void setJComponentValue(final JComponent component, final Object value) {
        if (component instanceof JTextComponent) {
            ((JTextComponent)component).setText(value.toString());
        } else if (component instanceof JComboBox) {
            ((JComboBox)component).setSelectedItem(value);
        }
    }
    
}


