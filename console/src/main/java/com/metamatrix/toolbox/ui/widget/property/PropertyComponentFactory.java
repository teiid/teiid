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
import java.awt.Dimension;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collection;

import javax.swing.JComboBox;
import javax.swing.JComponent;
import javax.swing.JOptionPane;
import javax.swing.JPasswordField;
import javax.swing.ListCellRenderer;

import com.metamatrix.common.object.PropertiedObject;
import com.metamatrix.common.object.PropertiedObjectEditor;
import com.metamatrix.common.object.PropertyDefinition;
import com.metamatrix.common.object.PropertyType;
import com.metamatrix.common.tree.directory.DirectoryEntry;
import com.metamatrix.common.util.crypto.Encryptor;
import com.metamatrix.toolbox.ui.UIConstants;
import com.metamatrix.toolbox.ui.UIDefaults;
import com.metamatrix.toolbox.ui.widget.CheckBox;
import com.metamatrix.toolbox.ui.widget.DialogPanel;
import com.metamatrix.toolbox.ui.widget.LabelWidget;
import com.metamatrix.toolbox.ui.widget.PasswordButton;
import com.metamatrix.toolbox.ui.widget.TextFieldWidget;
import com.metamatrix.toolbox.ui.widget.URLTextFieldWidget;
import com.metamatrix.toolbox.ui.widget.event.WidgetActionEvent;

/**
 * The factory for building PropertyComponent editors for the PropertiedObjectPanel.  This
 * class may be extended to override createComponentForPropertyDefinition, and subclasses
 * can intercept specific PropertyDefinition types to build customized editors.
 * @since 2.0
 * @version 2.1
 * @author Steve Jacobs
 * @author <a href="mailto:jverhaeg@metamatrix.com">John P. A. Verhaeg</a>
 */
public class PropertyComponentFactory
implements UIConstants {
    //############################################################################################################################
    //# Static Variables                                                                                                         #
    //############################################################################################################################

    public static final JComponent PROTOTYPE = new TextFieldWidget();
    public static final int HEIGHT = PROTOTYPE.getPreferredSize().height;

    public static Color DISABLED_BACKGROUND_COLOR;
    public static Color ENABLED_BACKGROUND_COLOR;

    private static SimpleDateFormat dateFmt = null;

    public static final Object NULL_OBJECT = new Object() {
        public String toString() {  return ""; }
    };

    //############################################################################################################################
    //# Static Initializer                                                                                                       #
    //############################################################################################################################

    static {
        LabelWidget w = new LabelWidget();
        ENABLED_BACKGROUND_COLOR = w.getBackground();
        w.setEnabled(false);
        DISABLED_BACKGROUND_COLOR = w.getBackground();
    }
    
    //############################################################################################################################
    //# Instance Variables                                                                                                       #
    //############################################################################################################################
    
    private PropertiedObject obj;
    private PropertiedObjectEditor editor; 
    private PropertyChangeAdapter adapter;
    private ObjectReferenceHandler objectReferenceHandler;
    private ListCellRenderer objectReferenceRenderer;
    private Encryptor encryptor;

    //############################################################################################################################
    //# Constructor
    //############################################################################################################################
    
    public PropertyComponentFactory(Encryptor encryptor) {
        this.encryptor = encryptor;
    }
    
    
    //############################################################################################################################
    //# Instance Methods                                                                                                         #
    //############################################################################################################################
     
    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    Creates and returns the 2D object array table model for the properties of the MetadataEntity.  The depth of the array is
    exactly 2, where column 0 is filled with PropertyDefinitions from the specified MetadataEntity, and column 1 contains
    JComponents initialized to display and edit the value of each PropertyDefinition for the entity.
    @since 2.0
    */
    public JComponent createComponentForPropertyDefinition(final PropertyDefinition def, Object value, boolean isReadOnly,
                                                           final int index) {
        boolean dfltVal = false;
        if (value == null) {
            dfltVal = true;
            value = def.getDefaultValue();
        }
        JComponent comp = null;
        final PropertyType type = def.getPropertyType();
        if (def.getMultiplicity().getMaximum() == 1) {
            // single-valued PropertyDefinition
            if (isReadOnly  ||  !def.isModifiable()  || (type.equals(PropertyType.OBJECT_REFERENCE) && objectReferenceHandler == null) ) {
                // treat property value as read-only
                if (type.equals(PropertyType.BOOLEAN)) {
                    final CheckBox cb = new CheckBox() {
                        public boolean isFocusTraversable() {
                            return false;
                        }
                    };
                    cb.setMinimumSize(new Dimension(cb.getMinimumSize().width, HEIGHT));
                    cb.setPreferredSize(new Dimension(cb.getPreferredSize().width, HEIGHT));
                    cb.setMaximumSize(new Dimension(Short.MAX_VALUE, HEIGHT));
                    if ( value != null ) {
                        cb.setSelected(new Boolean(value.toString()).booleanValue());
                    }
                    cb.setEnabled(false);
                    comp = cb;
                } else if (type.equals(PropertyType.DATE)) {
                    comp = createDateField(def, value, false);
                } else if (type.equals(PropertyType.OBJECT_REFERENCE)) {

					if ( objectReferenceHandler == null ) {
                        
                        Object mpcValue = value;
                        
                        if ( value != null && ( ! (value instanceof Object[] ) ) ) {
                            mpcValue = new Object[] { value };
                        }
                        
                        // without an objectReferenceHandler, can't edit ObjectReferences
	                    MultivaluedPropertyComponent mvpcComponent = new MultivaluedPropertyComponent(def, mpcValue, isReadOnly, index, obj, editor);
	                    if ( objectReferenceRenderer != null ) {
	                        mvpcComponent.getListWidget().setCellRenderer(objectReferenceRenderer);
	                    }                    
	                    comp = mvpcComponent;
					} else {
	                    ObjectReferencePropertyComponent orComponent = new ObjectReferencePropertyComponent(def, value, isReadOnly, index, obj, editor, objectReferenceHandler, adapter);
	
	                    if ( objectReferenceRenderer != null ) {
	                        orComponent.getListWidget().setCellRenderer(objectReferenceRenderer);
	                    }
	                    comp = orComponent;						
					}                    
                    
                    comp.setMaximumSize(new Dimension(Short.MAX_VALUE, comp.getPreferredSize().height));
                } else if ( type.equals(PropertyType.URL) ) { 
                    comp = createURLTextField(def, value, false);
                //} else if ( type.equals(PropertyType.URI) ) { 
                //    comp = createURLTextField(def, value, false);
                } else {
                    comp = createTextField(def, value, false);
                }
            } else {
                // determine the type of JComponent to use to display and edit this value

                if (type.equals(PropertyType.FILE)) {
                    comp = new DirectoryEntryPropertyComponent(def.getDisplayName(), null, (DirectoryEntry)value, index);
                    comp.setMaximumSize(new Dimension(Short.MAX_VALUE, comp.getPreferredSize().height));

                } else if ( type.equals(PropertyType.OBJECT_REFERENCE) ) {
	
					if ( objectReferenceHandler == null ) {
                        
                        Object mpcValue = value;
                        if ( value != null && ( ! (value instanceof Object[] ) ) ) {
                            mpcValue = new Object[] { value };
                        }
                        
                        // without an objectReferenceHandler, can't edit ObjectReferences
	                    MultivaluedPropertyComponent mvpcComponent = new MultivaluedPropertyComponent(def, mpcValue, isReadOnly, index, obj, editor);
	                    if ( objectReferenceRenderer != null ) {
	                        mvpcComponent.getListWidget().setCellRenderer(objectReferenceRenderer);
	                    }                    
	                    comp = mvpcComponent;
					} else {
	                    ObjectReferencePropertyComponent orComponent = new ObjectReferencePropertyComponent(def, value, isReadOnly, index, obj, editor, objectReferenceHandler, adapter);
	
	                    if ( objectReferenceRenderer != null ) {
	                        orComponent.getListWidget().setCellRenderer(objectReferenceRenderer);
	                    }
	                    comp = orComponent;						
					}                    

                    comp.setMaximumSize(new Dimension(Short.MAX_VALUE, comp.getPreferredSize().height));

                } else if ( def.hasAllowedValues() ) {
                    // use a JComboBox
                    JComboBox cob;
                    if (editor != null  &&  obj != null) {
                        Collection allowedValues =  editor.getAllowedValues(obj, def);
                        ArrayList displayedValues = new ArrayList( allowedValues );
                        if ( value != null ) {
                            // make sure the current value is in the list (for unconstrained defs)
                            if ( ! allowedValues.contains(value) ) {
                                displayedValues.add(0, value);
                            }
                            if ( ! def.isRequired() && ! def.hasDefaultValue() ) {
                                displayedValues.add(NULL_OBJECT);
                            }
                        }
                        cob = new JComboBox(displayedValues.toArray());
                        cob.setSelectedItem(value);
                    } else {
                        cob = new JComboBox();
                    }
                    // set if the user can type into the field
                    cob.setEditable( ! def.isConstrainedToAllowedValues() );
                    comp = cob;
                } else if (type.equals(PropertyType.BOOLEAN)) {
                    // use a JCheckBox
                    final CheckBox cb = new CheckBox();
                    cb.setMinimumSize(new Dimension(cb.getMinimumSize().width, HEIGHT));
                    final Dimension prefSize = new Dimension(cb.getPreferredSize().width, HEIGHT);
                    cb.setPreferredSize(prefSize);
                    cb.setMaximumSize(prefSize);
                    if ( value != null ) {
                        cb.setSelected(new Boolean(value.toString()).booleanValue());
                    }
                    comp = cb;
                } else if ( def.isMasked() ) {
                    if (value == null) {
                        // use a JPasswordField
                        final JPasswordField pf = new JPasswordField();
                        pf.setMaximumSize(new Dimension(Short.MAX_VALUE, pf.getPreferredSize().height));
                        if ( value != null ) {
                            pf.setText(value.toString());
                        }
                        comp = pf;
                    } else {
                        comp = new PasswordPropertyButton(def, ((String)value).toCharArray(), encryptor);
                    }

                } else if (type.equals(PropertyType.DATE)) {
                    comp = createDateField(def, value, true);

                } else if (type.equals(PropertyType.URL) ) {
                    // use a URLTextWidget
                    comp = createURLTextField(def, value, true);
                //{ else if ( type.equals(PropertyType.URI) ) {
                    // use a URLTextWidget
                    //comp = createURLTextField(def, value, true);
                } else {
                    // use a TextWidget
                    comp = createTextField(def, value, true);
                }
            }
        } else {
            // multi-valued PropertyDefinition
            if ( type.equals(PropertyType.OBJECT_REFERENCE) ) {
                if ( objectReferenceHandler != null ) {

                    ObjectReferencePropertyComponent orComponent = new ObjectReferencePropertyComponent(def, value, isReadOnly, index, obj, editor, objectReferenceHandler, adapter);
                    if ( objectReferenceRenderer != null ) {
                        orComponent.getListWidget().setCellRenderer(objectReferenceRenderer);
                    }
                    comp = orComponent;
                    
                } else {
                    
	                Object mpcValue = value;
	                if ( value != null && (! (value instanceof Object[]) ) ) {
	                    mpcValue = new Object[] { value };
	                }
	                
	                comp = new MultivaluedPropertyComponent(def, mpcValue, isReadOnly, index, obj, editor);

                }

            } else {
                comp = new MultivaluedPropertyComponent(def, value, isReadOnly, index, obj, editor);
            }

        }
        comp.setAlignmentY(0.0f);
        if (value != null) {
            if (dfltVal) {
                comp.setForeground(Color.blue);
            } else if (!editor.isValidValue(obj, def, value)) {
                comp.setForeground(Color.red);
            }
        }
        return comp;
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since 2.0
    */
    protected JComponent createDateField(final PropertyDefinition definition, final Object value, final boolean editable) {
        JComponent comp;
        final DateFormat dateFmt = getDateFormat(definition, value);
        if (value == null) {
            comp = createTextField(definition, value, editable);
        } else {
            comp = createTextField(definition, dateFmt.format(value), editable);
        }
        comp.putClientProperty(DATE_FORMAT_PROPERTY, dateFmt);
        return comp;
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since 2.0
    */
    protected JComponent createTextField(final PropertyDefinition definition, final Object value, final boolean editable) {
        final TextFieldWidget fld = new TextFieldWidget() {
            public boolean isFocusTraversable() {
                return editable;
            }
        };
        fld.setEditable(editable);
        if (value == null) {
            fld.setText(PropertyComponent.EMPTY_STRING);
        } else {
            fld.setText(value.toString());
        }
        return fld;
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since 3.1
    */
    protected JComponent createURLTextField(final PropertyDefinition definition, final Object value, final boolean editable) {
        final TextFieldWidget fld = new URLTextFieldWidget() {
            public boolean isFocusTraversable() {
                return editable;
            }
        };
        fld.setEditable(editable);
        if (value == null) {
            fld.setText(PropertyComponent.EMPTY_STRING);
        } else {
            fld.setText(value.toString());
        }
        return fld;
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since 2.0
    */
    protected DateFormat getDateFormat(final PropertyDefinition definition, final Object value) {
        if (dateFmt == null) {
            dateFmt = new SimpleDateFormat(UIDefaults.getInstance().getString(DATE_FORMAT_PROPERTY));
        }
        return dateFmt;
    }
    
    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since 2.0
    */
    protected PropertiedObject getPropertiedObject() {
        return obj;
    }
    
    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since 2.0
    */
    protected PropertiedObjectEditor getPropertiedObjectEditor() {
        return editor;
    }

	//////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
     * @return The current PropertyChangeAdapter
     * @since 2.1
     */
    protected PropertyChangeAdapter getPropertyChangeAdapter() {
        return adapter;
    }
       
    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since 2.0
    */
    public void setPropertiedObject(final PropertiedObject object) {
        obj = object;
    }
    
    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since 2.0
    */
    public void setPropertiedObjectEditor(final PropertiedObjectEditor editor) {
        this.editor = editor;
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
     * @param adapter
     * @since 2.1
     */
    public void setPropertyChangeAdapter(final PropertyChangeAdapter adapter) {
        this.adapter = adapter;
    }

    /**
     * @param handler
     * @since 3.0
     */
    public void setObjectReferenceHandler(final ObjectReferenceHandler handler) {
        this.objectReferenceHandler = handler;
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    Sets a ListCellRenderer for use with ObjectReferencePropertyComponent.
    @since 3.0
    */
    public void setObjectReferenceRenderer(final ListCellRenderer renderer) {
        this.objectReferenceRenderer = renderer;
    }

	//############################################################################################################################
    //# Inner Class: PasswordPropertyButton                                                                                      #
    //############################################################################################################################

    /**
     * @since 2.1
     */
    private class PasswordPropertyButton extends PasswordButton {
        //# PasswordPropertyButton ###############################################################################################
        //# Instance Variables                                                                                                   #
        //########################################################################################################################
        
        
        //# PasswordPropertyButton ###############################################################################################
        //# Constructors                                                                                                         #
        //########################################################################################################################
        
        /// PasswordPropertyButton ///////////////////////////////////////////////////////////////////////////////////////////////
        /**
         * @since 2.1
         */
        private PasswordPropertyButton(final PropertyDefinition def, final char[] password, Encryptor encryptor) {
            super(password, encryptor);
        }
        
        //# PasswordPropertyButton ###############################################################################################
        //# Instance Methods                                                                                                     #
        //########################################################################################################################
        
        /// PasswordPropertyButton ///////////////////////////////////////////////////////////////////////////////////////////////
        /**
         * @since 2.1
         */
        protected void accept(final DialogPanel panel, final WidgetActionEvent event) {
            if (adapter == null) {
                return;
            }
            if (editor != null  &&  !adapter.isValid(new String(getPassword()), null)) {
                event.destroy();
                JOptionPane.showMessageDialog(panel, "New password is invalid.", "Error", JOptionPane.ERROR_MESSAGE);
                return;
            }
            adapter.setEditorComponentAfterEdit(PasswordPropertyButton.this);
        }
    }
}

