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

package com.metamatrix.toolbox.ui.widget.property;

import java.awt.event.ActionListener;
import java.awt.event.FocusListener;

import javax.swing.JComponent;

/**
 * Defines interface that custom JComponents can implement to be used in the
 * PropertyTable.  Custom components that implement or provide adapters to this
 * interface can be used generically in the table.
 */
public interface PropertyComponent {

    /**
     * An empty String constant.  This constant may be used by PropertyComponent implementations
     * for the getNullValue( ) return Object.
     */
    public static final String EMPTY_STRING = "";

    void addActionListener(ActionListener listener);

    void addFocusListener(FocusListener listener);

    /**
     * Get the Object that this PropertyComponent will use to indicate null.  This Object
     * will be used by the PropertyChangeAdapter to detect that a PropertiedObject's value
     * for a particular PropertyDefintion is null, or, has no value.  If the PropertyComponent
     * returns an Object (not null), PropertyChangeAdapter will compare all Objects returned
     * by the getValue() method to detect null.
     * An example of the usage of this Object would be a text field which return an empty String
     * to indicates that the specified property has no value.
     * @return the Object that should be compared to the getValue() result to detect null.
     * Implementations of this method may return null.
     */
    Object getNullValue();

    /**
     * Get the value being displayed by this component.  This value will be obtained
     * from the PropertyChangeAdapter immediately prior to and after editing.  After
     * the user has stopped editing the value, the isEqualTo() method will be called
     * allowing this component to determine if the user has changed the value.
     * @return the property's value.  May be a single object or an Object[] array.
     */
    Object getValue();

    /**
     * Set whether or not this component should be enabled to allow user
     * editing of the value(s).
     * @param flag true if the component should enable editing.
     */
    void setEnabled(boolean flag);

    /**
     * <p>Set a listener on this component that will receive request to validate property
     * values as they are entered.  An example would be a custom component that allows
     * a user to type in an entry that should be validated keystroke-by-keystroke.  Such
     * a component would route KeyListener.keyReleased() events to the
     * PropertyValidationListener.checkValue(Object) method.  The result of the checkValue
     * call will be communicated to this component via the setValidity(boolean) method.</p>
     * <p>Not all components require validation; therefore it is permissable for
     * such components to no-op this method.</p>
     * @param listener the PropertyValidationListener that this object should call if
     * validation is required.
     */
    void setPropertyValidationListener(PropertyValidationListener listener);

    /**
     * <p>Remove the PropertyValidationListener for this component.  This method will
     * be called immediately after editing has stopped on this component.  Implementations
     * that no-op the setPropertyValidationListener method may no-op this method as well.
     * @param listener the PropertyValidationListener to be removed from this object.
     */
    void removePropertyValidationListener(PropertyValidationListener listener);

    /**
     * Set a visual indication that this component's displayed value is or is not
     * valid in the current context.  PropertyValidationListener calls this method after
     * a request to checkValidity of a specified value.  The method may also be called if
     * an invalid entry exists after editing has completed.  An example would be a collection
     * of values that are required to be unique, but contain a repeated value.
     * @param flag true if the value is valid, false if it is invalid.
     */
    void setValidity(boolean flag);

    /**
     * Return whether or not the specified value Object is equal to this component's
     * currently displayed value.  This method is called by PropertyChangeAdapter and
     * allows the value comparison logic to reside within the custom component, rather
     * than requiring custom components to hardcode comparison logic in the adapter.
     * @param value an Object that was previously obtained from this component's
     * getValue method.
     * @return true if the specified Object is the same as the value currently being
     * displayed in this component, false if it is not.  Returning true will cause the
     * new value to be "set" on the target propertied object.
     */
     boolean isCurrentValueEqualTo(Object value);

     /**
      * Notify this component that it has been activated and should enable any controls
      * necessary for editing property values.
      */
     void editingStarted();

     /**
      * Notification to this component that keyboard/mouse focus has moved away from the
      * component and it should deselect any items and deactivate any editing controls.
      */
     void editingStopped();

     /**
      * create a single-row JComponent from this component when needed.
      */
      JComponent getSingleRowComponent();

      /**
      * create a String from this component when needed.
      */
      String getSingleRowString();
}


