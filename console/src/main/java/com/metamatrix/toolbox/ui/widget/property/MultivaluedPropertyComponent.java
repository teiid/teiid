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
package com.metamatrix.toolbox.ui.widget.property;

// System imports
import java.awt.Component;
import java.awt.Dimension;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.awt.event.FocusEvent;
import java.awt.event.FocusListener;
import java.beans.PropertyChangeEvent;
import java.beans.PropertyChangeListener;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import javax.swing.BorderFactory;
import javax.swing.Box;
import javax.swing.BoxLayout;
import javax.swing.JComponent;
import javax.swing.JOptionPane;
import javax.swing.JPanel;
import javax.swing.SwingUtilities;
import javax.swing.event.EventListenerList;
import javax.swing.event.ListSelectionEvent;
import javax.swing.event.ListSelectionListener;

import com.metamatrix.common.object.PropertiedObject;
import com.metamatrix.common.object.PropertiedObjectEditor;
import com.metamatrix.common.object.PropertyDefinition;
import com.metamatrix.common.object.PropertyType;
import com.metamatrix.core.util.Assertion;
import com.metamatrix.toolbox.ui.widget.AccumulatorPanel;
import com.metamatrix.toolbox.ui.widget.ButtonWidget;
import com.metamatrix.toolbox.ui.widget.DialogWindow;
import com.metamatrix.toolbox.ui.widget.LabelWidget;
import com.metamatrix.toolbox.ui.widget.ListWidget;
import com.metamatrix.toolbox.ui.widget.SpacerWidget;
import com.metamatrix.toolbox.ui.widget.TextFieldWidget;
import com.metamatrix.toolbox.ui.widget.URLLabelWidget;
import com.metamatrix.toolbox.ui.widget.URLTextFieldWidget;
import com.metamatrix.toolbox.ui.widget.list.URLListCellRenderer;
import com.metamatrix.toolbox.ui.widget.util.WidgetUtilities;

/**
MultivaluedPropertyComponent is a compound JPanel for displaying and editing
values for multi-valued properties.  It is also an example of how PropertyComponent
can be implemented and used as complex property renderer/editor.
@since 2.0
@version 2.0
@author Steve Jacobs
@author <a href="mailto:jverhaeg@metamatrix.com">John P. A. Verhaeg</a>
*/
public class MultivaluedPropertyComponent extends JPanel implements PropertyComponent {


    private transient Object data;
    private List dataList;
    private ListWidget list;
    private boolean readOnly/*, valid*/;
    private PropertyDefinition def;
    private ButtonWidget removeButton;
    private boolean hasFocus = false;
    private int ndx;
    private PropertiedObjectEditor editor;
    private PropertiedObject obj;

    protected Box buttonBox;
    protected FocusListener focusListener = new FocusListener() {
                public void focusGained(final FocusEvent event) {
                    if (hasFocus) {
                        return;
                    }
                    fireFocusEvent(event);
                    hasFocus = true;
                }
                public void focusLost(final FocusEvent event) {
                    hasFocus = false;
                    processFocusLostEvent(event);
                }
            };


    // ************
    // Constructors
    // ************

    /**
    @since 2.0
    */
    public MultivaluedPropertyComponent(final PropertyDefinition def, final Object data, final boolean readOnly, final int index,
                                        final PropertiedObject object, final PropertiedObjectEditor editor) {
        this.def = def;
        this.data = data;
        this.readOnly = readOnly;
        ndx = index;
        obj = object;
        this.editor = editor;
        initializeMultivaluedPropertyComponent();
    }
    
    // ****************
    // Instance Methods
    // ****************

    /**
    @since 2.0
    */
    protected void addButtonPressed() {

        final String val = getInputValue();
        if (val != null) {
            dataList.add(val);
            updateList();
        }
    }

    protected String getInputValue() {
        String result = null;
        boolean tryAgain = true;
        while (tryAgain) {
            // TODO: replace JOptionPane with an input dialog that checks validity on the fly
            result = JOptionPane.showInputDialog(MultivaluedPropertyComponent.this,
                                                       "Enter a value to add:", "Add a value",
                                                       JOptionPane.INFORMATION_MESSAGE);
            if ( result != null && ! editor.isValidValue(obj, def, result) ) {
                JOptionPane.showMessageDialog(MultivaluedPropertyComponent.this,
                                                       "This entry is not a valid value.", 
                                                       "Invalid Entry",
                                                       JOptionPane.ERROR_MESSAGE);
                
            } else {
                tryAgain = false;
            }
        }
        return result;
    }        

    /**
     * called by the moreButton's action processor method.  The moreButton is generated when there are
     * "allowable values" available for this PropertyDefinition.  May be overridden by subclasses to provide
     * different functionality.
     * @since 3.0
     */
    protected void moreButtonPressed() {
        final AccumulatorPanel panel = new AccumulatorPanel(editor.getAllowedValues(obj, def), dataList);
        panel.setAllowsNewValues(!def.isConstrainedToAllowedValues());
        DialogWindow.show(MultivaluedPropertyComponent.this, "Select Values", panel);
        if (panel.getSelectedButton() == panel.getAcceptButton()) {
            dataList = panel.getValues();
            updateList();
        }
    }

    /**
    @since 2.0
    */
    public void addActionListener(final ActionListener listener) {
        listenerList.add(ActionListener.class, listener);
    }

    /**
    @since 2.0
    */
    public void addFocusListener(final FocusListener listener) {
        listenerList.add(FocusListener.class, listener);
    }
    
    /**
    @since 2.0
    */
    protected void fireFocusEvent(final FocusEvent event) {
        final Object[] listeners = listenerList.getListenerList();
        FocusEvent newEvent = null;
        for (int ndx = listeners.length - 2;  ndx >= 0;  ndx -= 2) {
            if (listeners[ndx] == FocusListener.class) {
                if (newEvent == null) {
                      newEvent = new FocusEvent(this, event.getID(), event.isTemporary());
                }
                if (event.getID() == FocusEvent.FOCUS_GAINED) {
                    ((FocusListener)listeners[ndx + 1]).focusGained(newEvent);
                } else {
                    ((FocusListener)listeners[ndx + 1]).focusLost(newEvent);
                }
            }          
        }
    }

    /**
    @since 2.0
    */
    protected List getList() {
        return dataList;
    }
    
    /**
    @since 2.0
    */
    public ListWidget getListWidget() {
        return list;
    }
    
    /**
    @return The remove button (if present)
    @since 2.0
    */ 
    protected ButtonWidget getRemoveButton() {
        return removeButton;
    }

    /**
     * Get the value being displayed by this component.  This value will be obtained
     * from the PropertyChangeAdapter immediately prior to and after editing.  After
     * the user has stopped editing the value, the isEqualTo() method will be called
     * allowing this component to determine if the user has changed the value.
     * @return the property's value.  May be a single object or a Collection.
     */
    public Object getValue(){
        return dataList.toArray().clone();
    }
    
    /**
    @since 2.0
    */
    public boolean isFocusTraversable() {
        return !readOnly;
    }

    /**
    @since 2.0
    */
    protected void processFocusLostEvent(final FocusEvent event) {
        if (isShowing()  &&  !event.isTemporary()) {
            final Component focusComp = SwingUtilities.findFocusOwner(getRootPane());
            if (focusComp == null) {
                SwingUtilities.invokeLater(new Runnable() {
                    public void run() {
                        processFocusLostEvent(event);
                    }
                });
                return;
            }
        }
        if (hasFocus) {
            return;
        }
        list.clearSelection();
        fireFocusEvent(event);
    }

    /**
    @since 2.0
    */
    protected void initializeMultivaluedPropertyComponent() {
        if ( data != null ) {
            Assertion.assertTrue( (data instanceof Object[]), "Value must be of type Object[]. PropertyDefinition " + def.getDisplayName() + " passed value type " + data.getClass().getName());
        }

        setLayout(new BoxLayout(this, BoxLayout.X_AXIS));
        list = new ListWidget();
        if ( def.getPropertyType().equals(PropertyType.URL)) {
            list.setCellRenderer(new URLListCellRenderer());
        }
        if (readOnly) {
            list.setBackground(PropertyComponentFactory.DISABLED_BACKGROUND_COLOR);
        }
        dataList = new ArrayList();
        if ( data != null ) {
            final Object[] array = (Object[])data;
            for (int ndx = 0;  ndx < array.length;  ++ndx) {
                dataList.add(array[ndx]);
            }
            list.setListData(array);
            list.setVisibleRowCount(dataList.size());
        } else {
            list.setVisibleRowCount(1);
        }
        list.setMaximumSize(new Dimension(Short.MAX_VALUE, Short.MAX_VALUE));
        list.setBorder(BorderFactory.createLineBorder(java.awt.Color.gray));
        list.setAlignmentY(0.0f);
        final EventListenerList listenerList = this.listenerList;
        list.addPropertyChangeListener(new PropertyChangeListener() {
            public void propertyChange(final PropertyChangeEvent event) {
                if (!event.getPropertyName().equals("model")) {
                    return;
                }
                final Object[] listeners = listenerList.getListenerList();
                ActionEvent newEvent = null;
                for (int ndx = listeners.length - 2;  ndx >= 0;  ndx -= 2) {
                    if (listeners[ndx] == ActionListener.class) {
                        if (newEvent == null) {
                            newEvent = new ActionEvent(MultivaluedPropertyComponent.this, ActionEvent.ACTION_PERFORMED, "");
                        }
                        ((ActionListener)listeners[ndx + 1]).actionPerformed(newEvent);
                    }
                }
            }
        });
        add(list);

        final JPanel vBox = new JPanel();
        vBox.setLayout(new BoxLayout(vBox, BoxLayout.Y_AXIS));
        vBox.setAlignmentY(0.0f);
        vBox.add(SpacerWidget.createVerticalExpandableSpacer());
        add(vBox);

        buttonBox = Box.createHorizontalBox();
        vBox.add(buttonBox);

        if(!readOnly) {
            list.addFocusListener(focusListener);
            list.setPrototypeCellValue("");
            final int hgt = list.getFixedCellHeight();
            Dimension size = new Dimension(20, hgt);
            ButtonWidget addButton = null;

            if (!def.isConstrainedToAllowedValues()) {
                addButton = new ButtonWidget("+");
                addButton.setName("MultivaluedPropertyComponent.addButton." + def.getDisplayName() + '.' + ndx);
                addButton.setToolTipText("Add Value");
                size = new Dimension(addButton.getPreferredSize().width, hgt);
                addButton.setMinimumSize(size);
                addButton.setPreferredSize(size);
                addButton.setMaximumSize(size);
                addButton.addActionListener(new ActionListener() {
                    public void actionPerformed(final ActionEvent event){
                        addButtonPressed();
                    }
                });
                addButton.addFocusListener(focusListener);
                buttonBox.add(addButton);
                removeButton = new ButtonWidget("-");
                removeButton.setToolTipText("Remove Selected Value");
                removeButton.setName("MultivaluedPropertyComponent.removeButton." + def.getDisplayName() + '.' + ndx);
                size = new Dimension(removeButton.getPreferredSize().width, hgt);
                removeButton.setMinimumSize(size);
                removeButton.setPreferredSize(size);
                removeButton.setMaximumSize(size);
                removeButton.addActionListener(new ActionListener() {
                    public void actionPerformed(final ActionEvent event) {
                        SwingUtilities.invokeLater(new Runnable() {
                            public void run() {
                                removeButtonPressed();
                            }
                        });
                    }
                });
                removeButton.addFocusListener(focusListener);
                buttonBox.add(removeButton);

                // enable the removeButton only when a list item is selected
                removeButton.setEnabled(false);
                list.addListSelectionListener(new ListSelectionListener() {
                    public void valueChanged(ListSelectionEvent e) {
                        SwingUtilities.invokeLater(new Runnable() {
                            public void run() {
                                boolean enableRemove = false;
                                if( list.getSelectedIndex() >= 0 && 
                                    list.getSelectedValue() != null )
                                    enableRemove = true;
                                    
                                removeButton.setEnabled(enableRemove);
                            }
                        });
                    }
                });

                if(def.getMultiplicity() != null && addButton != null && dataList != null){
                    int dataCount = 0;
                    if(!dataList.isEmpty() && dataList.get(0) != null){
                        dataCount = dataList.size();
                    }

                    boolean enable = dataCount < def.getMultiplicity().getMaximum();
                    addButton.setEnabled(enable);
                    if(!enable){
                        addButton.setToolTipText("Number of values equal to max allowed");
                    }
                }

                ArrayList buttonList = new ArrayList(2);
                buttonList.add(addButton);
                buttonList.add(removeButton);
                WidgetUtilities.equalizeSizeConstraints(buttonList);

            }
            if (def.hasAllowedValues()) {
                final ButtonWidget moreButton = new ButtonWidget("...");
                moreButton.setToolTipText("Set Value");
                moreButton.setName("MultivaluedPropertyComponent.moreButton." + def.getDisplayName() + '.' + ndx);
                moreButton.setAlignmentX(0.5f);
                moreButton.addActionListener(new ActionListener(){
                    public void actionPerformed(final ActionEvent event) {
                        moreButtonPressed();
                    }
                });
                moreButton.addFocusListener(focusListener);
                if (def.isConstrainedToAllowedValues()) {
                    size = new Dimension(moreButton.getPreferredSize().width, hgt);
                } else {
                    size = new Dimension(addButton.getPreferredSize().width + removeButton.getPreferredSize().width, hgt);
                }
                moreButton.setMinimumSize(size);
                moreButton.setPreferredSize(size);
                moreButton.setMaximumSize(size);
                buttonBox.add(moreButton);
                vBox.add(buttonBox);
            }
        } else {
            list.addListSelectionListener(new ListSelectionListener() {
                public void valueChanged(ListSelectionEvent e) {
                    list.clearSelection();
                }
            });
        }
    }

    /**
    @since 2.0
    */
    protected void removeButtonPressed() {
        final int[] indices = list.getSelectedIndices();
        if (indices == null) {
            return;
        }
        for (int ndx = indices.length;  --ndx >= 0;) {
            dataList.remove(indices[ndx]);
        }
        updateList();
    }
    
    /**
     * Set whether or not this component should be enabled to allow user
     * editing of the value(s).
     * @param flag true if the component should enable editing.
     */
    public void setEnabled(boolean flag){
        readOnly = !flag;
    }

    /**
     * Get the null value for this PropertyComponent.  Since MultivaluedProperties are
     * always set to an empty Collection, there need be no null value.
     */
    public Object getNullValue() {
        return null;
    }

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
    public void setPropertyValidationListener(PropertyValidationListener listener){
    }

    /**
     * <p>Remove the PropertyValidationListener for this component.  This method will
     * be called immediately after editing has stopped on this component.  Implementations
     * that no-op the setPropertyValidationListener method may no-op this method as well.
     * @param listener the PropertyValidationListener to be removed from this object.
     */
    public void removePropertyValidationListener(PropertyValidationListener listener){
    }

    /**
     * Set a visual indication that this component's displayed value is or is not
     * valid in the current context.  PropertyValidationListener calls this method after
     * a request to checkValidity of a specified value.  The method may also be called if
     * an invalid entry exists after editing has completed.  An example would be a collection
     * of values that are required to be unique, but contain a repeated value.
     * @param flag true if the value is valid, false if it is invalid.
     */
    public void setValidity(boolean flag) {
//        valid = flag;
    }

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
    public boolean isCurrentValueEqualTo(Object value) {
        Object[] previousData = (Object[])value;
        Object[] currentData = dataList.toArray();
        if ( previousData == null && currentData == null ) {
            return true;
        } else if ( previousData == null && currentData != null ) {
            return false;
        } else if ( previousData != null && currentData == null ) {
            return false;
        }

        if ( previousData.length != currentData.length) {
            return false;
        }
        java.util.List pl = Arrays.asList(previousData);
        java.util.List cl = Arrays.asList(currentData);
        if (pl.containsAll(cl) && cl.containsAll(pl)) {
            return true;
        }
        return false;
    }

     /**
      * Notify this component that it has been activated and should enable any controls
      * necessary for editing property values.
      */
    public void editingStarted() {
        //TODO: enable buttons when this method is called.
        //It's not needed.
    }

     /**
      * Notification to this component that keyboard/mouse focus has moved away from the
      * component and it should deselect any items and deactivate any editing controls.
      */
     public void editingStopped() {
         list.clearSelection();
     }
     

     /**
      * create a single-row JComponent from this component when needed.
      */
      public JComponent getSingleRowComponent(){
          if(readOnly) {
              if ( def.getPropertyType().equals(PropertyType.URL) ) {
                return new URLLabelWidget(getSingleRowString());
              }
              return new LabelWidget(getSingleRowString());
          }
          if ( def.getPropertyType().equals(PropertyType.URL) ) {
              return new URLTextFieldWidget(getSingleRowString());
          }
          return new TextFieldWidget(getSingleRowString());
      }

      /**
      * create a String from this component when needed.
      */
      public String getSingleRowString(){
      String delimiter = def.getValueDelimiter();
          StringBuffer sb = new StringBuffer((String)dataList.get(0));
          for(int i = 1; i<dataList.size(); i++) {
              sb.append(delimiter);
              sb.append((String)dataList.get(i));
          }
          return sb.toString();
      }

    /**
    @since 2.0
    */
    public void removeActionListener(final ActionListener listener) {
        listenerList.remove(ActionListener.class, listener);
    }

    /**
    @since 2.0
    */
    public void removeFocusListener(final FocusListener listener) {
        listenerList.remove(FocusListener.class, listener);
    }

    /**
    @since 2.0
    */
    protected void updateList() {
        list.setListData(dataList.toArray());
        final JComponent parent = (JComponent)getParent();
        parent.setMaximumSize(null);
        parent.setMaximumSize(new Dimension(Short.MAX_VALUE, parent.getPreferredSize().height));
        if( dataList.size() > 0 )
            list.setSelectedIndex(0);
    }
    
}
