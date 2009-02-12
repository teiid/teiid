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

//
package com.metamatrix.toolbox.ui.widget.property;

// System imports
import java.awt.Dimension;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.awt.event.FocusEvent;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;

import javax.swing.SwingUtilities;
import javax.swing.event.ListSelectionEvent;
import javax.swing.event.ListSelectionListener;

import com.metamatrix.common.log.LogManager;
import com.metamatrix.common.object.PropertiedObject;
import com.metamatrix.common.object.PropertiedObjectEditor;
import com.metamatrix.common.object.PropertyDefinition;
import com.metamatrix.common.transaction.TransactionException;
import com.metamatrix.common.transaction.UserTransaction;
import com.metamatrix.toolbox.ui.widget.ButtonWidget;

/**
 * ObjectReferencePropertyComponent is an extension of MultivaluedPropertyComponent for
 * displaying and editing PropertyDefinition values of type PropertyType.OBJECT_REFERENCE.
 * It differs from MultivaluedPropertyComponent in that it may be called for PropertyDefinitions
 * that are single valued.  This component will respond appropriately.
 */
public class ObjectReferencePropertyComponent extends MultivaluedPropertyComponent implements PropertyComponent {

//	private final static String DFLT_TTL = "Please select the item to reference";

    private ObjectReferenceHandler handler;
	private PropertyDefinition dfn;
    private PropertiedObject propertiedObject;
    private PropertiedObjectEditor propertiedObjectEditor;
    private Object currentValue;
    private PropertyChangeAdapter adapter;

    private ButtonWidget navigateButton;

	/**
	 * Constructor
	 */
	public ObjectReferencePropertyComponent(final PropertyDefinition dfn,
                                            final Object data,
                                            final boolean readOnly,
	                                        final int index,
                                            final PropertiedObject object,
	                                        final PropertiedObjectEditor editor,
                                            final ObjectReferenceHandler handler,
                                            final PropertyChangeAdapter adapter) {
        // if there is no ObjectReferenceHandler, then this property will be considered read-only
		super(dfn, getObjectArray(data), (handler == null ? true : readOnly), index, object, editor);
		this.dfn = dfn;
        this.currentValue = data;
        this.propertiedObject = object;
        this.propertiedObjectEditor = editor;
        this.handler = handler;
        this.adapter = adapter;

        if ( handler == null ) {
            throw new IllegalArgumentException("ObjectReferenceHandler may not be null");
        }

        createCustomComponent();

	}

    protected void moreButtonPressed() {
        addButtonPressed();
    }

	/**
	 * Method overridden from super class to call the appropriate ObjectReferenceHandler method
     * when the '+' button is clicked.  This method will never be called if the handler is null.
	 */
	protected void addButtonPressed() {

        if ( this.dfn.getMultiplicity().getMaximum() > 1 ) {
            // multi-valued
            Collection listData = Collections.EMPTY_LIST;
            // build a list of the current values
            if ( currentValue != null ) {
                listData = Arrays.asList((Object[]) currentValue);
            }

            // call the ObjectReferenceHandler method for multi-valued selection
            Object[] values = handler.getObjectReferences(propertiedObject, propertiedObjectEditor, dfn, listData);
            if ( values != null ) {
                // update the view's model in the super class
                getList().clear();
                getList().addAll(Arrays.asList(values));
                // set the new values on the object
                setValue(values);
            }

        } else {
            // call the ObjectReferenceHandler method for single-valued selection
            Object value = handler.getObjectReference(propertiedObject, propertiedObjectEditor, dfn, currentValue);
            if ( value != null ) {
                if ( value == ObjectReferenceHandler.NULL_OBJECT ) {
                    // update the view's model in the super class
                    getList().clear();
                    // null out the current value
                    currentValue = null;
                    setValue(null);
                } else {
                    // update the view's model in the super class
                    getList().clear();
                    getList().add(value);
                    // set the new value
                    currentValue = value;
                    setValue(value);
                }
            }
        }
    }

    protected void setValue(Object value) {
        //The PropertyChangeAdapter must do this, so just update the list.
		updateList();
        fireFocusEvent(new FocusEvent(this, FocusEvent.FOCUS_LOST, true));
	}


    /**
     * Override method which checks multiplicity of the propertied object, just in case
     * it is a single value and not multi valued.
     * Get the value being displayed by this component.  This value will be obtained
     * from the PropertyChangeAdapter immediately prior to and after editing.  After
     * the user has stopped editing the value, the isEqualTo() method will be called
     * allowing this component to determine if the user has changed the value.
     * @return the property's value.  May be a single object or a Collection.
     */
    public Object getValue(){
        if(getList().isEmpty() ){
            return null;
        }

        // Check dfn to see if multivalued.
        if( dfn.getMultiplicity().getMaximum() > 1 ) {
        	return super.getValue();
        }
      	return getList().get(0);
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
        boolean result = false;
        
        if( dfn.getMultiplicity().getMaximum() > 1 ) {
            result = super.isCurrentValueEqualTo(value);

        } else {
            // see if they are both null
            if ( value == null ) {
                if ( getList().size() == 0 ) {
                    result = true;
                }

            } else {
                if ( getList().size() == 1 ) {
                    Object currentValue = getList().get(0);
                    if ( currentValue != null && value.equals(currentValue) ) {
                        result = true;
                    }
                }
            }
        }
        return result;
    }
	
	/**
	 * Converts the data param from the constructor to an ObjectArray
	 * Creation date: (9/13/01 10:54:17 AM)
	 * @return java.lang.Object[]
	 * @param data java.lang.Object
	 */
	private static Object[] getObjectArray(Object data) {
		if(data instanceof Object[]){
			return (Object[])data;
		}

		Object[] value = new Object[1];
		value[0] = data;

		return value;
	}

	// ****************
	// Instance Methods
	// ****************

	/**
	@since Golden Gate
    */
	protected void removeButtonPressed() {

		//remove the selected values from the list
		final int[] indices = getListWidget().getSelectedIndices();

		if (indices == null) {
			return;
		}
		for (int ndx = indices.length;  --ndx >= 0;) {
			getList().remove(indices[ndx]);
		}

		//update the property value.
		UserTransaction txn = propertiedObjectEditor.createWriteTransaction(this);
		boolean wasErr = true;
		try {
			// Begin the transaction
			txn.begin();

			if(getList().isEmpty() ) {
				propertiedObjectEditor.setValue(propertiedObject, dfn, null );
			}else{
			    if( dfn.getMultiplicity().getMaximum() > 1 )
					propertiedObjectEditor.setValue(propertiedObject, dfn, getList().toArray() );
				else
					propertiedObjectEditor.setValue(propertiedObject, dfn, getList().get(0) );
			}

			wasErr = false;
		} catch (TransactionException e) {
			LogManager.logCritical(PropertiedObjectPanel.LOG_CONTEXT, e, "[ObjectReferencePropertyComponent.removeValue] caught exception");
		} finally {
			try {
				if ( wasErr ) {
	    			txn.rollback();
				} else {
					txn.commit();
				}
			} catch (TransactionException ex) {
				LogManager.logCritical(PropertiedObjectPanel.LOG_CONTEXT, ex, "Failed to " + (wasErr ? "rollback." : "commit."));
			}
		}


		updateList();
	}

    private Object getSelectedValue() {
        Object result = null;
        if ( this.dfn.getMultiplicity().getMaximum() > 1 ) {
            // multi-valued
            if ( getList().size() > 1 ) {
                result = getListWidget().getSelectedValue();
            } else if ( getList().size() == 1 ) {
                result = getList().get(0);
            }
        } else {
            result = currentValue;
        }
        return result;
    }

    protected void navigateButtonPressed() {
        adapter.setEditorComponentAfterEdit(this);

        SwingUtilities.invokeLater(new Runnable() {
            public void run() {
                handler.navigateTo(getSelectedValue());
            }
        });
    }

    protected void createCustomComponent() {

        if ( this.handler != null ) {

            navigateButton = new ButtonWidget("?");
            navigateButton.setAlignmentX(0.5f);
            navigateButton.setToolTipText("Show this Object's Properties");
            navigateButton.addActionListener(new ActionListener() {
                public void actionPerformed(final ActionEvent event) {
                    navigateButtonPressed();
                }
            });
            // don't add focus listener to the Navigate button becuase it does not edit this value
            //navigateButton.addFocusListener(focusListener);
            Dimension size = new Dimension( navigateButton.getPreferredSize().width, PropertyComponentFactory.HEIGHT);
            navigateButton.setMinimumSize(size);
            navigateButton.setPreferredSize(size);
            navigateButton.setMaximumSize(size);
            buttonBox.add(navigateButton);

            if ( this.dfn.getMultiplicity().getMaximum() > 1 ) {
                // multi-valued - enabling requires selection
                navigateButton.setEnabled(getList().size() == 1 && handler.canNavigateTo(getList().get(0)));

                // enable the navigateButton only when a there is one item or else a list item must be is selected
                getListWidget().addListSelectionListener(new ListSelectionListener() {
                    public void valueChanged(ListSelectionEvent e) {
                        if ( navigateButton != null ) {
                            SwingUtilities.invokeLater(new Runnable() {
                                public void run() {
                                    // enable the naviagte button for single selection and objects that the handler can navigate to
                                    boolean enable = false;
                                    if ( getList().size() > 1 ) {
                                        enable = ( getListWidget().getSelectedIndices().length == 1 ) &&
                                                     ( handler.canNavigateTo(getSelectedValue()) );
                                    } else if ( getList().size() == 1 ) {
                                        enable = handler.canNavigateTo(getList().get(0));
                                    }
                                    navigateButton.setEnabled(enable);
                                }
                            });
                        }
                    }
                });

            } else {
                if ( this.currentValue == null ) {
                    navigateButton.setEnabled(false);
                } else {
                    navigateButton.setEnabled(handler.canNavigateTo(this.currentValue));
                    
                }
                // enable the navigateButton only when a there is one item or else a list item must be is selected
                getListWidget().addListSelectionListener(new ListSelectionListener() {
                    public void valueChanged(ListSelectionEvent e) {
                        if ( navigateButton != null ) {
                            SwingUtilities.invokeLater(new Runnable() {
                                public void run() {
                                    // enable the naviagte button for single selection and objects that the handler can navigate to
                                    boolean enable = false;
                                    if ( getList().size() > 1 ) {
                                        enable = ( getListWidget().getSelectedIndices().length == 1 ) &&
                                                     ( handler.canNavigateTo(getSelectedValue()) );
                                    } else if ( getList().size() == 1 ) {
                                        enable = handler.canNavigateTo(getList().get(0));
                                    }
                                    navigateButton.setEnabled(enable);
                                }
                            });
                        }
                    }
                });
            }
        }
    }

}
