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

import java.awt.BorderLayout;
import java.awt.Color;
import java.awt.Component;
import java.awt.Dimension;
import java.awt.Frame;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.awt.event.FocusEvent;
import java.awt.event.FocusListener;
import java.io.File;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;

import javax.swing.JComponent;
import javax.swing.JPanel;
import javax.swing.SwingUtilities;

import com.metamatrix.common.tree.directory.DirectoryEntry;
import com.metamatrix.common.tree.directory.DirectoryEntryFilter;
import com.metamatrix.common.tree.directory.FileSystemEntry;
import com.metamatrix.common.tree.directory.FileSystemFilter;
import com.metamatrix.common.tree.directory.FileSystemView;

import com.metamatrix.toolbox.ui.widget.ButtonWidget;
import com.metamatrix.toolbox.ui.widget.DialogWindow;
import com.metamatrix.toolbox.ui.widget.DirectoryChooserPanel;
import com.metamatrix.toolbox.ui.widget.TextFieldWidget;


/**
DialogManager is a collection of static methods for accessing and displaying
the MetaData Modeler application dialogs and wizards.  DialogManager is responsible
for determining and setting the parent frame for dialogs and determining dialog
modality.  The class represents a single point of control for dialogs that can
be reused across the different parts of the application.
@author Steve Jacobs
@author <a href="mailto:jverhaeg@metamatrix.com">John P. A. Verhaeg</a>
*/
public class DirectoryEntryPropertyComponent extends JPanel implements PropertyComponent {

    private static final Color VALID_COLOR = new TextFieldWidget().getForeground();
    private static final Color INVALID_COLOR = Color.red;

    private DirectoryEntry currentValue;
    private TextFieldWidget fileNameField = new TextFieldWidget();
    private ButtonWidget moreButton;
//    private DirectoryEntry homeEntry;
    private String dialogTitle;
    private String initialPath = "C:\\";
//    private String rootPath;
    private Collection myFocusListenerList;
    private FileSystemView view;
    private boolean hasFocus = false;

    public DirectoryEntryPropertyComponent(String dialogTitle, String rootPath, DirectoryEntry homeEntry, final int index) {
        super(new BorderLayout(0, 0));

//        this.rootPath = rootPath;
        this.currentValue = homeEntry;
        this.dialogTitle = dialogTitle;

        final FocusListener focusListener = new FocusListener() {
            public void focusGained(final FocusEvent event) {
                if (hasFocus) {
                    return;
                }
                fireFocusGainedEvent();
                hasFocus = true;
            }
            public void focusLost(final FocusEvent event) {
                hasFocus = false;
                processFocusLostEvent(event);
            }
        };

        fileNameField = new TextFieldWidget();
        add(fileNameField);
        if ( currentValue != null ) {
            fileNameField.setText(currentValue.toString());
        }
        fileNameField.addActionListener(new ActionListener() {
            public void actionPerformed(ActionEvent e) {
                textFieldActionPerformed();
            }
        });
        fileNameField.addFocusListener(focusListener);

        moreButton = new ButtonWidget("...");
        moreButton.setName("DirectoryEntryPropertyComponent.moreButton." + dialogTitle + '.' + index);
        final Dimension size = new Dimension(moreButton.getPreferredSize().width, PropertyComponentFactory.HEIGHT);
        moreButton.setMinimumSize(size);
        moreButton.setPreferredSize(size);
        moreButton.setMaximumSize(size);
        moreButton.addActionListener(new ActionListener() {
            public void actionPerformed(ActionEvent e) {
                showDirectoryChooserDialog((Component) e.getSource());
            }
        });
        moreButton.addFocusListener(focusListener);

        add(moreButton, BorderLayout.EAST);

        if ( rootPath != null ) {
    		File rootDir = new File(rootPath);
            view = new FileSystemView(rootDir, true);
        } else {
            view = new FileSystemView();
        }
        if ( currentValue != null ) {
            FileSystemEntry entry = (FileSystemEntry) currentValue;
            if ( entry.isFolder() ) {
                view.setHome(entry);
            } else {
                view.setHome(view.getParent(entry));
            }
        } else {
            DirectoryEntry home = view.lookup(initialPath);
            if ( home != null ) {
                FileSystemEntry entry = (FileSystemEntry) home;
                if ( entry.isFolder() ) {
                    view.setHome(entry);
                } else {
                    view.setHome(view.getParent(entry));
                }
            }
        }

    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
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
        fireFocusLostEvent();
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since Golden Gate
    */
    public void addActionListener(final ActionListener listener) {
        listenerList.add(ActionListener.class, listener);
    }

	/**
	 * show the DirectoryChooserDialog in the mode specified, showing the Modeler's Local Directory.
     * @param trueForOpen true if the dialog should be set to "open" mode, false for "save" mode.
     * @param trueForSaveModel true if the dialog should configure to save Model files, false for saving
     * project files.  Value is not used if the operation mode is "open"
     * @param showModelAndProjectFilters
	 * @return the file selected when in either open or save mode, null if no file was selected
     * and/or the user hit cancel.
     */
    public void showDirectoryChooserDialog(Component c) {
		FileSystemFilter xmlFileFilter = new FileSystemFilter(view,new String[]{"xml"}, "Metadata Model files");
		FileSystemFilter prjFileFilter = new FileSystemFilter(view,new String[]{"prj"}, "Metadata Project files");
        DirectoryEntryFilter[] tnfArray = new DirectoryEntryFilter[] { xmlFileFilter, prjFileFilter };
		DirectoryChooserPanel cp = new DirectoryChooserPanel(view, DirectoryChooserPanel.TYPE_OPEN, tnfArray);
		DialogWindow dw = new DialogWindow((Frame) null, dialogTitle, cp, true);
		//dw.pack();
		dw.setSize(800, 400);
		dw.show();
        dw.setLocationRelativeTo(c);
		if ( cp.getSelectedButton() == cp.getAcceptButton() ) {
			currentValue = (DirectoryEntry) cp.getSelectedTreeNode();
            fileNameField.setText(currentValue.toString());
            fireActionEvent();
		}
    }

    /**
     * ActionListener calls this method to set values that were typed in by the user.
     */
    private void textFieldActionPerformed() {
        String pathName = fileNameField.getText();
        DirectoryEntry entry = view.lookup(pathName);
        if ( entry != null ) {
            fileNameField.setForeground(VALID_COLOR);
            currentValue = entry;
        } else {
            fileNameField.setForeground(INVALID_COLOR);
        }
        fireActionEvent();
    }

    /**
     * Manage the focus listener methods for the PropertyEventAdapter
     */
    public synchronized void addFocusListener(FocusListener l) {
        getFocusListenerList().add(l);
    }

    /**
     * Manage the focus listener methods for the PropertyEventAdapter
     */
    public synchronized void removeFocusListener(FocusListener l) {
        getFocusListenerList().remove(l);
    }

    /**
     * Early initialization of the focus listener list because of addFocusListener
     * call during construction of the superclass JPanel.
     */
    private Collection getFocusListenerList() {
        if ( myFocusListenerList == null ) {
            this.myFocusListenerList = new ArrayList();
        }
        return myFocusListenerList;
    }

    /**
     * Fire a FocusEvent of type FOCUS_GAINED to all listeners
     */
    private void fireFocusGainedEvent() {
        FocusEvent event = new FocusEvent(this, FocusEvent.FOCUS_GAINED);
        Iterator iter = myFocusListenerList.iterator();
        while ( iter.hasNext() ) {
            ((FocusListener) iter.next()).focusGained(event);
        }
    }

    /**
     * Fire a FocusEvent of type FOCUS_LOST to all listeners
     */
    private void fireFocusLostEvent() {
        FocusEvent event = new FocusEvent(this, FocusEvent.FOCUS_LOST);
        Iterator iter = myFocusListenerList.iterator();
        while ( iter.hasNext() ) {
            ((FocusListener) iter.next()).focusLost(event);
        }
    }

    protected void fireActionEvent() {
        final Object[] listeners = listenerList.getListenerList();
        ActionEvent newEvent = null;
        for (int ndx = listeners.length - 2;  ndx >= 0;  ndx -= 2) {
            if (listeners[ndx] == ActionListener.class) {
                if (newEvent == null) {
                      newEvent = new ActionEvent(this, ActionEvent.ACTION_PERFORMED, "");
                }
                ((ActionListener)listeners[ndx + 1]).actionPerformed(newEvent);
            }          
        }
    }


    // *************************
    // PropertyComponent methods
    // *************************

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
    public Object getNullValue() {
        return null;
    }

    /**
     * Get the value being displayed by this component.  This value will be obtained
     * from the PropertyChangeAdapter immediately prior to and after editing.  After
     * the user has stopped editing the value, the isEqualTo() method will be called
     * allowing this component to determine if the user has changed the value.
     * @return the property's value.  May be a single object or a Collection.
     */
    public Object getValue() {
        return this.currentValue;
    }


    /**
     * Set whether or not this component should be enabled to allow user
     * editing of the value(s).
     * @param flag true if the component should enable editing.
     */
    public void setEnabled(boolean flag) {
        moreButton.setVisible(flag);
        fileNameField.setEnabled(flag);
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
    public void setPropertyValidationListener(PropertyValidationListener listener) {
    }

    /**
     * <p>Remove the PropertyValidationListener for this component.  This method will
     * be called immediately after editing has stopped on this component.  Implementations
     * that no-op the setPropertyValidationListener method may no-op this method as well.
     * @param listener the PropertyValidationListener to be removed from this object.
     */
    public void removePropertyValidationListener(PropertyValidationListener listener) {
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
        if ( flag ) {
            fileNameField.setForeground(VALID_COLOR);
        } else {
            fileNameField.setForeground(INVALID_COLOR);
        }
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
        boolean result = true;
        if ( currentValue == null ) {
            result = ( value == null );
        } else {
            if ( value == null ) {
                result = false;
            } else {
                result = currentValue.equals(value);
            }
        }
        return result;
     }

     /**
      * Notify this component that it has been activated and should enable any controls
      * necessary for editing property values.
      */
     public void editingStarted() {
     }

     /**
      * Notification to this component that keyboard/mouse focus has moved away from the
      * component and it should deselect any items and deactivate any editing controls.
      */
     public void editingStopped() {
     }

     /**
      * create a single-row JComponent from this component when needed.
      */
     public JComponent getSingleRowComponent() {
        return this;
     }

      /**
      * create a String from this component when needed.
      */
     public String getSingleRowString() {
        if ( currentValue == null ) {
            return EMPTY_STRING;
        }
        return currentValue.toString();
     }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since Golden Gate
    */
    public void removeActionListener(final ActionListener listener) {
        listenerList.remove(ActionListener.class, listener);
    }
}


