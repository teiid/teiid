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

package com.metamatrix.toolbox.ui.callback;

import java.awt.BorderLayout;
import java.awt.Dialog;
import java.awt.Dimension;
import java.awt.Frame;
import java.awt.Window;
import java.io.IOException;

import javax.swing.Icon;
import javax.swing.JDialog;
import javax.swing.JLabel;
import javax.swing.JOptionPane;
import javax.swing.JPanel;
import javax.swing.JTabbedPane;
import javax.swing.UIManager;

import com.metamatrix.common.callback.Callback;
import com.metamatrix.common.callback.CallbackChoices;
import com.metamatrix.common.callback.CallbackHandler;
import com.metamatrix.common.callback.UnsupportedCallbackException;
import com.metamatrix.common.object.PropertiedObject;
import com.metamatrix.common.object.PropertiedObjectEditor;
import com.metamatrix.common.object.PropertyDefinitionGroup;
import com.metamatrix.common.util.crypto.NullCryptor;
import com.metamatrix.core.util.Assertion;
import com.metamatrix.toolbox.ui.widget.DialogPanel;
import com.metamatrix.toolbox.ui.widget.DialogWindow;
import com.metamatrix.toolbox.ui.widget.property.PropertiedObjectPanel;

/**
 * An application implements a CallbackHandler and passes it to underlying
 * security services so that they may interact with the application to retrieve
 * specific authentication data, such as usernames and passwords, or to display
 * certain information, such as error and warning messages.
 * <p>
 * CallbackHandlers are implemented in an application-dependent fashion.
 * For example, implementations for an application with a graphical user
 * interface (GUI) may pop up windows to prompt for requested information
 * or to display error messages. An implementation may also choose to
 * obtain requested information from an alternate source without asking
 * the end user.
 */
public class DialogFactoryCallbackHandler implements CallbackHandler {

    private ParentFrameSupplier parentFrameSupplier;
    private JTabbedPane tabbedPane;
    private JDialog dialog = null;
    private Callback currentCallback = null;
    private Dimension size = null;
    boolean choiceSelected = false;


    public DialogFactoryCallbackHandler() {
    }

    public void setParentFrameSupplier(ParentFrameSupplier supplier) {
        Assertion.isNotNull(supplier);
        this.parentFrameSupplier = supplier;
    }

    /**
     * set the Size for this handler to use for all dialogs.  if this method is not called, or called with
     * a null size, the handler will use pack() to set dialog size.
     */
    public void setDefaultDialogSize(Dimension size) {
        this.size = size;
    }

    /**
     * Retrieve or display the requested information in the Callback object.
     * <p>
     * The implementation should process all callback objects before returning, since
     * the caller of this method is free to retrieve the requested information from the
     * callback objects immediately after this method returns.
     * @param callbacks an array of Callback objects provided by the method caller,
     * and which contain the information requested to be retrieved or displayed.
     * @param source the object that is considered the source of the callbacks.
     * @throws IOException if an input or output error occurs
     * @throws UnsupportedCallbackException if the implementation of this method
     * does not support one or more of the Callbacks specified in the callbacks parameter
     */
    public void handle(Callback callback, Object source) throws IOException, UnsupportedCallbackException {

        Assertion.isNotNull(callback);
        this.currentCallback = callback;

        if ( callback.hasPropertiedObject() ) {

            DialogPanel panel = processPropertiedObjectCallback(callback);
            Window window = parentFrameSupplier.getParentFrameForCallback();

            if ( window instanceof Frame ) {
                dialog = new DialogWindow((Frame) window, callback.getDisplayName(), panel, true);
            } else if ( window instanceof Dialog ) {
                dialog = new DialogWindow((Dialog) window, callback.getDisplayName(), panel, true);
            }
            if ( dialog != null ) {

                dialog.addWindowListener(new java.awt.event.WindowAdapter() {
                    public void windowClosing(java.awt.event.WindowEvent e) {
                        dialogDismissed();
                    }
                });
            }

            if ( size != null ) {
                dialog.setSize(size);
            } else {
                dialog.pack();
            }
            if ( window != null ) {
                dialog.setLocationRelativeTo(window);
            }
            dialog.show();





        } else {

            int index = CallbackChoices.DISMISSED;

            if ( callback.getChoices().getOptions() == null ) {

                int messageType = callback.getChoices().getMessageType();

                if ( messageType == CallbackChoices.INFORMATION_MESSAGE || messageType == CallbackChoices.UNSPECIFIED_MESSAGE ) {

                    index = JOptionPane.showConfirmDialog(parentFrameSupplier.getParentFrameForCallback(),
                                                      callback.getChoices().getPrompt(),
                                                      callback.getDisplayName(),
                                                      JOptionPane.OK_OPTION,
                                                      JOptionPane.INFORMATION_MESSAGE);
                } else {

                    // for CallbackChoices with OptionType set
                    index = JOptionPane.showConfirmDialog(parentFrameSupplier.getParentFrameForCallback(),
                                                          callback.getChoices().getPrompt(),
                                                          callback.getDisplayName(),
                                                          callback.getChoices().getOptionType(),
                                                          callback.getChoices().getMessageType());
                }

            } else {


                // for CallbackChoices with an array of options
                index = JOptionPane.showOptionDialog(parentFrameSupplier.getParentFrameForCallback(),
                                                     callback.getChoices().getPrompt(),
                                                     callback.getDisplayName(),
                                                     JOptionPane.OK_CANCEL_OPTION,
                                                     callback.getChoices().getMessageType(),
                                                     getIconForMessageType(callback.getChoices().getMessageType()),
                                                     callback.getChoices().getOptions(),
                                                     callback.getChoices().getOptions()[callback.getChoices().getDefaultOption()]);
            }

            setChoice(index);

        }

    }

    private Icon getIconForMessageType(int messageType) {
        switch(messageType) {
            case CallbackChoices.ERROR_MESSAGE:
                return UIManager.getIcon("OptionPane.errorIcon");
            case CallbackChoices.INFORMATION_MESSAGE:
                return UIManager.getIcon("OptionPane.informationIcon");
            case CallbackChoices.WARNING_MESSAGE:
                return UIManager.getIcon("OptionPane.warningIcon");
            case CallbackChoices.QUESTION_MESSAGE:
                return UIManager.getIcon("OptionPane.questionIcon");
        }
        return null;
    }

    /**
     * create a DialogPanel to process the specified callback.  This method is designed to be
     * used and/or overridden by subclasses to build custom DialogPanels for specific callbacks
     * or to obtain the default DialogPanel built by this method and add/remove features.
     */
    protected DialogPanel processPropertiedObjectCallback(Callback callback) {

        DialogPanel result = new CallbackChoicesDialogPanel(callback);

        if ( callback.hasPropertyDefinitionGroups() ) {

            if ( ! callback.isSequential() ) {
                // handle a non-sequential callback by displaying a tabbed pane where each tab is a group
                tabbedPane = new JTabbedPane();
                result.setContent(tabbedPane);

                while ( callback.hasNextGroup() ) {
                    PropertyDefinitionGroup group = callback.getNextGroup();
                    JPanel panel = createCallbackPanel(callback, group, callback.getPropertiedObject(), callback.getEditor());
                    tabbedPane.addTab(group.getDisplayName(), panel);
                }

            } else {
                //TODO: handle a sequential callback in a Wizard where each panel is callback.getNextGroup()
                //result = new WizardCallbackPanel(callback);
            }

        } else {
            JPanel p = createCallbackPanel(callback, callback.getChoices().getPrompt(), callback.getPropertiedObject(), callback.getEditor());
            result.setContent(p);
        }

        return result;
    }

    /**
     * Create a JPanel to edit the specified callback containing a PropertyDefinitionGroup for the
     * specified PropertiedObject.  This method is designed to be used and/or overridden by subclasses
     * to generate customized panels for specific PropertyDefinitionGroups.
     */
    protected JPanel createCallbackPanel(Callback callback,
                                         PropertyDefinitionGroup definitionGroup,
                                         PropertiedObject object,
                                         PropertiedObjectEditor editor) {
        JPanel result = new JPanel(new BorderLayout());
        JLabel description = new JLabel(definitionGroup.getShortDescription());
        result.add(description, BorderLayout.NORTH);

        PropertiedObjectPanel properties = new PropertiedObjectPanel(editor, new NullCryptor());
        properties.setShowColumnHeaders(false);
        properties.setShowInvalidProperties(true);
        properties.setShowRequiredProperties(true);
        properties.createComponent();
        properties.setPropertiedObject(object, editor, definitionGroup.getPropertyDefinitions());
        result.add(properties, BorderLayout.CENTER);

        return result;
    }

    /**
     * Create a JPanel to edit the specified callback containing a PropertiedObject.
     */
    protected JPanel createCallbackPanel(Callback callback,
                                         String prompt,
                                         PropertiedObject object,
                                         PropertiedObjectEditor editor) {
        JPanel result = new JPanel(new BorderLayout());

        JLabel description = new JLabel(prompt);
        result.add(description, BorderLayout.NORTH);

        PropertiedObjectPanel properties = new PropertiedObjectPanel(editor, new NullCryptor());
        properties.setShowColumnHeaders(false);
        properties.setShowInvalidProperties(true);
        properties.setShowRequiredProperties(true);
        properties.createComponent();
        properties.setPropertiedObject(object);
        result.add(properties, BorderLayout.CENTER);

        return result;
    }

    /**
     * called by cancel/dismiss listeners to indicate that the user dismissed the dialog without making a choice.
     */
    protected void dialogDismissed() {
        if ( ! choiceSelected ) {
            currentCallback.getChoices().setSelectedIndex(CallbackChoices.DISMISSED);
            currentCallback.setResponse(CallbackChoices.DISMISSED);
            currentCallback = null;
        }
    }

    /**
     * set the specified choice as the callback response.
     */
    protected void setChoice(int index) {
        currentCallback.getChoices().setSelectedIndex(index);
        currentCallback.setResponse(index);
        choiceSelected = true;
    }

}

