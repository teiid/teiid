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
package com.metamatrix.toolbox.ui.widget;

import java.awt.AWTEvent;
import java.awt.BorderLayout;
import java.awt.Component;
import java.awt.Dialog;
import java.awt.Dimension;
import java.awt.Frame;
import java.awt.Toolkit;
import java.awt.Window;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.awt.event.WindowEvent;
import java.util.ArrayList;
import java.util.EventListener;
import java.util.Iterator;
import java.util.List;
import java.util.StringTokenizer;

import javax.swing.JDialog;
import javax.swing.SwingUtilities;

import com.metamatrix.toolbox.preference.UserPreferences;
import com.metamatrix.toolbox.ui.widget.event.WidgetActionEvent;

/**
 * This class is intended to be used everywhere within the application that a dialog needs to be displayed.  It may only be used
 * with a DialogPanel, which must be specified in the constructor.  By default, this class acts as the controller for the accept
 * and cancel buttons within the contained DialogPanel.  The default action for each of these buttons is to dispose the dialog.
 * @since 2.0
 * @version 2.0
 * @author <a href="mailto:jverhaeg@metamatrix.com">John P. A. Verhaeg</a>
 */
public class DialogWindow extends JDialog {
    //############################################################################################################################
    //# Constants                                                                                                                #
    //############################################################################################################################

    public static final boolean IS_MODAL = true;
    private static final String SIZE_PREFERENCE_PREFIX = "dialogWindow.";

    //############################################################################################################################
    //# Static Variables                                                                                                         #
    //############################################################################################################################

    private static boolean sizeToPreferences = false;

    //############################################################################################################################
    //# Static Methods                                                                                                           #
    //############################################################################################################################

    /**
     * Disposes the specified window as long as the specified event is not a WidgetActionEvent or has not been destroyed.  In the
     * latter case, the method will wait until the WidgetActionEvent is finished processing.
     * @param window   The window to be disposed
     * @param event    The event triggering the window's disposal
     * @since 2.0
     */
    public static void disposeConditionally(final Window window, final AWTEvent event) {
        final Thread thread = new Thread("DialogWindow Conditional Dispose Thread") {
            public void run() {
                if (event instanceof WidgetActionEvent) {
                    final WidgetActionEvent widgetEvent = (WidgetActionEvent)event;
                    widgetEvent.waitWhileProcessing();
                    if (widgetEvent.isDestroyed()) {
                        return;
                    }
                }
                if ( sizeToPreferences && (window instanceof DialogWindow) ) {
                    DialogWindow dialog = (DialogWindow) window;
                    String panelName = dialog.getDialogPanel().getName();
                    if ( panelName != null && panelName.length() > 0 ) {
                        storeDimensionFromPreferences(panelName, window.getSize());
                    }
                }
                window.dispose();
            }
        };
        thread.start();
    }

    /**
     * @since 3.0
     */
    protected static Dimension getDimensionFromPreferences(String propertyName) {
        Dimension result = null;
        Object obj = UserPreferences.getInstance().getValue(SIZE_PREFERENCE_PREFIX + propertyName);
        if ( obj != null ) {
            String sizeString = (String) obj;
            if ( sizeString.length() > 0 ) {
                StringTokenizer tok = new StringTokenizer(sizeString, ";");
                result = new Dimension(Integer.parseInt(tok.nextToken()), Integer.parseInt(tok.nextToken()));
            }
        }
        return result;
    }

    /**
     * @since 2.0
     */
    public static DialogWindow getInstance(final Component parent, final String title, final Component content) {
        Window owner = null;
        if (parent instanceof Window) {
            owner = (Window)parent;
        } else if (parent != null) {
            owner = SwingUtilities.windowForComponent(parent);
        }
        DialogPanel panel;
        if (content instanceof DialogPanel) {
            panel = (DialogPanel)content;
        } else {
            panel = new DialogPanel(content);
        }
        if (owner == null  ||  owner instanceof Frame) {
            return new DialogWindow((Frame)owner, title, panel);
        }
        if (owner instanceof Dialog) {
            return new DialogWindow((Dialog)owner, title, panel);
        }
        throw new IllegalArgumentException("Parent parameter must be within a Dialog (" + Dialog.class + ") or Frame (" +
                                           Frame.class + ")");
    }

    /**
     * @since 3.0
     */
    public static void setSizeToPreferences(boolean flag) {
        sizeToPreferences = flag;
    }

    /**
     * @since 2.0
     */
    public static DialogPanel show(final DialogWindow dialog) {
        final Dimension screenSize = Toolkit.getDefaultToolkit().getScreenSize();
        dialog.setSize(Math.min(dialog.getWidth(), screenSize.width), Math.min(dialog.getHeight(), screenSize.height));
        dialog.setLocationRelativeTo(dialog.getOwner());
        dialog.setVisible(true);
        return dialog.getDialogPanel();
    }

    /**
     * @since 2.0
     */
    public static DialogPanel show(final Component parent, final String title, final Component content) {
        return show(getInstance(parent, title, content));
    }

    /**
     * @since 3.0
     */
    protected static void storeDimensionFromPreferences(String propertyName, Dimension size) {
        String key = SIZE_PREFERENCE_PREFIX + propertyName;
        String value = new Integer(size.width).toString() + ';' + new Integer(size.height).toString();
        UserPreferences.getInstance().setValue(key, value);
        UserPreferences.getInstance().saveChanges();
    }

    //############################################################################################################################
    //# Instance Variables                                                                                                       #
    //############################################################################################################################

    private DialogPanel dlgPanel;
    
    // Cache the action listeners of the accept and cancel buttons registered within this class.
    // When the window is closed we can de-register the listeners. If a DialogPanel is being
    // reused and then shown via a static show method, this prevents multiple listeners
    // being registered for the same buttons.
    private List listeners;

    //############################################################################################################################
    //# Constructors                                                                                                             #
    //############################################################################################################################

    /**
     * Creates a modal dialog with the specified title, that will be displayed by the specified frame.
     * @param owner    The window that displayed the dialog
     * @param title    The dialog's title
     * @param panel    The contained DialogPanel
     * @since 2.0
     */
    public DialogWindow(final Frame owner, final String title, final DialogPanel panel) {
        this(owner, title, panel, IS_MODAL);
    }

    /**
     * Creates a modal dialog with the specified title, that will be displayed by the specified dialog.
     * @param owner    The window that displayed the dialog
     * @param title    The dialog's title
     * @param panel    The contained DialogPanel
     * @since 2.0
     */
    public DialogWindow(final Dialog owner, final String title, final DialogPanel panel) {
        this(owner, title, panel, IS_MODAL);
    }

    /**
     * Creates a dialog with the specified title and modality, that will be displayed by the specified frame.
     * @param owner    The window that displayed the dialog
     * @param title    The dialog's title
     * @param panel    The contained DialogPanel
     * @param isModal  Indicates whether the dialog is modal
     * @since 2.0
     */
    public DialogWindow(final Frame owner, final String title, final DialogPanel panel, final boolean isModal) {
        super(owner, title, isModal);
        this.dlgPanel = panel;
        initializeDialogWindow();
    }

    /**
     * Creates a dialog with the specified title and modality, that will be displayed by the specified dialog.
     * @param owner    The window that displayed the dialog
     * @param title    The dialog's title
     * @param panel    The contained DialogPanel
     * @param isModal  Indicates whether the dialog is modal
     * @since 2.0
     */
    public DialogWindow(final Dialog owner, final String title, final DialogPanel panel, final boolean isModal) {
        super(owner, title, isModal);
        this.dlgPanel = panel;
        initializeDialogWindow();
    }

    //############################################################################################################################
    //# Instance Methods                                                                                                         #
    //############################################################################################################################

    /**
    Called when the user activates the accept button.  Simply disposes the dialog by default.
    @param event The WidgetActionEvent generated from activating the button
    @since 2.0
    */
    // The event signature should be changed to a WidgetActionEvent
    protected void accept(final ActionEvent event) {
        // Ensure the panel's selection listener is executed before any others, assuming the list of listeners is always in
        // reverse order of how they were added, i.e., actionListeners[0] is the last listener added...
        final ButtonWidget button = (ButtonWidget)event.getSource();
        final ActionListener selectionListener = dlgPanel.getSelectionListener();
        final EventListener[] actionListeners = button.getListeners(ActionListener.class);
        if (actionListeners[0] != selectionListener) {
            selectionListener.actionPerformed(event);
        }
        dlgPanel.accept((WidgetActionEvent)event);
        if (dlgPanel.canAccept()) {
        	disposeConditionally(this, event);
			unregisterListeners();
        }
    }

    /**
    Called when the user activates the cancel button or cancels the dialog via the title bar close button (with the 'X' icon) or
    the title bar system menu 'Close' option.  Simply disposes the dialog by default.
    @param event The WidgetActionEvent generated from activating the button or closing the window
    @since 2.0
    */
    // The event signature should be changed to a WidgetActionEvent
    protected void cancel(final AWTEvent event) {
        dlgPanel.cancel((WidgetActionEvent)event);
        if (dlgPanel.canCancel()) {
        	disposeConditionally(this, event);
            unregisterListeners();
        }
    }

    /**
    @return The contained DialogPanel
    @since 2.0
    */
    public DialogPanel getDialogPanel() {
        return dlgPanel;
    }

    /**
    Initializes the dialog:
    <ol>
    <li>Adds the dlgPanel to the center of the dialog</li>
    <li>Centers dialog relative to its owner</li>
    <li>Adds listeners to the DialogPanel's accept and cancel buttons that call the accept and cancel methods, respectively,
    within this class</li>
    <li>Ties window close button ('X' button) to the cancel button's action</li>
    </ol>
    @since 2.0
    */
    protected void initializeDialogWindow() {
        listeners = new ArrayList(2);
        
        getContentPane().add(dlgPanel, BorderLayout.CENTER);
        // Add listeners to accept and cancel buttons
        final ActionListener acceptListener = new ActionListener() {
            public void actionPerformed(final ActionEvent event) {
                accept(event);
            }
        };
        for (final Iterator iter = dlgPanel.getAcceptButtons().iterator();  iter.hasNext();) {
            ((ButtonWidget)iter.next()).addActionListener(acceptListener);
        }
        listeners.add(acceptListener);
        final ButtonWidget cancelButton = dlgPanel.getCancelButton();
        if (cancelButton != null) {
            final ActionListener cancelListener = new ActionListener() {
                public void actionPerformed(final ActionEvent event) {
                    cancel(event);
                }
            };
            listeners.add(cancelListener);
            cancelButton.addActionListener(cancelListener);
        }
        // Set the first accept button as the window's default button
        getRootPane().setDefaultButton(dlgPanel.getAcceptButton());
        if ( sizeToPreferences ) {
            // See if there is a Dimension property for this panel's preferred size
            Dimension size = getDimensionFromPreferences(this.dlgPanel.getName());
            if ( size != null ) {
            	Dimension preferredSize = getPreferredSize();
            	setSize(new Dimension(Math.max(size.width, preferredSize.width),
            	                      Math.max(size.height, preferredSize.height)));
            } else {
                pack();
            }
        } else {
            // Set the window's size to just accommodate its fields
            pack();
        }
        setLocationRelativeTo(getOwner());
    }
    
    /**
     * Overridden to call the cancel() method in the event that the user cancels the window via its close button (the 'X' icon) or
     * the 'Close' option in the system menu.
     * @param event The window event to be processed
     * @since 2.0
     */
    protected void processWindowEvent(final WindowEvent event) {
        if (event.getID() == WindowEvent.WINDOW_CLOSING) {
            final WidgetActionEvent actionEvent = new WidgetActionEvent(event.getSource(), null);
            cancel(actionEvent);
            if (actionEvent.isDestroyed()  ||  !dlgPanel.canCancel()) {
                return;
            }
        }
        super.processWindowEvent(event);
    }

    /**
     * Unregisters the listeners for the accept and cancel buttons that callback to the contained {@link DialogPanel} to determine
     * if the this dialog may be disposed.
     * @since 3.1
     */
    protected void unregisterListeners() {
        if (!listeners.isEmpty()) {
            final ActionListener listener = (ActionListener)listeners.get(0);
            for (final Iterator iter = dlgPanel.getAcceptButtons().iterator();  iter.hasNext();) {
                // accept button will always be at index zero if it exists
                // see initializeDialogWindow()
                ((ButtonWidget)iter.next()).removeActionListener(listener);
            }
            if (listeners.size() > 0) {
                final ButtonWidget cancelButton = dlgPanel.getCancelButton();
                if (cancelButton != null) {
                    // cancel button will always be at index one if it exists
                    // see initializeDialogWindow()
                    cancelButton.removeActionListener((ActionListener)listeners.get(1));
                }
            }
            listeners.clear();
        }
    }
}
