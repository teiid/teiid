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

package com.metamatrix.toolbox.ui.widget;

// Imports
import java.awt.BorderLayout;
import java.awt.Component;
import java.awt.Container;
import java.awt.Dimension;
import java.awt.IllegalComponentStateException;
import java.awt.Insets;
import java.awt.LayoutManager;
import java.awt.Window;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.awt.event.WindowAdapter;
import java.awt.event.WindowEvent;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import javax.swing.BorderFactory;
import javax.swing.BoxLayout;
import javax.swing.JComponent;
import javax.swing.JPanel;
import javax.swing.border.Border;
import javax.swing.event.AncestorEvent;
import javax.swing.event.AncestorListener;

import com.metamatrix.toolbox.ui.UIConstants;
import com.metamatrix.toolbox.ui.UIDefaults;
import com.metamatrix.toolbox.ui.widget.event.WidgetActionEvent;

/**
 * This class is intended to be used everywhere within the application that a dialog panel needs to be displayed.  It provides for
 * a navigation button bar at the bottom of the panel, a set of methods to work with it, and a default pair of "accept" and
 * "cancel" buttons within the bar.  Support exists for multiple accept buttons, however it is up to the developer to ensure that
 * if accept buttons are removed or reordered, the {@link #getAcceptButtons() list of accept buttons} is updated appropriately.
 * @since 2.0
 */
public class DialogPanel extends JPanel
implements ButtonConstants, UIConstants {
    //############################################################################################################################
    //# Constants                                                                                                                #
    //############################################################################################################################

    private static final Border BORDER = UIDefaults.getInstance().getBorder(WINDOW_BORDER_PROPERTY);
    
    //############################################################################################################################
    //# Variables                                                                                                                #
    //############################################################################################################################

    private Component content;
    private Container contentPanel;
    private JComponent bar;
    
    private List acceptButtons;
    private ButtonWidget cancelButton;
    private ButtonWidget helpButton;
    private ButtonWidget selectedButton;
    
    private Window wdw;

    private LayoutManager layout;

    private int nextNdx;

	private boolean canAccept, canCancel;

    private ActionListener selectionListener;

    //############################################################################################################################
    //# Constructors                                                                                                             #
    //############################################################################################################################

    /**
     * Creates a panel with a BorderLayout and a navigation bar in the SOUTH region.
     * @since 2.0
     */
    public DialogPanel() {
        this(null);
    }

    /**
     * Creates a panel with a BorderLayout, a navigation bar in the SOUTH region, and the specified content in the CENTER region.
     * @param content The component to be displayed.
     * @since 2.0
     */
    public DialogPanel(final Component content) {
        super(null);
        this.content = content;
        initializeDialogPanel();
    }

    //############################################################################################################################
    //# Methods                                                                                                                  #
    //############################################################################################################################

    /**
    Called when the panel's parent is a DialogWindow and the user activates the accept button.  Does nothing by default.
    @param event The WidgetActionEvent generated from activating the button
    @since 2.0
    */
    protected void accept(final WidgetActionEvent event) {
    }

    /**
    Adds the specified button as the accept button to the end of the navigation bar.
    @param button The button to add as an accept button
    @since 2.0
    @return The index at which the button was added
    */
    public int addAcceptButton(final ButtonWidget button) {
		addAcceptButton(button, this.nextNdx);
        return this.nextNdx;
    }

    /**
    Adds the specified button as the accept button to the navigation bar at the specified index.
    @param button The button to add as an accept button
    @param index The index within the navigation bar to add the button
    @since 2.0
    */
    public void addAcceptButton(final ButtonWidget button, final int index) {
		final List acceptButtons = getAcceptButtons();
//        if (!acceptButtons.contains(button)) {
if (this.getNavigationButton(button.getText()) == null) {
			addNavigationButton(button, index);
            acceptButtons.add(button);
        }
    }

    /**
    Adds the specified button as the cancel button to the end of the navigation bar.
    @param button The button to add as an cancel button
    @since 2.0
    @return The index at which the button was added
    */
    public int addCancelButton(final ButtonWidget button) {
        final int ndx = addNavigationButton(button);
        cancelButton = button;
        return ndx;
    }

    /**
    Adds the specified button as the cancel button to the navigation bar at the specified index.
    @param button The button to add as an cancel button
    @param index The index within the navigation bar to add the button
    @since 2.0
    */
    public void addCancelButton(final ButtonWidget button, final int index) {
        addNavigationButton(button, index);
        cancelButton = button;
    }

    /**
    Adds the specified button as the help button to the front of the navigation bar.
    @param button The button to add as a help button
    @since 2.0
    @return The index at which the button was added
    */
    public int addHelpButton(final ButtonWidget button) {
        addNavigationButton(button, 0);
        helpButton = button;
        return 0;
    }

    /**
    Adds the specified button as the help button to the navigation bar at the specified index.
    @param button The button to add as a help button
    @param index The index within the navigation bar to add the button
    @since 2.0
    */
    public void addHelpButton(final ButtonWidget button, final int index) {
        addNavigationButton(button, index);
        helpButton = button;
    }

    /**
    Adds the specified button immediately before the space in front of the cancel button.
    @param button The button to add
    @since 2.0
    @return The index at which the button was added
    */
    public int addNavigationButton(final ButtonWidget button) {
        validateState();
        addNavigationButton(button, nextNdx);
        return nextNdx;
    }

    /**
    Adds the specified button to the navigation bar at the specified index.
    @param button The button to add
    @param index The index within the navigation bar to add the button
    @since 2.0
    */
    public void addNavigationButton(final ButtonWidget button, final int index) {
        // Add listener to update selectedButton
        if (this.selectionListener == null) {
            this.selectionListener = new ActionListener() {
                public void actionPerformed(final ActionEvent event) {
                    selectedButton = ((ButtonWidget)event.getSource());
                }
            };
        }
        button.addActionListener(this.selectionListener);
        addNavigationComponent(button, index);
        equalizeNavigationButtonWidths();
    }

    /**
    Adds the specified component to the navigation bar at the specified index.
    @param component The component to add
    @param index The index within the navigation bar at which to add the component
    @since 2.0
    */
    protected void addNavigationComponent(final Component component, final int index) {
        validateState();
        if (index <= nextNdx) {
            ++nextNdx;
        }
        bar.add(component, index);
    }

    /**
    Adds the specified spacer immediately before the space in front of the cancel button.
    @param spacer The spacer to add
    @return The index at which the button was added
    @since 2.0
    */
    public int addNavigationSpacer(final SpacerWidget spacer) {
        validateState();
        addNavigationSpacer(spacer, nextNdx);
        return nextNdx;
    }

    /**
    Adds the specified spacer to the navigation bar at the specified index.
    @param spacer The spacer to add
    @param index The index within the navigation bar to add the spacer
    @since 2.0
    */
    public void addNavigationSpacer(final SpacerWidget spacer, final int index) {
        addNavigationComponent(spacer, index);
    }

    /**
    Creates a navigation bar with default accept and cancel buttons, and if a context-sensitive-help property exists, a help
    button.
    @since 2.0
    */
    protected void buildNavigationBar() {
        bar.setLayout(new BoxLayout(bar, BoxLayout.X_AXIS));
        bar.setBorder(BorderFactory.createEmptyBorder(BORDER.getBorderInsets(this).bottom, 0, 0, 0));
        if (helpButton != null){
            addNavigationButton(helpButton);
        }
        addNavigationSpacer(SpacerWidget.createHorizontalExpandableSpacer());
        for (final Iterator iter = getAcceptButtons().iterator();  iter.hasNext();){
            addNavigationButton((ButtonWidget)iter.next());
        }
        if (cancelButton != null){
            bar.add(SpacerWidget.createHorizontalSpacer());
            addNavigationButton(cancelButton, nextNdx + 1);
        }
        // Make all button widths match
        equalizeNavigationButtonWidths();
    }
    
    /**
     * @since 3.0
     */
    public boolean canAccept() {
        return canAccept;
    }
    
    /**
     * @since 3.0
     */
    public boolean canCancel() {
        return canCancel;
    }

    /**
    Called when the panel's parent is a DialogWindow and the user activates the cancel button or cancels the dialog via the title
    bar close button (with the 'X' icon) or the title bar system menu 'Close' option.  Does nothing by default.
    @param event The WidgetActionEvent generated from activating the button or closing the window
    @since 2.0
    */
    protected void cancel(final WidgetActionEvent event) {
    }

    /**
    Creates an accept button with a default label (as determined by the ToolboxStandards class).
    @return The accept button
    @since 2.0
    */
    protected ButtonWidget createAcceptButton() {
        return WidgetFactory.createButton(ACCEPT_BUTTON);
    }

    /**
    Creates an cancel button with a default label (as determined by the ToolboxStandards class).
    @return The cancel button
    @since 2.0
    */
    protected ButtonWidget createCancelButton() {
        return WidgetFactory.createButton(CANCEL_BUTTON);
    }

    /**
    Creates a help button with a default label (as determined by the ToolboxStandards class).
    @return The help button
    @since 2.0
    */
    protected ButtonWidget createHelpButton() {
        Container cntr = this;
        while (cntr != null) {
            if (cntr instanceof JComponent  &&  ((JComponent)cntr).getClientProperty(CONTEXT_SENSITIVE_HELP) != null) {
                return WidgetFactory.createButton(HELP_BUTTON);
            }
            cntr = cntr.getParent();
        }
        return null;
    }

    /**
    Creates a navigation bar that will contain all buttons that apply to the entire panel.
    @since 2.0
    */
    protected JComponent createNavigationBar() {
        final JPanel bar = new JPanel();
        bar.setLayout(new BoxLayout(bar, BoxLayout.X_AXIS));
        return bar;
    }

    /**
    Sets the size of all navigation buttons to the size of the largest navigation button.
    @since 2.0
    */
    protected void equalizeNavigationButtonWidths() {
        final Dimension maxSize = new Dimension();
        Component comp;
        Dimension size;
        ButtonWidget button;
        for (int ndx = bar.getComponentCount();  --ndx >= 0;) {
            comp = bar.getComponent(ndx);
            if (comp instanceof ButtonWidget) {
                button = (ButtonWidget)comp;
                if (button.isPreferredSizeSet()) {
                    button.setPreferredSize(null);
                }
                size = button.getPreferredSize();
                maxSize.width = Math.max(maxSize.width, size.width);
                maxSize.height = Math.max(maxSize.height, size.height);
            }
        }
        for (int ndx = bar.getComponentCount();  --ndx >= 0;) {
            comp = bar.getComponent(ndx);
            if (comp instanceof ButtonWidget) {
                button = (ButtonWidget)comp;
                button.setPreferredSize(maxSize);
                button.setMinimumSize(maxSize);
                button.setMaximumSize(maxSize);
            }
        }
    }

    /**
    @return The first accept button
    @since 2.0
    */
    public ButtonWidget getAcceptButton() {
        final List acceptButtons = getAcceptButtons();
        if (acceptButtons.isEmpty()) {
            return null;
        }
        return (ButtonWidget)acceptButtons.get(0);
    }

    /**
     * @return The modifiable list of accept buttons; never null
     * @since 3.1
     */
    public List getAcceptButtons() {
        if (this.acceptButtons == null) {
            this.acceptButtons = new ArrayList(0);
        }
        return this.acceptButtons;
    }

    /**
    @return The cancel button
    @since 2.0
    */
    public ButtonWidget getCancelButton() {
        return cancelButton;
    }

    /**
    @return The content component
    @since 2.0
    */
    public Component getContent() {
       return content;
    }

    /**
    @return The content component container
    @since 2.0
    */
    public Container getContentContainer() {
       return contentPanel;
    }

    /**
    @return The help button
    @since 2.0
    */
    public ButtonWidget getHelpButton() {
        return helpButton;
    }

    /**
    Retrieves the navigation bar.
    @return The navigation bar
    @since 2.0
    */
    public JComponent getNavigationBar() {
        return bar;
    }

    /**
    Retrieves the button in the navigation bar with the specified text.
    @param text The button text
    @return The button if found, null otherwise
    @since 2.0
    */
    public ButtonWidget getNavigationButton(final String text) {
        validateState();
        Component comp;
        ButtonWidget button;
        for (int ndx = bar.getComponentCount();  --ndx >= 0;) {
            comp = bar.getComponent(ndx);
            if (!(comp instanceof ButtonWidget)) {
                continue;
            }
            button = (ButtonWidget)comp;
            if (button.getText().equals(text)) {
                return button;
            }
        }
        return null;
    }

    /**
    Retrieves the component in the navigation bar (either a {@link ButtonWidget button} or {@link SpacerWidget spacer}) at the
    specified index.
    @param index The index within the navigation bar of the component
    @return The component
    @since 2.0
    */
    public Component getNavigationComponent(final int index) {
        validateState();
        return bar.getComponent(index);
    }

    /**
    Retrieves the index of the specified component in the navigation bar.
    @param component The component
    @return The index of the component if found, -1 otherwise
    @since 2.0
    */
    public int getNavigationComponentIndex(final Component component) {
        validateState();
        for (int ndx = bar.getComponentCount();  --ndx >= 0;) {
            if (bar.getComponent(ndx) == component) {
                return ndx;
            }
        }
        return -1;
    }

    /**
    @return The last selected button, or null if none were selected.
    @since 2.0
    */
    public ButtonWidget getSelectedButton() {
       return selectedButton;
    }

    /**
     * Returns the listener used to set the selectedButton property.
     * @return The selection listener.
	 * @since 3.1
	 */
	ActionListener getSelectionListener() {
        return this.selectionListener;
	}

    /**
    Gets the Window ancestor of this panel
    @return The Window
    @since 2.0
    */
    public Window getWindowAncestor() {
        Component parent = getParent();
        while (parent != null) {
            if (parent instanceof Window) {
                return (Window)parent;
            }
            parent = parent.getParent();
        }
        return null;
    }

    /**
    Initializes the panel:
    <ul>
    <li>Adds a content panel to the center</li>
    <li>Adds the navigation bar to the bottom, initialized with default next and cancel buttons.</li>
    </ul>
    @since 2.0
    */
    protected void initializeDialogPanel() {
        canCancel = canAccept = true;
        // Customize the panel provide spacing in its border and between areas
        setBorder(BORDER);
        final Insets insets = BORDER.getBorderInsets(this);
        setLayout(new BorderLayout(insets.top, insets.left));
        // Add a content panel to the center of the DialogPanel.  Adding this panel cauese Swing to give focus to the first
        // component in the user provided content even if that content is not provided until after the DialogPanel is constructed.
        contentPanel = new JPanel(new BorderLayout());
        add(contentPanel, BorderLayout.CENTER);
        // If provided, add the content to the content panel
        if (content != null) {
            contentPanel.add(content, BorderLayout.CENTER);
        }
        // Build and add navigation button bar
        final ButtonWidget acceptButton = createAcceptButton();
        if (acceptButton != null) {
			getAcceptButtons().add(acceptButton);
        }
        cancelButton = createCancelButton();
        helpButton = createHelpButton();
        bar = createNavigationBar();
        if (bar != null) {
            buildNavigationBar();
            contentPanel.add(bar, BorderLayout.SOUTH);
        }
        // Add listener to discover when the panel gains a Window ancestor
        addAncestorListener(new AncestorListener() {
            public void ancestorAdded(final AncestorEvent event) {
                wdw = getWindowAncestor();
                DialogPanel.this.windowAdded();
                if (wdw != null) {
                    // Add listener to call windowActivated method when window is activated
                    wdw.addWindowListener(new WindowAdapter() {
                        public void windowActivated(final WindowEvent event) {
                            DialogPanel.this.windowActivated();
                        }
                    });
                }
            }
            public void ancestorMoved(final AncestorEvent event) {
            }
            public void ancestorRemoved(final AncestorEvent event) {
            }
        });
        // Add property change listener to listen for setting of context-sensitive help
        addPropertyChangeListener(new java.beans.PropertyChangeListener() {
            public void propertyChange(final java.beans.PropertyChangeEvent event) {
                if (!event.getPropertyName().equals(CONTEXT_SENSITIVE_HELP)) {
                    return;
                }
                if (helpButton == null  &&  Boolean.TRUE.equals(event.getNewValue())) {
                    addHelpButton(createHelpButton());
                } else if (helpButton != null  &&  !Boolean.TRUE.equals(event.getNewValue())) {
                    removeNavigationButton(helpButton);
                }
            }
        });
    }

    /**
    Removes the specified button from the navigation bar.
    @param button The button to remove
    @return The index from which the button was removed if found, -1 otherwise
    @since 2.0
    */
    public int removeNavigationButton(final ButtonWidget button) {
        validateState();
        if (button == null) {
            return -1;
        }
        final int ndx = getNavigationComponentIndex(button);
        if (ndx < nextNdx) {
            --nextNdx;
        }
        bar.remove(button);
        return ndx;
    }

    /**
    Removes the component (either a {@link ButtonWidget button} or {@link SpacerWidget spacer}) at the specified index from the
    navigation bar.
    @param index The index within the navigation bar of the component
    @return The component removed
    @since 2.0
    */
    public Component removeNavigationComponent(final int index) {
        validateState();
        final Component comp = bar.getComponent(index);
        if (index < nextNdx) {
            --nextNdx;
        }
        bar.remove(index);
        return comp;
    }

    /**
    Removes the specified spacer from the navigation bar.
    @param spacer The spacer to remove
    @since 2.0
    */
    public void removeNavigationSpacer(final SpacerWidget spacer) {
        validateState();
        final int ndx = getNavigationComponentIndex(spacer);
        if (ndx < nextNdx) {
            --nextNdx;
        }
        bar.remove(spacer);
    }

    /**
     * @since 3.0
     */
    protected void setCanAccept(final boolean canAccept) {
        this.canAccept = canAccept;
    }

    /**
     * @since 3.0
     */
    protected void setCanCancel(final boolean canCancel) {
        this.canCancel = canCancel;
    }
    
    /**
    Sets the specified component as the main content to be displayed in the CENTER region of the panel.
    @param content The component
    @since 2.0
    */
    public void setContent(final Component content) {
        Container south = null;
        if (bar != null) {
            south = bar;
            while (south != null  &&  south.getParent() != contentPanel) {
                south = south.getParent();
            }
            if (south != null) {
	            contentPanel.remove(south);
            }
        }
        if (this.content != null) {
            contentPanel.remove(this.content);
        }
        if (content != null) {
            contentPanel.add(content, BorderLayout.CENTER);
        }
        if (south != null) {
            contentPanel.add(south, BorderLayout.SOUTH);
        }
        this.content = content;
    }

    /**
    Overridden to ensure this class uses a BorderLayout
    @see Container#setLayout(LayoutManager)
    @since 2.0
    */
    public void setLayout(final LayoutManager layout) {
        if (this.layout != null  &&  !(layout instanceof BorderLayout)) {
            throw new IllegalArgumentException("Parameter must be an instance of BorderLayout.");
        }
        super.setLayout(layout);
        this.layout = layout;
    }

    /**
    Overridden to clear the selected button each time the panel is made visible.
    @since 2.0
    */
    public void setVisible(final boolean visible) {
        if (!isVisible()  &&  visible) {
            selectedButton = null;
        }
        super.setVisible(visible);
    }

    /**
    Ensures that the navigation bar exists in the panel.
    @since 2.0
    */
    protected void validateState() {
        if (bar == null) {
            throw new IllegalComponentStateException("Navigation bar doesn't exist");
        }
    }
    
    /**
    To be overridden by subclasses, allowing you to refresh your dialog panel when the focus on the parent
    window is lost and then reactivated.
    @since 2.0
    */
    protected void windowActivated() {
    }

    /**
    To be overridden by subclasses, allowing you to refresh your dialog panel when the focus on the parent
    window is added.
    @since 2.0
    */
    protected void windowAdded() {
    }
}
