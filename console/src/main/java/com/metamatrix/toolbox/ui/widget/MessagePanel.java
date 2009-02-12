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
package com.metamatrix.toolbox.ui.widget;

// System imports
import java.awt.BorderLayout;
import java.awt.Component;
import java.awt.Container;
import java.awt.Window;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.util.StringTokenizer;

import javax.swing.BorderFactory;
import javax.swing.BoxLayout;
import javax.swing.Icon;
import javax.swing.JComponent;
import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.JViewport;
import javax.swing.event.AncestorEvent;
import javax.swing.event.AncestorListener;

import com.metamatrix.toolbox.ui.MessageStandards;
import com.metamatrix.toolbox.ui.UIDefaults;

/**
This class is intended to be used everywhere within the application that a message panel needs to be displayed.
@since Golden Gate
@author John P. A. Verhaeg
@version Golden Gate
*/
public class MessagePanel extends DialogPanel {
    //############################################################################################################################
    //# Constants                                                                                                                #
    //############################################################################################################################
    
    public static final String BUTTON_TYPES_PROPERTY_PREFIX = "MessagePanel.buttonTypes.";

    public static final String TYPE_WARNING = "warning";
    public static final String TYPE_NOTIFICATION = "notification";
    public static final String TYPE_ERROR = "error";
    public static final String TYPE_CONFIRMATION = "confirmation";
    public static final String TYPE_CANCELLABLE_QUESTION = "cancellable_question";
    public static final String TYPE_QUESTION = "question";

    public static final String CONFIRMATION_TITLE   = "Confirm";
    public static final String ERROR_TITLE          = "Error";
    public static final String NOTIFICATION_TITLE   = "Message";
    public static final String QUESTION_TITLE       = "Question";
    public static final String WARNING_TITLE        = "Warning";
    
    //############################################################################################################################
    //# Instance Variables                                                                                                       #
    //############################################################################################################################

    /*
    the resource bundle key which maps to the message text, icon, and buttons for this MessagePanel
    */
    private String id;

    /*
    Specifies the message type, which in turn maps to an icon and buttons, via the ToolboxStandards class
    */
    private String type;

    /*
    Message text- can contain newlines.  We put this in a LabelWidget.
    */
    private String msg;

    /*
    Icon for the panel.
    */
    private Icon icon;

    /*
    Message details go in here.  If one is specified (through the appropriate constructor), its appearance on the screen
    will be toggled via a details button.
    */
    private JComponent detailComp;

    /*
    Toggles the appearance of the detail component on the screen.  Null unless a detail component is specified.
    */
    private ButtonWidget detailButton = null;

    /*
    size of insets for borders for some of the containers in the MessagePanel
    */
    private final int INSET_PIXELS = 5;

    //############################################################################################################################
    //# Constructors                                                                                                             #
    //############################################################################################################################

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    Creates a message panel containing the message identified by the specified ID.  The message text, icon, and buttons displayed
    are obtained from a resource bundle using <code>id</code> as a key.
    @param id The ID of the message information within the resource bundle
    @since Golden Gate
    */
    public MessagePanel(final String id) {
        this.id = id;
        initializeMessagePanel();
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    Creates a message panel containing the specified message.  The message icon and buttons displayed are obtained from the
    ToolboxStandards using the specified message type.
    @param type     The message type
    @param message  The message text, which may include multiple lines delimited by line-feeds ('\n')
    @since Golden Gate
    */
    public MessagePanel(final String type, final String message) {
        this.type = type;
        msg = message;
        initializeMessagePanel();
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    Creates a message panel containing the specified icon and message.
    @param icon     The message icon
    @param message  The message text, which may include multiple lines delimited by line-feeds ('\n')
    @since Golden Gate
    */
    public MessagePanel(final Icon icon, final String message) {
        this.icon = icon;
        msg = message;
        initializeMessagePanel();
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    Creates a message panel containing the message identified by the specified ID.  The message text, icon, and buttons displayed
    are obtained from a resource bundle using <code>id</code> as a key.  A 'detail' toggle button will also be present that
    displays the specified component between the current content and the navigation bar when selected.
    @param id           The ID of the message information within the resource bundle
    @param component    The detail component
    @since Golden Gate
    */
    public MessagePanel(final String id, final JComponent component) {
        this.id = id;
        this.detailComp = component;
        initializeMessagePanel();
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    Creates a message panel containing the specified message.  The message icon and buttons displayed are obtained from the
    ToolboxStandards using the specified message type.  A 'detail' toggle button will also be present that displays the specified
    component between the current content and the navigation bar when selected.
    @param type         The message type
    @param message      The message text, which may include multiple lines delimited by line-feeds ('\n')
    @param component    The detail component
    @since Golden Gate
    */
    public MessagePanel(final String type, final String message, final JComponent component) {
        this.type = type;
        this.msg = message;
        this.detailComp = component;
        initializeMessagePanel();
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    Creates a message panel containing the specified icon and message.  A 'detail' toggle button will also be present that
    displays the specified component between the current content and the navigation bar when selected.
    @param icon         The message icon
    @param message      The message text, which may include multiple lines delimited by line-feeds ('\n')
    @param component    The detail component
    @since Golden Gate
    */
    public MessagePanel(final Icon icon, final String message, final JComponent component) {
        this.icon = icon;
        this.msg = message;
        this.detailComp = component;
        initializeMessagePanel();
    }
    //############################################################################################################################
    //# Instance Methods                                                                                                         #
    //############################################################################################################################


    public ButtonWidget createAcceptButton(){
        return null;
    }

    public ButtonWidget createCancelButton(){
        return null;
    }


    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    Creates a detail button with a default label (as determined by the ToolboxStandards class).
    @return The detail button
    @since Golden Gate
    */
    protected ButtonWidget createDetailButton() {
        final ButtonWidget detailButton = WidgetFactory.createButton(DETAILS_BUTTON);
        return detailButton;
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**

    <p>
    Returns the detail button for this MessagePanel, or null.  The MessagePanel will
    have a detail button so long as there is a message detail available. (see @link isDetailAvailable)

    @return The detail button, if available, else null.
    @since Golden Gate
    */
    public ButtonWidget getDetailButton() {
        return detailButton;
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    Returns the icon for this message panel, if any.

    @return The message icon, if available, else null.
    @since Golden Gate
    */
    public Icon getIcon() {
        return icon;
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @return The message ID, if available, else null.
    @since Golden Gate
    */
    public String getID() {
        return id;
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @return The message type, if available, else null.
    @since Golden Gate
    */
    public String getType() {
        return type;
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    <ul>
    <li>Calls initializeDialogPanel</li>
    <li>Determines the icon and buttons from the type or id</li>
    <li>Adds the icon to the WEST region</li>
    <li>Modifies existing navigation bar buttons & adds additional buttons as determined by type or id</li>
    <li>Adds a panel to the CENTER region with a layout that allows for an easily created vertical list of components with the
    same height</li>
    <li>Creates a LabelWidget containing the message ID, followed by a blank line, then each line of the message</li>
    <li>Adds the LabelWidget to the panel mentioned above</li>
    <li>If detail component present, adds detail toggle button to navigation bar, along with a listener to show/hide the detail
    component and change the button text as appropriate</li>
    </ul>
    @since Golden Gate
    */
    protected void initializeMessagePanel() {
        // If ID specified, info from resource bundle may contain key used to obtain icon and buttons from ToolboxStandards

        //it doesn't need to call this, I don't think.  We call super(), which calls that, right?
        //initializeDialogPanel();

        //at this point, we can have exactly one of three things:
        //an ID, a type, or an icon.

        final JPanel contentPanel= new JPanel();
        contentPanel.setLayout(new BorderLayout());
        contentPanel.setBorder(BorderFactory.createEmptyBorder(5,5,5,5));

        this.addNavigationSpacer(SpacerWidget.createHorizontalExpandableSpacer());

        //if we have an ID, use it to find a type, and then look stuff up in ToolboxStandards with that
        if(id != null){
            //uh, yeah, do stuff

            type = MessageStandards.getType(id);
            msg  = MessageStandards.getMessage(id);


        }

        if(type != null){
            //find the associated icon and buttons

            final UIDefaults dflts = UIDefaults.getInstance();
            icon = dflts.getIcon(type);

            String listOfButtonTypes = dflts.getString(BUTTON_TYPES_PROPERTY_PREFIX + type);

            StringTokenizer tokenizer = new StringTokenizer(listOfButtonTypes, ",");
            String latestType;
            while(tokenizer.hasMoreTokens()){
                latestType= tokenizer.nextToken().trim();

                final ButtonWidget button = WidgetFactory.createButton(latestType);

                //we'll assume that the type we got was valid, and that the
                //things we got back are OK, too.

                //the action for all of these buttons is to dispose of the window they're in
                if(button != null){
                    button.addActionListener(new ActionListener(){
                        public void actionPerformed(ActionEvent e){
                            Container parent=button;
                            do{
                                parent= parent.getParent();
                            }while( ! (parent instanceof Window) );


                            ((Window)parent).dispose();
                        }
                    });

                    addNavigationSpacer(SpacerWidget.createHorizontalSpacer());
                    addNavigationButton(button);
                }

            }


        }

        if(icon != null){
            JPanel iconPanel= new JPanel();
            iconPanel.setBorder(BorderFactory.createEmptyBorder(INSET_PIXELS,INSET_PIXELS,INSET_PIXELS,INSET_PIXELS));
            iconPanel.add(new JLabel(icon));
            contentPanel.add(iconPanel, BorderLayout.WEST);

        }

        JPanel messagePanel = new JPanel();
        messagePanel.setBorder(BorderFactory.createEmptyBorder(INSET_PIXELS,INSET_PIXELS,INSET_PIXELS,INSET_PIXELS));
        messagePanel.setLayout(new BoxLayout(messagePanel, BoxLayout.Y_AXIS));


        if(msg != null){
            LabelWidget message = new LabelWidget(msg);
            messagePanel.add(message);
        }

        final JPanel messageAndDetailsPanel = new JPanel();
        messageAndDetailsPanel.setLayout(new BorderLayout());

        messageAndDetailsPanel.add(messagePanel, BorderLayout.NORTH);

        if(detailComp != null){
            detailButton = createDetailButton();
            detailButton.addActionListener(new ActionListener(){
                public void actionPerformed(ActionEvent e){
                    if(detailComp.isVisible()){
                        detailComp.setVisible(false);
                    }else{
                        detailComp.setVisible(true);
                    }

                    messageAndDetailsPanel.revalidate();
                    messageAndDetailsPanel.repaint();

                }

            });

            messageAndDetailsPanel.add(detailComp, BorderLayout.SOUTH);
            detailComp.setVisible(false);

            detailComp.addAncestorListener(new WindowResizingAncestorListener());

            //add detail button to navigation bar
            addNavigationSpacer(SpacerWidget.createHorizontalSpacer());
            addNavigationButton(detailButton);
            addNavigationSpacer(SpacerWidget.createHorizontalSpacer());

        }//if(detailComp != null)

        contentPanel.add(messageAndDetailsPanel, BorderLayout.CENTER);
        setContent(contentPanel);


        //after we've had a chance to add all the buttons, put a spacer at the end to center the buttons
        this.addNavigationSpacer(SpacerWidget.createHorizontalExpandableSpacer());



        //and that's all

    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    Returns whether or not a detail component is available.
    @return True if message detail is available
    @since Golden Gate
    */
    public boolean isDetailAvailable() {
        return detailComp != null;
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since Golden Gate
    */    
    protected void setTitle(final DialogWindow dlg, final String title) {
        if (!title.equals(dlg.getTitle())) {
            dlg.setTitle(title);
        }
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since Golden Gate
    */
    protected void windowAdded() {
        if (type == null) {
            return;
        }
        final Window wdw = getWindowAncestor();
        if (wdw == null  ||  !(wdw instanceof DialogWindow)) {
            return;
        }
        final DialogWindow dlg = (DialogWindow)wdw;
        if (type.equals(TYPE_CONFIRMATION)) {
            setTitle(dlg, CONFIRMATION_TITLE);
        } else if (type.equals(TYPE_ERROR)) {
            setTitle(dlg, ERROR_TITLE);
        } else if (type.equals(TYPE_NOTIFICATION)) {
            setTitle(dlg, NOTIFICATION_TITLE);
        } else if (type.equals(TYPE_QUESTION)  ||  type.equals(TYPE_CANCELLABLE_QUESTION)) {
            setTitle(dlg, QUESTION_TITLE);
        } else if (type.equals(TYPE_WARNING)) {
            setTitle(dlg, WARNING_TITLE);
        }
    }

    /*
    This class is responsible for resizing whichever window this MessagePanel sits in,
    unless it has been added to a scrollbar-controlled area.  This resizing will occur when
    the details button, if any, is pressed.  The details button toggles the detail component's
    appearance on screen.

    */
    private static class WindowResizingAncestorListener implements AncestorListener{
        Window windowToResize;

        public WindowResizingAncestorListener(){
        }

        public void ancestorAdded(AncestorEvent e){
            //find the closest ancestor that is a window, provided we're not in a JViewPort
            //resize the window, or kick out if we're in a viewport
            Component target = e.getComponent();

            boolean done = false;
            do{
                target=target.getParent();

                if(target instanceof Window){
                    windowToResize= (Window)target;
                    done = true;
                }else if(target instanceof JViewport){
                    windowToResize = null;
                    done = true;                               //TODO: maybe just return here and handle the
                }                                              //pack/repaint stuff in the instanceof window block?
            }while(! done);

            if(windowToResize != null){
                windowToResize.pack();
                windowToResize.repaint();
            }

        }

        public void ancestorRemoved(AncestorEvent e){
             if(windowToResize != null){
                windowToResize.pack();
                windowToResize.repaint();
            }

        }

        public void ancestorMoved(AncestorEvent e){
            //do nothing
        }


    }//inner class
}
