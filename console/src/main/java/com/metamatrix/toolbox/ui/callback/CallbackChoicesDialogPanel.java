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

package com.metamatrix.toolbox.ui.callback;

import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;

import com.metamatrix.common.callback.Callback;
import com.metamatrix.common.callback.CallbackChoices;

import com.metamatrix.toolbox.ui.widget.ButtonWidget;
import com.metamatrix.toolbox.ui.widget.DialogPanel;
import com.metamatrix.toolbox.ui.widget.DialogWindow;
import com.metamatrix.toolbox.ui.widget.event.WidgetActionEvent;


/**
 * CallbackChoicesDialogPanel is a specialization of DialogPanel that automatically
 * configures the DialogPanel's Accept, Cancel, and navigation buttons to the
 * CallbackChoices options, and sets the appropriate response on the callback.
 *
 * When CallbackChoicesDialogPanel is used to handle a Callback, there is no need
 * to capture the selected button in the panel and set the callback response.
 *
 * The content component for this panel must be built and set by the callback handler,
 * just like normal DialogPanel instances.
 */
public class CallbackChoicesDialogPanel extends DialogPanel {

    private Callback callback;
    private boolean responseSet = false;

    /**
     * Construct a CallbackChoicesDialogPanel to configure it's buttons to the callback's choice options.
     */
    public CallbackChoicesDialogPanel(Callback callback) {
        super();
        this.callback = callback;
        configureCallbackChoices();
    }

    private void configureCallbackChoices() {
        CallbackChoices choice = callback.getChoices();
        if ( choice.getOptionType() == CallbackChoices.OK_CANCEL_OPTION ) {
            // default dialog panels work here
        } else if ( choice.getOptionType() == CallbackChoices.YES_NO_OPTION ) {
            getAcceptButton().setText("Yes");
            getCancelButton().setText("No");
        } else if ( choice.getOptionType() == CallbackChoices.YES_NO_CANCEL_OPTION ) {
            getAcceptButton().setText("Yes");
            createButton("No", CallbackChoices.NO, 1);
        } else if ( choice.getOptionType() == CallbackChoices.UNSPECIFIED_OPTION ) {
            getAcceptButton().setVisible(false);
        } else if ( choice.getOptions() != null ) {
            getAcceptButton().setVisible(false);
            getCancelButton().setVisible(false);
            for ( int i=0 ; i<choice.getOptions().length ; ++i ) {
                createButton(choice.getOptions()[i], i);
            }
        }
    }

    private void createButton(final String optionName, final int choice) {
        ButtonWidget button = new ButtonWidget(optionName);
        button.addActionListener(new ActionListener() {
            public void actionPerformed(ActionEvent e) {
                setChoice(choice);
                DialogWindow.disposeConditionally(getWindowAncestor(), e);
            }
        });
        addNavigationButton(button);
    }

    private void createButton(final String optionName, final int choice, int navIndex) {
        ButtonWidget button = new ButtonWidget(optionName);
        button.addActionListener(new ActionListener() {
            public void actionPerformed(ActionEvent e) {
                setChoice(choice);
                DialogWindow.disposeConditionally(getWindowAncestor(), e);
            }
        });
        addNavigationButton(button, navIndex);
    }


    protected void accept(final WidgetActionEvent event) {
        if ( ! responseSet ) {
            CallbackChoices choice = callback.getChoices();
            if ( choice.getOptionType() == CallbackChoices.OK_CANCEL_OPTION ) {
                setChoice(CallbackChoices.OK);
            } else if ( choice.getOptionType() == CallbackChoices.YES_NO_OPTION ) {
                setChoice(CallbackChoices.YES);
            } else if ( choice.getOptionType() == CallbackChoices.YES_NO_CANCEL_OPTION ) {
                setChoice(CallbackChoices.YES);
            } else if ( choice.getOptionType() == CallbackChoices.UNSPECIFIED_OPTION ) {
                setChoice(CallbackChoices.OK);
            }
        }
        responseSet = true;
    }

    protected void setChoice(int index) {
        if ( ! responseSet ) {
            callback.getChoices().setSelectedIndex(index);
            callback.setResponse(index);
            responseSet = true;
        }
    }

    protected void cancel(final WidgetActionEvent event) {
        if ( ! responseSet ) {
            CallbackChoices choice = callback.getChoices();
            if ( choice.getOptionType() == CallbackChoices.OK_CANCEL_OPTION ) {
                setChoice(CallbackChoices.CANCEL);
            } else if ( choice.getOptionType() == CallbackChoices.YES_NO_OPTION ) {
                setChoice(CallbackChoices.NO);
            } else if ( choice.getOptionType() == CallbackChoices.YES_NO_CANCEL_OPTION ) {
                setChoice(CallbackChoices.CANCEL);
            } else {
                setChoice(CallbackChoices.DISMISSED);
            }
        }
        responseSet = true;
    }

}

