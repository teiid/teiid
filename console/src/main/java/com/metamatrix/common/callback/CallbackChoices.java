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

package com.metamatrix.common.callback;

import com.metamatrix.core.util.Assertion;
import com.metamatrix.toolbox.ToolboxPlugin;
/**
 * This callback class is used when a component requires an application to
 * ask for YES/NO, OK/CANCEL, YES/NO/CANCEL or other similar confirmations
 */
public class CallbackChoices {

    //
    // Design Note: the constant values for Option, Return, and Message types below were
    // chosen to be consistent with javax.swing.JOptionPane.
    //

    //
    // Option Types.  Used by the CallbackHandler to determine the options
    // available for return choices.
    //
    /** Type used to provied Yes or No choice options. */
    public static final int UNSPECIFIED_OPTION = -1;
    /** Type used to provied Yes or No choice options. */
    public static final int YES_NO_OPTION = 0;
    /** Type used to provied Yes or No choice options. */
    public static final int YES_NO_CANCEL_OPTION = 1;
    /** Type used to provied Yes or No choice options. */
    public static final int OK_CANCEL_OPTION = 2;
    /** Internal type for detecting the use of String[] options*/
    private static final int STRING_ARRAY_OPTIONS = 3;

    //
    // Response values.
    //
    /** Return value form class method if this callback is simply dismissed. */
    public static final int DISMISSED = -1;
    /** Return value from class method if YES is chosen. */
    public static final int YES = 0;
    /** Return value from class method if NO is chosen. */
    public static final int NO = 1;
    /** Return value from class method if CANCEL is chosen. */
    public static final int CANCEL = 2;
    /** Return value form class method if OK is chosen. */
    public static final int OK = 0;

    //
    // Message types. Used by the CallbackHandler to determine what type of message
    // is being handled and possibly what behavior to give based on the type.
    //
    /** Used for generic message. */
    public static final int UNSPECIFIED_MESSAGE = -1;
    /** Used for error messages. */
    public static final int ERROR_MESSAGE = 0;
    /** Used for information messages. */
    public static final int INFORMATION_MESSAGE = 1;
    /** Used for warning messages. */
    public static final int WARNING_MESSAGE = 2;
    /** Used for questions. */
    public static final int QUESTION_MESSAGE = 3;


    private String prompt;
    private int optionType;
    private int messageType;
    private int defaultOption;
    private String[] options;
    private int selection;

    /**
     * Construct a CallbackChoices with a message type, an option type and a default option.
     * <p>
     * This is used if either a YES/NO, YES/NO/CANCEL or OK/CANCEL confirmation is required.
     * @param prompt the prompt used to request the information
     * @param messageType the message type (INFORMATION_MESSAGE, WARNING_MESSAGE or ERROR_MESSAGE).
     * @param optionType the option type (YES_NO_OPTION, YES_NO_CANCEL_OPTION or OK_CANCEL_OPTION).
     * @param defaultOption the default option from the provided optionType (YES, NO, CANCEL or OK).
     */
    public CallbackChoices(String prompt, int messageType, int optionType, int defaultOption) {
        boolean toAssert = messageType == INFORMATION_MESSAGE || messageType == WARNING_MESSAGE || messageType == ERROR_MESSAGE || messageType == QUESTION_MESSAGE ;
        if(!toAssert){
            Assertion.assertTrue(toAssert, ToolboxPlugin.Util.getString("ERR.003.009.0001"));
        }
        if(prompt == null){
            Assertion.isNotNull(prompt, ToolboxPlugin.Util.getString("ERR.003.009.0002"));
        }
        if(prompt.length() == 0){
            Assertion.isNotZeroLength(prompt, ToolboxPlugin.Util.getString("ERR.003.009.0003"));
        }
        toAssert = optionType == YES_NO_OPTION || optionType == YES_NO_CANCEL_OPTION || optionType == OK_CANCEL_OPTION || optionType == UNSPECIFIED_OPTION;
        if(!toAssert){
            Assertion.assertTrue(toAssert, ToolboxPlugin.Util.getString("ERR.003.009.0004"));
        }
        toAssert = defaultOption == YES || defaultOption == NO || defaultOption == CANCEL || defaultOption == OK;
        if(!toAssert){
            Assertion.assertTrue(toAssert,  ToolboxPlugin.Util.getString("ERR.003.009.0005"));
        }

        this.prompt = prompt;
        this.messageType = messageType;
        this.optionType = optionType;
        this.options = null;
        this.defaultOption = defaultOption;
    }

    /**
     * Construct a CallbackChoices with a message type, a list of options and a default option.
     * <p>
     * This is used if either a YES/NO, YES/NO/CANCEL or OK/CANCEL confirmation
     * different from the available preset confirmations provided (for example, CONTINUE/ABORT or STOP/GOis required.
     * The confirmation options are listed in the options array, and are displayed by the
     * CallbackHandler implementation in a manner consistent with the way preset options are displayed.
     * @param prompt the prompt used to request the information
     * @param messageType the message type (INFORMATION_MESSAGE, WARNING_MESSAGE or ERROR_MESSAGE).
     * @param options the list of confirmation options.
     * @param defaultOption the default option, represented as an index into the options array.
     */
    public CallbackChoices(String prompt, int messageType, String[] options, int defaultOption) {
        boolean toAssert = messageType == INFORMATION_MESSAGE || messageType == WARNING_MESSAGE || messageType == ERROR_MESSAGE || messageType == QUESTION_MESSAGE ;
        if(!toAssert){
            Assertion.assertTrue(toAssert, ToolboxPlugin.Util.getString("ERR.003.009.0001"));
        }
        if(prompt == null){
            Assertion.isNotNull(prompt, ToolboxPlugin.Util.getString("ERR.003.009.0002"));
        }
        if(prompt.length() == 0){
            Assertion.isNotZeroLength(prompt, ToolboxPlugin.Util.getString("ERR.003.009.0003"));
        }
        if(options == null){
            Assertion.isNotNull(options, ToolboxPlugin.Util.getString("ERR.003.009.0006"));
        }
        if(options.length == 0){
            Assertion.assertTrue( (options.length != 0) , ToolboxPlugin.Util.getString("ERR.003.009.0007"));
        }
        for (int i=0; i!=options.length; ++i ) {
            String option = options[i];
            Assertion.isNotNull( option, ToolboxPlugin.Util.getString("ERR.003.009.0008", String.valueOf(i)));
            Assertion.isNotZeroLength( option, ToolboxPlugin.Util.getString("ERR.003.009.0009", String.valueOf(i)));
        }

        this.prompt = prompt;
        this.messageType = messageType;
        this.options = options;
        this.optionType = STRING_ARRAY_OPTIONS;
        this.defaultOption = defaultOption;
        if(isOutOfBounds(defaultOption) ){
            Assertion.assertTrue( ! isOutOfBounds(defaultOption) , ToolboxPlugin.Util.getString("ERR.003.009.0010",
        		new Object[] {String.valueOf(defaultOption), String.valueOf(options.length)} ));
        }
    }

    /**
     * Get the prompt.
     * @return the prompt used to request the information; never null or zero-length
     */
    public String getPrompt() {
        return this.prompt;
    }

    /**
     * Get the message type.
     * @return the message type (INFORMATION, WARNING or ERROR).
     */
    public int getMessageType() {
        return this.messageType;
    }
    /**
     * Get the option type.
     * <p>
     * If this method returns UNSPECIFIED_OPTION, then this ConfirmationCallback
     * was instantiated with options instead of an optionType. In this case,
     * invoke the getOptions method to determine which confirmation options to display
     * @return the option type (YES_NO_OPTION, YES_NO_CANCEL_OPTION or OK_CANCEL_OPTION),
     * or UNSPECIFIED_OPTION if this ConfirmationCallback was instantiated
     * with options instead of an optionType.
     */
    public int getOptionType() {
        return this.optionType;
    }

    /**
     * Get the confirmation options.
     * @return the list of confirmation options, or null if this ConfirmationCallback
     * was instantiated with an optionType instead of options
     */
    public String[] getOptions() {
        return this.options;
    }

    /**
     * Get the default option.
     * @return the default option, represented as YES, NO, OK or CANCEL if an
     * optionType was specified to the constructor of this ConfirmationCallback.
     * Otherwise, this method returns the default option as an index into the
     * options array specified to the constructor of this ConfirmationCallback
     */
    public int getDefaultOption() {
        return this.defaultOption;
    }

    /**
     * Set the selected confirmation option.
     * @param selection the selection represented as YES, NO, OK or CANCEL if
     * an optionType was specified to the constructor of this ConfirmationCallback.
     * Otherwise, the selection represents the index into the options array
     * specified to the constructor of this ConfirmationCallback
     */
    public void setSelectedIndex(int selection) {
        if ( optionType == STRING_ARRAY_OPTIONS ) {
            if(isOutOfBounds(selection) ){
                Assertion.assertTrue( ! isOutOfBounds(selection) ,  ToolboxPlugin.Util.getString("ERR.003.009.0011",
        		  new Object[] {String.valueOf(selection), String.valueOf(options.length)}));
            }

        } else {
            if(isOutOfBounds(selection) ){
                Assertion.assertTrue( ! isOutOfBounds(selection) , ToolboxPlugin.Util.getString("ERR.003.009.0012", selection));
            }

        }
        this.selection = selection;

        if ( selection == DISMISSED && optionType != STRING_ARRAY_OPTIONS ) {
            // convert DISMISSED to an expected option type
            if ( optionType == YES_NO_CANCEL_OPTION || optionType == OK_CANCEL_OPTION ) {
                this.selection = CANCEL;
            } else if ( optionType == YES_NO_OPTION ) {
                this.selection = NO;
            }
        }
    }

    /**
     * Get the selected confirmation option.
     * @return the selected confirmation option represented as YES, NO, OK or CANCEL
     * if an optionType was specified to the constructor of this ConfirmationCallback.
     * Otherwise, this method returns the selected confirmation option as an index
     * into the options array specified to the constructor of this ConfirmationCallback.
     */
    public int getSelectedIndex() {
        return this.selection;
    }

    protected boolean isOutOfBounds( int choice ) {
        if ( optionType == STRING_ARRAY_OPTIONS ) {
            if ( choice < 0 || choice >= options.length || choice == DISMISSED ) {
                return true;
            }
            return false;
        } else if ( optionType == YES_NO_OPTION && ( choice == YES || choice == NO || choice == DISMISSED ) ) {
            return false;
        } else if ( optionType == YES_NO_CANCEL_OPTION && ( choice == YES || choice == NO || choice == CANCEL || choice == DISMISSED ) ) {
            return false;
        } else if ( optionType == OK_CANCEL_OPTION && ( choice == OK || choice == CANCEL || choice == DISMISSED ) ) {
            return false;
        } else if ( optionType == UNSPECIFIED_MESSAGE ) {
            return false;
        }
        return true;
    }
}

