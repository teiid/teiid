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

package com.metamatrix.api.core.message;

import java.util.StringTokenizer;

import com.metamatrix.common.CommonPlugin;
import com.metamatrix.common.util.ErrorMessageKeys;

/**
 * <p>
 * The default implementation of {@link Message}.  The presence of each of the three properties (text, type, or target) is
 * optional.  A few pre-defined {@link MessageTypes message types} have been defined for which messages can be created using one
 * of several static create methods.  In addition, a {@link #NULL_MESSAGE "null" message} may be created that does not allow for
 * any associated text or target.  Attempts to set either the text or a target on a null message will throw an
 * {@link IllegalStateException}.
 * </p><p>
 * <strong>Warnings:</strong>
 * </p><ul>
 * <li>Since this class's hash code is dependent upon modifiable properties, instances of this class should not be modified when
 * stored within a hashing collection.</li>
 * <li>Instances of this class are not thread-safe.
 *
 * @since     3.0
 */
public class DefaultMessage
implements Message, MessageTypes {
    //############################################################################################################################
    //# Constants                                                                                                                #
    //############################################################################################################################

    // Prime number used in improving distribution in hash code calculations
    private static final int PRIME = 1000003;

    //############################################################################################################################
    //# Static Methods                                                                                                           #
    //############################################################################################################################

    /**
     * Creates a new error message containing the specified text and no target.
     * @param text The message text
     * @return the error message; never null
     */
    public static Message createErrorMessage(final String text) {
        return createErrorMessage(text, null);
    }

    /**
     * Creates a new error message containing the specified text and target.
     * @param text		The message text
     * @param target	The message target
     * @return the error message; never null
     */
    public static Message createErrorMessage(final String text, final Object target) {
        return new DefaultMessage(text, ERROR_MESSAGE, target);
    }

    /**
     * Creates a new notification message containing the specified text and no target.
     * @param text The message text
     * @return the error message; never null
     */
    public static Message createNotificationMessage(final String text) {
        return createNotificationMessage(text, null);
    }

    /**
     * Creates a new notification message containing the specified text and target.
     * @param text		The message text
     * @param target	The message target
     * @return the error message; never null
     */
    public static Message createNotificationMessage(final String text, final Object target) {
        return new DefaultMessage(text, NOTIFICATION_MESSAGE, target);
    }

    /**
     * Creates a new warning message containing the specified text and no target.
     * @param text The message text
     * @return the error message; never null
     */
    public static Message createWarningMessage(final String text) {
        return createWarningMessage(text, null);
    }

    /**
     * Creates a new warning message containing the specified text and target.
     * @param text		The message text
     * @param target	The message target
     * @return the error message; never null
     */
    public static Message createWarningMessage(final String text, final Object target) {
        return new DefaultMessage(text, WARNING_MESSAGE, target);
    }

    //############################################################################################################################
    //# Instance Variables                                                                                                       #
    //############################################################################################################################

	private String text;
	private int type;
	private Object target;

	//############################################################################################################################
    //# Constructors                                                                                                             #
    //############################################################################################################################

    /**
     * Creates a new null message (with no text or target).
     * @since 3.0
     */
    public DefaultMessage() {
        this(null, NULL_MESSAGE);
    }

    /**
     * Creates a new notification message containing the specified text and no target.
     * @param text The message text
     * @since 3.0
     */
    public DefaultMessage(final String text) {
        this(text, NOTIFICATION_MESSAGE);
    }

    /**
     * Creates a new message of the specified type containing the specified text and no target.
     * @param text The message text
     * @param type The message type
     * @since 3.0
     */
    public DefaultMessage(final String text, final int type) {
        this(text, type, null);
    }

    /**
     * Creates a new message of the specified type containing the specified text and target.
     * @param text		The message text
     * @param type		The message type
     * @param target	The message target
     * @since 3.0
     */
    public DefaultMessage(final String text, final int type, final Object target) {
        constructDefaultMessage(text, type, target);
    }

    /**
     * Creates a new message using the properties of the specified message.
     * @param message An instance of Message
     * @since 3.0
     */
    public DefaultMessage(final Message message) {
        setMessage(message);
    }

    //############################################################################################################################
    //# Instance Methods                                                                                                         #
    //############################################################################################################################

    /**
     * Clears the content of this message, which simply sets the message type to be null
     * and clears the text and target.
     */
    public void clear() {
        setType(NULL_MESSAGE);
    }

    /**
     * Initializes all of this message's properties using the specified values.
     * @param text The message text
     * @param type The message type
     * @param target The message target
     */
    protected void constructDefaultMessage(final String text, final int type, final Object target) {
        set(text, type, target);
    }

    /**
     * Overridden to indicate messages are equal as long as their properties (text, type, and target) are equal.
     * @param object the Object to be compared to this object
     * @return true if the specified object is considered equivalent to this object, or false
     * otherwise
     */
    public boolean equals(final Object object) {
        if (object == null  ||  !(object instanceof Message)) {
            return false;
        }
        final Message msg = (Message)object;
        final String msgText = msg.getText();
        final String text = getText();
        return ((text == msgText  ||  (text != null  &&  text.equals(msgText)))  &&  getType() == msg.getType()
        		&&  getTarget() == msg.getTarget());
    }

    /**
     * Gets the target object to which this message applies.
     * @return The target object if set, otherwise null
     */
    public Object getTarget() {
        return target;
    }

    /**
     * Gets the text describing this message.
     * @return The message text if set, otherwise null
     */
    public String getText() {
        return text;
    }

    /**
     * Gets the message type.  This is a context-sensitive value, defined by the application or obtained from one of the built-in
     * {@link MessageTypes types}.
     * @return The message type if set, otherwise {@link MessageTypes#NULL_MESSAGE NULL_MESSAGE}
     */
    public int getType() {
        return type;
    }

    /**
     * Return the hash code for this object.  This method is overridden to return the same
     * hashCode for messages that are {@link #equals(Object) equal}.  <i>Note that since the hash code
     * is dependent upon modifiable properties, instances of this class should not be modified when stored within a hashing
     * collection.</i>
     * @return the hash code value
     */
    public int hashCode() {
        int code = (text == null) ? 0 : text.hashCode();
        code = (PRIME * code) + type;
        return (target == null) ? (PRIME * code) : (PRIME * code) + target.hashCode();
    }

    /**
     * Sets the message text to the specified value, the type to a notification message, and the target to null.
     * @param text The message text
     */
    public void set(final String text) {
        set(text, NOTIFICATION_MESSAGE);
    }

    /**
     * Sets the message text and type to the specified values, and the target to null.
     * @param text The message text
     * @param type The message type
     */
    public void set(final String text, final int type) {
        set(text, type, null);
    }

    /**
     * Sets all of this message's properties using the specified values.
     * @param text The message text
     * @param type The message type; may be any value, although use of {@link MessageTypes} constants
     * are encouraged.
     * @param target The message target
     */
    public void set(final String text, final int type, final Object target) {
        // Type should be set first to handle previous type of NULL_MESSAGE
        setType(type);
        setText(text);
        setTarget(target);
    }

    /**
     * Sets all of this message's properties using the property values from the specified message.
     * @param message An instance of Message
     */
    public void setMessage(final Message message) {
        if (message == null) {
            throw new NullPointerException(CommonPlugin.Util.getString(ErrorMessageKeys.API_ERR_0003));
        }
        set(message.getText(), message.getType(), message.getTarget());
    }

    /**
     * Sets the message target.
     * @param target The message target
     * @throws IllegalStateException If the message type is {@link MessageTypes#NULL_MESSAGE NULL_MESSAGE}.
     */
    public void setTarget(final Object target) {
        if (target != null  &&  type == NULL_MESSAGE) {
            throw new IllegalStateException(CommonPlugin.Util.getString(ErrorMessageKeys.API_ERR_0004));
        }
        this.target = target;
    }

    /**
     * Sets the message text.
     * @param text The message text
     * @throws IllegalStateException If the message type is {@link MessageTypes#NULL_MESSAGE NULL_MESSAGE}.
     */
    public void setText(final String text) {
        if (text != null  &&  type == NULL_MESSAGE) {
            throw new IllegalStateException(CommonPlugin.Util.getString(ErrorMessageKeys.API_ERR_0005));
        }


        this.text = removeEscapeSequences(text);
    }

    /**
     * Sets the message type.  Clears the text and target properties if type is {@link MessageTypes#NULL_MESSAGE NULL_MESSAGE}.
     * @param type The message type; may be any value, although use of {@link MessageTypes} constants
     * are encouraged.
     */
    public void setType(final int type) {
        if (type == NULL_MESSAGE) {
            setText(null);
            setTarget(null);
        }
        this.type = type;
    }

    /**
     * Return the message text.
     * @return the String representation of this message, which is the message text.
     */
    public String toString() {
        return getText();
    }

    private String removeEscapeSequences(String value){
        if(value == null){
            return null;
        }

        StringBuffer buffer = new StringBuffer();
        boolean hasNewLine = value.indexOf("\n") != -1; //$NON-NLS-1$
        boolean hasReturn = value.indexOf("\r") != -1; //$NON-NLS-1$
        boolean hasTabs = value.indexOf("\t") != -1; //$NON-NLS-1$

        if(hasNewLine){
            StringTokenizer s = new StringTokenizer(value, "\n"); //$NON-NLS-1$
            while(s.hasMoreTokens() ){
                String next = s.nextToken();
                buffer.append(next + " "); //$NON-NLS-1$
            }

            value = buffer.toString().trim();
        }

        if(hasReturn){
            StringTokenizer s = new StringTokenizer(value, "\r"); //$NON-NLS-1$
            while(s.hasMoreTokens() ){
                String next = s.nextToken();
                buffer.append(next + " "); //$NON-NLS-1$
            }

            value = buffer.toString().trim();
        }

        if(hasTabs){
            StringTokenizer s = new StringTokenizer(value, "\t"); //$NON-NLS-1$
            while(s.hasMoreTokens() ){
                String next = s.nextToken();
                buffer.append(next + " "); //$NON-NLS-1$
            }

            value = buffer.toString().trim();
        }

        return value;
    }
}
