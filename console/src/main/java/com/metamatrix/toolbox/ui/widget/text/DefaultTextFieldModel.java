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
package com.metamatrix.toolbox.ui.widget.text;

// System imports
import java.awt.Toolkit;
import java.beans.PropertyChangeEvent;
import java.beans.PropertyChangeListener;
import java.text.ParseException;
import java.text.StringCharacterIterator;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;

import javax.swing.text.AttributeSet;
import javax.swing.text.BadLocationException;
import javax.swing.text.PlainDocument;

import com.metamatrix.toolbox.ui.Validator;

/**
The default model for TextFields.
@since 2.0
@version 2.0
@author John P. A. Verhaeg
*/
public class DefaultTextFieldModel extends PlainDocument
implements TextConstants {
    //############################################################################################################################
    //# Constants                                                                                                                #
    //############################################################################################################################
    
    private static final int MAX_CEILING = Short.MAX_VALUE;
    
    //############################################################################################################################
    //# Instance Variables                                                                                                       #
    //############################################################################################################################
    
    private boolean isModified;
    private boolean isValid = true;
    private Object validationResult;
    private List validChrs, invalidChrs;
    private List validChrRanges, invalidChrRanges;   // Each range is stored as two entries (beginning & end) in the list
    private List validators;
    private String oldText;
    private List listeners;
    private int maxLen = MAX_CEILING;

    //############################################################################################################################
    //# Constructors                                                                                                             #
    //############################################################################################################################
    
    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since 2.0
    */
    public DefaultTextFieldModel() {
        initializeDefaultTextFieldModel();
    }
    
    //############################################################################################################################
    //# Instance Methods                                                                                                         #
    //############################################################################################################################

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    Registers a PropertyChangeListener to be notified of text and validity changes to the model.
    @param listener A PropertyChangeListener
    @since 2.0
    */
    public void addPropertyChangeListener(final PropertyChangeListener listener) {
        if (listeners == null) {
            listeners = new ArrayList();
        }
        listeners.add(listener);
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    Adds the specified text validator to the end of the list of registered validators.  Validation occurs by iterating through
    this list, starting with the validator that was registered first and finishing with the most recently registered validator.
    All validators must return Boolean.TRUE if validation succeeds.
    @param validator An instance of Validator
    @since 2.0
    */
    public void addValidator(final Validator validator) {
        if (validators == null) {
            validators = new ArrayList();
        }
        validators.add(validator);
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    Adds the specified text validator to the list of registered validators at the specified index.  Validation occurs by
    iterating through this list, starting with the validator that was registered first and finishing with the most recently
    registered validator.  All validators must return Boolean.TRUE if validation succeeds.
    @param validator  An instance of Validator
    @param index      The index within the validation list that the validator should be added
    @since 2.0
    */
    public void addValidator(final Validator validator, final int index) {
        if (validators == null) {
            validators = new ArrayList();
        }
        validators.add(index, validator);
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    Commits any new text set on the model.
    @since 2.0
    */
    public void commit() {
        oldText = null;
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    Notifies all registered PropertyChangeListeners of a property change.
    @since 2.0
    */
    protected void firePropertyChangeEvent(final PropertyChangeEvent event) {
        final ListIterator iterator = listeners.listIterator(listeners.size());
        while (iterator.hasPrevious()) {
            ((PropertyChangeListener)iterator.previous()).propertyChange(event);
        }
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    Notifies all registered PropertyChangeListeners of a text change.
    @since 2.0
    */
    protected void firePropertyChangeEvent(final String prop, final String oldText, final String newText) {
        if (listeners == null) {
            return;
        }
        firePropertyChangeEvent(new PropertyChangeEvent(this, prop, oldText, newText));
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    Notifies all registered PropertyChangeListeners of a boolean value (flag) change.
    @since 2.0
    */
    protected void firePropertyChangeEvent(final String prop, final boolean oldVal, final boolean newVal) {
        if (listeners == null) {
            return;
        }
        firePropertyChangeEvent(new PropertyChangeEvent(this, prop, new Boolean(oldVal), new Boolean(newVal)));
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    Notifies all registered PropertyChangeListeners of a integer value change.
    @since 2.0
    */
    protected void firePropertyChangeEvent(final String prop, final int oldVal, final int newVal) {
        if (listeners == null) {
            return;
        }
        firePropertyChangeEvent(new PropertyChangeEvent(this, prop, new Integer(oldVal), new Integer(newVal)));
    }
    
    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @return The maximum length of the text, zero if not set
    @since 2.0
    */
    public int getMaximumLength()
    {
        return maxLen;
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    Retrieves the result of the last validation check.
    @return The result of the last validation check if it failed, null otherwise
    @since 2.0
    */
    public Object getValidationResult() {
        return validationResult;
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since 2.0
    */
    protected void initializeDefaultTextFieldModel() {
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    Overridden to prevent insertion of invalid characters and mark model as modified.
    @since 2.0
    */
    public void insertString(final int index, String text, final AttributeSet ignored)
    throws BadLocationException {
        if (oldText == null) {
            oldText = getText(0, getLength());
        }
        // Restrict insertions to maximum length
        if (text != null) {
            if (maxLen > 0) {
		        final int availLen = maxLen - getLength();
	            if (text.length() > availLen)
	            {
	                invalidInsertionAttempted(text.charAt(availLen), maxLen);
	                if (availLen == 0)
	                    return;
	                text = text.substring(0, availLen);
	            }
            }
            // Check if characters to be inserted are valid (if valid characters have been previously specified)
            if (validChrs != null  ||  validChrRanges != null  ||  invalidChrs != null  ||  invalidChrRanges != null) {
                final StringCharacterIterator iterator = new StringCharacterIterator(text);
                char chr;
                boolean isValid;
                Iterator chrIterator;
                int subNdx = index;
                for (chr = iterator.current();  chr != StringCharacterIterator.DONE;  chr = iterator.next()) {
                    isValid = true;
                    // Check if character in list of individual invalid characters
                    if (invalidChrs != null) {
                        chrIterator = invalidChrs.iterator();
                        while (chrIterator.hasNext()) {
                            if (chr == ((Character)chrIterator.next()).charValue()) {
                                isValid = false;
                                break;
                            }
                        }
                    }
                    // If character still not marked as valid, check if within valid range
                    if (isValid  &&  invalidChrRanges != null) {
                        chrIterator = invalidChrRanges.iterator();
                        while (chrIterator.hasNext()) {
                            if (chr >= ((Character)chrIterator.next()).charValue()
                                &&  chr <= ((Character)chrIterator.next()).charValue()) {
                                isValid = false;
                                break;
                            }
                        }
                    }
                    if (isValid  &&  (validChrs != null  ||  validChrRanges != null)) {
                        isValid = false;
                        // Check if character in list of individual valid characters
                        if (validChrs != null) {
                            chrIterator = validChrs.iterator();
                            while (chrIterator.hasNext()) {
                                if (chr == ((Character)chrIterator.next()).charValue()) {
                                    isValid = true;
                                    break;
                                }
                            }
                        }
                        // If character still not marked as valid, check if within valid range
                        if (!isValid  &&  validChrRanges != null) {
                            chrIterator = validChrRanges.iterator();
                            while (chrIterator.hasNext()) {
                                if (chr >= ((Character)chrIterator.next()).charValue()
                                    &&  chr <= ((Character)chrIterator.next()).charValue()) {
                                    isValid = true;
                                    break;
                                }
                            }
                        }
                    }
                    if (!isValid) {
                        invalidInsertionAttempted(chr, subNdx);
                        return;
                    }
                    ++subNdx;
                }
            }
        }
        final String oldText = getText(0, getLength());
        super.insertString(index, text, ignored);
        if (text != null  &&  text.length() > 0) {
            firePropertyChangeEvent(TEXT_PROPERTY, oldText, getText(0, getLength()));
            setModified(true);
        }
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    Called whenever an attempt is made to insert an invalid character.  Just beeps by default.
    @param character  The invalid character for which insertion was attempted
    @param subIndex   The index within the model's text where the insertion was attempted
    @since 2.0
    */
    protected void invalidInsertionAttempted(final char character, final int subIndex) {
        Toolkit.getDefaultToolkit().beep();
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    Determines whether the model's contents are valid by invoking each of its registered Validators.
    @return The results of the last validity check if the model hasn't been modified since then, else the results of any
    registered Validators, else true (meaning no Validators have been registered)
    @since 2.0
    */
    public boolean isValid() {
        if (!isModified) {
            return isValid;
        }
        setModified(false);
        boolean isValid = true;
        if (validators != null) {
            final Iterator iterator = validators.iterator();
            while (iterator.hasNext()) {
                try {
                    validationResult = ((Validator)iterator.next()).validate(getText(0, getLength()));
                } catch (final Exception err) {
                    validationResult = err;
                }
                if (Boolean.TRUE.equals(validationResult)) {
                    validationResult = null;
                } else {
                    isValid = false;
                }
            }
        }
        setValid(isValid);
        return isValid;
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    Unregisters a PropertyChangeListener.
    @param listener A PropertyChangeListener
    @since 2.0
    */
    public void removePropertyChangeListener(final PropertyChangeListener listener) {
        listeners.remove(listener);
        if (listeners.size() == 0) {
            listeners = null;
        }
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    Overridden to mark model as modified.
    @since 2.0
    */
    public void remove(final int index, final int length)
    throws BadLocationException {
        if (oldText == null) {
            oldText = getText(0, getLength());
        }
        final String oldText = getText(0, getLength());
        super.remove(index, length);
        if (length > 0) {
            firePropertyChangeEvent(TEXT_PROPERTY, oldText, getText(0, getLength()));
            setModified(true);
        }
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    Removes the text validator at the specified index from the list of registered validators.
    @since 2.0
    */
    public void removeValiditor(final int index) {
        if (validators == null  ||  validators.isEmpty()) {
            return;
        }
        validators.remove(index);
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    Removes the specified text validator from the list of registered validators.
    @since 2.0
    */
    public void removeValiditor(final Validator validator) {
        if (validators == null) {
            return;
        }
        validators.remove(validator);
    }
    
    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    Rollsback to the previous value in the model.
    @since 2.0
    */
    public void rollback()
    throws BadLocationException {
        remove(0, getLength());
        insertString(0, oldText, null);
        oldText = null;
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    Sets the list of invalid characters that cannot be inserted into this model.  All characters in the specified string will be
    added to the list, with the exception of dash ('-') characters, which are used to specify characters ranges (e.g., 
    <code>"A-Z"</code>).  To explicitly specify a dash as an invalid character, it must be entered as either the first character
    in the string (not part of a range) or the final character in any range within the string.
    Note that <code>"A-Z"</code> specifies a different range than <code>"a-z"</code>.
    @param invalidCharacters A list of invalid characters and/or character ranges
    @throws ParseException If a dash appears at the end of the string
    @since 2.0
    */
    public void setInvalidCharacters(final String invalidCharacters)
    throws ParseException {
        invalidChrs = invalidChrRanges = null;
        // Parse the characters within validCharacters and add to appropriate list (Note there is no check for duplicates)
        final StringCharacterIterator iterator = new StringCharacterIterator(invalidCharacters);
        char chr = iterator.current();
        if (chr == StringCharacterIterator.DONE) {
            return;
        }
        char nextChr;
        int ndx = 0;
        do {
            nextChr = iterator.next();
            ++ndx;
            if (nextChr == '-') {
                if (invalidChrRanges == null) {
                    invalidChrRanges = new ArrayList();
                }
                invalidChrRanges.add(new Character(chr));
                chr = iterator.next();
                ++ndx;
                if (chr == StringCharacterIterator.DONE) {
                    throw new ParseException("Missing character after dash", ndx);
                }
                invalidChrRanges.add(new Character(chr));
                nextChr = iterator.next();
                ++ndx;
            } else {
                if (invalidChrs == null) {
                    invalidChrs = new ArrayList();
                }
                invalidChrs.add(new Character(chr));
            }
            chr = nextChr;
        } while (nextChr != StringCharacterIterator.DONE);
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since 2.0
    */
    public void setMaximumLength(final int length) {
        final int oldLen = maxLen;
        if (length <= 0) {
            maxLen = MAX_CEILING;
        } else {
            maxLen = (length < MAX_CEILING) ? length : MAX_CEILING;
            if (getLength() > length) {
                try {
                    remove(length, getLength() - length);
                } catch (final BadLocationException err) {
                    throw new RuntimeException(err.getMessage());
                }
            }
        }
        // Notify property listeners
        firePropertyChangeEvent(MAXIMUM_LENGTH_PROPERTY, oldLen, maxLen);
    }
    
    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    Sets whether the model is marked as modified.
    @param isModified True if the model should be marked as modified
    @since 2.0
    */
    public void setModified(final boolean isModified) {
        final boolean wasModified = this.isModified;
        if (wasModified != isModified) {
            this.isModified = isModified;
            firePropertyChangeEvent(IS_MODIFIED_PROPERTY, wasModified, isModified);
        }
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    Sets whether the model's contents are marked as valid.
    @param isValid True if the model's contents should be marked as valid
    @since 2.0
    */
    public void setValid(final boolean isValid) {
        final boolean wasValid = this.isValid;
        if (wasValid != isValid) {
            this.isValid = isValid;
            firePropertyChangeEvent(IS_VALID_PROPERTY, wasValid, isValid);
        }
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    Sets the list of valid characters that can be inserted into this model.  All characters in the specified string will be
    added to the list, with the exception of dash ('-') characters, which are used to specify characters ranges (e.g., 
    <code>"A-Z"</code>).  To explicitly specify a dash as a valid character, it must be entered as either the first character in
    the string (not part of a range) or the final character in any range within the string.
    Note that <code>"A-Z"</code> specifies a different range than <code>"a-z"</code>.
    @param validCharacters A list of valid characters and/or character ranges
    @throws ParseException If a dash appears at the end of the string
    @since 2.0
    */
    public void setValidCharacters(final String validCharacters)
    throws ParseException {
        validChrs = validChrRanges = null;
        // Parse the characters within validCharacters and add to appropriate list (Note there is no check for duplicates)
        final StringCharacterIterator iterator = new StringCharacterIterator(validCharacters);
        char chr = iterator.current();
        if (chr == StringCharacterIterator.DONE) {
            return;
        }
        char nextChr;
        int ndx = 0;
        do {
            nextChr = iterator.next();
            ++ndx;
            if (nextChr == '-') {
                if (validChrRanges == null) {
                    validChrRanges = new ArrayList();
                }
                validChrRanges.add(new Character(chr));
                chr = iterator.next();
                ++ndx;
                if (chr == StringCharacterIterator.DONE) {
                    throw new ParseException("Missing character after dash", ndx);
                }
                validChrRanges.add(new Character(chr));
                nextChr = iterator.next();
                ++ndx;
            } else {
                if (validChrs == null) {
                    validChrs = new ArrayList();
                }
                validChrs.add(new Character(chr));
            }
            chr = nextChr;
        } while (nextChr != StringCharacterIterator.DONE);
    }
}
