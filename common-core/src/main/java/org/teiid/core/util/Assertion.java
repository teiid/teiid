/*
 * Copyright Red Hat, Inc. and/or its affiliates
 * and other contributors as indicated by the @author tags and
 * the COPYRIGHT.txt file distributed with this work.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.teiid.core.util;

import org.teiid.core.CorePlugin;


/**
 * <p>
 * This class contains a set of static utility methods for assertion checking.
 * Assertions are used to document the assumptions a programmer is making about the
 * code they are writing.  Assertions should not be used in cases where the user of
 * a class can affect whether the assertion is true or false, such as argument
 * checking.  Rather, assertions should be considered a much stronger statement by
 * the programmer that such a condition is NEVER true.  In fact, this statement is
 * so strong that assertions should be considered optional as they should never occur.
 * However, these assertions may be violated during development and that is primarily
 * where these assertions are useful.
 * <p>
 * In JDK 1.4, Sun introduces the "assert" keyword and builds assertion support directly
 * into the language.  When MetaMatrix begins using JDK 1.4 across the board, this
 * class should no longer be needed and all usage of assertions should be replaced with
 * use of the built-in JDK assertion facility.
 *
 */
public final class Assertion {
    //============================================================================================================================
    // Constructors

    // Can't construct - just utility methods
    private Assertion() {
    }

    //============================================================================================================================
    // Methods

    // ########################## BASIC METHODS ###################################

    public static final void assertTrue(boolean condition) {
        assertTrue(condition,null);
    }

    public static final void assertTrue(boolean condition, String msgKey) {
        if(! condition) {
            final String msg = msgKey != null ?
                               msgKey :
                               CorePlugin.Util.getString("Assertion.Assertion_failed"); //$NON-NLS-1$
            failed(msg);
        }
    }

    public static final void failed(String msg) {
        throw new AssertionError(msg);
    }

    // ########################## OBJECT METHODS ###################################

    public static final void isNull(Object value) {
        isNull(value,null);
    }

    public static final void isNull(Object value, String message) {
        if ( value != null ) {
            final String msg = message != null ?
                               message :
                               CorePlugin.Util.getString("Assertion.isNull"); //$NON-NLS-1$
            failed(msg);
        }
    }

    public static final void isNotNull(Object value) {
        isNotNull(value,null);
    }

    public static final void isNotNull(Object value, String message) {
        if ( value == null ) {
            final String msg = message != null ?
                               message :
                               CorePlugin.Util.getString("Assertion.isNotNull"); //$NON-NLS-1$
            failed(msg);
        }
    }

    /**
     * Verifies that the specified value is an instance of the specified class.
     * @param object            The value to verify
     * @param expectedClass   The class of which the value must be an instance
     * @param name             The text identifying the name or type of value
     * @throws ClassCastException If the value is not an instance of the specified class.
     * @since 2.1
     */
    public static final <T> T isInstanceOf(final Object object, final Class<T> expectedClass, final String name) {
        if (object == null) {
            return null;
        }
        final Class<?> objClass = object.getClass();
        if (!expectedClass.isAssignableFrom(objClass)) {
            final Object[] params = new Object[]{name, expectedClass, objClass.getName()};
            final String msg = CorePlugin.Util.getString("Assertion.invalidClassMessage",params); //$NON-NLS-1$
            throw new ClassCastException(msg);
        }
        return expectedClass.cast(object);
    }

}
