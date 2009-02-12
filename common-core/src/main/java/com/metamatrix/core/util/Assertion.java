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

package com.metamatrix.core.util;

import java.util.Collection;
import java.util.Map;

import com.metamatrix.core.CorePlugin;

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
 * </p><p>
 * In JDK 1.4, Sun introduces the "assert" keyword and builds assertion support directly
 * into the language.  When MetaMatrix begins using JDK 1.4 across the board, this
 * class should no longer be needed and all usage of assertions should be replaced with 
 * use of the built-in JDK assertion facility.
 * </p>
 * @see com.metamatrix.common.util.ArgCheck
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

	// ########################## boolean METHODS ###################################

	public static final void isEqual(boolean value1, boolean value2) {
        isEqual(value1,value2,null);
	}

	public static final void isEqual(boolean value1, boolean value2, String message) {
        if ( value1 != value2 ) {
            final String msg = message != null ?
                               message :
                               CorePlugin.Util.getString("Assertion.isEqual",new Object[]{new Boolean(value1),new Boolean(value2)}); //$NON-NLS-1$
            failed(msg);
        }
	}

	public static final void isNotEqual(boolean value1, boolean value2) {
        isNotEqual(value1,value2,null);
	}

	public static final void isNotEqual(boolean value1, boolean value2, String message) {
        if ( value1 == value2 ) {
            final String msg = message != null ?
                               message :
                               CorePlugin.Util.getString("Assertion.isNotEqual",new Object[]{new Boolean(value1),new Boolean(value2)}); //$NON-NLS-1$
            failed(msg);
        }
	}

	// ########################## int METHODS ###################################

	public static final void isEqual(int value1, int value2) {
        isEqual(value1,value2,null);
	}

	public static final void isEqual(int value1, int value2, String message) {
        if ( value1 != value2 ) {
            final String msg = message != null ?
                               message :
                               CorePlugin.Util.getString("Assertion.isEqual",new Object[]{new Integer(value1),new Integer(value2)}); //$NON-NLS-1$
            failed(msg);
        }
	}

	public static final void isNotEqual(int value1, int value2) {
        isNotEqual(value1,value2,null);
	}

	public static final void isNotEqual(int value1, int value2, String message) {
        if ( value1 == value2 ) {
            final String msg = message != null ?
                               message :
                               CorePlugin.Util.getString("Assertion.isNotEqual",new Object[]{new Integer(value1),new Integer(value2)}); //$NON-NLS-1$
            failed(msg);
        }
	}

	public static final void isNonNegative(int value) {
        isNonNegative(value,null);
	}

	public static final void isNonNegative(int value, String message) {
        if ( value < 0 ) {
            final String msg = message != null ?
                               message :
                               CorePlugin.Util.getString("Assertion.isNonNegative",new Object[]{new Integer(value)}); //$NON-NLS-1$
            failed(msg);
        }
	}

	public static final void isNonPositive(int value) {
        isNonPositive(value,null);
	}

	public static final void isNonPositive(int value, String message) {
        if ( value > 0 ) {
            final String msg = message != null ?
                               message :
                               CorePlugin.Util.getString("Assertion.isNonPositive",new Object[]{new Integer(value)}); //$NON-NLS-1$
            failed(msg);
        }
	}

	public static final void isNegative(int value) {
        isNegative(value,null);
	}

	public static final void isNegative(int value, String message) {
        if ( value >= 0 ) {
            final String msg = message != null ?
                               message :
                               CorePlugin.Util.getString("Assertion.isNegative",new Object[]{new Integer(value)}); //$NON-NLS-1$
            failed(msg);
        }
	}

	public static final void isPositive(int value) {
        isPositive(value,null);
	}

	public static final void isPositive(int value, String message) {
        if ( value <= 0 ) {
            final String msg = message != null ?
                               message :
                               CorePlugin.Util.getString("Assertion.isPositive",new Object[]{new Integer(value)}); //$NON-NLS-1$
            failed(msg);
        }
	}

	// ########################## long METHODS ###################################

    public static final void isEqual(long value1, long value2) {
        isEqual(value1,value2,null);
    }

    public static final void isEqual(long value1, long value2, String message) {
        if ( value1 != value2 ) {
            final String msg = message != null ?
                               message :
                               CorePlugin.Util.getString("Assertion.isEqual",new Object[]{new Long(value1),new Long(value2)}); //$NON-NLS-1$
            failed(msg);
        }
    }

    public static final void isNotEqual(long value1, long value2) {
        isNotEqual(value1,value2,null);
    }

    public static final void isNotEqual(long value1, long value2, String message) {
        if ( value1 == value2 ) {
            final String msg = message != null ?
                               message :
                               CorePlugin.Util.getString("Assertion.isNotEqual",new Object[]{new Long(value1),new Long(value2)}); //$NON-NLS-1$
            failed(msg);
        }
    }

    public static final void isNonNegative(long value) {
        isNonNegative(value,null);
    }

    public static final void isNonNegative(long value, String message) {
        if ( value < 0 ) {
            final String msg = message != null ?
                               message :
                               CorePlugin.Util.getString("Assertion.isNonNegative",new Object[]{new Long(value)}); //$NON-NLS-1$
            failed(msg);
        }
    }

    public static final void isNonPositive(long value) {
        isNonPositive(value,null);
    }

    public static final void isNonPositive(long value, String message) {
        if ( value > 0 ) {
            final String msg = message != null ?
                               message :
                               CorePlugin.Util.getString("Assertion.isNonPositive",new Object[]{new Long(value)}); //$NON-NLS-1$
            failed(msg);
        }
    }

    public static final void isNegative(long value) {
        isNegative(value,null);
    }

    public static final void isNegative(long value, String message) {
        if ( value >= 0 ) {
            final String msg = message != null ?
                               message :
                               CorePlugin.Util.getString("Assertion.isNegative",new Object[]{new Long(value)}); //$NON-NLS-1$
            failed(msg);
        }
    }

    public static final void isPositive(long value) {
        isPositive(value,null);
    }

    public static final void isPositive(long value, String message) {
        if ( value <= 0 ) {
            final String msg = message != null ?
                               message :
                               CorePlugin.Util.getString("Assertion.isPositive",new Object[]{new Long(value)}); //$NON-NLS-1$
            failed(msg);
        }
    }

	// ########################## String METHODS ###################################

	public static final void isNotZeroLength(String value) {
        isNotZeroLength(value,null);
	}

	public static final void isNotZeroLength(String value, String message) {
		isNotNull(value);
        if ( value.length() == 0 ) {
            final String msg = message != null ?
                               message :
                               CorePlugin.Util.getString("Assertion.isNotZeroLength"); //$NON-NLS-1$
            failed(msg);
        }
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
	 * Compares with object1 == object2.
	 */
	public static final void isIdentical(Object object1, Object object2) {
        isIdentical(object1,object2,null);
	}

	/**
	 * Compares with object1 == object2.
	 */
	public static final void isIdentical(Object object1, Object object2, String message) {
        if ( object1 != object2 ) {
            final String msg = message != null ?
                               message :
                               CorePlugin.Util.getString("Assertion.isIdentical",new Object[]{object1,object2}); //$NON-NLS-1$
            failed(msg);
        }
	}

	/**
	 * Compares with object1.equals(object2).
	 */
	public static final void isEqual(Object object1, Object object2) {
        isEqual(object1,object2,null);
	}

	/**
	 * Compares with object1.equals(object2).
	 */
	public static final void isEqual(Object object1, Object object2, String message) {
		if(object1 == null) {
            if ( object2 != null ) {
                final String msg = message != null ?
                                   message :
                                   CorePlugin.Util.getString("Assertion.isEqual",new Object[]{object1,object2}); //$NON-NLS-1$
                failed(msg);
            }
            // else both are null
		} else {
            if ( object2 == null ) {
                final String msg = message != null ?
                                   message :
                                   CorePlugin.Util.getString("Assertion.isEqual",new Object[]{object1,object2}); //$NON-NLS-1$
                failed(msg);
            }
            // else both are not null
            if ( !object1.equals(object2) ) {
                final String msg = message != null ?
                                   message :
                                   CorePlugin.Util.getString("Assertion.isEqual",new Object[]{object1,object2}); //$NON-NLS-1$
                failed(msg);
            }
		}
	}

    /**
     * Verifies that the specified value is an instance of the specified class.
     * @param value            The value to verify
     * @param exprectedClass   The class of which the value must be an instance
     * @param name             The text identifying the name or type of value
     * @throws ClassCastException If the value is not an instance of the specified class.
     * @since 2.1
     */
    public static final Object isInstanceOf(final Object object, final Class expectedClass, final String name) {
        if (object == null) {
            return null;
        }
        final Class objClass = object.getClass();
        if (!expectedClass.isAssignableFrom(objClass)) {
            final Object[] params = new Object[]{name, expectedClass, objClass.getName()};
            final String msg = CorePlugin.Util.getString("Assertion.invalidClassMessage",params); //$NON-NLS-1$
            throw new ClassCastException(msg);
        }
        return object;
    }

	// ########################## COLLECTION METHODS ###################################
	
    public static final void isNotEmpty(Collection collection) {
        isNotEmpty(collection,null);
    }

    public static final void isNotEmpty(Collection collection, String message) {
        isNotNull(collection);
        if ( collection.isEmpty() ) {
            final String msg = message != null ?
                               message :
                               CorePlugin.Util.getString("Assertion.isNotEmpty_Collection"); //$NON-NLS-1$
            failed(msg);
        }
    }

    public static final void isNotEmpty(Map map) {
        isNotEmpty(map,null);
    }

    public static final void isNotEmpty(Map map, String message) {
        isNotNull(map);
        if ( map.isEmpty() ) {
            final String msg = message != null ?
                               message :
                               CorePlugin.Util.getString("Assertion.isNotEmpty_Map"); //$NON-NLS-1$
            failed(msg);
        }
    }

    public static final void contains(Collection collection, Object value) {
        contains(collection,value,null);
    }

    public static final void contains(Collection collection, Object value, String message) {
		isNotNull(collection);
        if ( !collection.contains(value) ) {
            final String msg = message != null ?
                               message :
                               CorePlugin.Util.getString("Assertion.contains_Collection"); //$NON-NLS-1$
            failed(msg);
        }
	}

	public static final void containsKey(Map map, Object key) {
        containsKey(map,key,null);
	}

	public static final void containsKey(Map map, Object key, String message) {
		isNotNull(map);
        if ( !map.containsKey(key) ) {
            final String msg = message != null ?
                               message :
                               CorePlugin.Util.getString("Assertion.contains_Map"); //$NON-NLS-1$
            failed(msg);
        }
	}
}
