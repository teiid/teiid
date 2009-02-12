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

package com.metamatrix.common.object;

import java.io.Serializable;
import java.text.CharacterIterator;
import java.text.StringCharacterIterator;
import java.util.*;

import com.metamatrix.common.CommonPlugin;
import com.metamatrix.common.util.ErrorMessageKeys;

/**
 * Multiplicity is the specification of the range of allowable cardinality values
 * that a set may assume.  Essentially, multiplicity is a (possibly infinite)
 * subset of the nonnegative integers.  The Multiplicity class represents
 * an interface for handling and manipulating multiplicity specifications.
 * <p>
 * In practice, it is usually a finite set of integer intervals, most often a
 * single interval with a minimum and maximum value.  Any set must be
 * finite, but the upper bound can be finite or unbounded
 * (the latter is called "many"); the upper bound must be greater than zero
 * (multiplicity of zero is not useful).  Note that although the set may be unbounded,
 * any particular cardinality is always finite.
 * <p>
 * The multiplicity is usually defined simply as a text expression consisting
 * of a comman-separated list of integer intervals, each in the form
 * "<i>minimum</i>..<i>maximum</i>", where <i>minimum</i> and <i>maximum</i>
 * are integers, or <i>maximum</i> can be a "*" which indicates an unbounded
 * interval.  An interval may also have the form <i>number</i>, where <i>number</i>
 * is an integer representing an interval of a single integer.  The multiplicity
 * defined as a single star ("*") is equivalent to the expression "0..*", and
 * indicates that the cardinality is unrestricted (i.e., "zero or more", or "many").
 * Examples of well-formed multiplicity text expressions include:
 * <ul>0..1</ul>
 * <ul>1</ul>
 * <ul>0..*</ul>
 * <ul>*</ul>
 * <ul>1..*</ul>
 * <ul>1..5</ul>
 * <ul>1..5,10,13..18,20..*</ul>
 * Generally, a well-formed multiplicity text expression will have intervals
 * that monotonically increase (e.g., <i>1..5,10,13</i>
 * rather than <i>10,1..5,13</i>), and will have two continguous intervals combined
 * into a single interval (e.g., <i>1..8</i> rather than <i>1..5,6..8</i>, and
 * <i>0..1</i> rather than <i>0,1</i>).
 * <p>
 * The Multiplicity class is an abstract class that is intended to hide all
 * implementation details from the user.  In this manner, the user only sees
 * and uses the Multiplicity interface definition and its static <code>getInstance</code>
 * and <code>getUnboundedInstance</code> methods, but the best possible
 * implementation class with the most efficient representation is used for each instance.
 * Various implementation classes are provided to efficiently handle the
 * unbounded case and the single interval case, both of which are by far
 * the most commonly used.  However, any well-formed multiplicity text expression
 * will be handled as well.
 * <p>
 * The Multiplicity class (and subclasses) are also immutable, and therefore
 * may be referenced by multiple users without the chance of the instance
 * being modified.  This also simplifies the interface (since no modification
 * methods need be provided) and makes possible the use of different underlying
 * classes to handle different situations without exposing the specific
 * implementation classes to the user; modification methods would make this
 * very difficult to successfully implement.
 */
public abstract class Multiplicity implements Serializable, Comparable {

    /**
     * The String definition of completely unbounded  of either the maximum or both minimum and maximum if the multiplicity
     * is considered unlimited.
     */
    public static final String UNBOUNDED_DEFINITION = "*"; //$NON-NLS-1$
    protected static final char UNBOUNDED_CHAR = '*';

    /**
     * The delimiter string ".." used between the minimum and maximum values.
     */
    public static final String RANGE_DELIMITER = ".."; //$NON-NLS-1$
    protected static final char RANGE_DELIMITER_CHAR = '.';
    protected static final int RANGE_DELIMITER_LENGTH = RANGE_DELIMITER.length();

    /**
     * The delimiter string "," used between two intervals.
     */
    public static final String INTERVAL_DELIMITER = ","; //$NON-NLS-1$
    protected static final char INTERVAL_DELIMITER_CHAR = ',';
    protected static final int INTERVAL_DELIMITER_LENGTH = INTERVAL_DELIMITER.length();

    /**
     * The value of either the maximum or both minimum and maximum if the multiplicity
     * is considered unlimited.
     */
    public static final int UNBOUNDED_VALUE = Integer.MAX_VALUE;

    /**
     * The minimum single multiplicity value allowed.  A multiplicity of 0 is undefined.
     */
    protected static final int MINIMUM_SINGLE_VALUE = 1;

    /**
     * The default single multiplicity value.
     */
    protected static final int DEFAULT_SINGLE_VALUE = 1;

    /**
     * The default value for <code>isOrdered</code>.
     */
    public static final boolean DEFAULT_ORDERING = false;

    /**
     * The default value for <code>isUnique</code>.
     */
    public static final boolean DEFAULT_UNIQUENESS = false;

    /**
     * @label UNBOUNDED
     * @supplierCardinality 1
     */
    protected static final Multiplicity UNBOUNDED = new UnlimitedMultiplicity();

    /**
     * @label DEFAULT
     * @supplierCardinality 1
     */
    protected static final Multiplicity DEFAULT = new IntervalMultiplicity(DEFAULT_SINGLE_VALUE);

    private boolean isOrdered = false;
    private boolean isUnique = false;

    /**
     * Parse the specified text expression and return the list of Multiplicity
     * objects that capture the expression.
     * @param expression the well-formed text expression for the Multiplicity
     * @param isOrdered the ordering constraint for the multiplicity
     * @param isUnique the uniqueness constraint for the multiplicity
     * @return the list of intervals defined by the expression
     * @throws MultiplicityExpressionException if the specified expression cannot
     * be parsed without errors or is not well-formed
     */
    static List parseExpression( String defn, boolean isOrdered, boolean isUnique ) throws MultiplicityExpressionException {
        List results = new ArrayList();
        if (defn == null || defn.trim().length() == 0) {
            results.add(DEFAULT);
            return results;
        }

        // If it is "*" then return ...
        if (defn.equals(UNBOUNDED_DEFINITION)) {
            results.add(UNBOUNDED);
            return results;
        }

        // Parse the expression by iterating through it and finding all intervals ...
        int minimum = -1;
        int maximum = -1;
        int startIndex = 0;
        int currentIndex = 0;
        CharacterIterator iter = new StringCharacterIterator(defn);
        for( char c = iter.first(); c != CharacterIterator.DONE; c = iter.next() ) {
            currentIndex = iter.getIndex();

            // If a range delimiter is found ...
            if ( c == RANGE_DELIMITER_CHAR ) {

                // If minimum was not yet found, parse it ...
                if ( minimum == -1 ) {
                    try {
                        minimum = Integer.parseInt(defn.substring(startIndex, currentIndex));
                    } catch ( NumberFormatException e ) {
                        throw new MultiplicityExpressionException(ErrorMessageKeys.OBJECT_ERR_0001,
							CommonPlugin.Util.getString(ErrorMessageKeys.OBJECT_ERR_0001,
            				new Object[] {RANGE_DELIMITER, new Integer(currentIndex), defn}));
                    }
                }
                startIndex = currentIndex + RANGE_DELIMITER_LENGTH;
                iter.setIndex(startIndex);
            }

            // If an interval delimiter is found ...
            if ( c == INTERVAL_DELIMITER_CHAR ) {

                if ( defn.charAt(startIndex) == UNBOUNDED_CHAR ) {
                    throw new MultiplicityExpressionException(ErrorMessageKeys.OBJECT_ERR_0002,
							CommonPlugin.Util.getString(ErrorMessageKeys.OBJECT_ERR_0002,
            				new Object[] {new Integer(startIndex), defn}));
                }
                try {
                    maximum = Integer.parseInt(defn.substring(startIndex, currentIndex));
                    // If maximum was not yet found, set it to the minimum ...
                    if ( minimum == -1 ) {
                        minimum = maximum;
                    }
                } catch ( NumberFormatException e ) {
                    throw new MultiplicityExpressionException(ErrorMessageKeys.OBJECT_ERR_0003,
							CommonPlugin.Util.getString(ErrorMessageKeys.OBJECT_ERR_0003,
            				new Object[] {new Integer(startIndex), new Integer(currentIndex), defn}));
                }
                try {
                    results.add( new IntervalMultiplicity(minimum,maximum,isOrdered,isUnique) );
                } catch ( IllegalArgumentException e ) {
                    throw new MultiplicityExpressionException(ErrorMessageKeys.OBJECT_ERR_0004,
							CommonPlugin.Util.getString(ErrorMessageKeys.OBJECT_ERR_0004,
            				new Object[] {new Integer(minimum), new Integer(maximum), defn}));
                }
                minimum = -1;
                maximum = -1;

                startIndex = currentIndex + INTERVAL_DELIMITER_LENGTH;
                iter.setIndex(startIndex);
            }
        }
        if ( defn.length() <= startIndex ) {
            throw new MultiplicityExpressionException(ErrorMessageKeys.OBJECT_ERR_0005,
							CommonPlugin.Util.getString(ErrorMessageKeys.OBJECT_ERR_0005,
            				new Object[] {new Integer(defn.length()-1), defn}));
        }

        // Check if the last value is a "*"
        if ( defn.charAt(startIndex) == UNBOUNDED_CHAR ) {
            maximum = UNBOUNDED_VALUE;
        }

        // Parse the last value ...
        else {
            try {
                maximum = Integer.parseInt(defn.substring(startIndex));
            } catch ( NumberFormatException e ) {
                throw new MultiplicityExpressionException(ErrorMessageKeys.OBJECT_ERR_0003,
							CommonPlugin.Util.getString(ErrorMessageKeys.OBJECT_ERR_0003,
            				new Object[] {new Integer(startIndex), new Integer(defn.length()-1), defn}));
            }
        }
        // If maximum was not yet found, set it to the minimum ...
        if ( minimum == -1 ) {
            minimum = maximum;
        }
        try {
            results.add( new IntervalMultiplicity(minimum,maximum,isOrdered,isUnique) );
        } catch ( IllegalArgumentException e ) {
            throw new MultiplicityExpressionException(ErrorMessageKeys.OBJECT_ERR_0004,
							CommonPlugin.Util.getString(ErrorMessageKeys.OBJECT_ERR_0004,
            				new Object[] {new Integer(minimum), new Integer(maximum), defn}));
        }

        // Sort the intervals ...
        Collections.sort(results);

        // Merge adjacent intervals ...
        if ( results.size() > 1 ) {
            int index = 0;
            Multiplicity first = (Multiplicity) results.get(index);
            Multiplicity next = (Multiplicity) results.get(++index);
            while ( next != null ) {
                if ( first.getMaximum() >= next.getMinimum() || first.getMaximum() == (next.getMinimum()-1) ) {
                    int min = Math.min( first.getMinimum(), next.getMinimum() );
                    int max = Math.max( first.getMaximum(), next.getMaximum() );
                    first = new IntervalMultiplicity(min,max,isOrdered,isUnique);
                    results.set(index-1, first);
                    results.remove(index);
                } else {
                    ++index;
                    first = next;
                }
                if ( index < results.size() ) {
                    next = (Multiplicity) results.get(index);
                } else {
                    next = null;
                }
            }
        }

        // Return the intervals ...
        return results;
    }

    protected Multiplicity( boolean isOrdered, boolean isUnique ) {
        this.isOrdered = isOrdered;
        this.isUnique = isUnique;
    }

    protected Multiplicity() {
        this(DEFAULT_ORDERING,DEFAULT_UNIQUENESS);
    }

    /**
     * Obtain an instance by parsing the specified multiplicity text expression.
     * The text expression must be well-formed.
     * @param expression the string definition of the multiplicity specification;
     * if null, then the default Multiplicity instance of "1" is returned.
     * @param isOrdered the ordering constraint for the multiplicity
     * @param isUnique the uniqueness constraint for the multiplicity
     * @return the MultiplicityInstance that best captures the specified definition.
     * @throws MultiplicityExpressionException if the expression is not well-formed and
     * cannot be parsed.
     */
    public static Multiplicity getInstance(String defn, boolean isOrdered, boolean isUnique ) throws MultiplicityExpressionException {
        List intervals = parseExpression(defn,isOrdered,isUnique);
        if ( intervals.size() == 1 ) {
            return (Multiplicity) intervals.get(0);
        }
        return new RangeMultiplicity(intervals,isOrdered,isUnique);
    }

    /**
     * Obtain an instance by parsing the specified multiplicity text expression,
     * that is unordered and not unique.  The text expression must be well-formed.
     * @param expression the string definition of the multiplicity specification;
     * if null, then the default Multiplicity instance of "1" is returned.
     * @return the MultiplicityInstance that best captures the specified definition.
     * @throws MultiplicityExpressionException if the expression is not well-formed and
     * cannot be parsed.
     */
    public static Multiplicity getInstance(String defn) throws MultiplicityExpressionException {
        List intervals = parseExpression(defn,DEFAULT_ORDERING,DEFAULT_UNIQUENESS);
        if ( intervals.size() == 1 ) {
            return (Multiplicity) intervals.get(0);
        }
        return new RangeMultiplicity(intervals,DEFAULT_ORDERING,DEFAULT_UNIQUENESS);
    }

    /**
     * Obtain an instance that represents an unbounded multiplicity, which is
     * equivalent to the text expression "*", that is unordered and not unique.
     * @return the MultiplicityInstance representing "*".
     */
    public static Multiplicity getUnboundedInstance() {
        return UNBOUNDED;
    }

    /**
     * Obtain an instance that represents an unbounded multiplicity, which is
     * equivalent to the text expression "*".
     * @param isOrdered the ordering constraint for the multiplicity
     * @param isUnique the uniqueness constraint for the multiplicity
     * @return the MultiplicityInstance representing "*".
     */
    public static Multiplicity getUnboundedInstance( boolean isOrdered, boolean isUnique) {
        return new UnlimitedMultiplicity(isOrdered,isUnique);
    }

    /**
     * Obtain an instance that represents a singular multiplicity of "1"
     * that is unordered and not unique.
     * This is considered the default multiplicity,
     * since many situations will require a cardinality of 1.
     * @return the MultiplicityInstance representing "1".
     */
    public static Multiplicity getInstance() {
        return DEFAULT;
    }

    /**
     * Obtain an instance for a single-interval multiplicity of the specified
     * value.  The corresponding text expression is "<i>number</i>".
     * @param number the size of the single interval;  must be a positive integer value.
     * @param isOrdered the ordering constraint for the multiplicity
     * @param isUnique the uniqueness constraint for the multiplicity
     * @return the MultiplicityInstance that best captures the specified definition.
     * @throws IllegalArgumentException if the value is negative or zero.
     */
    public static Multiplicity getInstance(int number, boolean isOrdered, boolean isUnique) {
        return new IntervalMultiplicity(number, number, isOrdered, isUnique);
    }

    /**
     * Obtain an instance for a single-interval multiplicity over the specified
     * range.  The corresponding text expression is "<i>minimum</i>..<i>maximum</i>".
     * @param minimum the minimum value for the interval; must be positive, and
     * may be zero only if <code>maximum</code> is non-zero.
     * @param maximum the maximum value for the interval; must be equal to or
     * greater than <code>minimum</code>, and may be Multiplicity.UNBOUNDED_VALUE for
     * an unbounded maximum value.
     * @param isOrdered the ordering constraint for the multiplicity
     * @param isUnique the uniqueness constraint for the multiplicity
     * @return the MultiplicityInstance that best captures the specified definition.
     * @throws IllegalArgumentException if the two values are not compatible.
     */
    public static Multiplicity getInstance(int minimum, int maximum, boolean isOrdered, boolean isUnique) {
        return new IntervalMultiplicity(minimum, maximum, isOrdered, isUnique);
    }

    /**
     * Obtain an instance for a single-interval multiplicity of the specified
     * value that is unordered and not unique.  The corresponding text expression is "<i>number</i>".
     * @param number the size of the single interval;  must be a positive integer value.
     * @return the MultiplicityInstance that best captures the specified definition.
     * @throws IllegalArgumentException if the value is negative or zero.
     */
    public static Multiplicity getInstance(int number) {
        return new IntervalMultiplicity(number);
    }

    /**
     * Obtain an instance for a single-interval multiplicity over the specified
     * range that is unordered and not unique.  The corresponding text expression is "<i>minimum</i>..<i>maximum</i>".
     * @param minimum the minimum value for the interval; must be positive, and
     * may be zero only if <code>maximum</code> is non-zero.
     * @param maximum the maximum value for the interval; must be equal to or
     * greater than <code>minimum</code>, and may be Multiplicity.UNBOUNDED_VALUE for
     * an unbounded maximum value.
     * @return the MultiplicityInstance that best captures the specified definition.
     * @throws IllegalArgumentException if the two values are not compatible.
     */
    public static Multiplicity getInstance(int minimum, int maximum) {
        return new IntervalMultiplicity(minimum, maximum,DEFAULT_ORDERING,DEFAULT_UNIQUENESS);
    }

    /**
     * Get the maximum number of values required for this property.
     * The result from this method will be equal to or greater than
     * zero.
     * @return the maximum number of property values required.
     */
    public abstract int getMaximum();

    /**
     * Return whether the multiplicity is defined as requiring an order.
     * @return true if the instances order is important.
     */
    public boolean isOrdered() {
        return this.isOrdered;
    }

    /**
     * Return whether the multiplicity is defined as requiring uniqueness.
     * @return true if the instances uniqueness is important.
     */
    public boolean isUnique() {
        return this.isUnique;
    }

    /**
     * Get the minimum number of values required for this property.
     * The result from this method will be equal to or greater than
     * zero.
     * @return the minimum number of property values required.
     */
    public abstract int getMinimum();

    /**
     * Determine whether the specified cardinality is included in this multiplicity
     * expression.
     * @return true if the cardinality is included in the range of allowable values
     * for this multiplicity.
     */
    public abstract boolean isIncluded( int cardinality );

    /**
     * Obtain whether the multiplicity has a maximum value that is unlimited.
     * @return true if the maximum value of this multiplicity is unlimited.
     */
    public abstract boolean isUnlimited();

    /**
     * Returns a string representing the current state of the object.
     * @return the string representation of this instance.
     */
    public abstract String toString();

    /**
     * Compares this object to another. If the specified object is
     * an instance of the MetaMatrixSessionID class, then this
     * method compares the contents; otherwise, it throws a
     * ClassCastException (as instances are comparable only to
     * instances of the same
     *  class).
     * <p>
     * Note:  this method <i>is</i> consistent with
     * <code>equals()</code>, meaning
     *  that
     * <code>(compare(x, y)==0) == (x.equals(y))</code>.
     * <p>
     * @param obj the object that this instance is to be compared to.
     * @return a negative integer, zero, or a positive integer as this object
     *      is less than, equal to, or greater than the specified object, respectively.
     * @throws IllegalArgumentException if the specified object reference is null
     * @throws ClassCastException if the specified object's type prevents it
     *      from being compared to this instance.
     */
    public abstract int compareTo(Object obj);

    protected int compareFlags(Multiplicity that) {
        // Assumed to not be null ...
        if ( this.isOrdered() ) {
            if ( !that.isOrdered() ) {
                return -1;
            }
        } else {
            if ( that.isOrdered() ) {
                return 1;
            }
        }

        if ( this.isUnique() ) {
            if ( !that.isUnique() ) {
                return -1;
            }
        } else {
            if ( that.isUnique() ) {
                return 1;
            }
        }
        return 0;   // otherwise these values are the same ...
    }

    static int compare(Multiplicity obj1, Multiplicity obj2) {

        if (obj1 instanceof UnlimitedMultiplicity) {
            return obj1.compareTo(obj2);
        }
        if (obj2 instanceof UnlimitedMultiplicity) {
            return - obj2.compareTo(obj1);
        }

        if (obj1 instanceof IntervalMultiplicity) {
            int diffInMinimum = obj1.getMinimum() - obj2.getMinimum();
            int diffInMaximum = obj1.getMaximum() - obj2.getMaximum();
            if ( obj2 instanceof IntervalMultiplicity ) {
                if (diffInMinimum != 0) {
                    return diffInMinimum;
                }
                return obj1.compareFlags(obj2);
            } // else if ( obj2 instanceof RangeMultiplicity ) {
            if (diffInMinimum != 0) {
                return diffInMinimum;
            }
            if (diffInMaximum != 0) {
                return diffInMaximum;
            }
            return 1;   // Interval is a complete interval, so is greater
//            }
        }

        if ( obj1 instanceof RangeMultiplicity ) {
            if ( obj2 instanceof IntervalMultiplicity ) {
                return - compare(obj2,obj1);
            } // else if ( obj2 instanceof RangeMultiplicity ) {
            int diffInMinimum = obj1.getMinimum() - obj2.getMinimum();
            int diffInMaximum = obj1.getMaximum() - obj2.getMaximum();
            if (diffInMinimum != 0) {
                return diffInMinimum;
            }
            if (diffInMaximum != 0) {
                return diffInMaximum;
            }
            RangeMultiplicity r1 = (RangeMultiplicity) obj1;
            RangeMultiplicity r2 = (RangeMultiplicity) obj2;
            Iterator r1Iter = r1.getIntervals().iterator();
            Iterator r2Iter = r2.getIntervals().iterator();
            while ( r1Iter.hasNext() ) {
                if ( ! r2Iter.hasNext() ) {
                    return 1;
                }
                IntervalMultiplicity r1Interval = (IntervalMultiplicity) r1Iter.next();
                IntervalMultiplicity r2Interval = (IntervalMultiplicity) r2Iter.next();
                int diff = r1Interval.compareTo(r2Interval);
                if ( diff != 0 ) {
                    return diff;
                }
            }
            return obj1.compareFlags(obj2);
//            }

        }

        throw new IllegalArgumentException(CommonPlugin.Util.getString(ErrorMessageKeys.OBJECT_ERR_0006,obj1));
    }

}


final class UnlimitedMultiplicity extends Multiplicity {
    UnlimitedMultiplicity() {
        super();
    }

    UnlimitedMultiplicity( boolean isOrdered, boolean isUnique ) {
        super(isOrdered,isUnique);
    }

    public int getMaximum() {
        return Multiplicity.UNBOUNDED_VALUE;
    }

    public int getMinimum() {
        return 0;
    }

    public boolean isUnlimited() {
        return true;
    }

    public boolean isIncluded( int cardinality ) {
        return true;
    }

    public String toString() {
        return Multiplicity.UNBOUNDED_DEFINITION;
    }

    public int compareTo(Object obj) {
        if (obj == null) {
            throw new IllegalArgumentException(CommonPlugin.Util.getString(ErrorMessageKeys.OBJECT_ERR_0007, this.getClass().getName()));
        }
        Multiplicity that = (Multiplicity)obj; // May throw ClassCastException
        if (that instanceof UnlimitedMultiplicity) {
            return super.compareFlags(that);
        }
        return 1;   // this is always greater than that
    }

    public boolean equals(Object obj) {
        // Check if instances are identical ...
        if (this == obj) {
            return true;
        }

        // Check if object can be compared to this one
        // (this includes checking for null ) ...
        if (obj instanceof UnlimitedMultiplicity) {
            // equal so far ...
            return super.compareFlags((Multiplicity)obj) == 0;
        }

        // Otherwise not comparable ...
        return false;
    }

}


final class IntervalMultiplicity extends Multiplicity {
    private int minimum = 0;
    private int maximum = Multiplicity.UNBOUNDED_VALUE;

    IntervalMultiplicity(int number) {
        super();
        if (number < 0) {
            throw new IllegalArgumentException(CommonPlugin.Util.getString(ErrorMessageKeys.OBJECT_ERR_0008));
        }
        if (number < Multiplicity.MINIMUM_SINGLE_VALUE) {
            throw new IllegalArgumentException(CommonPlugin.Util.getString(ErrorMessageKeys.OBJECT_ERR_0009));
        }
        this.maximum = number;
        this.minimum = number;
    }

    IntervalMultiplicity(int minimum, int maximum, boolean isOrdered, boolean isUnique ) {
        super(isOrdered,isUnique);
        if (maximum < 0) {
            throw new IllegalArgumentException(CommonPlugin.Util.getString(ErrorMessageKeys.OBJECT_ERR_0010,UNBOUNDED_VALUE ));
        }
        if (minimum > maximum) {
            throw new IllegalArgumentException(CommonPlugin.Util.getString(ErrorMessageKeys.OBJECT_ERR_0011));
        }
        this.maximum = maximum;
        this.minimum = minimum;
    }

    /**
     * Constructs an instance that represents the default multiplicity of "1..1"
     */
    IntervalMultiplicity() {
        this(MINIMUM_SINGLE_VALUE);
    }

    public int getMaximum() {
        return maximum;
    }

    public int getMinimum() {
        return minimum;
    }

    public boolean isUnlimited() {
        return this.maximum == UNBOUNDED_VALUE;
    }

    public boolean isIncluded( int cardinality ) {
        return ( cardinality >= minimum && cardinality <= maximum );
    }

    public String toString() {
        String result = null;
        if (this.minimum != this.maximum) {
            if (this.maximum != UNBOUNDED_VALUE) {
                result = Integer.toString(this.minimum) + RANGE_DELIMITER + Integer.toString(this.maximum);
            } else {
                result = Integer.toString(this.minimum) + RANGE_DELIMITER + UNBOUNDED_CHAR;
            }
        } else {
            result = Integer.toString(this.minimum);
        }
        return result;
    }

    public int compareTo(Object obj) {
        if (obj == null) {
            throw new IllegalArgumentException(CommonPlugin.Util.getString(ErrorMessageKeys.OBJECT_ERR_0007, this.getClass().getName()));
        }
        Multiplicity that = (Multiplicity)obj; // May throw ClassCastException
        return Multiplicity.compare(this,that);
    }

    public boolean equals(Object obj) {
        // Check if instances are identical ...
        if (this == obj) {
            return true;
        }

        // Check if object can be compared to this one
        // (this includes checking for null ) ...
        //if ( this.getClass().isInstance(obj) ) {
        if (obj instanceof IntervalMultiplicity) {
            IntervalMultiplicity that = (IntervalMultiplicity)obj;
            if (that.minimum == this.minimum && that.maximum == this.maximum) {
                // equal so far ...
                return super.compareFlags(that) == 0 ;
            }
            return false;
        }

        // Otherwise not comparable ...
        return false;
    }
}


final class RangeMultiplicity extends Multiplicity {
    private List intervals = new ArrayList();
    private Multiplicity first;
    private Multiplicity last;

    RangeMultiplicity(List intervals, boolean isOrdered, boolean isUnique ) {
        super(isOrdered,isUnique);
        this.intervals = intervals;
        if ( this.intervals == null || this.intervals.size() == 0 ) {
            throw new IllegalArgumentException(CommonPlugin.Util.getString(ErrorMessageKeys.OBJECT_ERR_0012));
        }
        this.first = (Multiplicity) this.intervals.get(0);
        this.last = (Multiplicity) this.intervals.get(this.intervals.size()-1);
    }

    public List getIntervals() {
        return intervals;
    }

    public int getMaximum() {
        return this.last.getMaximum();
    }

    public int getMinimum() {
        return this.first.getMinimum();
    }

    public boolean isUnlimited() {
        return this.last.isUnlimited();
    }

    public boolean isIncluded( int cardinality ) {
        boolean result = false;
        Iterator iter = intervals.iterator();
        while ( iter.hasNext() ) {
            Multiplicity m = (Multiplicity) iter.next();
            if ( m.isIncluded(cardinality) ) {
                result = true;
                break;
            }
        }
        return result;
    }

    public String toString() {
        StringBuffer sb = new StringBuffer();
        Iterator iter = intervals.iterator();
        if ( iter.hasNext() ) {
            sb.append(iter.next().toString());
        }
        while ( iter.hasNext() ) {
            sb.append(INTERVAL_DELIMITER_CHAR);
            sb.append(iter.next().toString());
        }
        return sb.toString();
    }

    public int compareTo(Object obj) {
        if (obj == null) {
            throw new IllegalArgumentException(CommonPlugin.Util.getString(ErrorMessageKeys.OBJECT_ERR_0007, this.getClass().getName()));
        }
        Multiplicity that = (Multiplicity)obj; // May throw ClassCastException
        return Multiplicity.compare(this,that);
    }

    public boolean equals(Object obj) {
        // Check if instances are identical ...
        if (this == obj) {
            return true;
        }

        // Check if object can be compared to this one
        if (obj instanceof Multiplicity) {
            Multiplicity that = (Multiplicity)obj;
            return Multiplicity.compare(this,that) == 0;
        }

        // Otherwise not comparable ...
        return false;
    }

}










