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

package com.metamatrix.common.util;

import java.io.Serializable;
import java.util.*;

import com.metamatrix.common.CommonPlugin;

/**
 * <p>This class can be used to represent a collection of intervals.  As new intervals are
 * added, they are merged into existing intervals to maintain the most compact description
 * of the intervals that is possible.  Interval endpoints are inclusive - a single index
 * can be identified by an interval like [1,1].</p.
 *
 * <p>For instance, if you added the intervals 1-3, 5-9, 4-4, they would be stored as 1-9.</p>
 */
public class Intervals implements Serializable {

    static int BEFORE_FIRST = 0;
    static int WITHIN_INTERVAL = 1;
    static int BETWEEN_INTERVALS = 2;
    static int AFTER_LAST = 3;

    private LinkedList intervals;

    /**
     * Constructor for Intervals.
     */
    public Intervals() {
        intervals = new LinkedList();
    }

    public Intervals(int begin, int end) {
        this();
        addInterval(begin, end);
    }
    
    /**
     * Copy constructor
     * @param intervals
     * @since 4.2
     */
    private Intervals(Collection i) {
        this.intervals = new LinkedList(i);
    }

    /**
     * True if this set of intervals contains any intervals.
     * @return True if covers anything, false otherwise
     */
    public boolean hasIntervals() {
        return (intervals.size() > 0);
    }

    /**
     * Add an interval from begin to end, inclusive.
     * @param begin Begin index
     * @param end End index
     * @throws IllegalArgumentException If begin > end
     */
    public void addInterval(int begin, int end) {
        if(begin > end) {
            throw new IllegalArgumentException(CommonPlugin.Util.getString(ErrorMessageKeys.CM_UTIL_ERR_0023));
        }

        if(intervals.size() == 0) {
            // Add first interval
            intervals.add(new Integer(begin));
            intervals.add(new Integer(end));

        } else {
            int[] locations = findLocations(intervals, begin, end);

            if(locations[0] == BEFORE_FIRST) {
                if(locations[2] == BEFORE_FIRST) {
                    intervals.addFirst(new Integer(end));
                    intervals.addFirst(new Integer(begin));
                    condense(intervals);
                    return;
                } else if(locations[2] == AFTER_LAST) {
                    intervals.clear();
                    intervals.add(new Integer(begin));
                    intervals.add(new Integer(end));
                    return;
                }
            } else if(locations[0] == AFTER_LAST) {
                intervals.addLast(new Integer(begin));
                intervals.addLast(new Integer(end));
                condense(intervals);
                return;

            } else if(locations[0] == BETWEEN_INTERVALS && locations[2] == BETWEEN_INTERVALS && locations[1] > locations[3]) {
                // Insert between intervals
                intervals.add(locations[1], new Integer(end));
                intervals.add(locations[1], new Integer(begin));
                condense(intervals);
                return;
            }

            // All other cases:

            // Merge affected intervals
            mergeIntervals(intervals, locations[1], locations[3]);

            // Expand to handle added interval
            expandInterval(intervals, locations[1], begin, end);

            // Merge adjacent intervals
            condense(intervals);
        }
    }

    /**
     * Determines the type of location and relevant interval index for the specified values.  These
     * are returned stuffed into an int[] (yes this is ugly).
     * @param intervals Intervals to examine
     * @param beginValue Begin value we're trying to place within the intervals
     * @param endValue End value we're trying to place within the intervals
     * @return Four ints, first is begin location type as defined in javadoc, second is begin index into intervals,
     * third is end location type as defined in javadoc, fourth is end index into intervals
     */
    static int[] findLocations(LinkedList intervals, int beginValue, int endValue) {
        // Initialize return structure
        int[] locations = new int[4];
        locations[0] = -1;
        locations[2] = -1;

        int interval = -1;
        Iterator iter = intervals.iterator();
        while(iter.hasNext()) {
            // Update state
            int intervalBegin = ((Integer)iter.next()).intValue();
            int intervalEnd = ((Integer)iter.next()).intValue();
            interval++;

            // Determine if beginValue is before or within current interval
            if(locations[0] == -1) {
                if(beginValue < intervalBegin) {
                    locations[0] = BETWEEN_INTERVALS;
                    locations[1] = interval*2;      // record this interval begin
                } else if(beginValue >= intervalBegin && beginValue <= intervalEnd) {
                    locations[0] = WITHIN_INTERVAL;
                    locations[1] = interval*2;      // record this interval begin
                }
            }

            // Determine if endValue is before or within current interval
            if(endValue < intervalBegin) {
                locations[2] = BETWEEN_INTERVALS;
                locations[3] = ((interval-1)*2)+1;  // record previous interval end
                break;
            } else if(endValue >= intervalBegin && endValue <= intervalEnd) {
                locations[2] = WITHIN_INTERVAL;
                locations[3] = (interval*2)+1;      // record this interval end
                break;
            }
        }

        // Check for hanging conditions
        if(locations[2] == -1) {
            locations[2] = AFTER_LAST;
            locations[3] = intervals.size() - 1;

            if(locations[0] == -1) {
                locations[0] = AFTER_LAST;
                locations[1] = intervals.size() - 2;
            }
        }

        // Check for special case beginning conditions
        if(locations[0] == BETWEEN_INTERVALS && locations[1] == 0) {
            locations[0] = BEFORE_FIRST;
        }

        if(locations[2] == BETWEEN_INTERVALS && locations[3] < 0) {
            locations[2] = BEFORE_FIRST;
        }

        return locations;
    }

    static void mergeIntervals(LinkedList intervals, int firstIntervalIndex, int lastIntervalIndex) {
        intervals.subList(firstIntervalIndex+1, lastIntervalIndex).clear();
    }

    static void expandInterval(LinkedList intervals, int firstIntervalIndex, int begin, int end) {
        // Set up iterator
        ListIterator iter = intervals.listIterator(firstIntervalIndex);

        // Get merged interval bounds
        int mergedBegin = ((Integer) iter.next()).intValue();
        int mergedEnd = ((Integer) iter.next()).intValue();

        int newBegin = Math.min(begin, mergedBegin);
        int newEnd = Math.max(end, mergedEnd);

        // Change begin and end
        iter.previous();
        iter.set(new Integer(newEnd));
        iter.previous();
        iter.set(new Integer(newBegin));
    }

    static void reduceInterval(LinkedList intervals, int firstIntervalIndex, int begin, int end) {
        // Set up iterator
        ListIterator iter = intervals.listIterator(firstIntervalIndex);

        // Get merged interval bounds
        int mergedBegin = ((Integer) iter.next()).intValue();
        int mergedEnd = ((Integer) iter.next()).intValue();

        if(begin <= mergedBegin) {
            if(end >= mergedEnd) {
                // Removed interval completely covers merged interval
                iter.previous();
                iter.remove();
                iter.previous();
                iter.remove();
                return;
            }
            // There is some left over at the right end of merged interval
            iter.previous();
            iter.previous();
            iter.set(new Integer(end+1));
        } else {
            if(end >= mergedEnd) {
                // There is some left over at the left end of merged interval
                iter.previous();
                iter.set(new Integer(begin-1));

            } else {
                // Removed interval is completely within merged interval - split in two
                iter.previous();
                iter.add(new Integer(end+1));
                iter.previous();
                iter.add(new Integer(begin-1));
            }
        }
    }

    static void condense(LinkedList intervals) {
        if(intervals.size() <= 2) {
            return;
        }

        ListIterator iter = intervals.listIterator();

        // Read first interval
        iter.next();
        int lastEnd = ((Integer) iter.next()).intValue();

        while(iter.hasNext()) {
            int begin = ((Integer) iter.next()).intValue();
            if(begin == lastEnd + 1) {
                iter.remove();
                iter.previous();
                iter.remove();
            }
            lastEnd = ((Integer) iter.next()).intValue();
        }
    }

    /**
     * Remove an interval from begin to end, inclusive.  If the
     * current intervals do not contain anything in removed interval,
     * no error is thrown.
     * @param begin Begin index
     * @param end End index
     * @throws IllegalArgumentException If begin > end
     */
    public void removeInterval(int begin, int end) {
        if(begin > end) {
            throw new IllegalArgumentException(CommonPlugin.Util.getString(ErrorMessageKeys.CM_UTIL_ERR_0024));
        }

        // Check whether we have any intervals
        if(intervals.size() == 0) {
            return;
        }

        // Check the locations of begin and end across locations
        int[] locations = findLocations(intervals, begin, end);

        if(locations[0] == BEFORE_FIRST) {
            if(locations[2] == BEFORE_FIRST) {
                return;
            } else if(locations[2] == AFTER_LAST) {
                intervals.clear();
                return;
            }
        } else if(locations[0] == AFTER_LAST) {
            return;

        } else if(locations[0] == BETWEEN_INTERVALS && locations[2] == BETWEEN_INTERVALS && locations[1] > locations[3]) {
            // Between intervals
            return;
        }

        // All other cases:

        // Merge affected intervals
        mergeIntervals(intervals, locations[1], locations[3]);

        // Expand to handle added interval
        reduceInterval(intervals, locations[1], begin, end);

        // Merge adjacent intervals
        condense(intervals);
    }

    public Intervals removeIntervals(Intervals intervalsToRemove) {
        List intervalList = intervalsToRemove.getIntervals();
        for (Iterator iterator = intervalList.iterator(); iterator.hasNext(); ) {
            int[] interval = (int[]) iterator.next();
            removeInterval(interval[0], interval[1]);
        }
        return this;
    }
    
    /**
     * Returns true if the specified interval is contained in the
     * current intervals.
     * @param begin Begin index
     * @param end End index
     * @throws IllegalArgumentException If begin > end
     */
    public boolean containsInterval(int begin, int end) {
        if(begin > end) {
            throw new IllegalArgumentException(CommonPlugin.Util.getString(ErrorMessageKeys.CM_UTIL_ERR_0025));
        }

        // Check whether we have any intervals
        int size = intervals.size();
        if(size == 0) {
            return false;
        } else if (size == 2) {
            return (begin >= ((Integer) intervals.getFirst()).intValue() && end <= ((Integer) intervals.getLast()).intValue());
        }

        // Check whether the interval is contained completely within an existing interval
        int[] locations = findLocations(intervals, begin, end);
        if(locations[0] == WITHIN_INTERVAL && locations[2] == WITHIN_INTERVAL && locations[1]+1 == locations[3]) {
            return true;
        }
        return false;
    }

    /**
     * Determine the portion of the current intervals that overlaps with the
     * specified interval.  For example, if the current intervals were [1-5, 10-15]
     * and we asked for the overlap with region [4-11], we would get a result
     * [4-5, 10-11].
     *
     * @param begin
     * @param end
     * @return Intervals set defining overlap
     * @throws IllegalArgumentException If begin > end
     */
    public Intervals getIntersection(int begin, int end) {
        if(begin > end) {
            throw new IllegalArgumentException(CommonPlugin.Util.getString(ErrorMessageKeys.CM_UTIL_ERR_0026));
        }

        Intervals overlap = new Intervals();

        // Check whether we have any intervals
        if(intervals.size() == 0) {
            return overlap;
        }

        // Determine intersection points
        int[] locations = findLocations(intervals, begin, end);
        if(locations[2] == BEFORE_FIRST || locations[0] == AFTER_LAST) {
            // Completely before or after
            return overlap;
        } else if(locations[0] == BETWEEN_INTERVALS && locations[2] == BETWEEN_INTERVALS && locations[1] > locations[3]) {
            // Completely between intervals
            return overlap;
        } else if(locations[0] == WITHIN_INTERVAL && locations[2] == WITHIN_INTERVAL && locations[1]+1 == locations[3]) {
            // Completely within an interval
            overlap.addInterval(begin, end);
            return overlap;
        } else {
            // There is an overlap of one or more intervals
            int startIndex = locations[1];
            int endIndex = locations[3];

            // Determine partial beginning interval
            overlap.addInterval(Math.max(((Integer)intervals.get(startIndex)).intValue(), begin),
                                Math.min(((Integer)intervals.get(startIndex+1)).intValue(), end) );

            // Add all intervals in the middle, if there are any
            int diff = endIndex-startIndex;
            if(diff > 3) {
                Iterator internalIter = intervals.listIterator(startIndex+2);
                int stop = diff-3;
                for(int i=0; i < stop; i=i+2) {
                    overlap.addInterval( ((Integer) internalIter.next()).intValue(),
                                         ((Integer) internalIter.next()).intValue() );
                }
            }

            // Determine partial ending interval
            if(diff > 1) {
                overlap.addInterval(Math.max(((Integer)intervals.get(endIndex-1)).intValue(), begin),
                                    Math.min(((Integer)intervals.get(endIndex)).intValue(), end) );
            }

            return overlap;
        }
    }
    
    public Intervals getIntersectionIntervals(Intervals intersectionIntervals) {
        Intervals result = new Intervals();
        List intervalList = intersectionIntervals.getIntervals();
        for (Iterator iterator = intervalList.iterator(); iterator.hasNext(); ) {
            int[] interval = (int[]) iterator.next();
            result.addIntervals(getIntersection(interval[0], interval[1]));
        }
        return result;
    }
    
    public Intervals addIntervals(Intervals additionalIntervals) {
        List intervalList = additionalIntervals.getIntervals();
        for (Iterator iterator = intervalList.iterator(); iterator.hasNext(); ) {
            int[] interval = (int[]) iterator.next();
            addInterval(interval[0], interval[1]);
        }
        return this;
    }

    /**
     * Return ordered list of intervals representing current intervals.
     * Each element of the returned list is an int[2] representing a
     * begin/end pair of included interval.
     * @return Ordered list of int[2] representing intervals
     */
    public List getIntervals() {
        List list = new ArrayList();
        Iterator iter = intervals.iterator();
        while(iter.hasNext()) {
            list.add(new int[] { ((Integer)iter.next()).intValue(), ((Integer)iter.next()).intValue() });
        }
        return list;
    }

    /**
     * Determine there is one interval with no gaps.
     * Returns true for an empty interval.
     * @return Boolean indicator of whether there is one interval.
     */
    public boolean isContiguous() {
    	return intervals.size() <= 2;
    }
    
    /**
     * Compares two intervals for equality
     * @param obj Other object
     * @return True if this equal to obj
     */
    public boolean equals(Object obj) {
        if(this == obj) {
            return true;
        }

        if(obj == null || ! (obj instanceof Intervals)) {
            return false;
        }
        Intervals other = (Intervals) obj;

        List thisIntervals = getIntervals();
        List otherIntervals = other.getIntervals();

        if(thisIntervals.size() != otherIntervals.size()) {
            return false;
        }

        for(int i=0; i<thisIntervals.size(); i++) {
            int[] thisInt = (int[]) thisIntervals.get(i);
            int[] otherInt = (int[]) otherIntervals.get(i);

            if(thisInt[0] != otherInt[0] || thisInt[1] != otherInt[1]) {
                return false;
            }
        }

        return true;
    }

    /**
     * Returns hash code for the set of intervals.  This hash code is
     * based on the intervals and WILL change with any add or remove
     * call.  This requires re-hashing the object.
     * @return Hash code
     */
    public int hashCode() {
        if(intervals.size() == 0) {
            return 0;
        } 

        return intervals.getLast().hashCode();
    }

    /**
     * Returns string representation of intervals.  This should be used for debugging only.
     * @return String representing intervals
     */
    public String toString() {
        return intervals.toString();
    }

    public int[] getBoundingInterval() {
        int[] result = new int[2];
        if (hasIntervals()) {
            result[0] = ((Integer) intervals.getFirst()).intValue();
            result[1] = ((Integer) intervals.getLast()).intValue();
        } else {
            result[0] = Integer.MIN_VALUE;
            result[1] = Integer.MAX_VALUE;
         }
        return result;
    }
    
    public Intervals copy() {
        return new Intervals(this.intervals);
    }
}
