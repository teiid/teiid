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

package com.metamatrix.console.util;

import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

/**
 * Class with methods to handle a static quicksort.  Requires that caller
 * implements Compares interface to order any two given items.
 */
public class StaticQuickSorter {

//Static methods

    /**
     * Do a quick sort on an array of objects.
     *
     * @param array         array of objects
     * @param comparator    caller implementation of Compares interface
     * @return              new array representing array in sorted order
     */
    public static Object[] quickSort(Object[] array, Compares comparator) {
        Object[] newArray = new Object[array.length];
        for (int i = 0; i < array.length; i++) {
            newArray[i] = array[i];
        }
        if (newArray.length > 1) {
            sort(newArray, 0, newArray.length - 1, comparator);
        }
        return newArray;
    }

    /**
     * Do a quick sort using default comparator.
     *
     * @param array         array of objects
     * @return              new array representing array in sorted order
     */
    public static Object[] quickSort(Object[] array) {
        return quickSort(array, new DefaultComparer());
    }

    /**
     * Do a quick sort starting from a List.
     *
     * @param list          List of objects to be sorted
     * @param comparator    Caller implementation of Compares interface
     * @return              new array representing items of list in sorted order
     */
    public static Object[] quickSort(List list, Compares comparator) {
        Object[] array = new Object[list.size()];
        Iterator it = list.iterator();
        int loc = 0;
        while (it.hasNext()) {
            array[loc] = it.next();
            loc++;
        }
        return quickSort(array, comparator);
    }

    /**
     * Do a quick sort starting from a List and using default comparator.
     *
     * @param list          List of objects to be sorted
     * @return              new array representing items of list in sorted order
     */
    public static Object[] quickSort(List list) {
        return quickSort(list, new DefaultComparer());
    }

    /**
     * Do a quick sort of a String array, using default comparator.
     *
     * @param array         String array
     * @return              new array representing items of array in sorted order
     */
    public static String[] quickStringSort(String[] array) {
        String[] result = new String[array.length];
        Object[] obj = new Object[array.length];
        for (int i = 0; i < array.length; i++) {
            obj[i] = array[i];
        }
        obj = quickSort(obj);
        for (int i = 0; i < obj.length; i++) {
            result[i] = (String)obj[i];
        }
        return result;
    }

    public static Collection /*<String>*/ quickStringCollectionSort(
            Collection /*<String>*/ strings) {
        Object[] unsorted = new Object[strings.size()];
        Iterator it = strings.iterator();
        for (int i = 0; it.hasNext(); i++) {
            unsorted[i] = it.next();
        }
        Object[] sorted = quickSort(unsorted);
        return Arrays.asList(sorted);
    }
    
    /**
     * Internal recursively called quick sorting method.
     *
     * @param array         array to be sorted
     * @param lo0           first index in array for current sort or sub-sort
     * @param hi0           last index in array for current sort of sub-sort
     * @param comparator    caller-supplied implementation of Compares used in sort
     */
    private static void sort(Object[] array, int lo0, int hi0, Compares comparator) {
        int lo = lo0;
        int hi = hi0;
        Object mid;
        if (hi0 > lo0) {
            mid = array[(lo0 + hi0) / 2];
            while (lo <= hi) {
                while ((lo < hi0) && (comparator.compare(array[lo], mid) < 0)) {
                    ++lo;
                }
                while ((hi > lo0) && (comparator.compare(array[hi], mid) > 0)) {
                    --hi;
                }
                if (lo <= hi) {
                    Object temp = array[hi];
                    array[hi] = array[lo];
                    array[lo] = temp;
                    ++lo;
                    --hi;
                }
            }
            if (lo0 < hi) {
                sort(array, lo0, hi, comparator);
            }
            if (lo < hi0) {
                sort(array, lo, hi0, comparator);
            }
        }
    }

    /**
     * Another useful auxilliary method: find the index in a sorted String array
     * of a given string by doing a binary search.  Returns -1 if not found.
     * Assumes the array is sorted ignoring case.
     *
     * @param array         input array which must be sorted ignoring case
     * @param string        String to look for in array
     * @return              index of string in array; -1 if not found
     */
    public static int sortedStringArrayIndex(String[] array, String string) {
        int loc = -1;
        if (array.length > 0) {
            int low = 0;
            int high = array.length - 1;
            int current = ((low + high) / 2);
            boolean continuing = true;
            while (continuing) {
                String curString = array[current];
                int compResult = string.compareToIgnoreCase(curString);
                if (compResult == 0) {
                    loc = current;
                    continuing = false;
                } else {
                    if (high == low) {
                        continuing = false;
                    } else {
                        if (compResult < 0) {
                            if (current == 0) {
                                continuing = false;
                            } else {
                                high = current;
                                current = ((low + high) / 2);
                            }
                        } else {
                            if (current == array.length - 1) {
                                continuing = false;
                            } else {
                                if ((current == low) && (low == high - 1)) {
                                    low = high;
                                } else {
                                    low = current;
                                }
                                current = ((low + high) / 2);
                            }
                        }
                    }
                }
            }
        }
        return loc;
    }

    public static int unsortedStringArrayIndex(String[] array, String string) {
        int matchLoc = -1;
        int loc = 0;
        while ((matchLoc < 0) && (loc < array.length)) {
            if (array[loc].equals(string)) {
                matchLoc = loc;
            } else {
                loc++;
            }
        }
        return matchLoc;
    }

    public static int stringArrayIndex(String[] array, String string) {
        return StaticQuickSorter.sortedStringArrayIndex(array, string);
    }
}//end StaticQuickSorter

//Auxilliary classes

/**
 * Default implementation of Compares used within StaticQuickSorter
 */
class DefaultComparer implements Compares {
    public DefaultComparer() {
        super();
    }

    /**
     * Do the comparison using toString() of each object
     */
    public int compare(Object first, Object second) {
        String firstStr = first.toString();
        String secondStr = second.toString();
        return firstStr.compareToIgnoreCase(secondStr);
    }
}

