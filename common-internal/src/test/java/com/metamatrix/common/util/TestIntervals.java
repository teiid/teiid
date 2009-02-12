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

package com.metamatrix.common.util;

import java.util.LinkedList;

import junit.framework.TestCase;

/**
 */
public class TestIntervals extends TestCase {

    /**
     * Constructor for TestIntervals.
     * @param arg0
     */
    public TestIntervals(String arg0) {
        super(arg0);
    }

    private LinkedList toLinkedList(int[] array) {
        LinkedList intervals = new LinkedList();
        for(int i=0; i<array.length; i++) {
            intervals.add(new Integer(array[i]));
        }
        return intervals;
    }

    public void helpTestFindLocations(int[] intervalArray, int begin, int end, int[] expected) {
        int[] actual = Intervals.findLocations(toLinkedList(intervalArray), begin, end);

        assertEquals("Unexpected begin type ", expected[0], actual[0]); //$NON-NLS-1$
        assertEquals("Unexpected begin index ", expected[1], actual[1]); //$NON-NLS-1$
        assertEquals("Unexpected end type ", expected[2], actual[2]); //$NON-NLS-1$
        assertEquals("Unexpected end index ", expected[3], actual[3]); //$NON-NLS-1$
    }

    // one range - all before first
    public void testFindLocations1() {
        int[] intervals = new int[] { 5, 9 };
        int[] expected = new int[] { Intervals.BEFORE_FIRST, 0, Intervals.BEFORE_FIRST, -1 };
        helpTestFindLocations(intervals, 0, 1, expected);
    }

    // one range - all after last
    public void testFindLocations2() {
        int[] intervals = new int[] { 5, 9 };
        int[] expected = new int[] { Intervals.AFTER_LAST, 0, Intervals.AFTER_LAST, 1 };
        helpTestFindLocations(intervals, 10, 11, expected);
    }

    // one range - overlap begin
    public void testFindLocations3() {
        int[] intervals = new int[] { 5, 9 };
        int[] expected = new int[] { Intervals.BEFORE_FIRST, 0, Intervals.WITHIN_INTERVAL, 1 };
        helpTestFindLocations(intervals, 0, 7, expected);
    }

    // one range - overlap end
    public void testFindLocations4() {
        int[] intervals = new int[] { 5, 9 };
        int[] expected = new int[] { Intervals.WITHIN_INTERVAL, 0, Intervals.AFTER_LAST, 1 };
        helpTestFindLocations(intervals, 6, 10, expected);
    }

    // one range - within
    public void testFindLocations5() {
        int[] intervals = new int[] { 5, 9 };
        int[] expected = new int[] { Intervals.WITHIN_INTERVAL, 0, Intervals.WITHIN_INTERVAL, 1 };
        helpTestFindLocations(intervals, 6, 7, expected);
    }

    // multiple ranges - before, within
    public void testFindLocations6() {
        int[] intervals = new int[] { 5, 10, 15, 20, 25, 30 };
        int[] expected = new int[] { Intervals.BEFORE_FIRST, 0, Intervals.WITHIN_INTERVAL, 3 };
        helpTestFindLocations(intervals, 0, 17, expected);
    }

    // multiple ranges - within, between
    public void testFindLocations7() {
        int[] intervals = new int[] { 5, 10, 15, 20, 25, 30 };
        int[] expected = new int[] { Intervals.WITHIN_INTERVAL, 0, Intervals.BETWEEN_INTERVALS, 1 };
        helpTestFindLocations(intervals, 6, 12, expected);
    }

    // multiple ranges - within, within
    public void testFindLocations8() {
        int[] intervals = new int[] { 5, 10, 15, 20, 25, 30 };
        int[] expected = new int[] { Intervals.WITHIN_INTERVAL, 0, Intervals.WITHIN_INTERVAL, 5 };
        helpTestFindLocations(intervals, 6, 27, expected);
    }

    // multiple ranges - between, within
    public void testFindLocations9() {
        int[] intervals = new int[] { 5, 10, 15, 20, 25, 30 };
        int[] expected = new int[] { Intervals.BETWEEN_INTERVALS, 2, Intervals.WITHIN_INTERVAL, 5 };
        helpTestFindLocations(intervals, 12, 27, expected);
    }

    // multiple ranges - between, after
    public void testFindLocations10() {
        int[] intervals = new int[] { 5, 10, 15, 20, 25, 30 };
        int[] expected = new int[] { Intervals.BETWEEN_INTERVALS, 2, Intervals.AFTER_LAST, 5 };
        helpTestFindLocations(intervals, 12, 31, expected);
    }

    // multiple ranges - within, after
    public void testFindLocations11() {
        int[] intervals = new int[] { 5, 10, 15, 20, 25, 30 };
        int[] expected = new int[] { Intervals.WITHIN_INTERVAL, 2, Intervals.AFTER_LAST, 5 };
        helpTestFindLocations(intervals, 17, 31, expected);
    }

    public void helpTestMergeIntervals(int[] intervalArray, int firstIndex, int lastIndex, int[] expected) {
        LinkedList intervals = toLinkedList(intervalArray);
        Intervals.mergeIntervals(intervals, firstIndex, lastIndex);

        assertEquals("Didn't get expected merge results ", toLinkedList(expected), intervals); //$NON-NLS-1$
    }

    public void testMergeIntervals1() {
        int[] intervals = new int[] { 5, 10 };
        int[] expected = new int[] { 5, 10 };
        helpTestMergeIntervals(intervals, 0, 1, expected);
    }

    public void testMergeIntervals2() {
        int[] intervals = new int[] { 5, 10, 15, 20, 25, 30, 35, 40 };
        int[] expected = new int[] { 5, 40 };
        helpTestMergeIntervals(intervals, 0, 7, expected);
    }

    public void testMergeIntervals3() {
        int[] intervals = new int[] { 5, 10, 15, 20, 25, 30, 35, 40 };
        int[] expected = new int[] { 5, 10, 15, 30, 35, 40 };
        helpTestMergeIntervals(intervals, 2, 5, expected);
    }

    public void testMergeIntervals4() {
        int[] intervals = new int[] { 5, 10, 15, 20, 25, 30, 35, 40 };
        int[] expected = new int[] { 5, 10, 15, 40 };
        helpTestMergeIntervals(intervals, 2, 7, expected);
    }

    public void helpTestExpandIntervals(int[] intervalArray, int firstIndex, int begin, int end, int[] expected) {
        LinkedList intervals = toLinkedList(intervalArray);
        Intervals.expandInterval(intervals, firstIndex, begin, end);

        assertEquals("Didn't get expected expanded results ", toLinkedList(expected), intervals); //$NON-NLS-1$
    }

    public void testExpandInterval1() {
        int[] intervals = new int[] { 5, 10 };
        int[] expected = new int[] { 0, 10 };
        helpTestExpandIntervals(intervals, 0, 0, 9, expected);
    }

    public void testExpandInterval2() {
        int[] intervals = new int[] { 5, 10 };
        int[] expected = new int[] { 5, 10 };
        helpTestExpandIntervals(intervals, 0, 6, 9, expected);
    }

    public void testExpandInterval3() {
        int[] intervals = new int[] { 5, 10 };
        int[] expected = new int[] { 0, 15 };
        helpTestExpandIntervals(intervals, 0, 0, 15, expected);
    }

    public void testExpandInterval4() {
        int[] intervals = new int[] { 5, 10 };
        int[] expected = new int[] { 5, 15 };
        helpTestExpandIntervals(intervals, 0, 6, 15, expected);
    }

    public void helpTestCondenseIntervals(int[] intervalArray, int[] expected) {
        LinkedList intervals = toLinkedList(intervalArray);
        Intervals.condense(intervals);
        assertEquals("Didn't get expected condensed results ", toLinkedList(expected), intervals); //$NON-NLS-1$
    }

    public void testCondensedInterval1() {
        int[] intervals = new int[] { 5, 10 };
        int[] expected = new int[] { 5, 10 };
        helpTestCondenseIntervals(intervals, expected);
    }

    public void testCondensedInterval2() {
        int[] intervals = new int[] { 5, 10, 11, 15 };
        int[] expected = new int[] { 5, 15 };
        helpTestCondenseIntervals(intervals, expected);
    }

    public void testCondensedInterval3() {
        int[] intervals = new int[] { 5, 10, 11, 15, 16, 20 };
        int[] expected = new int[] { 5, 20 };
        helpTestCondenseIntervals(intervals, expected);
    }

    public void testCondensedInterval4() {
        int[] intervals = new int[] { 5, 10, 11, 15, 20, 25, 26, 30 };
        int[] expected = new int[] { 5, 15, 20, 30 };
        helpTestCondenseIntervals(intervals, expected);
    }

    public void testAddIntervalDistinct() {
        Intervals i = new Intervals();
        i.addInterval(5, 10);
        i.addInterval(15, 20);
        i.addInterval(25, 30);
        String expected = "[5, 10, 15, 20, 25, 30]"; //$NON-NLS-1$

        assertEquals("Invalid after adds ", expected, i.toString());     //$NON-NLS-1$
    }

    public void testAddIntervalBeforeBeginning() {
        Intervals i = new Intervals();
        i.addInterval(5, 10);
        i.addInterval(0, 1);
        String expected = "[0, 1, 5, 10]"; //$NON-NLS-1$

        assertEquals("Invalid after adds ", expected, i.toString());     //$NON-NLS-1$
    }

    public void testAddIntervalAtBeginning() {
        Intervals i = new Intervals();
        i.addInterval(5, 10);
        i.addInterval(2, 5);
        String expected = "[2, 10]"; //$NON-NLS-1$

        assertEquals("Invalid after adds ", expected, i.toString());     //$NON-NLS-1$
    }

    public void testAddIntervalAtEnd() {
        Intervals i = new Intervals();
        i.addInterval(5, 10);
        i.addInterval(9, 15);
        String expected = "[5, 15]"; //$NON-NLS-1$

        assertEquals("Invalid after adds ", expected, i.toString());     //$NON-NLS-1$
    }

    public void testAddIntervalOverlapping1() {
        Intervals i = new Intervals();
        i.addInterval(5, 10);
        i.addInterval(7, 15);
        i.addInterval(12, 20);
        String expected = "[5, 20]"; //$NON-NLS-1$

        assertEquals("Invalid after adds ", expected, i.toString());     //$NON-NLS-1$
    }

    public void testAddIntervalOverlapping2() {
        Intervals i = new Intervals();
        i.addInterval(5, 10);
        i.addInterval(7, 15);
        i.addInterval(2, 5);
        String expected = "[2, 15]"; //$NON-NLS-1$

        assertEquals("Invalid after adds ", expected, i.toString());     //$NON-NLS-1$
    }

    public void testAddIntervalOverlapping3() {
        Intervals i = new Intervals();
        i.addInterval(5, 10);
        i.addInterval(15, 20);
        i.addInterval(6, 19);
        String expected = "[5, 20]"; //$NON-NLS-1$

        assertEquals("Invalid after adds ", expected, i.toString());     //$NON-NLS-1$
    }

    public void testAddIntervalExpand() {
        Intervals i = new Intervals();
        i.addInterval(5, 10);
        i.addInterval(2, 20);
        String expected = "[2, 20]"; //$NON-NLS-1$

        assertEquals("Invalid after adds ", expected, i.toString());     //$NON-NLS-1$
    }

    public void testAddIntervalInsert() {
        Intervals i = new Intervals();
        i.addInterval(5, 10);
        i.addInterval(15, 20);
        i.addInterval(12, 13);
        String expected = "[5, 10, 12, 13, 15, 20]"; //$NON-NLS-1$

        assertEquals("Invalid after adds ", expected, i.toString());     //$NON-NLS-1$
    }

    public void testAddIntervalWithin() {
        Intervals i = new Intervals();
        i.addInterval(5, 10);
        i.addInterval(6, 7);
        String expected = "[5, 10]"; //$NON-NLS-1$

        assertEquals("Invalid after adds ", expected, i.toString());     //$NON-NLS-1$
    }

    public void testAddIntervalBetweenBetween() {
        Intervals i = new Intervals();
        i.addInterval(5, 10);
        i.addInterval(15, 20);
        i.addInterval(25, 30);
        i.addInterval(12, 22);
        String expected = "[5, 10, 12, 22, 25, 30]"; //$NON-NLS-1$

        assertEquals("Invalid after adds ", expected, i.toString());     //$NON-NLS-1$
    }

    public void testAddIntervalBetweenWithin() {
        Intervals i = new Intervals();
        i.addInterval(5, 10);
        i.addInterval(15, 20);
        i.addInterval(25, 30);
        i.addInterval(12, 27);
        String expected = "[5, 10, 12, 30]"; //$NON-NLS-1$

        assertEquals("Invalid after adds ", expected, i.toString());     //$NON-NLS-1$
    }

    public void testAddIntervalComplex() {
        Intervals i = new Intervals();
        i.addInterval(5, 10);
        i.addInterval(15, 20);
        i.addInterval(25, 30);
        i.addInterval(12, 27);
        i.addInterval(11, 11);
        String expected = "[5, 30]"; //$NON-NLS-1$

        assertEquals("Invalid after adds ", expected, i.toString());     //$NON-NLS-1$
    }

    public void testAddIntervalError() {
        Intervals i = new Intervals();
        try {
            i.addInterval(5, 3);
            fail("Expected to get IllegalArgumentException when adding illegal interval"); //$NON-NLS-1$
        } catch(IllegalArgumentException e) {
        }
    }

    public void testRemoveInterval1() {
        Intervals i = new Intervals();
        i.removeInterval(0, 10);
        String expected = "[]";    //$NON-NLS-1$
        assertEquals("Invalid after remove ", expected, i.toString());     //$NON-NLS-1$
    }

    public void testRemoveInterval2() {
        Intervals i = new Intervals();
        i.addInterval(5, 10);
        i.removeInterval(0, 2);
        String expected = "[5, 10]"; //$NON-NLS-1$
        assertEquals("Invalid after remove ", expected, i.toString());     //$NON-NLS-1$
    }

    public void testRemoveInterval3() {
        Intervals i = new Intervals();
        i.addInterval(5, 10);
        i.removeInterval(0, 5);
        String expected = "[6, 10]";    //$NON-NLS-1$
        assertEquals("Invalid after remove ", expected, i.toString());     //$NON-NLS-1$
    }

    public void testRemoveInterval4() {
        Intervals i = new Intervals();
        i.addInterval(5, 10);
        i.removeInterval(0, 6);
        String expected = "[7, 10]";    //$NON-NLS-1$
        assertEquals("Invalid after remove ", expected, i.toString());     //$NON-NLS-1$
    }

    public void testRemoveInterval5() {
        Intervals i = new Intervals();
        i.addInterval(5, 10);
        i.removeInterval(4, 8);
        String expected = "[9, 10]";    //$NON-NLS-1$
        assertEquals("Invalid after remove ", expected, i.toString());     //$NON-NLS-1$
    }

    public void testRemoveInterval6() {
        Intervals i = new Intervals();
        i.addInterval(5, 10);
        i.removeInterval(7, 10);
        String expected = "[5, 6]";    //$NON-NLS-1$
        assertEquals("Invalid after remove ", expected, i.toString());     //$NON-NLS-1$
    }

    public void testRemoveInterval7() {
        Intervals i = new Intervals();
        i.addInterval(5, 10);
        i.removeInterval(7, 11);
        String expected = "[5, 6]";    //$NON-NLS-1$
        assertEquals("Invalid after remove ", expected, i.toString());     //$NON-NLS-1$
    }

    public void testRemoveInterval8() {
        Intervals i = new Intervals();
        i.addInterval(5, 10);
        i.removeInterval(5, 10);
        String expected = "[]";    //$NON-NLS-1$
        assertEquals("Invalid after remove ", expected, i.toString());     //$NON-NLS-1$
    }

    public void testRemoveInterval9() {
        Intervals i = new Intervals();
        i.addInterval(5, 10);
        i.removeInterval(6, 9);
        String expected = "[5, 5, 10, 10]";    //$NON-NLS-1$
        assertEquals("Invalid after remove ", expected, i.toString());     //$NON-NLS-1$
    }

    public void testRemoveInterval10() {
        Intervals i = new Intervals();
        i.addInterval(5, 10);
        i.addInterval(15, 20);
        i.removeInterval(12, 14);
        String expected = "[5, 10, 15, 20]";    //$NON-NLS-1$
        assertEquals("Invalid after remove ", expected, i.toString());     //$NON-NLS-1$
    }

    public void testRemoveInterval11() {
        Intervals i = new Intervals();
        i.addInterval(5, 10);
        i.addInterval(15, 20);
        i.removeInterval(8, 17);
        String expected = "[5, 7, 18, 20]";    //$NON-NLS-1$
        assertEquals("Invalid after remove ", expected, i.toString());     //$NON-NLS-1$
    }

    public void testRemoveInterval12() {
        Intervals i = new Intervals();
        i.addInterval(5, 10);
        i.addInterval(15, 20);
        i.removeInterval(12, 17);
        String expected = "[5, 10, 18, 20]";    //$NON-NLS-1$
        assertEquals("Invalid after remove ", expected, i.toString());     //$NON-NLS-1$
    }

    public void testRemoveInterval13() {
        Intervals i = new Intervals();
        i.addInterval(5, 10);
        i.addInterval(15, 20);
        i.removeInterval(7, 25);
        String expected = "[5, 6]";    //$NON-NLS-1$
        assertEquals("Invalid after remove ", expected, i.toString());     //$NON-NLS-1$
    }

    public void testRemoveInterval14() {
        Intervals i = new Intervals();
        i.addInterval(2, 3);
        i.removeInterval(0, 4);
        assertEquals("Invalid after remove ", "[]", i.toString());     //$NON-NLS-1$ //$NON-NLS-2$
    }

    public void testRemoveInterval15() {
        Intervals i = new Intervals();
        i.addInterval(2, 3);
        i.removeInterval(4, 5);
        assertEquals("Invalid after remove ", "[2, 3]", i.toString());     //$NON-NLS-1$ //$NON-NLS-2$
    }

    public void testRemoveIntervalInvalid() {
        Intervals i = new Intervals();
        try {
            i.removeInterval(1, 0);
            fail("Expected IllegalArgumentException when removing invalid interval"); //$NON-NLS-1$
        } catch(IllegalArgumentException e) {
        }
    }

    public void testContainsInterval1() {
        Intervals i = new Intervals();
        assertEquals("Contains is wrong ", false, i.containsInterval(0, 1));     //$NON-NLS-1$
    }

    public void testContainsInterval2() {
        Intervals i = new Intervals();
        i.addInterval(5, 10);
        assertEquals("Contains is wrong ", true, i.containsInterval(5, 10));     //$NON-NLS-1$
    }

    public void testContainsInterval3() {
        Intervals i = new Intervals();
        i.addInterval(5, 10);
        assertEquals("Contains is wrong ", true, i.containsInterval(6, 9));     //$NON-NLS-1$
    }

    public void testContainsInterval4() {
        Intervals i = new Intervals();
        i.addInterval(5, 10);
        assertEquals("Contains is wrong ", false, i.containsInterval(1, 2));     //$NON-NLS-1$
    }

    public void testContainsInterval5() {
        Intervals i = new Intervals();
        i.addInterval(5, 10);
        assertEquals("Contains is wrong ", false, i.containsInterval(20, 21));     //$NON-NLS-1$
    }

    public void testContainsInterval6() {
        Intervals i = new Intervals();
        i.addInterval(5, 10);
        assertEquals("Contains is wrong ", false, i.containsInterval(0, 7));     //$NON-NLS-1$
    }

    public void testContainsInterval7() {
        Intervals i = new Intervals();
        i.addInterval(5, 10);
        assertEquals("Contains is wrong ", false, i.containsInterval(7, 20));     //$NON-NLS-1$
    }

    public void testContainsInterval8() {
        Intervals i = new Intervals();
        i.addInterval(5, 10);
        i.addInterval(12, 15);
        assertEquals("Contains is wrong ", false, i.containsInterval(5, 15));     //$NON-NLS-1$
    }

    public void testContainsIntervalInvalid() {
        Intervals i = new Intervals();
        try {
            i.containsInterval(1, 0);
            fail("Expected IllegalArgumentException when checking containment of invalid interval"); //$NON-NLS-1$
        } catch(IllegalArgumentException e) {
        }
    }

    public void testIntersection1() {
        Intervals i = new Intervals();
        i.addInterval(5, 10);
        Intervals expected = new Intervals();
        assertEquals("Intersection is wrong ", expected, i.getIntersection(0, 1)); //$NON-NLS-1$
    }

    public void testIntersection2() {
        Intervals i = new Intervals();
        i.addInterval(5, 10);
        Intervals expected = new Intervals();
        expected.addInterval(5, 7);
        assertEquals("Intersection is wrong ", expected, i.getIntersection(0, 7)); //$NON-NLS-1$
    }

    public void testIntersection3() {
        Intervals i = new Intervals();
        i.addInterval(5,10);
        Intervals expected = new Intervals();
        expected.addInterval(5, 10);
        assertEquals("Intersection is wrong ", expected, i.getIntersection(0, 12)); //$NON-NLS-1$
    }
    public void testIntersection4() {
        Intervals i = new Intervals();
        i.addInterval(5,10);
        Intervals expected = new Intervals();
        expected.addInterval(6, 9);
        assertEquals("Intersection is wrong ", expected, i.getIntersection(6, 9)); //$NON-NLS-1$
    }

    public void testIntersection5() {
        Intervals i = new Intervals();
        i.addInterval(5,10);
        Intervals expected = new Intervals();
        expected.addInterval(6, 10);
        assertEquals("Intersection is wrong ", expected, i.getIntersection(6, 12)); //$NON-NLS-1$
    }

    public void testIntersection6() {
        Intervals i = new Intervals();
        i.addInterval(5, 10);
        i.addInterval(15, 20);
        i.addInterval(25, 30);
        Intervals expected = new Intervals();
        expected.addInterval(6, 10);
        expected.addInterval(15, 16);
        assertEquals("Intersection is wrong ", expected, i.getIntersection(6, 16)); //$NON-NLS-1$
    }

    public void testIntersection7() {
        Intervals i = new Intervals();
        i.addInterval(5, 10);
        i.addInterval(15, 20);
        i.addInterval(25, 30);
        Intervals expected = new Intervals();
        expected.addInterval(6, 10);
        expected.addInterval(15, 20);
        expected.addInterval(25, 26);
        assertEquals("Intersection is wrong ", expected, i.getIntersection(6, 26)); //$NON-NLS-1$
    }

    public void testIntersection8() {
        Intervals i = new Intervals();
        i.addInterval(5, 10);
        i.addInterval(15, 20);
        i.addInterval(25, 30);
        Intervals expected = new Intervals();
        expected.addInterval(5, 10);
        expected.addInterval(15, 20);
        expected.addInterval(25, 30);
        assertEquals("Intersection is wrong ", expected, i.getIntersection(1, 35)); //$NON-NLS-1$
    }

    public void testIntersection9() {
        Intervals i = new Intervals();
        i.addInterval(5, 10);
        i.addInterval(15, 20);
        i.addInterval(25, 30);
        Intervals expected = new Intervals();
        expected.addInterval(15, 20);
        assertEquals("Intersection is wrong ", expected, i.getIntersection(11, 24)); //$NON-NLS-1$
    }

    public void testIntersection10() {
        Intervals i = new Intervals();
        i.addInterval(5, 10);
        i.addInterval(15, 20);
        i.addInterval(25, 30);
        Intervals expected = new Intervals();
        expected.addInterval(15, 20);
        expected.addInterval(25, 26);
        assertEquals("Intersection is wrong ", expected, i.getIntersection(11, 26)); //$NON-NLS-1$
    }

    public void testIntersection11() {
        Intervals i = new Intervals();
        i.addInterval(5, 10);
        i.addInterval(15, 20);
        i.addInterval(25, 30);
        Intervals expected = new Intervals();
        expected.addInterval(25, 30);
        assertEquals("Intersection is wrong ", expected, i.getIntersection(24, 40)); //$NON-NLS-1$
    }

    public void testIntersection12() {
        Intervals i = new Intervals();
        Intervals expected = new Intervals();
        assertEquals("Intersection is wrong ", expected, i.getIntersection(5, 10)); //$NON-NLS-1$
    }

    public void testIntersection13() {
        Intervals i = new Intervals();
        i.addInterval(5, 10);
        i.addInterval(15, 20);
        Intervals expected = new Intervals();
        assertEquals("Intersection is wrong ", expected, i.getIntersection(11,12)); //$NON-NLS-1$
    }

    public void testIntersectionInvalid() {
        Intervals i = new Intervals();
        try {
            i.getIntersection(1, 0);
            fail("Expected IllegalArgumentException when checking containment of invalid interval"); //$NON-NLS-1$
        } catch(IllegalArgumentException e) {
        }
    }

    public void testEquals1() {
        Intervals i1 = new Intervals();
        i1.addInterval(5, 10);
        i1.addInterval(15, 20);

        Intervals i2 = new Intervals();
        i2.addInterval(5, 10);
        i2.addInterval(15, 20);

        assertTrue("Equals is wrong ", i1.equals(i2)); //$NON-NLS-1$
        assertTrue("Equals is wrong ", i2.equals(i1)); //$NON-NLS-1$
        assertTrue("Equals is wrong ", i1.hashCode() == i2.hashCode());             //$NON-NLS-1$
    }

    public void testEquals2() {
        Intervals i1 = new Intervals();
        i1.addInterval(5, 10);

        Intervals i2 = new Intervals();
        i2.addInterval(5, 11);

        assertTrue("Equals is wrong ", !i1.equals(i2)); //$NON-NLS-1$
        assertTrue("Equals is wrong ", !i2.equals(i1)); //$NON-NLS-1$
        assertTrue("Equals is wrong ", i1.hashCode() != i2.hashCode());             //$NON-NLS-1$
    }

    public void testEquals3() {
        Intervals i1 = new Intervals();
        i1.addInterval(5, 10);
        i1.addInterval(14, 20);

        Intervals i2 = new Intervals();
        i2.addInterval(5, 10);
        i2.addInterval(15, 20);

        assertTrue("Equals is wrong ", !i1.equals(i2)); //$NON-NLS-1$
        assertTrue("Equals is wrong ", !i2.equals(i1)); //$NON-NLS-1$
    }

    public void testEquals4() {
        Intervals i1 = new Intervals();
        i1.addInterval(5, 10);
        i1.addInterval(15, 20);

        Intervals i2 = new Intervals();
        i2.addInterval(5, 10);

        assertTrue("Equals is wrong ", !i1.equals(i2)); //$NON-NLS-1$
        assertTrue("Equals is wrong ", !i2.equals(i1)); //$NON-NLS-1$
        assertTrue("Equals is wrong ", i1.hashCode() != i2.hashCode());             //$NON-NLS-1$
    }

    public void testEquals5() {
        Intervals i1 = new Intervals();
        assertTrue("Equals is wrong ", i1.equals(i1)); //$NON-NLS-1$
    }

    public void testEquals6() {
        Intervals i1 = new Intervals();
        assertTrue("Equals is wrong ", ! i1.equals(null)); //$NON-NLS-1$
    }

    public void testEquals7() {
        Intervals i1 = new Intervals();
        assertTrue("Equals is wrong ", ! i1.equals("abc")); //$NON-NLS-1$ //$NON-NLS-2$
    }

    public void testRemoveIntervals() {
        Intervals x = new Intervals();
        x.addInterval(1, 100);
        Intervals y = new Intervals();
        y.addInterval(21, 30);
        x.removeIntervals(y);
        assertEquals("[1, 20, 31, 100]", x.toString()); //$NON-NLS-1$
    }

    public void testRemoveMultipleIntervals() {
        Intervals x = new Intervals();
        x.addInterval(1, 100);
        Intervals y = new Intervals();
        y.addInterval(21, 30);
        y.addInterval(41, 50);
        x.removeIntervals(y);
        assertEquals("[1, 20, 31, 40, 51, 100]", x.toString()); //$NON-NLS-1$
    }

    public void testGetBoundingInterval() {
        Intervals x = new Intervals();
        x.addInterval(1, 100);
        int[] result = x.getBoundingInterval();
        assertEquals(1, result[0]);
        assertEquals(100, result[1]);
    }

    public void testGetBoundingIntervalWithMultipleIntervals() {
        Intervals x = new Intervals();
        x.addInterval(1, 100);
        x.addInterval(200, 300);
        int[] result = x.getBoundingInterval();
        assertEquals(1, result[0]);
        assertEquals(300, result[1]);
    }

    public void testGetBoundingIntervalWithNoIntervals() {
        Intervals x = new Intervals();
        int[] result = x.getBoundingInterval();
        assertEquals(Integer.MIN_VALUE, result[0]);
        assertEquals(Integer.MAX_VALUE, result[1]);
    }

    public void testGetIntersectionIntervals() {
        Intervals x = new Intervals();
        x.addInterval(1, 100);
        Intervals y = new Intervals();
        y.addInterval(10, 20);
        assertEquals("[10, 20]", x.getIntersectionIntervals(y).toString()); //$NON-NLS-1$

    }

    public void testGetIntersectionIntervalsMultiple() {
        Intervals x = new Intervals();
        x.addInterval(1, 100);
        x.addInterval(201, 300);
        Intervals y = new Intervals();
        y.addInterval(90, 210);
        y.addInterval(290, 300);
        assertEquals("[90, 100, 201, 210, 290, 300]", x.getIntersectionIntervals(y).toString()); //$NON-NLS-1$
    }

    public void testConstructorTakesInterval() {
        Intervals x = new Intervals(1, 100);
        assertEquals("[1, 100]", x.toString()); //$NON-NLS-1$
    }

    public void testCopy() {
        Intervals x = new Intervals(1, 10);
        x.addInterval(20, 30);
        Intervals y = x.copy();
        assertEquals("[1, 10, 20, 30]", y.toString()); //$NON-NLS-1$
        x.removeInterval(5, 25);
        //changing x should not affect y
        assertEquals("[1, 10, 20, 30]", y.toString()); //$NON-NLS-1$

    }

    public void testIsContiguous() {
        Intervals x = new Intervals();
        x.addInterval(5, 10);
        x.addInterval(0, 10);
        assertTrue( x.isContiguous() );
    }

    public void testIsNotContiguous() {
        Intervals x = new Intervals();
        x.addInterval(5, 10);
        x.addInterval(1, 3);
        assertFalse( x.isContiguous() );
    }

    public void testEmptyIntervalsIsContiguous() {
        Intervals x = new Intervals();
        assertTrue( x.isContiguous() );
    }
}
