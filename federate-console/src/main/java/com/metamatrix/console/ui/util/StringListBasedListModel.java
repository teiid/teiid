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

package com.metamatrix.console.ui.util;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import javax.swing.DefaultListModel;

import com.metamatrix.console.util.StaticQuickSorter;

/**
 * Extension to AbstractListModel to base model on List of String objects,
 * which may be specified to be kept sorted. 
 */
public class StringListBasedListModel extends DefaultListModel {
    //The list
    private List /*<String>*/ list;

    //Does the list need to be kept sorted?
    private boolean keepSorted;

//Constructors

    /**
     * Constructor using input List
     *
     * @param l     List of Strings
     * @param sortFlag  Should list be kept sorted?
     */
    public StringListBasedListModel(List /*<String>*/ l, boolean sortFlag) {
        super();
        list = l;
        keepSorted = sortFlag;
        if (keepSorted) {
            sort();
        }
    }

    /**
     * Constructor using input String array
     *
     * @param s     Array of Strings
     * @param sortFlag  Should list be kept sorted?
     */
    public StringListBasedListModel(String[] s, boolean sortFlag) {
        super();
        keepSorted = sortFlag;
        list = new ArrayList();
        for (int i = 0; i < s.length; i++) {
            list.add(s[i]);
        }
        if (keepSorted) {
            sort();
        }
    }

//Overridden methods

    /**
     * Return the model size
     *
     * @return model size
     */
    public int getSize() {
        return list.size();
    }

    /**
     * Return a particular element
     *
     * @param index     index of requested element
     * @return          the element
     */
    public Object getElementAt(int index) {
        Object obj = null;
        if ((index >= 0) && (index < getSize())) {
            obj = list.get(index);
        }
        return obj;
    }

//Processing methods

    /**
     * Add an element.
     *
     * @param element   new element
     */
    public void addElement(String element) {
        list.add(element);
        if (keepSorted) {
            sort();
        }
        refresh();
    }

    /**
     * Add list of String objects
     *
     * @param elements   List of Strings
     */
    public void addElements(List /*<String>*/ elements) {
        Iterator it = elements.iterator();
        while (it.hasNext()) {
            String s = (String)it.next();
            list.add(s);
        }
        if (keepSorted) {
            sort();
        }
        refresh();
    }

    /**
     * Add array of Strings
     *
     * @param s   Array of Strings
     */
    public void addElements(String[] s) {
        for (int i = 0; i < s.length; i++) {
            list.add(s[i]);
        }
        if (keepSorted) {
            sort();
        }
        refresh();
    }

    /**
     * Remove an element
     *
     * @param element   String to be removed
     */
    public void removeElement(String element) {
        int index = list.indexOf(element);
        if (index >= 0) {
            list.remove(element);
            refresh();
        }
    }

    /**
     * Remove all elements
     */
    public void removeAllElements() {
        for (int i = list.size() - 1; i >= 0; i--) {
            list.remove(i);
        }
        refresh();
    }

    /**
     * Inform listeners that contents have changed.
     */
    private void refresh() {
        fireContentsChanged(this, 0, list.size() - 1);
    }

    /**
     * Sort the list
     */
    private void sort() {
        String[] unsorted = new String[list.size()];
        Iterator it = list.iterator();
        int loc = 0;
        while (it.hasNext()) {
            unsorted[loc] = (String)it.next();
            loc++;
        }
        //Do a quick sort
        String[] sorted = StaticQuickSorter.quickStringSort(unsorted);
        for (int i = 0; i < sorted.length; i++) {
            list.set(i, sorted[i]);
        }
    }
}

