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

package com.metamatrix.console.ui.util;

import javax.swing.JList;
import javax.swing.event.ListSelectionEvent;
import javax.swing.event.ListSelectionListener;

/**
 * Implementation of ListSelectionListener that will block the selection of any
 * in a given set of items.  Match is determined by toString().equals() of
 * selected item when compared to input string.
 */
public class ItemsBlockedListSelectionListener implements ListSelectionListener {
    private String[] blockedItems;
    private JList theList;

    public ItemsBlockedListSelectionListener(String[] blocked, JList list) {
        super();
        blockedItems = blocked;
        if (blockedItems == null) {
            blockedItems = new String[] {};
        }
        theList = list;
    }

    public void valueChanged(ListSelectionEvent ev) {
        int numRows = theList.getModel().getSize();
        for (int i = 0; i < numRows; i++) {
            if (theList.isSelectedIndex(i)) {
                String selectedItem = theList.getModel().getElementAt(i).toString();
                boolean found = false;
                int loc = 0;
                while ((!found) && (loc < blockedItems.length)) {
                    if (blockedItems[loc].equals(selectedItem)) {
                        found = true;
                    } else {
                        loc++;
                    }
                }
                if (found) {
                    theList.getSelectionModel().removeSelectionInterval(i, i);
                }
            }
        }
    }
}


