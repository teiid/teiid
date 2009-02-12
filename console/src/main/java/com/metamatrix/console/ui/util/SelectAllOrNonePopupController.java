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

import java.awt.Component;
import java.awt.event.ActionEvent;
import java.awt.event.MouseAdapter;
import java.awt.event.MouseEvent;

import javax.swing.AbstractAction;
import javax.swing.Action;
import javax.swing.JPopupMenu;

/**
 * Class which will add a mouse listener to each of an array of Components.  The
 * mouse listener will put up a pop-up menu.  The menu will by
 * default have Actions labeled "Select All" and "Select None", as well as
 * "Cancel".  Upon "Select All" or "Select None" being selected, a method
 * supplied by the caller is invoked.
 */
public class SelectAllOrNonePopupController {
    public final static String DEFAULT_ALL_TEXT = "Select All"; //$NON-NLS-1$
    public final static String DEFAULT_NONE_TEXT = "Select None"; //$NON-NLS-1$

    private SelectAllOrNoneMessageReceiver receiver;
        //Caller interface containing methods selectAll() and selectNone(), to
        //be called when corresponding menu item selected.
    private Component[] components;
        //Components to which the pop-up menu applies
    private String selectAllText;
    private String selectNoneText;
    JPopupMenu popupMenu = new JPopupMenu();

    public SelectAllOrNonePopupController(SelectAllOrNoneMessageReceiver rcvr,
            Component[] comp, String allText, String noneText) {
        super();
        receiver = rcvr;
        components = comp;
        selectAllText = allText;
        selectNoneText = noneText;
        init();
    }

    public SelectAllOrNonePopupController(SelectAllOrNoneMessageReceiver rcvr,
            Component[] comp) {
        this(rcvr, comp, DEFAULT_ALL_TEXT, DEFAULT_NONE_TEXT);
    }

    /**
     * Populate menu with 'select all', 'select none', and 'cancel' actions, and add
     * mouse listener to supplied Component objects.
     */
    private void init() {
        //Add the actions to our pop-up menu
        Action allAction = new AllAction(selectAllText, receiver);
        popupMenu.add(allAction);
        Action noneAction = new NoneAction(selectNoneText, receiver);
        popupMenu.add(noneAction);
        Action cancelAction = new CancelAction();
        popupMenu.add(cancelAction);
        //Add mouse listener to supplied components.  Listener will determine
        //if mouse click is a pop-up trigger, and if so display our pop-up menu.
        for (int i = 0; i < components.length; i++) {
            final int ss = i;
            components[i].addMouseListener(new MouseAdapter() {
                //public void mousePressed(MouseEvent ev) {
                //    check(ev);
                //}
                public void mouseReleased(MouseEvent ev) {
                    check(ev);
                }
                private void check(MouseEvent ev) {
                    boolean notShowing = (!popupMenu.isVisible());
                    boolean isTrigger = ev.isPopupTrigger();
                    if (notShowing && isTrigger) {
                        popupMenu.show(components[ss], ev.getX() + 10, ev.getY());
                    }
                }
            });
        }
    }
}//end SelectAllOrNonePopupController

//
//Internal auxilliary classes
//

/**
 * Action corresponding to 'All' selection, will invoke caller's selectAll() method.
 */
class AllAction extends AbstractAction {
    private SelectAllOrNoneMessageReceiver receiver;

    public AllAction(String text, SelectAllOrNoneMessageReceiver rcvr) {
        super(text);
        receiver = rcvr;
    }

    public void actionPerformed(ActionEvent ev) {
        receiver.selectAll();
    }
}//end AllAction

/**
 * Action corresponding to 'None' selection, will invoke caller's selectNone() method.
 */
class NoneAction extends AbstractAction {
    private SelectAllOrNoneMessageReceiver receiver;

    public NoneAction(String text, SelectAllOrNoneMessageReceiver rcvr) {
        super(text);
        receiver = rcvr;
    }

    public void actionPerformed(ActionEvent ev) {
        receiver.selectNone();
    }
}//end NoneAction

/**
 * Action implementing a 'cancel', so does no processing.
 */
class CancelAction extends AbstractAction {
    public CancelAction() {
        super("Cancel"); //$NON-NLS-1$
    }

    public void actionPerformed(ActionEvent ev) {
    }
}//end CancelAction
