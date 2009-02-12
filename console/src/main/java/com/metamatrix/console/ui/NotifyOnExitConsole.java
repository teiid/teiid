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

package com.metamatrix.console.ui;

/**
 * Interface to be implemented by any tab that wants notification that the user is attempting to
 * to exit the Console.  This is presumably so that it can notify the user of pending changes
 * made but not yet saved.  It also provides the opportunity to cancel the exit.
 */
public interface NotifyOnExitConsole {
    boolean havePendingChanges();
        //Does tab have pending changes?  If so, focus will be given to the tab.  The tab can
        //then put up a dialog asking the user whether to save or complete the changes, or 
        //whatever.
    boolean finishUp();
        //Finish up any pending work.  Returns true if exit still okay, false if exiting should
        //be cancelled.
}
