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

package com.metamatrix.console.ui.layout;

import java.awt.Graphics;

import javax.swing.JPanel;
import javax.swing.SwingUtilities;

public abstract class BasePanel extends JPanel {
    private boolean hasBeenPainted = false;

    public BasePanel() {
        super();
    }

    /**
     * To be overridden by any subclass that wants to do anything immediately
     * after the first paint call, such as position a splitter.
     *
     */
    public void postRealize() {
    }

    protected void setHasBeenPainted(boolean flag) {
        hasBeenPainted = flag;
    }

    /**
     * Overridden paint() method.  Runs postRealize() method upon first paint.
     */
    public void paint(Graphics g) {
        super.paint(g);
        if (!hasBeenPainted) {
            hasBeenPainted = true;
            SwingUtilities.invokeLater(new Runnable() {
                public void run() {
                    postRealize();
                }
            });
        }
    }
}

