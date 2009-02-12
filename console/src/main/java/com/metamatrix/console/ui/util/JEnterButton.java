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

import java.awt.event.KeyAdapter;
import java.awt.event.KeyEvent;

import javax.swing.Icon;

import com.metamatrix.toolbox.ui.widget.ButtonWidget;

/**
 * JEnterButton is a specialization of JButton that responds to the Enter key
 * when it has keyboard focus.  Good old JButton uses the spacebar as the doClick
 * mechanism, but most Windows GUI buttons respond to the Enter key.  JEnterButton
 * does change the behavior of setDefaultButton() - in fact, it "fixes" the
 * questionable behavior of hitting Enter for a JButton that has keyboard
 * focus will trigger the default button, not the button that has focus.
 */
public class JEnterButton extends ButtonWidget {

    /**
     * Creates a button with no set text or icon.
     */
    public JEnterButton() {
        this(null, null);
    }
    
    /**
     * Creates a button with an icon.
     *
     * @param icon  the Icon image to display on the button
     */
    public JEnterButton(Icon icon) {
        this(null, icon);
    }
    
    /**
     * Creates a button with text.
     *
     * @param text  the text of the button
     */
    public JEnterButton(String text) {
        this(text, null);
    }
    
    /**
     * Creates a button with initial text and an icon.
     *
     * @param text  the text of the button.
     * @param icon  the Icon image to display on the button
     */
    public JEnterButton(String text, Icon icon) {
        super(text, icon);
        this.addKeyListener(new KeyAdapter() {
            public void keyPressed(KeyEvent e) {
                if ( e.getKeyCode() == KeyEvent.VK_ENTER ) {
                    enterPressed();
                }
            }
        });
    }

    private void enterPressed() {
        this.doClick();
    }

}
