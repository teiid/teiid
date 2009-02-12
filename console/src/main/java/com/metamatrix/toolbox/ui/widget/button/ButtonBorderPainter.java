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

package com.metamatrix.toolbox.ui.widget.button;

import java.awt.event.MouseAdapter;
import java.awt.event.MouseEvent;

import javax.swing.AbstractButton;
import javax.swing.event.ChangeEvent;
import javax.swing.event.ChangeListener;

/**
 * A utility class for painting and hiding toolbar button borders based on mouse-over and
 * current toggle state.
 */
public class ButtonBorderPainter extends MouseAdapter implements ChangeListener {

    private static final ButtonBorderPainter instance = new ButtonBorderPainter();

    /**
     * Register the specified button with the ButtonBorderPainter so that it's border
     * is shown only on mouse-over and if isSelected returns true, for JToggleButtons.
     * @param b the button which should be controlled by the ButtonBorderPainter. 
     */
    public static void registerButton(AbstractButton b) {
        b.addChangeListener(instance);
        b.addMouseListener(instance);
        b.setBorderPainted(b.isSelected());
    }

    private AbstractButton currentMouseOverButton = null;
    
    /**
     * Private constructor for singleton class
     */
    private ButtonBorderPainter() {
    }

	/**
	 * @see java.awt.event.ActionListener#actionPerformed(java.awt.event.ActionEvent)
	 */
	public void stateChanged(ChangeEvent e) {
        AbstractButton b = (AbstractButton) e.getSource();
        if ( ! b.isSelected() ) {
            if ( b != currentMouseOverButton ) {
                b.setBorderPainted(false);
            }
        } else {
            b.setBorderPainted(true);
        }
	}

	/**
     * Paint the border on mouse entered as long as the button is enabled or selected.
	 * @see java.awt.event.MouseListener#mouseEntered(java.awt.event.MouseEvent)
	 */
	public void mouseEntered(MouseEvent e) {
        AbstractButton b = (AbstractButton) e.getSource();
        if ( b.isEnabled() ) {
            b.setBorderPainted(true);
        } else if ( b.isSelected() ) {
            b.setBorderPainted(true);
        }
        currentMouseOverButton = b;
	}

	/**
     * Do not paint the border after mouse entered unless the button is selected.
	 * @see java.awt.event.MouseListener#mouseExited(java.awt.event.MouseEvent)
	 */
	public void mouseExited(MouseEvent e) {
        AbstractButton b = (AbstractButton) e.getSource();
        if ( ! b.isSelected() ) {
            b.setBorderPainted(false);
        }
        currentMouseOverButton = null;
	}

}
