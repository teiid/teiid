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

//################################################################################################################################
package com.metamatrix.toolbox.ui.widget.laf;

import java.awt.event.FocusEvent;
import java.awt.event.FocusListener;
import java.awt.event.KeyAdapter;
import java.awt.event.KeyEvent;
import java.awt.event.KeyListener;

import javax.swing.JComponent;
import javax.swing.JPasswordField;
import javax.swing.SwingUtilities;
import javax.swing.event.CaretEvent;
import javax.swing.event.CaretListener;
import javax.swing.plaf.ComponentUI;
import javax.swing.plaf.basic.BasicPasswordFieldUI;

/**
 * @since 2.1
 * @version 2.1
 * @author <a href="mailto:jverhaeg@metamatrix.com">John P. A. Verhaeg</a>
 */
public class PasswordLookAndFeel extends BasicPasswordFieldUI {
    //############################################################################################################################
    //# Constants                                                                                                                #
    //############################################################################################################################
    
//    private static final int ECHO_CHARACTER_COUNT_MAX = 3;
//    private static final int UNMODIFIED_ECHO_CHARACTER_COUNT = 8;
    
    //############################################################################################################################
    //# Instance Variables                                                                                                       #
    //############################################################################################################################

    private CaretListener	caretListener;
	private FocusListener	focusListener;
    private KeyListener		keyListener;
    
    private boolean keyTyped;
    
    //############################################################################################################################
    //# Instance Methods                                                                                                         #
    //############################################################################################################################
    
    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
     * @since 2.1
     */
    public static ComponentUI createUI(final JComponent component) {
        return new PasswordLookAndFeel();
    }

	//////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
     * @since 2.1
     *//*
    public View create(final Element element) {
		return new PasswordView(element);
    }
    
    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
     * @since 2.1
     */
    protected void installListeners() {
        final JPasswordField fld = (JPasswordField)getComponent();
        caretListener = new CaretListener() {
            public void caretUpdate(final CaretEvent event) {
                final int len = fld.getPassword().length;
                if (event.getDot() != 0  ||  event.getMark() != len) {
               		fld.removeCaretListener(caretListener);
                    SwingUtilities.invokeLater(new Runnable() {
                        public void run() {
                    		if (fld.hasFocus()  &&  !keyTyped) {
	                    		fld.selectAll();
	                    		fld.addCaretListener(caretListener);
                    		}
                        }
                    });
                }
            }
        };
        focusListener = new FocusListener() {
            public final void focusGained(final FocusEvent event) {
                keyTyped = false;
                fld.selectAll();
           		fld.addCaretListener(caretListener);
        		fld.addKeyListener(keyListener);
            }
            public final void focusLost(final FocusEvent event) {
           		fld.removeCaretListener(caretListener);
        		fld.removeKeyListener(keyListener);
            }
        };
        keyListener = new KeyAdapter() {
            public void keyTyped(final KeyEvent event) {
                keyTyped = true;
           		fld.removeCaretListener(caretListener);
        		fld.removeKeyListener(keyListener);
            }
        };
        fld.addFocusListener(focusListener);
    }
    
    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
     * @since 2.1
     */
    protected void uninstallListeners() {
        final JPasswordField fld = (JPasswordField)getComponent();
		fld.removeCaretListener(caretListener);
        fld.removeFocusListener(focusListener);
        fld.removeKeyListener(keyListener);
		caretListener = null;
        focusListener = null;
        keyListener = null;
    }
    
    //############################################################################################################################
    //# Inner Class: PasswordView                                                                                                #
    //############################################################################################################################
    
//    /**
//     * @since 2.1
//     */
//    private class PasswordView extends javax.swing.text.PasswordView {
//		//# PasswordView #########################################################################################################
//        //# Instance Variables                                                                                                   #
//        //########################################################################################################################
//        
//        private ArrayList echoChrs = new ArrayList();
//        
//        //# PasswordView #########################################################################################################
//        //# Constructors                                                                                                         #
//        //########################################################################################################################
//        
//        /// PasswordView /////////////////////////////////////////////////////////////////////////////////////////////////////////
//        /**
//         * @since 2.1
//         */
//        private PasswordView(final Element element) {
//            super(element);
//        }
//        
//        //# PasswordView #########################################################################################################
//        //# Instance Methods                                                                                                     #
//        //########################################################################################################################
//        
//        /// PasswordView /////////////////////////////////////////////////////////////////////////////////////////////////////////
//        /**
//         * @since 2.1
//         */
//        protected int drawSelectedText(final Graphics painter, final int x, final int y, final int startIndex, final int endIndex)
//        throws BadLocationException {
//    	    final JPasswordField fld = (JPasswordField)getContainer();
//        	final Color selected = fld.getCaret().isSelectionVisible() ? fld.getSelectedTextColor()
//        	: ((fld.isEnabled()) ? fld.getForeground() : fld.getDisabledTextColor());
//        	painter.setColor(selected);
//    	    if (!fld.echoCharIsSet()) {
//	    		return super.drawSelectedText(painter, x, y, startIndex, endIndex);
//    	    }
//        	return drawText(fld, painter, x, y, startIndex, endIndex);
//        }
//        
//        /// PasswordView /////////////////////////////////////////////////////////////////////////////////////////////////////////
//        /**
//         * @since 2.1
//         */
//        protected int drawText(final JPasswordField field, final Graphics painter, int x, final int y, final int startIndex,
//        					   final int endIndex)
//        throws BadLocationException {
//            updateEchoCharacterList(field);
//    	    final char echoChr = field.getEchoChar();
//    	    if (keyTyped) {
//        	    for (int ndx = startIndex; ndx < endIndex; ++ndx) {
//        	        for (int ndx2 = ((Integer)echoChrs.get(ndx)).intValue();  --ndx2 >= 0;) {
//    	    			x = drawEchoCharacter(painter, x, y, echoChr);
//        	        }
//        	    }
//    	    } else if (endIndex > 0) {
//        	    for (int ndx = UNMODIFIED_ECHO_CHARACTER_COUNT; --ndx >= 0;) {
//					x = drawEchoCharacter(painter, x, y, echoChr);
//        	    }
//    	    }
//        	return x;
//        }
//        
//        /// PasswordView /////////////////////////////////////////////////////////////////////////////////////////////////////////
//        /**
//         * @since 2.1
//         */
//        protected int drawUnselectedText(final Graphics painter, final int x, final int y, final int startIndex,
//        								 final int endIndex)
//        throws BadLocationException {
//    	    final JPasswordField fld = (JPasswordField)getContainer();
//    	    if (!fld.echoCharIsSet()) {
//	    		return super.drawUnselectedText(painter, x, y, startIndex, endIndex);
//    	    }
//    	    painter.setColor(fld.getForeground());
//        	return drawText(fld, painter, x, y, startIndex, endIndex);
//        }
//        
//        /// PasswordView /////////////////////////////////////////////////////////////////////////////////////////////////////////
//        /**
//         * @since 2.1
//         */
//        public Shape modelToView(final int index, final Shape shape, final Position.Bias bias)
//        throws BadLocationException {
//    	    final JPasswordField fld = (JPasswordField)getContainer();
//    	    if (!fld.echoCharIsSet()) {
//	    		return super.modelToView(index, shape, bias);
//    	    }
//    	    updateEchoCharacterList(fld);
//    	    final char echoChr = fld.getEchoChar();
//    	    final FontMetrics metrics = fld.getFontMetrics(fld.getFont());
//    	    final Rectangle viewShape = adjustAllocation(shape).getBounds();
//    	    if (keyTyped) {
//        	    for (int ndx = getStartOffset();  ndx < index;  ++ndx) {
//    	    	    viewShape.x += ((Integer)echoChrs.get(ndx)).intValue() * metrics.charWidth(echoChr);
//        	    }
//        	    // Return width as the number of characters instead of as width of echo characters...strange
//        	    if (index >= fld.getPassword().length) {
//        	        viewShape.width = 0;
//        	    } else {
//    	    	    viewShape.width = ((Integer)echoChrs.get(index)).intValue() * metrics.charWidth(echoChr);
//    	        }
//    	    } else {
//    	        viewShape.x = 2;
//    	        if (fld.getDocument().getLength() > 0) {
//    	        	viewShape.width = UNMODIFIED_ECHO_CHARACTER_COUNT * metrics.charWidth(echoChr);
//    	        } else {
//        	        viewShape.width = 0;
//    	        }
//    	    }
//    	    return viewShape;
//        }
//        
//        /// PasswordView /////////////////////////////////////////////////////////////////////////////////////////////////////////
//        /**
//         * @since 2.1
//         */
//        protected void updateEchoCharacterList(final JPasswordField field) {
//    	    final char[] chrs = field.getPassword();
//    	    while (chrs.length < echoChrs.size()) {
//      	        echoChrs.remove(echoChrs.size() - 1);
//    	    }
//    	    while (chrs.length > echoChrs.size()) {
//    	        echoChrs.add(new Integer((int)(Math.random() * ECHO_CHARACTER_COUNT_MAX) + 1));
//    	    }
//        }
//    }
}
