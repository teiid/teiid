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

package com.metamatrix.toolbox.ui.widget;

import java.awt.Color;
import java.awt.Cursor;
import java.awt.FontMetrics;
import java.awt.Graphics;
import java.awt.Graphics2D;
import java.awt.Rectangle;
import java.awt.event.MouseAdapter;
import java.awt.event.MouseEvent;
import java.awt.event.MouseMotionListener;

import com.metamatrix.toolbox.ui.widget.util.BrowserControl;

/**
 * Editable TextWidget to display a URL that can be rendered as hyperlink
 * and clicked upon.
 */
public class URLTextFieldWidget extends TextFieldWidget {

	private static final Cursor HAND_CURSOR =
		Cursor.getPredefinedCursor(Cursor.HAND_CURSOR);
    private static final Cursor DEFAULT_CURSOR =
        Cursor.getPredefinedCursor(Cursor.TEXT_CURSOR);

//	private String textValue;
//	private boolean enableLink = false;
	private boolean dragged = false;
    private Rectangle clickRect;
    private boolean enableClick = true;
    private boolean overrideHyperlinkClick = false;

	/**
	 * Constructor for URLTextFieldWidget.
	 */
	public URLTextFieldWidget() {
		super();
		init();
	}

	/**
	 * Constructor for URLTextFieldWidget.
	 * @param text
	 */
	public URLTextFieldWidget(String text) {
		super(text);
		init();
	}

	/**
	 * Constructor for URLTextFieldWidget.
	 * @param characters
	 */
	public URLTextFieldWidget(int characters) {
		super(characters);
		init();
	}

	/**
	 * Constructor for URLTextFieldWidget.
	 * @param text
	 * @param characters
	 */
	public URLTextFieldWidget(String text, int characters) {
		super(text, characters);
		init();
	}

	/**
	 * Set up the MouseListener for this URLTextFieldWidget
	 */
	private void init() {
		this.setForeground(Color.blue);
        this.clickRect = new Rectangle(0,0,0,0);
        this.setDisabledTextColor(Color.blue);
        
        // control the cursor to be HAND_CURSOR within the text boundary.
		this.addMouseListener(new MouseAdapter() {
			public void mouseReleased(final MouseEvent theEvent) {
				String url = getText();
                if ( url.length() > 0 && enableClick && clickRect.contains(theEvent.getPoint()) && !dragged) {
					doLinkClick(url);
				}
				dragged = false;
			}
			public void mouseEntered(MouseEvent theEvent) {
                String url = getText();
                if ( url.length() > 0 && enableClick && clickRect.contains(theEvent.getPoint()) ) {
					setCursor(HAND_CURSOR);
				} else {
                    setCursor(DEFAULT_CURSOR);
                }
			}
			public void mouseExited(MouseEvent theEvent) {
				setCursor(DEFAULT_CURSOR);
			}
		});

		this.addMouseMotionListener(new MouseMotionListener() {
            // if the user is dragging the cursor, disable the call to doLinkClick
			public void mouseDragged(MouseEvent theEvent) {
				dragged = true;
			}
			public void mouseMoved(MouseEvent theEvent) {
                String url = getText();
                if ( url.length() > 0 && enableClick && clickRect.contains(theEvent.getPoint()) ) {
                    setCursor(HAND_CURSOR);
                } else {
                    setCursor(DEFAULT_CURSOR);
                }
			}
		});
	}

	/**
	 * Allows subclasses to override the behavior when the link is clicked
	 * @param url String version of the url stored in this label
	 */
	protected void doLinkClick(String url) {
		BrowserControl.displayURL(url);
	}

    /**
     * Overridden to underline the font, which is not supported by awt.Font
     * @see javax.swing.JComponent#paintComponent(Graphics)
     */
	public void paintComponent(Graphics g) {
		super.paintComponent(g);

		Graphics2D g2D = (Graphics2D) g;
		g2D.setPaint(getForeground());

		String text = getText();

		FontMetrics fontMetrics = g2D.getFontMetrics(getFont());
		int x = 0;
		int y = fontMetrics.getMaxAscent() + 4;
		int width = fontMetrics.stringWidth(text);

		g2D.drawLine(x, y, x + width, y);
        clickRect.width = width;
        clickRect.height = y;
	}

    public void setForeground(Color c) {
        if ( c.equals(Color.red) ) {
            super.setForeground(c);
            enableClick = false;
        } else {
            super.setForeground(Color.blue);
            if ( ! overrideHyperlinkClick ) {
                enableClick = true;
            }
        }
    }
    
    /**
     * Allow user to tell this widget to never allow clicks to hyperlink to the browser.
     * Not fully tested, this might not actually work in all cases, and it probably does
     * not prevent the hand cursor from getting set.
     * @param flag false if this widget should not send clicks to the browser.  default
     * setting is true.
     */
    public void setHyperlinkEnabled(boolean flag) {
        if ( flag ) {
            overrideHyperlinkClick = false;
        } else {
            overrideHyperlinkClick = true;
        }
    }

}
