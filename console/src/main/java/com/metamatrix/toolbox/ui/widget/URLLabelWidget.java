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

import javax.swing.Icon;

import com.metamatrix.toolbox.ui.widget.util.BrowserControl;

/**
 * Specialization of LabelWidget to display a URL and allow clicking to send
 * the URL text to the browser.
 */
public class URLLabelWidget extends LabelWidget {

    private static final Cursor HAND_CURSOR = Cursor.getPredefinedCursor(Cursor.HAND_CURSOR);
    private static final Cursor DEFAULT_CURSOR = Cursor.getDefaultCursor();
                                
    private Rectangle clickRect;
//    private boolean dragged = false;

	/**
	 * Constructor for URLLabelWidget.
	 */
	public URLLabelWidget() {
		super();
        init();
	}

	/**
	 * Constructor for URLLabelWidget.
	 * @param text
	 */
	public URLLabelWidget(String text) {
		super(text);
        init();
	}

	/**
	 * Constructor for URLLabelWidget.
	 * @param icon
	 */
	public URLLabelWidget(Icon icon) {
		super(icon);
        init();
	}

	/**
	 * Constructor for URLLabelWidget.
	 * @param text
	 * @param icon
	 */
	public URLLabelWidget(String text, Icon icon) {
		super(text, icon);
        init();
	}

    /**
     * Set up the MouseListener for this URLTextFieldWidget
     */
    private void init() {
        super.setForeground(Color.blue);
        this.clickRect = new Rectangle(0,0,0,0);
        this.addMouseListener(new MouseAdapter() {
            Cursor prevCursor = null;
            public void mouseReleased(final MouseEvent event) {
                if ( getText().length() > 0 && clickRect.contains(event.getPoint()) ) {
                    doLinkClick(getText());
                }
//                dragged = false;
            }
            public void mouseEntered(MouseEvent theEvent) {
                if ( getText().length() > 0 && clickRect.contains(theEvent.getPoint()) ) {
                    setCursor(HAND_CURSOR);
                } else {
                    setCursor(DEFAULT_CURSOR);
                }
            }
            public void mouseExited(MouseEvent theEvent) {
                setCursor(prevCursor);
            }
        });
        
        this.addMouseMotionListener(new MouseMotionListener() {
            // if the user is dragging the cursor, disable the call to doLinkClick
            public void mouseDragged(MouseEvent theEvent) {
//                dragged = true;
            }
            public void mouseMoved(MouseEvent theEvent) {
                String url = getText();
                if ( url.length() > 0 && clickRect.contains(theEvent.getPoint()) ) {
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
        int y = fontMetrics.getMaxAscent() + 3;
        int width = fontMetrics.stringWidth(text);

        g2D.drawLine(x, y, x + width, y);
        clickRect.width = width;
        clickRect.height = y;
    }

}
