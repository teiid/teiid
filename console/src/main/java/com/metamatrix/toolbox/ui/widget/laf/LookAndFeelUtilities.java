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

//################################################################################################################################
package com.metamatrix.toolbox.ui.widget.laf;

// System imports
import java.awt.Color;
import java.awt.Dimension;
import java.awt.FontMetrics;
import java.awt.Graphics;
import java.awt.Insets;
import java.awt.Rectangle;
import java.util.StringTokenizer;

import javax.swing.Icon;
import javax.swing.JComponent;
import javax.swing.SwingConstants;
import javax.swing.plaf.basic.BasicGraphicsUtils;

/**
@since 2.0
@version 2.0
@author John P. A. Verhaeg
*/
class LookAndFeelUtilities
    implements SwingConstants {
    //############################################################################################################################
    //# Static Variables                                                                                                         #
    //############################################################################################################################

    static final Rectangle VIEW_BOUNDS = new Rectangle();
    static final Rectangle TEXT_BOUNDS = new Rectangle();
    static final Rectangle ICON_BOUNDS = new Rectangle();
    static final Rectangle TMP_BOUNDS = new Rectangle();
    static final Insets INSETS = new Insets(0, 0, 0, 0);
    
    //############################################################################################################################
    //# Static Methods                                                                                                           #
    //############################################################################################################################
   
    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since 2.0
    */
    static void clearViewBounds() {
        VIEW_BOUNDS.x = VIEW_BOUNDS.y = 0;
        VIEW_BOUNDS.width = VIEW_BOUNDS.height = Short.MAX_VALUE;
    }
   
    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since 2.0
    */
    static Dimension getPreferredSize(final JComponent component) {
        union(ICON_BOUNDS, TEXT_BOUNDS, VIEW_BOUNDS);
        // Adjust size by INSETS
        component.getInsets(INSETS);
        VIEW_BOUNDS.width += INSETS.left + INSETS.right;
        VIEW_BOUNDS.height += INSETS.top + INSETS.bottom;
        return VIEW_BOUNDS.getSize();
    }
    
    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    Computes the bounds for the specified icon and/or text relative to the specified view bounds, taking into consideration
    multiple lines of text (delimited by HTML break tags (<br>).  A text string is returned in case it was necessary to clip
    one or more lines to fit within <code>VIEW_BOUNDS</code>.  Characters will be removed (clipped) from each long line until that
    line appended with an ellipsis (...) will fit within <code>VIEW_BOUNDS</code>.
    @since 2.0
    */
    static String layoutText(String text, final Icon icon, int gap, final int verticalAlignment, int horizontalAlignment,
     	                     final int verticalTextPosition, int horizontalTextPosition, final FontMetrics metrics) {
    	final StringBuffer textBuf = new StringBuffer();
        // Ignore textIconGap unless both text & icon are present
        final boolean isTextEmpty = (text == null)  ||  text.length() == 0;
        if (isTextEmpty  ||  icon == null) {
            gap = 0;
        }
        // Initialize the icon's bounds
        ICON_BOUNDS.x = ICON_BOUNDS.y = 0;
        if (icon == null) {
            ICON_BOUNDS.width = ICON_BOUNDS.height = 0;
        } else {
            ICON_BOUNDS.width = icon.getIconWidth();
            ICON_BOUNDS.height = icon.getIconHeight();
        }
        // Initialize the text's bounds
        if (isTextEmpty) {
            TEXT_BOUNDS.width = TEXT_BOUNDS.height = 0;
            text = "";
        } else {
            // Calculate maximum text width that remains within view bounds
            int maxTextWth;
            if (horizontalTextPosition == CENTER) {
                maxTextWth = VIEW_BOUNDS.width;
            } else {
                maxTextWth = VIEW_BOUNDS.width - (ICON_BOUNDS.width + gap);
            }
            // Calculate text bounds, clipping text if necessary to remain within view bounds
            final StringTokenizer lines = new StringTokenizer(text, "\n");
			TEXT_BOUNDS.height = metrics.getHeight() * lines.countTokens();
        	TEXT_BOUNDS.width = 0;
        	String line;
        	int lineWth = 0;
        	int ellipsisWth = 0;
        	boolean isClipped = false;
            while (lines.hasMoreTokens()) {
                line = lines.nextToken();
                lineWth = metrics.stringWidth(line);
                if (lineWth > maxTextWth) {
                    if (!isClipped) {
                        ellipsisWth = metrics.stringWidth("...");
                        isClipped = true;
                    }
                    if (line.length() > 0) {
                        do {
                            line = line.substring(0, line.length() - 1);
                            lineWth = metrics.stringWidth(line);
                        } while (lineWth + ellipsisWth > maxTextWth  &&  line.length() > 0);
                    }
                    lineWth += ellipsisWth;
                    line += "...";
                }
                if (textBuf.length() > 0)
                    textBuf.append('\n');
                textBuf.append(line);
				TEXT_BOUNDS.width = Math.max(TEXT_BOUNDS.width, lineWth);
			}
        }
        // Calculate vertical location of text
        horizontalAlignment = translateOrientationValue(horizontalAlignment);
        horizontalTextPosition = translateOrientationValue(horizontalTextPosition);
        if (verticalTextPosition == TOP) {
            if (horizontalTextPosition == CENTER) {
                TEXT_BOUNDS.y = -(TEXT_BOUNDS.height + gap);
            } else {
                TEXT_BOUNDS.y = 0;
            }
        } else if (verticalTextPosition == CENTER) {
            TEXT_BOUNDS.y = ICON_BOUNDS.height / 2 - TEXT_BOUNDS.height / 2;
        } else {// (verticalTextPosition == SwingUtilities.BOTTOM)
            if (horizontalTextPosition != CENTER) {
                TEXT_BOUNDS.y = ICON_BOUNDS.height - TEXT_BOUNDS.height;
            } else {
                TEXT_BOUNDS.y = ICON_BOUNDS.height + gap;
            }
        }
        // Calculate horizontal location of text
        if (horizontalTextPosition == LEFT) {
            TEXT_BOUNDS.x = -(TEXT_BOUNDS.width + gap);
        } else if (horizontalTextPosition == CENTER) {
            TEXT_BOUNDS.x = ICON_BOUNDS.width / 2 - TEXT_BOUNDS.width / 2;
        } else {// (horizontalTextPosition == SwingUtilities.RIGHT)
            TEXT_BOUNDS.x = ICON_BOUNDS.width + gap;
        }
        // Adjust text & icon locations based upon alignment
        union(ICON_BOUNDS, TEXT_BOUNDS, TMP_BOUNDS);
        int dx, dy;
        // Calculate vertical alignment adjustment
        if (verticalAlignment == TOP) {
            dy = VIEW_BOUNDS.y - TMP_BOUNDS.y;
        } else if (verticalAlignment == CENTER) {
            dy = (VIEW_BOUNDS.y + VIEW_BOUNDS.height / 2) - (TMP_BOUNDS.y + TMP_BOUNDS.height / 2);
        } else {// (verticalAlignment == SwingUtilities.BOTTOM)
            dy = (VIEW_BOUNDS.y + VIEW_BOUNDS.height) - (TMP_BOUNDS.y + TMP_BOUNDS.height);
        }
        // Calculate horizontal alignment adjustment
        if (horizontalAlignment == LEFT) {
            dx = VIEW_BOUNDS.x - TMP_BOUNDS.x;
        } else if (horizontalAlignment == RIGHT) {
            dx = (VIEW_BOUNDS.x + VIEW_BOUNDS.width) - (TMP_BOUNDS.x + TMP_BOUNDS.width);
        } else {// (horizontalAlignment == SwingUtilities.CENTER)
            dx = (VIEW_BOUNDS.x + VIEW_BOUNDS.width / 2) - (TMP_BOUNDS.x + TMP_BOUNDS.width / 2);
        }
        // Translate TEXT_BOUNDS and ICON_BOUNDS by dx & dy
        TEXT_BOUNDS.x += dx;
        TEXT_BOUNDS.y += dy;
        ICON_BOUNDS.x += dx;
        ICON_BOUNDS.y += dy;
        
        return textBuf.toString();
    }
    
    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since 2.0
    */
    static void paintText(final Graphics graphic, final JComponent component, final String text, final int mnemonic) {
        final FontMetrics metrics = graphic.getFontMetrics();
        final int ascent = metrics.getAscent();
        final int hgt = metrics.getHeight();
        final StringTokenizer lines = new StringTokenizer(text, "\n");
        final int x = TEXT_BOUNDS.x;
        int y = TEXT_BOUNDS.y + ascent;
        if (component.isEnabled()) {
            graphic.setColor(component.getForeground());
            while (lines.hasMoreTokens()) {
                BasicGraphicsUtils.drawString(graphic, lines.nextToken(), mnemonic, x, y);
                y += hgt;
            }
        }
        else {
            // Paint the text disabled
            final Color bkgd = component.getBackground();
            String line;
            while (lines.hasMoreTokens()) {
                line = lines.nextToken();
                graphic.setColor(bkgd.brighter());
                BasicGraphicsUtils.drawString(graphic, line, mnemonic, x, y);
                graphic.setColor(bkgd.darker());
                BasicGraphicsUtils.drawString(graphic, line, mnemonic, x - 1, y - 1);
                y += hgt;
            }
        }
    }
    
    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since 2.0
    */
    static void setViewBounds(final JComponent component) {
        component.getInsets(INSETS);
        VIEW_BOUNDS.x = INSETS.left;
        VIEW_BOUNDS.y = INSETS.top;
        VIEW_BOUNDS.width = component.getWidth() - (INSETS.right + VIEW_BOUNDS.x);
        VIEW_BOUNDS.height = component.getHeight() - (INSETS.bottom + VIEW_BOUNDS.y);
    }
    
    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    Translates LEADING/TRAILING horizontal orientation values to LEFT/RIGHT values.
    @since 2.0
    */
    private static int translateOrientationValue(final int orientation) {
    	if (orientation == TRAILING) {
            return RIGHT;
        }
        if (orientation == LEADING) {
            return LEFT;
        }
        return orientation;
    }
    
    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since 2.0
    */
    static void union(final Rectangle firstRectangle, final Rectangle secondRectangle, final Rectangle targetRectangle) {
        targetRectangle.x = Math.min(firstRectangle.x, secondRectangle.x);
        targetRectangle.y = Math.min(firstRectangle.y, secondRectangle.y);
        targetRectangle.width = Math.max(firstRectangle.x + firstRectangle.width, secondRectangle.x + secondRectangle.width) -
                                targetRectangle.x;
        targetRectangle.height = Math.max(firstRectangle.y + firstRectangle.height, secondRectangle.y + secondRectangle.height) -
                                 targetRectangle.y;
    }
}
