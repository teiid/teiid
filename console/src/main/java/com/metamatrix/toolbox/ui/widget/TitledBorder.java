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
package com.metamatrix.toolbox.ui.widget;

// JDK imports
import java.awt.Color;
import java.awt.Component;
import java.awt.Font;
import java.awt.FontMetrics;
import java.awt.Graphics;
import java.awt.Insets;
import java.awt.Point;
import java.awt.Rectangle;

import javax.swing.border.Border;

/**
TitledBorder overrides the JDK class by the same name merely to provide space between the title and the border.
@since 2.1
@version 2.1
@author <a href="mailto:jverhaeg@metamatrix.com">John P. A. Verhaeg</a>
*/
public class TitledBorder extends javax.swing.border.TitledBorder {
    //############################################################################################################################
    //# Constants                                                                                                                #
    //############################################################################################################################
    
    private static final Point textLoc = new Point();

    //############################################################################################################################
    //# Static Methods                                                                                                           #
    //############################################################################################################################

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    Copied directly from JDK's TitledBorder class.
    @since 2.1
    */
    protected static boolean computeIntersection(Rectangle dest, int rx, int ry, int rw, int rh) {
        int x1 = Math.max(rx, dest.x);
        int x2 = Math.min(rx + rw, dest.x + dest.width);
        int y1 = Math.max(ry, dest.y);
        int y2 = Math.min(ry + rh, dest.y + dest.height);
            dest.x = x1;
            dest.y = y1;
            dest.width = x2 - x1;
            dest.height = y2 - y1;
    
        if (dest.width <= 0 || dest.height <= 0) {
            return false;
        }
        return true;
    }  
    
    //############################################################################################################################
    //# Constructors                                                                                                             #
    //############################################################################################################################
    
    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since 2.1
    */
    public TitledBorder(final String text) {
        super(text);
    }
    
    //############################################################################################################################
    //# Instance Methods                                                                                                         #
    //############################################################################################################################
    
    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    Copied directly from JDK's TitledBorder class.
    @since 2.1
    */
    public void paintBorder(final Component c, final Graphics g, final int x, final int y, final int width, final int height) {
        Border border = getBorder();

        if (getTitle() == null || getTitle().equals("")) {
            if (border != null) {
                border.paintBorder(c, g, x, y, width, height);
            }
            return;
        }

        Rectangle grooveRect = new Rectangle(x + EDGE_SPACING, y + EDGE_SPACING,
                                             width - (EDGE_SPACING * 2),
                                             height - (EDGE_SPACING * 2));
        Font font = g.getFont();
        Color color = g.getColor();

        g.setFont(getFont(c));

        FontMetrics fm = g.getFontMetrics();
        int         fontHeight = fm.getHeight();
        int         descent = fm.getDescent();
        int         ascent = fm.getAscent();
        int         diff;
        int         stringWidth = fm.stringWidth(getTitle());
        int         halfChrWth = stringWidth / getTitle().length() / 2;
        stringWidth += halfChrWth * 2;
        Insets      insets;

        if (border != null) {
            insets = border.getBorderInsets(c);
        } else {
            insets = new Insets(0, 0, 0, 0);
        }

        int titlePos = getTitlePosition();
        switch (titlePos) {
            case ABOVE_TOP:
                diff = ascent + descent + (Math.max(EDGE_SPACING,
                                TEXT_SPACING*2) - EDGE_SPACING);
                grooveRect.y += diff;
                grooveRect.height -= diff;
                textLoc.y = grooveRect.y - (descent + TEXT_SPACING);
                break;
            case TOP:
            case DEFAULT_POSITION:
                diff = Math.max(0, ((ascent/2) + TEXT_SPACING) - EDGE_SPACING);
                grooveRect.y += diff;
                grooveRect.height -= diff;
                textLoc.y = (grooveRect.y - descent) +
                (insets.top + ascent + descent)/2;
                break;
            case BELOW_TOP:
                textLoc.y = grooveRect.y + insets.top + ascent + TEXT_SPACING;
                break;
            case ABOVE_BOTTOM:
                textLoc.y = (grooveRect.y + grooveRect.height) -
                (insets.bottom + descent + TEXT_SPACING);
                break;
            case BOTTOM:
                grooveRect.height -= fontHeight/2;
                textLoc.y = ((grooveRect.y + grooveRect.height) - descent) +
                        ((ascent + descent) - insets.bottom)/2;
                break;
            case BELOW_BOTTOM:
                grooveRect.height -= fontHeight;
                textLoc.y = grooveRect.y + grooveRect.height + ascent +
                        TEXT_SPACING;
                break;
        }

        int justification = getTitleJustification();
        if (c.getComponentOrientation().isLeftToRight()) {
            if(justification==LEADING || 
               justification==DEFAULT_JUSTIFICATION) {
                justification = LEFT;
            }
            else if(justification==TRAILING) {
                justification = RIGHT;
            }
        }
        else {
            if(justification==LEADING ||
               justification==DEFAULT_JUSTIFICATION) {
                justification = RIGHT;
            }
            else if(justification==TRAILING) {
                justification = LEFT;
            }
        }

        switch (justification) {
            case LEFT:
                textLoc.x = grooveRect.x + TEXT_INSET_H + insets.left;
                break;
            case RIGHT:
                textLoc.x = (grooveRect.x + grooveRect.width) -
                        (stringWidth + TEXT_INSET_H + insets.right);
                break;
            case CENTER:
                textLoc.x = grooveRect.x +
                        ((grooveRect.width - stringWidth) / 2);
                break;
        }

        // If title is positioned in middle of border we'll
        // need to paint the border in sections to leave
        // space for the component's background to show
        // through the title.
        //
        if (border != null) {
            if (titlePos == TOP || titlePos == BOTTOM ||
        titlePos == DEFAULT_POSITION) {
                Rectangle clipRect = new Rectangle();
                
                // save original clip
                Rectangle saveClip = g.getClipBounds();            

                // paint strip left of text
                clipRect.setBounds(saveClip);
                if (computeIntersection(clipRect, x, y, textLoc.x, height)) {
                    g.setClip(clipRect);
                    border.paintBorder(c, g, grooveRect.x, grooveRect.y,
                                  grooveRect.width, grooveRect.height);
                }

                // paint strip right of text
                clipRect.setBounds(saveClip);
                if (computeIntersection(clipRect, textLoc.x+stringWidth, 0,
                               width-stringWidth-textLoc.x, height)) {
                    g.setClip(clipRect);
                    border.paintBorder(c, g, grooveRect.x, grooveRect.y,
                                  grooveRect.width, grooveRect.height);
                }

                // paint strip below or above text
                clipRect.setBounds(saveClip);
                if (titlePos == TOP || titlePos == DEFAULT_POSITION) {
                    if (computeIntersection(clipRect, textLoc.x, grooveRect.y+insets.top, 
                                        stringWidth, height-grooveRect.y-insets.top)) {
                        g.setClip(clipRect);
                        border.paintBorder(c, g, grooveRect.x, grooveRect.y,
                                  grooveRect.width, grooveRect.height);
                    }
                } else { // titlePos == BOTTOM
                    if (computeIntersection(clipRect, textLoc.x, y, 
                          stringWidth, height-insets.bottom-
                                        (height-grooveRect.height-grooveRect.y))) {
                        g.setClip(clipRect); 
                        border.paintBorder(c, g, grooveRect.x, grooveRect.y,
                                  grooveRect.width, grooveRect.height);
                    }
                }

                // restore clip
                g.setClip(saveClip);   

            } else {
                border.paintBorder(c, g, grooveRect.x, grooveRect.y,
                                  grooveRect.width, grooveRect.height);
            }
        }

        g.setColor(getTitleColor());
        g.drawString(getTitle(), textLoc.x + halfChrWth, textLoc.y);

        g.setFont(font);
        g.setColor(color);
    }
}
