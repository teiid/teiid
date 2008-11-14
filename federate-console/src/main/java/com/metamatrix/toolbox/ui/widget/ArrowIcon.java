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
import java.awt.Component;
import java.awt.Graphics;

import javax.swing.Icon;
import javax.swing.SwingConstants;
import javax.swing.UIManager;

public final class ArrowIcon
  implements Icon,
             SwingConstants
{
  // -------------------------------------------------------------------- //
    // Icon interface - paint a small arrow
    // -------------------------------------------------------------------- //
    private boolean bMenuStyle;
    private int   nDirection, iconWidth, iconHeight;
  private Color   enabledColour, disabledColour;
    // -------------------------------------------------------------------- //


    // -------------------------------------------------------------------- //
    public ArrowIcon()
    {
      this(EAST, 8, false,
        (Color)UIManager.getDefaults().get("Label.foreground"),
        (Color)UIManager.getDefaults().get("Label.disabledForeground"));
    }
    // -------------------------------------------------------------------- //
    public ArrowIcon(int direction, int size)
    {
      this(direction, size, false,
        (Color)UIManager.getDefaults().get("Label.foreground"),
        (Color)UIManager.getDefaults().get("Label.disabledForeground"));
    }
    // -------------------------------------------------------------------- //
    public ArrowIcon(int direction, int size, boolean isMenu)
    {
      this(direction, size, isMenu,
        (Color)UIManager.getDefaults().get("Label.foreground"),
        (Color)UIManager.getDefaults().get("Label.disabledForeground"));
  }
    // -------------------------------------------------------------------- //
    public ArrowIcon(int direction, int size, boolean isMenu, Color enb,
      Color dsb)
    {
      bMenuStyle = isMenu;
    nDirection = direction;
    enabledColour = enb;
    disabledColour = dsb;
    if (isMenu)
    {
      switch (nDirection)
      {
        case NORTH:
        case SOUTH:
          iconWidth = size;
          iconHeight = size/2;
          break;
        case EAST:
        case WEST:
          iconWidth = size/2;
          iconHeight = size;
          break;
      }
    }
    else
    {
      iconWidth = size;
      iconHeight = size;
    }
  }
    // -------------------------------------------------------------------- //
  public int getIconWidth() { return iconWidth; }
    // -------------------------------------------------------------------- //
  public int getIconHeight() { return iconHeight; }
    // -------------------------------------------------------------------- //
    public void paintIcon( Component c, Graphics g, int x, int y )
    {
        // Move the graphics origin
        g.translate(x-1, y);
        // Set the appropriate colour
        g.setColor( (c.isEnabled()) ? enabledColour : disabledColour );
    if (bMenuStyle)
      paintMenuArrow(g);
    else
      paintDirectionArrow(g);
        // Shift the origin back
        g.translate( -x, -y );
    }
  // -------------------------------------------------------------------- //
  private void paintMenuArrow(Graphics g)
  {
    switch (nDirection)
    {
      case NORTH:
        for (int y = 0; y < iconHeight; y++)
          g.drawLine(y, iconHeight-y, iconWidth-y, iconHeight-y);
            break;
      case SOUTH:
        for (int y = 0; y < iconHeight; y++)
          g.drawLine(y, y, iconWidth-y, y);
            break;
      case WEST:
        for (int x = 0; x < iconWidth; x++)
          g.drawLine(x, (iconWidth-x), x, iconHeight-iconWidth+x);
            break;
      case EAST:
        for (int x = 0; x < iconWidth; x++)
          g.drawLine(x, x, x, iconHeight-x);
            break;
      default:
        g.drawLine(0, 0, iconHeight, iconHeight);
        g.drawLine(iconHeight, 0, 0, iconHeight);
        break;
    }
  }
  // -------------------------------------------------------------------- //
  private void paintDirectionArrow(Graphics g)
  {
    switch (nDirection)
    {
      case NORTH:
        for (int y = 0; y < iconHeight; y++)
          g.drawLine((iconHeight-y)/2, y, iconWidth - ((iconHeight-y)/2), y);
            break;
      case SOUTH:
        for (int y = 0; y < iconHeight; y++)
          g.drawLine((y+1)/2, y, iconWidth-(y+1)/2, y);
            break;
      case EAST:
        for (int x = 0; x < iconWidth; x++)
          g.drawLine(x, (x+1)/2, x, iconHeight-((x+1)/2));
            break;
      case WEST:
        for (int x = 0; x < iconWidth; x++)
          g.drawLine(iconWidth-x, (x+1)/2, iconWidth-x, iconHeight-((x+1)/2));
            break;
      default:
        g.drawLine(0, 0, iconHeight, iconHeight);
        g.drawLine(iconHeight, 0, 0, iconHeight);
        break;
    }
  }
  // -------------------------------------------------------------------- //
}
