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

// System imports
import java.awt.Component;
import java.awt.Dimension;
import java.awt.Point;
import java.awt.Toolkit;

import javax.swing.JPopupMenu;

/**
@since 2.0
@version 2.0
@author <a href="mailto:jverhaeg@metamatrix.com">John P. A. Verhaeg</a>
*/
public class PopupMenu extends JPopupMenu {
    //############################################################################################################################
    //# Instance Methods                                                                                                         #
    //############################################################################################################################
     
    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    Overridden to guarantee the menu is shown within the boundaries of the screen.
    @since 2.0
    */
    public void setLocation(final int x, final int y) {
        final Point point = new Point(x, y);
        final Component invoker = getInvoker();
        if (invoker != null) {
            final Dimension screenSize = Toolkit.getDefaultToolkit().getScreenSize();
            final Dimension size = getPreferredSize();
            point.x = Math.max(Math.min(point.x + size.width, screenSize.width) - size.width, 0);
            point.y = Math.max(Math.min(point.y + size.height, screenSize.height) - size.height, 0);
        }
        super.setLocation(point.x, point.y);
    }
}
