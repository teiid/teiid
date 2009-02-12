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

import javax.swing.JScrollPane;
import javax.swing.SwingConstants;

// Application imports
//import com.metamatrix.toolbox.ui.widget.laf.ScrollLookAndFeel;

/**
This class is intended to be used everywhere within the application that a scrollable component needs to be displayed.
@since Golden Gate
@version Golden Gate
@author John P. A. Verhaeg
*/
public class ScrollWidget extends JScrollPane
implements SwingConstants {
    //############################################################################################################################
    //# Constructors                                                                                                             #
    //############################################################################################################################

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since Golden Gate
    */
    public ScrollWidget(final Component component) {
        super(component);
    }

    //############################################################################################################################
    //# Instance Methods                                                                                                         #
    //############################################################################################################################

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since Golden Gate
    *//*
    public JScrollBar createHorizontalScrollBar() {
        return new ScrollBar(HORIZONTAL);
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since Golden Gate
    *//*
    public JScrollBar createVerticalScrollBar() {
        return new ScrollBar(VERTICAL);
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since Golden Gate
    *//*
    public void updateUI() {
        setUI(ScrollLookAndFeel.createUI(this));
    }

    //############################################################################################################################
    //# Inner Class: ScrollBar                                                                                                   #
    //############################################################################################################################
    protected class ScrollBar extends com.metamatrix.toolbox.ui.widget.ScrollBar
    implements javax.swing.plaf.UIResource
    {
        //# ScrollBar ############################################################################################################
        //# Instance Variables                                                                                                   #
        //########################################################################################################################

        private boolean isUnitIncrementSet = false;
        private boolean isBlockIncrementSet = false;

        //# ScrollBar ############################################################################################################
        //# Constructors                                                                                                         #
        //########################################################################################################################

        // ScrollBar /////////////////////////////////////////////////////////////////////////////////////////////////////////////
        /**
        @since Golden Gate
        *//*
        public ScrollBar(final int orientation) {
            super(orientation);
        }

        //# ScrollBar ############################################################################################################
        //# Instance Methods                                                                                                     #
        //########################################################################################################################

        // ScrollBar /////////////////////////////////////////////////////////////////////////////////////////////////////////////
        /**
        @since Golden Gate
        *//*
        public int getBlockIncrement(final int direction) {
            final JViewport port = getViewport();
            if (isBlockIncrementSet  ||  port == null) {
                return super.getBlockIncrement(direction);
            }
            final Component view = (Component)port.getView();
            if (view instanceof Scrollable) {
                return ((Scrollable)view).getScrollableBlockIncrement(port.getViewRect(), getOrientation(), direction);
            }
            if (getOrientation() == VERTICAL) {
                return port.getExtentSize().height;
            }
            return port.getExtentSize().width;
        }

        // ScrollBar /////////////////////////////////////////////////////////////////////////////////////////////////////////////
        /**
        @since Golden Gate
        *//*
        public int getUnitIncrement(final int direction) {
            final JViewport port = getViewport();
            if (!isUnitIncrementSet  &&  port != null) {
                final Component view = (Component)port.getView();
                if (view instanceof Scrollable) {
                    return ((Scrollable)view).getScrollableUnitIncrement(port.getViewRect(), getOrientation(), direction);
                }
            }
            return super.getUnitIncrement(direction);
        }

        // ScrollBar /////////////////////////////////////////////////////////////////////////////////////////////////////////////
        /**
        @since Golden Gate
        *//*
        public void setBlockIncrement(final int blockIncrement) { 
            isBlockIncrementSet = true;
            super.setBlockIncrement(blockIncrement);
        }

        // ScrollBar /////////////////////////////////////////////////////////////////////////////////////////////////////////////
        /**
        @since Golden Gate
        *//*
        public void setUnitIncrement(final int unitIncrement) { 
            isUnitIncrementSet = true;
            super.setUnitIncrement(unitIncrement);
        }
    }
*/
}
