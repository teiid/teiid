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
package com.metamatrix.toolbox.ui.widget;

// System imports
import java.awt.Component;

import javax.swing.JComponent;
import javax.swing.JSplitPane;

/**
@since 2.0
@version 2.0
@author <a href="mailto:jverhaeg@metamatrix.com">John P. A. Verhaeg</a>
*/
public class Splitter extends JSplitPane {
    //############################################################################################################################
    //# Constants                                                                                                                #
    //############################################################################################################################

    public static final String WEIGHT_PROPERTY = "resizeWeight";
 
    //############################################################################################################################
    //# Instance Variables                                                                                                       #
    //############################################################################################################################

    private double weight = -1.0;
     
    //############################################################################################################################
    //# Constructors                                                                                                             #
    //############################################################################################################################
    
    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since 2.0
    */
    public Splitter() {
        this(Splitter.HORIZONTAL_SPLIT, false, null, null);
    }
    
    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since 2.0
    */
    public Splitter(final int orientation) {
        this(orientation, false, null, null);
    }
    
    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since 2.0
    */
    public Splitter(final int orientation, final boolean continuousLayout) {
        this(orientation, continuousLayout, null, null);
    }
    
    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since 2.0
    */
    public Splitter(final int orientation, final Component firstComponent, final Component secondComponent) {
        this(orientation, false, firstComponent, secondComponent);
    }
    
    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since 2.0
    */
    public Splitter(final int orientation, final boolean continuousLayout, final Component firstComponent,
                    final Component secondComponent) {
        super(orientation, continuousLayout, firstComponent, secondComponent);
    }
    
    //############################################################################################################################
    //# Instance Methods                                                                                                         #
    //############################################################################################################################
    
    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since 2.0
    */
    public double getResizeWeight() {
        if (weight >= 0.0) {
            return weight;
        }
        final Component firstComp = getLeftComponent();
        int len;
        if (orientation == Splitter.HORIZONTAL_SPLIT) {
            len = getWidth();
        } else {
            len = getHeight();
        }
        if (firstComp == null  ||  len == 0) {
            return 0.0;
        }
        if (orientation == Splitter.HORIZONTAL_SPLIT) {
            return (firstComp.getWidth() + getDividerSize() / 2.0) / len;
        }
        return (firstComp.getHeight() + getDividerSize() / 2.0) / len;
    }
    
    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since 2.0
    */
    public void setDividerLocation(final double location) {
        super.setDividerLocation(location);
        setResizeWeight(location);
    }
    
    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
     * Overridden from super because using this method with javac 1.3.1 is causing us to
     * cast Splitters to JSplitPane
     * @since 3.0
     */
    public void setDividerLocation(int location) {
        super.setDividerLocation(location);
    }
    
    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since 2.0
    */
    public void setResizeWeight(final double weight) {
        if (weight < 0.0  ||  weight > 1.0) {
            throw new IllegalArgumentException("Weight must be between 0 and 1");
        }
        final double oldWeight = this.weight;
        this.weight = weight;
        firePropertyChange(WEIGHT_PROPERTY, oldWeight, weight);
    }
    
    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since 2.0
    */
    public void setRightComponent(final Component component) {
        final int loc = getDividerLocation();
        super.setRightComponent(component);
        if (isShowing()  &&  loc >= 0) {
            super.setDividerLocation(loc);
        }
    }
    
    
	/**
	 * @see java.awt.Container#addImpl(Component, Object, int)
     * @since 3.0
	 */
	protected void addImpl(Component comp, Object constraints, int index) {
        if ( comp instanceof JComponent && ( ! ( comp instanceof JSplitPane) ) ) {
            JComponent jcomp = (JComponent) comp;
            jcomp.setBorder(new SplitterShadowBorder());
        }
		super.addImpl(comp, constraints, index);
	}


}
