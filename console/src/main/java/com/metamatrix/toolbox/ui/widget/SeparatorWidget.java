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

import java.awt.Dimension;
import java.awt.Graphics;
import java.awt.Insets;

import javax.swing.BorderFactory;
import javax.swing.JSeparator;
import javax.swing.JToolBar;

/*
A separator for menus, toolbars, etc.  Can take on fixed or variable (based on a Toolbar parent) orientation.
*/
public class SeparatorWidget extends JSeparator {

    /*
    whether or not to base orientation on a toolbar
    default is false, in which case the SeparatorWidget has fixed orientation
    */
    private boolean getsOrientationFromToolBarParent=false;
    private ToolBar toolbarParent;


    /*
    # of pixels to leave between the top and bottom borders when aligned vertically,
    left and right when horizontal
    */
//    private int gap;

    /*
    Not used anymore.

    private static final int SEPARATOR_SIZE=9;
    private static final int FIRST_LINE_COORD=3;
    private static final int SECOND_LINE_COORD=4;
    */

    private static final int LEFT_INSET_MINIMUM = 5;
    private static final int RIGHT_INSET_MINIMUM = 5;
    private static final int TOP_INSET_MINIMUM = 0;
    private static final int BOTTOM_INSET_MINIMUM =0;

    /*
    Default vertical fixed-orientation SeparatorWidget with no gap between
    the little separator line and the toolbar border at either end of said line.
    */
    public SeparatorWidget(){
        this(JSeparator.VERTICAL,0);
    }

    /*
    Default vertical fixed-orientation SeparatorWidget with space between borders
    determined by input parameter <code>gap</code>.
    */
    public SeparatorWidget(int gap){
        this(JSeparator.VERTICAL, gap);
    }

    /*
    Fixed orientation constructor.  Use JSeparator.HORIZONTAL or VERTICAL.
    Does not check for illegal arguments.
    Gap between borders and drawn portion determined by <code>gap</code>.
    */
    public SeparatorWidget(int orientation, int gap){
        this(orientation, new Insets(gap, 0, gap, 0));
    }

    /*
    Fixed orientation.  Insets determine gap at top and bottom, min size
    for left and right.  These values hold regardless of orientation.  See setBorder().
    */
    public SeparatorWidget(int orientation, Insets insets){
        super(orientation);
        setBorder(insets);
        initializeSeparator();
    }



    /*
    Variable-orientation constructor.  If placed in a horizontal Toolbar, it will align itself vertically.
    Similarly, it will go horizontal in a vertically-anchored Toolbar.
    This does not check that the toolbar is actually an ancestor to the SeparatorWidget.  Please
    excercise caution.
    */
    public SeparatorWidget(ToolBar parent){
        this(parent, 0);
    }

    /*
    Variable-orientation constructor.  If placed in a horizontal Toolbar, it will align itself vertically.
    Similarly, it will go horizontal in a vertically-anchored Toolbar.
    This does not check that the toolbar is actually an ancestor to the SeparatorWidget.  Please
    excercise caution.  Gap between drawn portion and border det'mined by <code>gap</code>.
    */
    public SeparatorWidget(ToolBar parent, int gap){
        this(parent, new Insets(gap, 0, gap, 0));
    }

    public SeparatorWidget(ToolBar parent, Insets insets){
        super(JSeparator.VERTICAL);
        getsOrientationFromToolBarParent=true;
        toolbarParent = parent;
//        this.gap = insets.top;
        setBorder(insets);
        initializeSeparator();

    }


    /*
    widget initializer
    */
    protected void initializeSeparator(){
        //check that we leave some space on the left and right of our insets,
        //and make sure that we don't have negative values for the gaps at the
        //top and bottom
        Insets myInsets = getInsets();

        if(myInsets.left < LEFT_INSET_MINIMUM){
            myInsets.left = LEFT_INSET_MINIMUM;
        }
        if(myInsets.right < RIGHT_INSET_MINIMUM){
            myInsets.right = RIGHT_INSET_MINIMUM;
        }

        if(myInsets.top < TOP_INSET_MINIMUM){
            myInsets.top = TOP_INSET_MINIMUM;
        }
        if(myInsets.bottom < BOTTOM_INSET_MINIMUM){
            myInsets.bottom = BOTTOM_INSET_MINIMUM;
        }

        setBorder(myInsets);
    }

    /*
    Draws the separator.

    Could check that toolbar is an ancestor in this method.  Worth it?
    */
    public void paint(Graphics g){
        Dimension s = getSize();

        /*
        System.out.println("insets: "+getInsets());
        System.out.println("size: "+getSize());
        System.out.println("horiz?: "+(getOrientation()==JSeparator.HORIZONTAL));
        System.out.println("parent: "+new Dimension(getParentWidth(), getParentHeight()));
        */

        int firstLine;
        int secondLine;

        firstLine = getInsets().left;
        secondLine = firstLine +1;

        if ( getOrientation() == JSeparator.VERTICAL )	{
            g.setColor( getBackground().darker().darker().darker() );
            g.drawLine( firstLine, getInsets().bottom, firstLine, s.height-getInsets().top );

            g.setColor( getBackground().brighter() );
            g.drawLine( secondLine, getInsets().bottom, secondLine, s.height-getInsets().top );
	    } else {
            // HORIZONTAL
            g.setColor( getBackground().darker().darker().darker() );
            g.drawLine( getInsets().bottom, firstLine, s.width-getInsets().top, firstLine );

            g.setColor( getBackground().brighter() );
            g.drawLine( getInsets().bottom, secondLine , s.width-getInsets().top, secondLine );
        }


    }

    //careful here
    /*
    If the separator has fixed orientation, it gets that orientation.
    If the separator has a variable orientation based on a Toolbar, it checks the
    Toolbar's orientation and returns the orientation normal to that.  (ie, if the
    toolbar is horizontal, you get a vertical orientation.
    Compare to JSeparator.HORIZONTAL/VERTICAL.
    */
    public int getOrientation(){
        if(getsOrientationFromToolBarParent){
            //do that
            if(toolbarParent.getOrientation() == JToolBar.HORIZONTAL){
                //if we're in a horizontal toolbar, then we want to be vertical
                return JSeparator.VERTICAL;
            }
            //and vice-versa
            return JSeparator.HORIZONTAL;
        }
        return super.getOrientation();
    }


    /*
    Convenience method for use in other methods.
    */
//    private int getParentHeight(){
//        if(getParent() == null){
//            return 0;
//        }
//        else{
//            return getParent().getSize().height;
//        }
//    }

    /*
    Convenience method for use in other methods.
    */
//    private int getParentWidth(){
//        if(getParent() == null){
//            return 0;
//        }
//        else{
//            return getParent().getSize().width;
//        }
//    }

    /*
    Overrides superclass impl. to keep size in check- we don't want these to
    take up too much room on the toolbar.
    */
    public Dimension getPreferredSize(){
        if(getOrientation() == JSeparator.VERTICAL){
            //return new Dimension(super.getPreferredSize().width, SEPARATOR_SIZE);
            //return new Dimension(super.getPreferredSize().width, getInsets().top+getInsets().bottom +2);
            return new Dimension(getInsets().left+getInsets().right+2, super.getPreferredSize().height);
        }
        //return new Dimension(SEPARATOR_SIZE, super.getPreferredSize().height);
        //return new Dimension(getInsets().top+getInsets().bottom +2, super.getPreferredSize().height);
        return new Dimension(super.getPreferredSize().width,getInsets().left+getInsets().right+2);
    }

    /*
    Overrides superclass impl. to keep size in check- we don't want these to
    take up too much room on the toolbar.
    */
    public Dimension getMiniumumSize(){
        if(getOrientation() == JSeparator.VERTICAL){
            //return new Dimension(super.getMinimumSize().width, SEPARATOR_SIZE);
            //return new Dimension(getInsets().left+getInsets().right, getInsets().top+getInsets().bottom +2);
            return new Dimension(getInsets().left+getInsets().right+2, super.getMinimumSize().height);
        }
        //return new Dimension(SEPARATOR_SIZE, super.getMinimumSize().height);
        //return new Dimension(getInsets().top+getInsets().bottom +2, getInsets().left+getInsets().right);
        return new Dimension(super.getMinimumSize().width, getInsets().left+getInsets().right+2);
    }


    /*
    Overrides superclass impl. to keep size in check- we don't want these to
    take up too much room on the toolbar.
    */
    public Dimension getMaximumSize(){
        if(getOrientation() == JSeparator.VERTICAL){
            //return new Dimension(super.getMaximumSize().width, SEPARATOR_SIZE);
            //return new Dimension(super.getMaximumSize().width, getInsets().top+getInsets().bottom +2);
            return new Dimension(getInsets().left+getInsets().right+2, super.getMaximumSize().height);
        }
        //return new Dimension(SEPARATOR_SIZE, super.getMaximumSize().height);
        //return new Dimension(getInsets().top+getInsets().bottom +2, super.getMaximumSize().height);
        return new Dimension(super.getMaximumSize().width,getInsets().left+getInsets().right+2);
    }



    /*
    WARNING:

    The way the SeparatorWidget class uses Insets is funny.  Since the orientation
    can change, we use an Insets object's instance variables for the following:
    *left/right specify the min width or height (depending on orientation, vert or horiz respectively)
    *top/bottom specify the gap

    Basically, we use the Insets as if the SeparatorWidget was always laid out vertically.

    */
    public void setBorder(Insets insets){
        setBorder(BorderFactory.createEmptyBorder(insets.top,insets.left, insets.bottom, insets.right));
    }



}
