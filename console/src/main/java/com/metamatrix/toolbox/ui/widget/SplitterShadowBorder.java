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
import java.awt.Insets;

import javax.swing.border.AbstractBorder;


/**
 * A shaded panel border for Splitter child components
 */
public class SplitterShadowBorder extends AbstractBorder {

    protected static Color background = new Color(192, 192, 192);
    protected static Color farShadow = new Color(171,168, 165);
    protected static Color nearShadow = new Color(143, 141, 138);
    protected static Color innerLine = new Color(128, 128, 128);
    protected static Insets insets = new Insets(2,2,3,3);

    /**
     * Creates a lowered etched border whose colors will be derived
     * from the background color of the component passed into 
     * the paintBorder method.
     */
    public SplitterShadowBorder()    {
    }

    /**
     * Paints the border for the specified component with the 
     * specified position and size.
     * @param c the component for which this border is being painted
     * @param g the paint graphics
     * @param x the x position of the painted border
     * @param y the y position of the painted border
     * @param width the width of the painted border
     * @param height the height of the painted border
     */
    public void paintBorder(Component c, Graphics g, int x, int y, int width, int height) {

        int w = width;
        int h = height;
	
    	g.translate(x, y);
           
	    g.setColor(innerLine);
    	g.drawRect(0, 0, w-3, h-3);
        g.setColor(Color.white);
        g.drawLine(1, 1, w-4, 1);
        g.drawLine(1, 1, 1, h-4);
	
	    g.setColor(nearShadow);
        g.drawLine(w-2, 1, w-2, h-2);
        g.drawLine(1, h-2, w-2, h-2);
	
        g.setColor(farShadow);
        g.drawLine(w-1, 2, w-1, h-2);
        g.drawLine(2, h-1, w-2, h-1);
	
        g.translate(-x, -y);
    }

    /**
     * Returns the insets of the border.
     * @param c the component for which this border insets value applies
     */
    public Insets getBorderInsets(Component c)       {
        return insets;
    }

    /** 
     * Reinitialize the insets parameter with this Border's current Insets. 
     * @param c the component for which this border insets value applies
     * @param insets the object to be reinitialized
     */
    public Insets getBorderInsets(Component c, Insets insets) {
        insets.left = 2;
        insets.top = 2;
        insets.right = 3;
        insets.bottom = 3;
        return insets;
    }

    /**
     * Returns whether or not the border is opaque.
     */
    public boolean isBorderOpaque() { 
        return true; 
    }

    public static void main(String[] args) {

        try {
             javax.swing.UIManager.setLookAndFeel(javax.swing.UIManager.getSystemLookAndFeelClassName());
        } catch(Exception ex) {
        }

        javax.swing.UIManager.put( "SplitPane.border", new javax.swing.border.EmptyBorder(0,0,0,0));
        javax.swing.UIManager.put( "SplitPaneDivider.border", new javax.swing.border.EmptyBorder(0,0,0,0));
        javax.swing.UIManager.put( "TabbedPane.border", new javax.swing.border.EmptyBorder(0,0,0,0));

        javax.swing.JFrame frame = new javax.swing.JFrame("Splitter L&F Test");
        frame.setLocation(300,300);
        frame.setSize(200,200);
        frame.addWindowListener(new java.awt.event.WindowAdapter() {
           public void windowClosing(java.awt.event.WindowEvent e) {System.exit(0);}
        });

       
        javax.swing.JPanel p = new javax.swing.JPanel(new java.awt.BorderLayout());
        //p.setBorder(new SplitterShadowBorder());
        frame.getContentPane().setLayout(new java.awt.BorderLayout());
        
        javax.swing.JPanel p2 = new javax.swing.JPanel();
        //p2.setBorder(new SplitterShadowBorder());
        
        Splitter s = new Splitter(Splitter.HORIZONTAL_SPLIT, p, p2);
        frame.getContentPane().add(s, java.awt.BorderLayout.CENTER);
        
        frame.setVisible(true);
        s.setDividerLocation(0.5);
       
    }

}
