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

// JDK imports
import java.awt.BorderLayout;
import java.awt.Component;
import java.awt.Container;
import java.awt.Dimension;
import java.awt.Insets;
import java.awt.LayoutManager;
import java.awt.Point;
import java.awt.Rectangle;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.awt.event.ComponentAdapter;
import java.awt.event.ComponentEvent;
import java.beans.PropertyChangeEvent;
import java.beans.PropertyChangeListener;
import java.util.Hashtable;

import javax.swing.Action;
import javax.swing.BoxLayout;
import javax.swing.Icon;
import javax.swing.JButton;
import javax.swing.JPanel;
import javax.swing.JToolBar;
import javax.swing.JViewport;
import javax.swing.plaf.basic.BasicArrowButton;

import com.metamatrix.toolbox.ui.UIDefaults;
import com.metamatrix.toolbox.ui.widget.laf.ToolBarLookAndFeel;

/**
Like the JToolBar class, the Toolbar is a UI component meant to house
commonly-used controls.  The only major change, compared to the JToolBar,
is that the Toolbar creates ButtonWidgets when dealing with Action objects.
<p>
Because of this change, we had to copy in some of the private implementation
mechanisms used in the JToolBar class.  This includes the registerButtonForAction
and unregisterButtonForAction methods, as well as the inner class ActionChangedListener.
The two aforementioned methods utilize this inner class.  Other calls to superclass
methods will call this class's implementation of the register/unregister methods.
<p>
@see add(Action)

@since 2.1
@version 2.1
@author mbrinkmeier
@author <a href="mailto:jverhaeg@metamatrix.com">John P. A. Verhaeg</a>
*/
public class ToolBar extends JToolBar {
    
    private static final String MARGIN_PROPERTY = "ToolBar.margin";


    private Container buttons;
    private BasicArrowButton leftButton, rightButton;
    private JViewport port;
    private Hashtable listenerRegistry;

    
    /**
    Creates an empty, floatable toolbar oriented in the X direction
    @since 2.1
    */
    public ToolBar(){
        this(HORIZONTAL);
    }

    /**
    Creates and empty, floatable toolbar oriented according to the <code>orientation</code> parameter.
    @since 2.1
    */
    public ToolBar(int orientation){
        super(orientation);
        initializeToolBar();
    }

    /*
    Overrides JToolBar implementation to add (and return) a ButtonWidget.
    The ButtonWidget created has text equal to the Action objects getValue(Action.NAME) property,
    and an Icon corresponding to the getValue(Action.SMALL_ICON) property.  The button is enabled
    only if the Action object is.  !!!WARNING!!! This method is not currently working in a way that
    will allow the button to respond to Action methods like setEnabled.  This functionality was broken
    in JDK 1.3, where the preferred behavior is to create a component (like ButtonWidget) from the action
    and then add the component, rather than adding the Action directly to the toolbar.
    @param action The Action object to be called when a button, corresponding to this parameter and created
    under the sheets, is pressed.
    @return A reference to the ButtonWidget created to correlate with the Action object passed in.
    */
    public JButton add(Action action){
        final ButtonWidget button = new ButtonWidget((String)action.getValue(Action.NAME),
                                                     (Icon)action.getValue(Action.SMALL_ICON));
        button.setHorizontalTextPosition(JButton.CENTER);
        button.setVerticalTextPosition(JButton.BOTTOM);
        button.setEnabled(action.isEnabled());
        button.addActionListener(action);
        add(button);
        return button;
    }
    
    /**
    @since 2.1
    */
    protected void addImpl(final Component component, final Object constraints, int index) {
        buttons.add(component, constraints, index);
    }

    /*
    Adds a SeparatorWidget whose orientation is determined from this ToolBar
    */
    public void addSeparator(){
        SeparatorWidget sep = new SeparatorWidget(this);
        add(sep);
    }

    /*
    Adds a SeparatorWidget whose orientation is determined from this ToolBar, with
    <code>gap</code> number of pixels spacing between the top/bottom border of the
    toolbar (if the toolbar is oriented horizontally; left and right for vertical
    toolbars.)
    */
    public void addSeparator(int gap){
        SeparatorWidget sep = new SeparatorWidget(this, gap);
        add(sep);
    }

    /*
    Adds a SeparatorWidget whose orientation is determined from this ToolBar and size
    is determined by the Dimension input parameter.  Sort of.  The Dimension doesn't
    really seem to do anything at this point.  So use the other addSeparator method.
    */
    public void addSeparator(Dimension d){
        addSeparator(d, 0);
    }

    /*
    Adds a SeparatorWidget whose orientation is determined from this ToolBar and size
    is determined by the Dimension input parameter.  Sort of.  The Dimension doesn't
    really seem to do anything at this point.  So use the other addSeparator method.
    <code>gap</code> specifies space (in pixels) between separator and border normal to
    ToolBar's orientation.
    */
    public void addSeparator(Dimension d, int gap){
        SeparatorWidget sep = new SeparatorWidget(this, gap);
        sep.setSize(d);
        add(sep);
    }

    /*
    Give this an Insets object with the following values in the following instance vars:
    left -> space on left/top (depending on orientation) (yes, it goes there regardless of orientation)
    right -> space on right/bottom
    (total size is left+right+2 if that's useful knowledge)
    top -> gap on top/left (again, regardless of orientation)
    bottom -> gap at bottom/right
    */
    public void addSeparator(Insets insets){
        SeparatorWidget sep= new SeparatorWidget(this,insets);
        add(sep);
    }

    /**
    @since 2.1
    */
    public Container getButtonContainer() {
        return buttons;
    }
    
    /**
    @since 2.1
    */
    public int getComponentIndex(final Component component) {
        final Component[] comps = buttons.getComponents();
        for (int ndx = comps.length; --ndx >= 0;) {
            if (comps[ndx] == component) {
                return ndx;
            }
        }
        return -1;
    }

    /**
    @since 2.1
    */
    public Component getComponentAtIndex(final int index) {
        if (index >= 0  &&  index < getComponentCount()) {
            return buttons.getComponents()[index];
        }
        return null;
    }
    
    /**
    Sets toolBar margins, changes layout to BorderLayout to accomodate left and right scrolling buttons, and adds
    ComponnentListener to dynamically add and remove scrolling buttons as necessary.
    @since 2.1
    */
    protected void initializeToolBar() {
        setMargin(UIDefaults.getInstance().getInsets(MARGIN_PROPERTY));
        super.setLayout(new BorderLayout());
        port = new JViewport();
        buttons = new JPanel(null);
        port.setView(buttons);
        super.addImpl(port, BorderLayout.CENTER, -1);
        setLayout(new BoxLayout(buttons, BoxLayout.X_AXIS));    // Direction is actually determined within overridden setLayout
        port.addComponentListener(new ComponentAdapter() {
            public void componentResized(final ComponentEvent event) {
                updateScrollButtons();
            }
        });
        ((ToolBarLookAndFeel)getUI()).installSubclassListeners();
    }

    /**
    @since 2.1
    */
    public void setLayout(final LayoutManager layout)
    {
        if (buttons != null  &&  layout instanceof BoxLayout) {
            if (leftButton != null) {
                remove(leftButton);
            }
            if (rightButton != null) {
                remove(rightButton);
            }
            if (getOrientation() == HORIZONTAL) {
                buttons.setLayout(new BoxLayout(buttons, BoxLayout.X_AXIS));
                leftButton = new BasicArrowButton(WEST);
                rightButton = new BasicArrowButton(EAST);
                super.addImpl(leftButton, BorderLayout.WEST, -1);
                super.addImpl(rightButton, BorderLayout.EAST, -1);
            } else {
                buttons.setLayout(new BoxLayout(buttons, BoxLayout.Y_AXIS));
                leftButton = new BasicArrowButton(NORTH);
                rightButton = new BasicArrowButton(SOUTH);
                super.addImpl(leftButton, BorderLayout.NORTH, -1);
                super.addImpl(rightButton, BorderLayout.SOUTH, -1);
            }
            leftButton.setVisible(false);
            rightButton.setVisible(false);
            leftButton.addActionListener(new ActionListener() {
                public void actionPerformed(final ActionEvent event) {
                    final Rectangle bounds = port.getViewRect();
                    final Component[] comps = buttons.getComponents();
                    Component comp;
                    if (getOrientation() == HORIZONTAL) {
                        for (int ndx = comps.length;  --ndx >= 0;) {
                            comp = comps[ndx];
                            if (bounds.x > comp.getX()) {
                                port.setViewPosition(new Point(comp.getX(), 0));
                                updateScrollButtons();
                                port.repaint();
                                break;
                            }
                        }
                    } else {
                        for (int ndx = comps.length;  --ndx >= 0;) {
                            comp = comps[ndx];
                            if (bounds.y > comp.getY()) {
                                port.setViewPosition(new Point(0, comp.getY()));
                                updateScrollButtons();
                                port.repaint();
                                break;
                            }
                        }
                    }
                }
            });
            rightButton.addActionListener(new ActionListener() {
                public void actionPerformed(final ActionEvent event) {
                    final Rectangle bounds = port.getViewRect();
                    final Component[] comps = buttons.getComponents();
                    Component comp;
                    int delta;
                    if (getOrientation() == HORIZONTAL) {
                        final int end = bounds.x + bounds.width;
                        for (int ndx = 0;  ndx < comps.length;  ++ndx) {
                            comp = comps[ndx];
                            delta = comp.getX() + comp.getWidth() - end;
                            if (delta > 0) {
                                port.setViewPosition(new Point(bounds.x + delta, 0));
                                updateScrollButtons();
                                port.repaint();
                                break;
                            }
                        }
                    } else {
                        final int end = bounds.y + bounds.height;
                        for (int ndx = 0;  ndx < comps.length;  ++ndx) {
                            comp = comps[ndx];
                            delta = comp.getY() + comp.getHeight() - end;
                            if (delta > 0) {
                                port.setViewPosition(new Point(0, bounds.y + delta));
                                updateScrollButtons();
                                port.repaint();
                                break;
                            }
                        }
                    }
                }
            });
        }
    }

    /*
    Hack to workaround add(Action) modifications.
    Copied from jdk1.2.2 version of JToolbar.
    */
    protected void unregisterButtonForAction(JButton item) {
	    if (listenerRegistry != null) {
	        ActionChangedListener p = (ActionChangedListener)listenerRegistry.remove(item);
    	    if (p!=null) {
	        	Action a = (Action)listenerRegistry.remove(p);
        		if (a!=null) {
		            item.removeActionListener(a);
    		        a.removePropertyChangeListener(p);
	        	}
        		p.setTarget(null);
	        }
    	}
    }

    /**
    @since 2.1
    */
    protected void updateScrollButtons() {
        final Rectangle bounds = port.getViewRect();
        if (getOrientation() == HORIZONTAL) {
            leftButton.setVisible(bounds.x > 0);
            if (rightButton.isVisible()) {
                rightButton.setVisible(bounds.x + bounds.width + rightButton.getWidth() < buttons.getWidth());
            } else {
                rightButton.setVisible(bounds.x + bounds.width < buttons.getWidth());
            }
        } else {
            leftButton.setVisible(bounds.y > 0);
            if (rightButton.isVisible()) {
                rightButton.setVisible(bounds.y + bounds.height + rightButton.getHeight() < buttons.getHeight());
            } else {
                rightButton.setVisible(bounds.y + bounds.height < buttons.getHeight());
            }
        }
    }

    /**
    @since 2.1
    */
    public void updateUI() {
        putClientProperty("JToolBar.isRollover", Boolean.TRUE);
        setUI(ToolBarLookAndFeel.createUI(this));
    }   

    /*
    Hack to workaround add(Action) modifications.
    Copied from jdk1.2.2 version of JToolbar.
    */
    private class ActionChangedListener implements PropertyChangeListener {
        JButton button;
        ActionChangedListener(JButton b) {
            super();
            setTarget(b);
        }
        public void propertyChange(PropertyChangeEvent e) {
            String propertyName = e.getPropertyName();
            if (e.getPropertyName().equals(Action.NAME)) {
                String text = (String) e.getNewValue();
                button.setText(text);
                button.repaint();
            } else if (propertyName.equals("enabled")) {
                Boolean enabledState = (Boolean) e.getNewValue();
                button.setEnabled(enabledState.booleanValue());
                button.repaint();
            } else if (e.getPropertyName().equals(Action.SMALL_ICON)) {
                Icon icon = (Icon) e.getNewValue();
                button.setIcon(icon);
                button.invalidate();
                button.repaint();
            }
        }
    	public void setTarget(JButton b) {
	        this.button = b;
	    }
    }
    
    
	/**
	 * @see java.awt.Container#remove(Component)
	 */
	public void remove(Component comp) {
		buttons.remove(comp);
	}


	/**
	 * @see java.awt.Container#remove(int)
	 */
	public void remove(int index) {
		buttons.remove(index);
	}


	/**
	 * @see java.awt.Container#removeAll()
	 */
	public void removeAll() {
		buttons.removeAll();
	}


}
