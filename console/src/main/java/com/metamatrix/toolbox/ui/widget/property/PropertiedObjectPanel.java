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

package com.metamatrix.toolbox.ui.widget.property;

import java.awt.BorderLayout;
import java.awt.Component;
import java.awt.Container;
import java.awt.Cursor;
import java.awt.Dimension;
import java.awt.Point;
import java.awt.Rectangle;
import java.awt.Window;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.awt.event.ComponentAdapter;
import java.awt.event.ComponentEvent;
import java.awt.event.FocusAdapter;
import java.awt.event.FocusEvent;
import java.awt.event.FocusListener;
import java.awt.event.KeyEvent;
import java.awt.event.MouseAdapter;
import java.awt.event.MouseEvent;
import java.beans.PropertyChangeListener;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import javax.swing.BoxLayout;
import javax.swing.FocusManager;
import javax.swing.JComboBox;
import javax.swing.JComponent;
import javax.swing.JPanel;
import javax.swing.JPasswordField;
import javax.swing.JScrollBar;
import javax.swing.JScrollPane;
import javax.swing.JViewport;
import javax.swing.KeyStroke;
import javax.swing.RepaintManager;
import javax.swing.SwingUtilities;
import javax.swing.border.Border;
import javax.swing.event.ChangeEvent;
import javax.swing.event.ChangeListener;
import javax.swing.event.EventListenerList;
import javax.swing.event.MouseInputAdapter;
import javax.swing.event.MouseInputListener;

import com.metamatrix.common.log.LogManager;
import com.metamatrix.common.object.PropertiedObject;
import com.metamatrix.common.object.PropertiedObjectEditor;
import com.metamatrix.common.object.PropertyDefinition;
import com.metamatrix.common.transaction.TransactionException;
import com.metamatrix.common.transaction.UserTransaction;
import com.metamatrix.common.util.crypto.Encryptor;
import com.metamatrix.toolbox.ui.UIConstants;
import com.metamatrix.toolbox.ui.UIDefaults;
import com.metamatrix.toolbox.ui.widget.ButtonWidget;
import com.metamatrix.toolbox.ui.widget.LabelWidget;
import com.metamatrix.toolbox.ui.widget.PasswordButton;
import com.metamatrix.toolbox.ui.widget.TextFieldWidget;

/**
 * @since 2.0
 */
public class PropertiedObjectPanel extends JPanel
implements UIConstants {
		
    //############################################################################################################################
    //# Constants                                                                                                                #
    //############################################################################################################################

    private static final String DEFAULT_NAME_COLUMN_NAME = "Name";
    private static final String DEFAULT_VALUE_COLUMN_NAME = "Value";
    
    private static final int GAP = UIDefaults.getInstance().getInt(SPACER_HORIZONTAL_LENGTH_PROPERTY);
    
    private static final String NAME_ID = "PropertyPanel.NameCell.";
    private static final String VALUE_ID = "PropertyPanel.ValueCell.";
    
    static final String LOG_CONTEXT = "PROPERTY";
     
    //############################################################################################################################
    //# Instance Variables                                                                                                       #
    //############################################################################################################################

    private PropertiedObject propObj;
    private JComponent[][] propComps = null;
    private PropertyChangeAdapter adapter;
    private PropertiedObjectEditor editor;
    private Object xActionSrc;

    private JPanel propsPanel = new JPanel(null) {
        public Dimension getPreferredSize() {
            final Dimension size = super.getPreferredSize();
            final int wth = getWidth();
            if (wth > 0) {
                size.width = wth;
            }
            return size;
        }
    };
    private final JPanel hdr = new JPanel(null);
    private JScrollPane scroller;
    private final LabelWidget nameColLabel = new LabelWidget(DEFAULT_NAME_COLUMN_NAME);
    private final LabelWidget valColLabel = new LabelWidget(DEFAULT_VALUE_COLUMN_NAME) {
        public Dimension getPreferredSize() {
            final int wth = propsPanel.getWidth();
            if (wth > 0) {
                return new Dimension(wth - nameColLabel.getPreferredSize().width, super.getPreferredSize().height);
            }
            return super.getPreferredSize();
        }
    };

    private PropertyComponentFactory factory;
    
    private boolean showHidden = false;
    private boolean showExpert = false;
    private boolean showToolTips = true;
    private boolean showRequired = false;
    private boolean showInvalid = false;
    private boolean readOnlyForced = false;
    
    private Collection propertiesToFilterOut = Collections.EMPTY_LIST;
    private Collection propertiesToDisplay = Collections.EMPTY_LIST;

    private int currentWth;

    private boolean isFocusCycleRoot;
    
    //############################################################################################################################
    //# Constructors                                                                                                             #
    //############################################################################################################################

    /**
     * Constructs and returns an instance of this class using itself as a transaction source.
     * @since 2.0
     */
    public PropertiedObjectPanel(final PropertiedObjectEditor editor, final Encryptor encryptor) {
        this(editor, encryptor, null);
    }

    /**
    Constructs and returns an instance of this class.
    @since 2.0
    */
    public PropertiedObjectPanel(final PropertiedObjectEditor editor, final Encryptor encryptor, final Object transactionSource) {
        super(new BorderLayout());
        if (transactionSource == null) {
            xActionSrc = this;
        } else {
            xActionSrc = transactionSource;
        }
        adapter = new PropertyChangeAdapter(editor, xActionSrc);
        this.editor = editor;
        factory = new PropertyComponentFactory(encryptor);

        factory.setPropertiedObjectEditor(editor);
        factory.setPropertyChangeAdapter(adapter);
        
        // Viewport overridden below to stop autoscrolling horizontally.  Code copied verbatim from JDK source.
        // 'X/width' portions removed from scrollRectToVisible(Rectangle) method, and certain unnecessary checks from each method.
        scroller = new JScrollPane(propsPanel) {
            protected JViewport createViewport() {
                return new JViewport() {
                    private int positionAdjustment(int parentWidth, int childWidth, int childAt)    {
                        if (childAt >= 0 && childWidth + childAt <= parentWidth)    {
                            return 0;
                        }
                        if (childAt <= 0 && childWidth + childAt >= parentWidth) {
                            return 0;
                        }
                        if (childAt > 0 && childWidth <= parentWidth)    {
                            return -childAt + parentWidth - childWidth;
                        }
                        if (childAt >= 0 && childWidth >= parentWidth)   {
                            return -childAt;
                        }
                        if (childAt <= 0 && childWidth <= parentWidth)   {
                            return -childAt;
                        }
                        if (childAt < 0 && childWidth >= parentWidth)    {
                            return -childAt + parentWidth - childWidth;
                        }
                        return 0;
                    }
                    public void scrollRectToVisible(final Rectangle bounds) {
                        final Component view = getView();
                        if (view == null) {
                            return;
                        }
                	    if (!view.isValid()) {
                    		validateView();
                	    }
                        final int dy = positionAdjustment(getHeight(), bounds.height, bounds.y);
                        if (dy != 0) {
                            final Point viewPosition = getViewPosition();
                    		final Dimension viewSize = view.getSize();
                    		final int startY = viewPosition.y;
                    		final Dimension extent = getExtentSize();
                    		viewPosition.y -= dy;
                            if (view.isValid()) {
                                if (viewPosition.y + extent.height > viewSize.height) {
                                    viewPosition.y = Math.max(0, viewSize.height - extent.height);
                                } else if (viewPosition.y < 0) {
                                    viewPosition.y = 0;
                                }
                            }
                    		if (viewPosition.y != startY) {
                    		    setViewPosition(viewPosition);
                    		    scrollUnderway = false;
                    		}
                        }
                    }
                    private void validateView() {
                        if ( SwingUtilities.isEventDispatchThread() ) {
	                        Component validateRoot = null;
	                        for(Component c = this; c != null; c = c.getParent()) {
	                    	    if (!c.isDisplayable()) {
	                    			return;
	                    	    }
	                    	    if ((c instanceof JComponent)  &&  (((JComponent)c).isValidateRoot())) {
	                        		validateRoot = c;
	                        		break;
	                    	    }
	                    	}
	                    	if (validateRoot == null) {
	                    	    return;
	                    	}
	                    	Component root = null;
	                    	for(Component c = validateRoot; c != null; c = c.getParent()) {
	                    	    if (!c.isDisplayable()) {
		                    		return;
	                    	    }
	                    	    if (c instanceof Window) {
	                        		root = c;
	                        		break;
	                    	    }
	                    	}
	                    	if (root == null) {
	                    	    return;
	                    	}
	                    	validateRoot.validate();
	                    	RepaintManager rm = RepaintManager.currentManager(this);
	                    	if (rm != null) {
	                    	    rm.removeInvalidComponent((JComponent)validateRoot);
	                    	}
                        } else {
                            SwingUtilities.invokeLater(new Runnable() {
                                public void run() {
                                    validateView();
                                }
                            });
                        }
                                
                    }
                };
            }
        };
        sizeColumnsToFitViewport(scroller);
    }

    //############################################################################################################################
    //# Instance Methods                                                                                                         #
    //############################################################################################################################

    /**
    Registers the specified listener to be notified whenever the value of one of the displayed components changes.  An event will
    be fired to the listener regardless of whether the new value if valid.  Since the property whose value the component is
    displaying is only changed if the value if valid, the PropertiedObjectEditor's getValue may return the either the new or old
    value depending on the validity of the new value.  This method only works if a call has NOT been made to the
    setPropertyChangeAdapter method with a null adapter parameter.
    @param listener The PropertyChangeListener
    @since 2.0
    */
    public void addPropertyChangeListener(final PropertyChangeListener listener) {
        if (adapter != null) {
            adapter.addPropertyChangeListener(listener);
        }
    }

    /**
    @since 2.0
    */
    public void addPropertySelectionListener(final PropertySelectionListener listener) {
        listenerList.add(PropertySelectionListener.class, listener);
    }
    
    /**
    Clears the panel contents.
    @since 2.0
    */
    public void clear() {
        setPropertiedObject(null);
    }

    /**
    Clear the list of properties not to display.
    @since 2.0
    */
    public void clearHiddenPropertyDefinitions() {
        propertiesToFilterOut = Collections.EMPTY_LIST;
    }

    /**
    Construct the visual components for this object.
    @since 2.0
    */
    public void createComponent() {
        // Set layout of properties panel so name-value property panels get added vertically
        propsPanel.setLayout(new BoxLayout(propsPanel, BoxLayout.Y_AXIS));
        propsPanel.setBorder(javax.swing.BorderFactory.createEmptyBorder(2, 2, 2, 2));
        // Make header labels look like buttons
        final Border border = UIDefaults.getInstance().getBorder(ButtonWidget.BORDER_PROPERTY);
        nameColLabel.setBorder(border);
        valColLabel.setBorder(border);
        valColLabel.setMaximumSize(new Dimension(Short.MAX_VALUE, valColLabel.getPreferredSize().height));
        // Add header panel to scroller column header
        hdr.setLayout(new BoxLayout(hdr, BoxLayout.X_AXIS));
        hdr.add(nameColLabel);
        hdr.add(valColLabel);
        scroller.setColumnHeaderView(hdr);
        scroller.getVerticalScrollBar().setUnitIncrement(new TextFieldWidget().getPreferredSize().height);
        // Add scroller as sole component in this panel
        add(scroller, BorderLayout.CENTER);
        addComponentListener(new ComponentAdapter() {
            public void componentResized(final ComponentEvent event) {
                final int viewWth = scroller.getViewport().getWidth();
                if (propsPanel.getWidth() > viewWth) {
                    final int x = nameColLabel.getWidth();
                    final int wth = viewWth - x;
                    valColLabel.setLocation(x, valColLabel.getY());
                    Dimension size = new Dimension(wth, valColLabel.getHeight());
                    valColLabel.setSize(size);
                    if (propComps != null) {
                        JComponent comp;
                        for (int ndx = propComps.length;  --ndx >= 0;) {
                            comp = propComps[ndx][1];
                            comp.setLocation(x, comp.getY());
                            size = new Dimension(wth, comp.getHeight());
                            comp.setSize(size);
                        }
                        propsPanel.setSize(viewWth, propsPanel.getHeight());
                    }
                }
                valColLabel.revalidate();
            }
        });
        // Add column resizing ability
        final MouseInputListener resizeListener = new MouseInputAdapter() {
            JComponent hdr;
            int origX;
            private JComponent getResizingHeader(final JComponent hdr, final int x) {
                if (x >= hdr.getWidth() - 3) {
                    return hdr;
                }
                if (hdr == valColLabel  &&  x < 3) {
                    return nameColLabel;
                }
                return null;
            }
            private int getX(final MouseEvent event) {
                final Point point = event.getPoint();
                SwingUtilities.convertPointToScreen(point, event.getComponent());
                return point.x;
            }
            public void mouseDragged(final MouseEvent event) {
                if (hdr == null) {
                    return;
                }
                final int x = getX(event);
                int delta = origX - x;
                int wth = hdr.getWidth() - delta;
                final int minWth = hdr.getMinimumSize().width;
                if (wth < minWth) {
                    wth = minWth;
                    delta = hdr.getWidth() - minWth;
                }
                Dimension size = new Dimension(wth, hdr.getHeight());
                hdr.setSize(size);
                hdr.setPreferredSize(size);
                if (hdr == nameColLabel) {
                    valColLabel.setLocation(valColLabel.getX() - delta, valColLabel.getY());
                    if (propComps != null) {
                        JComponent comp;
                        for (int ndx = propComps.length;  --ndx >= 0;) {
                            comp = propComps[ndx][0];
                            size = new Dimension(comp.getWidth() - delta, comp.getHeight());
                            comp.setSize(size);
                            comp.setPreferredSize(size);
                            comp.setMinimumSize(size);
                            comp.setMaximumSize(size);
                            comp = propComps[ndx][1];
                            comp.setLocation(comp.getX() - delta, comp.getY());
                        }
                    }
                    wth = propsPanel.getParent().getWidth();
                    if (propsPanel.getWidth() - delta < wth) {
                        size = new Dimension(wth - hdr.getWidth(), valColLabel.getHeight());
                        valColLabel.setSize(size);
                        valColLabel.setPreferredSize(size);
                        delta = 0;
                    }
                } else {
                    wth = propsPanel.getParent().getWidth();
                    if (propsPanel.getWidth() - delta < wth) {
                        delta = wth - (propsPanel.getWidth() - delta);
                        wth = nameColLabel.getWidth() + delta;
                        size = new Dimension(wth, nameColLabel.getHeight());
                        nameColLabel.setSize(size);
                        nameColLabel.setPreferredSize(size);
                        if (propComps != null) {
                            JComponent comp;
                            for (int ndx = propComps.length;  --ndx >= 0;) {
                                comp = propComps[ndx][0];
                                size = new Dimension(comp.getWidth() + delta, comp.getHeight());
                                comp.setSize(size);
                                comp.setPreferredSize(size);
                                comp.setMinimumSize(size);
                                comp.setMaximumSize(size);
                                comp = propComps[ndx][1];
                                comp.setLocation(comp.getX() - delta, comp.getY());
                            }
                        }
                        delta = 0;
                    }
                }
                size = new Dimension(propsPanel.getWidth() - delta, propsPanel.getHeight());
                propsPanel.setSize(size);
                propsPanel.revalidate();
                origX = x;
                currentWth = nameColLabel.getWidth();
            }
            public void mouseMoved(final MouseEvent event) {
                final JComponent hdr = getResizingHeader((JComponent)event.getComponent(), event.getX());
                if (hdr != null) {
                    hdr.setCursor(Cursor.getPredefinedCursor(Cursor.E_RESIZE_CURSOR));
                } else {
                    event.getComponent().setCursor(Cursor.getDefaultCursor());
                }
            }
            public void mousePressed(final MouseEvent event) {
                hdr = getResizingHeader((JComponent)event.getComponent(), event.getX());
                if (hdr != null) {
                    origX = getX(event);
                }
            }
        };
        nameColLabel.addMouseListener(resizeListener);
        nameColLabel.addMouseMotionListener(resizeListener);
        valColLabel.addMouseListener(resizeListener);
        valColLabel.addMouseMotionListener(resizeListener);
        nameColLabel.setMaximumSize(new Dimension(Short.MAX_VALUE, nameColLabel.getPreferredSize().height));
        valColLabel.setMaximumSize(new Dimension(Short.MAX_VALUE, valColLabel.getPreferredSize().height));
    }

    /**
     * @since 2.1
     */
    protected JComponent createComponentFromPropertyDefinition(final PropertyDefinition def, final Object val,
    														   final boolean isReadOnly, final int ndx,
    														   final PropertyDefinitionLabel nameComp) {
        final JComponent valComp = factory.createComponentForPropertyDefinition(def, val, isReadOnly, ndx);
        valComp.setName(VALUE_ID + def.getDisplayName() + '.' + ndx);
        // Make value component get focus when label selected
        nameComp.setLabelFor(valComp);
        final int finalNdx = ndx;
        if (adapter != null  &&  !isReadOnly  &&  def.isModifiable()) {
            final FocusListener focusListener = new FocusListener() {
                public void focusGained(final FocusEvent event) {
                    final JViewport port = scroller.getViewport();
                    final Component comp = event.getComponent();
                    port.scrollRectToVisible(SwingUtilities.convertRectangle(comp, comp.getBounds(), port));
                    adapter.setPropertyDefinition(def);
                    adapter.setEditorComponentBeforeEdit((JComponent)event.getSource());
                }
                public void focusLost(final FocusEvent event) {
                    final JComponent comp = (JComponent)event.getSource();
                    if (comp instanceof PasswordButton) {
                        return;
                    }
                    adapter.setPropertyDefinition(def);
                    adapter.focusLost(event);
                    if (comp instanceof JPasswordField) {
                        final char[] pwd = ((JPasswordField)comp).getPassword();
                        if (pwd != null  &&  pwd.length > 0) {
                            SwingUtilities.invokeLater(new Runnable() {
                                public void run() {
                                    if (!comp.isShowing()) {
                                        return;
                                    }
                                    final Container parent = comp.getParent();
                                    parent.remove(comp);
                                    final JComponent newComp = createComponentFromPropertyDefinition(def, new String(pwd),
                                    																 isReadOnly, ndx, nameComp);
                                    PropertiedObjectPanel.this.propComps[finalNdx][1] = newComp;
                                    parent.add(newComp);
                                    newComp.invalidate();
                                    validate();
                                    parent.repaint();
                                }
                            });
                        }
                    }
                    if (showInvalid) {
                        nameComp.refreshDisplay(!hasValidValue(def,
                                                PropertyChangeAdapter.getValueFromJComponent((JComponent)event.getSource(), true)));
                    }
                }
            };
            final ActionListener actionListener = new ActionListener() {
                public void actionPerformed(final ActionEvent event) {
                    adapter.setPropertyDefinition(def);
                    adapter.actionPerformed(event);
                    if (showInvalid) {
                        nameComp.refreshDisplay(!hasValidValue(def,
                                                PropertyChangeAdapter.getValueFromJComponent((JComponent)event.getSource(), true)));
                    }
                }
            };
            JComponent comp = valComp;
            if (valComp instanceof JComboBox) {
                final JComboBox comboBox = (JComboBox)valComp;
                if (comboBox.isEditable()) {
                    comp = (JComponent)comboBox.getEditor().getEditorComponent();
                }
            }
            comp.addFocusListener(focusListener);
            if (comp instanceof PasswordButton) {
                ((PasswordButton)comp).addChangeListener(new ChangeListener() {
                    public void stateChanged(final ChangeEvent event) {
                        
                        actionListener.actionPerformed(new ActionEvent(event.getSource(), ActionEvent.ACTION_PERFORMED, null));
                    }
                });
            } else {
                try {
                    final Method meth = comp.getClass().getMethod("addActionListener", new Class[] {ActionListener.class});
                    if (meth != null) {
                        meth.invoke(comp, new Object[] {actionListener});
                    }
                } catch (final Exception err) {
                    LogManager.logCritical(LOG_CONTEXT, err,
                                           "Failed to add ActionListener to property editor component.");
                }
            }
        }
        final EventListenerList listenerList = this.listenerList;
        valComp.addFocusListener(new FocusAdapter() {
            public void focusGained(final FocusEvent event) {
                final Object[] listeners = listenerList.getListenerList();
                PropertySelectionEvent newEvent = null;
                for (int ndx2 = listeners.length - 2;  ndx2 >= 0;  ndx2 -= 2) {
                    if (listeners[ndx2] == PropertySelectionListener.class) {
                        if (newEvent == null) {
                              newEvent = new PropertySelectionEvent(PropertiedObjectPanel.this, def);
                        }
                        ((PropertySelectionListener)listeners[ndx2 + 1]).propertySelected(newEvent);
                    }          
                }
            }
        });
        if (valComp.isFocusTraversable()) {
            valComp.registerKeyboardAction(new ActionListener() {
                public void actionPerformed(final ActionEvent event) {
                    FocusManager.getCurrentManager().focusNextComponent((Component)event.getSource());
                }
            }, KeyStroke.getKeyStroke(KeyEvent.VK_DOWN, 0, true), WHEN_FOCUSED);
            valComp.registerKeyboardAction(new ActionListener() {
                public void actionPerformed(final ActionEvent event) {
                    FocusManager.getCurrentManager().focusPreviousComponent((Component)event.getSource());
                }
            }, KeyStroke.getKeyStroke(KeyEvent.VK_UP, 0, true), WHEN_FOCUSED);
        }
        return valComp;
    }
    
    /**
    Creates and returns the 2D object array table model for the properties of the MetadataEntity.
    The depth of the array is exactly 2, where column 0 is filled with PropertyDefinitions from
    the specified MetadataEntity, and column 1 contains JComponents initialized to display and edit
    the value of each PropertyDefinition for the entity.
    @since 2.0
    */
    protected JComponent[][] createModelFromProperties() {
        JComponent[][] propComps = new JComponent[0][2];
        if (propObj == null) {
            return propComps;
        }
        Collection defs;
        if (propertiesToDisplay != null) {
            defs = filterPropertyDefinitionList(propertiesToDisplay);
        } else {
            defs = filterPropertyDefinitionList(editor.getPropertyDefinitions(propObj));
        }
        propComps = new JComponent[defs.size()][2];
        final Iterator iterator = defs.iterator();
        boolean isReadOnly;
        for (int ndx = 0;  iterator.hasNext();  ++ndx) {
            final PropertyDefinition def = (PropertyDefinition)iterator.next();
            isReadOnly = readOnlyForced  ||  editor.isReadOnly(propObj);
            if (!isReadOnly) {
                isReadOnly = editor.isReadOnly(propObj, def);
            }
            final Object val = editor.getValue(propObj, def);
            boolean isInvalid = false;
            if (showInvalid) {
                isInvalid = !hasValidValue(def, val);
            }
            final PropertyDefinitionLabel nameComp = new PropertyDefinitionLabel(def, showToolTips, showRequired, isInvalid);
            nameComp.setName(NAME_ID + def.getDisplayName() + '.' + ndx);
            nameComp.addMouseListener(new MouseAdapter() {
                public void mouseClicked(final MouseEvent event) {
                    nameComp.getLabelFor().requestFocus();
                }
            });
            propComps[ndx][0] = nameComp;
            propComps[ndx][1] = createComponentFromPropertyDefinition(def, val, isReadOnly, ndx, nameComp);
        }
        return propComps;
    }

    /**
    Build a modifiable List of the specified PropertyDefintions based on if the configurable settings for this panel.
    @since 2.0
    */
    protected List filterPropertyDefinitionList(Collection defs) {
        ArrayList result = new ArrayList(defs.size());
        Iterator iter = defs.iterator();
        while ( iter.hasNext() ) {
            PropertyDefinition def = (PropertyDefinition) iter.next();
            if ( ! showHidden && !def.isModifiable()) {
                continue;
            }
            if ( ! showExpert && def.isExpert() && (!def.isRequired() || def.hasDefaultValue())) {
            	continue;
            }
            if ( propertiesToFilterOut.contains(def) ) {
                continue;
            }
            // Skip read-only password fields
            if (def.isMasked()  &&  (readOnlyForced  ||  !def.isModifiable()  ||  editor.isReadOnly(propObj))) {
                continue;
            }
            result.add(def);
        }
        return result;
    }

    /**
    Returns a List of the PropertyDefinitions for which the value columns displayed in the panel
    are invalid for this PropertiedObject.  Typically these will be required properties that have
    not been set.
    @return an ordered List of PropertyDefinition instances, empty if none are invalid.
    @since 2.0
    */
    public List getInvalidDefinitions() {
        List result = Collections.EMPTY_LIST;
        UserTransaction txn = null;
        boolean wasErr = true;
        try {
            txn = editor.createReadTransaction();
            txn.begin();

            final Collection defs = filterPropertyDefinitionList(editor.getPropertyDefinitions(propObj));
            result = new ArrayList(defs.size());
            final Iterator iterator = defs.iterator();
            for (int ndx = 0;  iterator.hasNext();  ++ndx) {
                PropertyDefinition def = (PropertyDefinition)iterator.next();
                if (!hasValidValue(def, PropertyChangeAdapter.getValueFromJComponent(propComps[ndx][1], true))) {
                    result.add(def);
                }
            }
            wasErr = false;
        } catch (TransactionException e) {
            LogManager.logCritical(LOG_CONTEXT, e, "[PropertiedObjectPanel.getInvalidDefinitions] caught exception");
        } finally {
            try {
                if (wasErr) {
                    txn.rollback();
                } else {
                    txn.commit();
                }
            } catch (final TransactionException err) {
                LogManager.logCritical(LOG_CONTEXT, err, "Failed to " + (wasErr ? "rollback." : "commit."));
            }
        }
        txn = null;
        return result;
    }

    /**
    @return The current width of the header for the name column.
    @since 2.0
    */
    public int getNameColumnHeaderWidth() {
        return currentWth;
    }
    
    /**
    @return The current PropertiedObject.
    @since 2.0
    */
    public PropertiedObject getPropertiedObject() {
        return propObj;
    }

    /**
    @return The current PropertyChangeAdapter.
    @since 2.0
    */
    public PropertyChangeAdapter getPropertyChangeAdapter() {
        return adapter;
    }

    /**
    @return The current PropertyComponentFactory.
    @since 2.0
    */
    public PropertyComponentFactory getPropertyComponentFactory() {
        return factory;
    }

    /**
    @return An array of all the components used to display/edit each of the current propertied object's property values
    @since 2.0
    */
    public Component[] getPropertyComponents() {
        if (propComps == null) {
            return null;
        }
        final Component[] comps = new Component[propComps.length];
        for (int ndx = 0;  ndx < propComps.length;  ++ndx) {
            comps[ndx] = propComps[ndx][1];
        }
        return comps;
    }

    /**
    @since 3.0
    */
    public JScrollPane getScrollPane() {
    	return scroller;
    }
    
    /**
    @since 2.0
    */
    public boolean getShowExpertProperties() {
        return showExpert;
    }

    /**
    @since 2.0
    */
    public boolean getShowHiddenProperties() {
        return showHidden;
    }

    /**
    @since 2.0
    */
    public boolean getShowInvalidProperties() {
        return showInvalid;
    }

    /**
    @since 2.0
    */
    public boolean getShowRequiredProperties() {
        return showRequired;
    }

    /**
    @since 2.0
    */
    protected boolean hasValidValue(final PropertyDefinition def, Object value) {
        if (value == null) {
            value = def.getDefaultValue();
        } else if (def.isMasked()  &&  value instanceof char[]) {
            value = new String((char[])value);
        }
        return (editor.isValidValue(propObj, def, value)  &&  (value != null  ||  !def.isRequired()));
    }

    /**
    Determines if this panel is in the act of editing a property.  This method should be
    used to determine if it is safe to use the object displayed in the panel.  Since there
    may be changes pending on the object immediately after focus is obtained by another
    component (by pressing a JButton, for example), it is not safe to use this panel's object
    until this method returns false.  The following is an example of how to use isEditing when
    the PropertiedObject will be used in response to a button's ActionEvent:
    <pre>
    // button listener method that will use the currently displayed PropertiedObject
    public void actionPerformed(final ActionEvent e) {
        if (propertiedObjectPanel.isEditing()) {
            // the panel has not finished setting values on the PropertiedObject
            SwingUtilities.invokeLater(new Runnable() {
                public void run() {
                    // call back on this method and try again
                    actionPerformed(e);
                }
            });
            return;
        }
        // the PropertiedObject is safe to use
    }
    </pre>
    @return True if a property is currently being edited
    */
    public boolean isEditing() {
        if (adapter == null) {
            return false;
        }
        return adapter.isEditing();
    }

    /**
    Indicates whether this panel is acting as a root focus handler when cycling through fields using [Tab]/[Shift-Tab].  If so,
    pressing the [Tab] key while on the last focusable field will cause focus to be transferred to the first focusable field in
    this panel.
    @return True if this panel is acting as a root focus handler
    @since 2.1
    */
    public boolean isFocusCycleRoot() {
        return isFocusCycleRoot;
    }

    /**
    Indicates whether read-only state is currently forced 
    @return True if read-only is forced;  i.e. setReadOnlyForced(true) called
    @since 3.0
    */
    public boolean isReadOnlyForced() {
        return readOnlyForced;
    }
    
    /**
    Refresh the panel's displayed contents.
    @since 2.0
    */
    public void refreshDisplay() {
        refreshDisplay(true);
    }

    /**
    Refresh the panel's displayed contents.
    @since 2.0
    */
    protected void refreshDisplay(final boolean defaultValuesUsed) {
        // Clear the panel
        propsPanel.removeAll();
        // Create array of property name-value component pairs
        propComps = createModelFromProperties();
        // Add name label and corresponding value component to properties panel
        JPanel panel;
        for (int ndx = 0;  ndx < propComps.length;  ++ndx) {
            panel = new JPanel(null);
            panel.setLayout(new BoxLayout(panel, BoxLayout.X_AXIS));
            panel.add(propComps[ndx][0]);
            panel.add(propComps[ndx][1]);
            panel.setMaximumSize(new Dimension(Short.MAX_VALUE, panel.getPreferredSize().height));
            propsPanel.add(panel);
        }
        // Resize name column to fit labels
        resizeNameColumn();
        // Set propertied object in adapter
        if ( adapter != null ) {
            adapter.setPropertiedObject(propObj);
        }
        propsPanel.revalidate();
        propsPanel.repaint();
    }

    /**
    Unregisters the specified listener from getting notifications of value changes within the component displaying values for one
    of the PropertiedObject's properties.
    @param listener The PropertyChangeListener currently registered to receive events
    @since 2.0
    */
    public void removePropertyChangeListener(final PropertyChangeListener listener) {
        if (adapter != null) {
            adapter.removePropertyChangeListener(listener);
        }
    }

    /**
    @since 2.0
    */
    public void removePropertySelectionListener(final PropertySelectionListener listener) {
        listenerList.remove(PropertySelectionListener.class, listener);
    }

    /**
    Overridden to call requestFocus on the component containing the first property value.
    @since 2.0
    */
    public void requestFocus() {
        if (propComps != null  &&  propComps.length > 0) {
            propComps[0][1].requestFocus();
        }
    }
    
    /**
    Resizes the name column to exactly fit its labels (including the header label) plus a small gap.
    @since 2.0
    */
    protected void resizeNameColumn() {
        int wth;
        int hdrWth;
        if (currentWth == 0) {
            // Initialize max width to header label width
            wth = nameColLabel.getMinimumSize().width;
            // Find max property name width
            if (propComps != null) {
                JComponent name;
                for (int ndx = propComps.length;  --ndx >= 0;) {
                    name = propComps[ndx][0];
                    name.setPreferredSize(null);
                    wth = Math.max(wth, name.getPreferredSize().width);
                }
            }
            // Add small gap to width to 'prettify' column
            wth += GAP;
            hdrWth = wth + scroller.getInsets().left;
        } else {
            wth = currentWth - scroller.getInsets().left;
            hdrWth = currentWth;
        }
        // Set all label width's, including header label, to new width
        if (propComps != null) {
            JComponent name;
            Dimension prefSize;
            for (int ndx = propComps.length;  --ndx >= 0;) {
                name = propComps[ndx][0];
                prefSize = new Dimension(wth, name.getPreferredSize().height);
                name.setMinimumSize(prefSize);
                name.setPreferredSize(prefSize);
                name.setMaximumSize(prefSize);
                name.invalidate();
            }
        }
        nameColLabel.setPreferredSize(null);
        nameColLabel.setPreferredSize(new Dimension(hdrWth, nameColLabel.getPreferredSize().height));
        nameColLabel.revalidate();
    }

    /**
    @since 2.0
    */
    public void setBounds(final int x, final int y, final int width, final int height) {
        super.setBounds(x, y, width, height);
        if (currentWth == 0  &&  width > 0  &&  height > 0) {
            currentWth = nameColLabel.getPreferredSize().width;
        }
    }

    /**
    Sets each column header name to the respective specified name, or to the default value if the name is null.
    @param nameColumnName   The new name of the name column
    @param valueColumnName  The new name of the value column
    @since 2.0
    */
    public void setColumnHeaderNames(final String nameColumnName, final String valueColumnName) {
        if (nameColumnName == null) {
            nameColLabel.setText(DEFAULT_NAME_COLUMN_NAME);
        } else {
            nameColLabel.setText(nameColumnName);
        }
        if (valueColumnName == null) {
            valColLabel.setText(DEFAULT_VALUE_COLUMN_NAME);
        } else {
            valColLabel.setText(valueColumnName);
        }
        // Resize name column in case header name was or now is longest name
        resizeNameColumn();
    }

    /**
    Sets whether this panel should act as a root focus handler when cycling through fields using [Tab]/[Shift-Tab].  If so,
    pressing the [Tab] key while on the last focusable field will cause focus to be transferred to the first focusable field in
    this panel.
    @param isFocusCycleRoot True if this panel should act as a focus cycle root
    @since 2.1
    */
    public void setFocusCycleRoot(final boolean isFocusCycleRoot) {
        this.isFocusCycleRoot = isFocusCycleRoot;
    }

    /**
    Sets the properties that should not be shown.
    @param propertiesToFilterOut The Collection of PropertyDefinitions that should not be shown
    @since 2.0
    */
    public void setHiddenPropertyDefinitions(final Collection propertiesToFilterOut) {
        if (propertiesToFilterOut == null) {
            this.propertiesToFilterOut = Collections.EMPTY_LIST;
        } else {
            this.propertiesToFilterOut = propertiesToFilterOut;
        }
    }

    /**
    Sets the width of the header for the name column.    Change will not take effect until
    setPropertiedObject is called.
    @param width The width of the header for the name column.  If width is set to 0, the name
    column width will be reset to fit the name column data.
    @since 2.0
    */
    public void setNameColumnHeaderWidth(final int width) {
        currentWth = width;
    }

    /**
     * Resets the width of the header for the name column to it's default size and refreshes
     * the display.
     * @param refreshImmediately if true, will cause the panel to refresh it's layout. Use
     * false when your code will be calling setPropertiedObject() to avoid unnecessary re-
     * generation of the panel's components.
     * @since 3.0
     */
    public void resetNameColumnHeaderWidth(boolean refreshImmediately) {
        currentWth = 0;
        if ( refreshImmediately ) {
            refreshDisplay();
        }
    }

    /**
    Sets this panel to display the properties for the specified PropertiedObject.
    @param propObj the PropertiedObject to display and edit in this panel.
    @since 2.0
    */
    public void setPropertiedObject(final PropertiedObject propObj) {
        setPropertiedObject(propObj, null, null);
    }

    /**
    Sets this panel to display the properties for the specified PropertiedObject.
    @param propObj the PropertiedObject to display and edit in this panel.
    @param editor the PropertiedObjectEditor to use for displaying and editing.
    @since 2.0
    */
    public void setPropertiedObject(final PropertiedObject propObj, final PropertiedObjectEditor editor) {
        setPropertiedObject(propObj, editor, null);
    }

    /**
    Sets this panel to display the specified properties for the specified PropertiedObject.
    @param propObj the PropertiedObject to display and edit in this panel.
    the panel will display.
    @param definitionsToDisplay a subset of the PropertyDefinitions for this PropertiedObject that
    @since 2.0
    */
    public void setPropertiedObject(final PropertiedObject propObj, final List propertiesToDisplay) {
        setPropertiedObject(propObj, null, propertiesToDisplay);
    }

    /**
    Sets the PropertiedObject whose properties are to be shown to the specified object.
    @param propObj              The PropertiedObject to display and edit in this panel.
    @param editor               The PropertiedObjectEditor to use for displaying and editing.
    @param propertiesToDisplay  An ordered List of PropertyDefinitions for the properties to be shown for the PropertiedObject
    @since 2.0
    */
    public void setPropertiedObject(final PropertiedObject propObj, final PropertiedObjectEditor editor, final List propertiesToDisplay) {
        PropertiedObject oldPropObj = null;
        int ndx = 0;
        final Component comp = SwingUtilities.findFocusOwner(this);
        if (comp != null  &&  propObj != null) {
            oldPropObj = this.propObj;
            while (ndx < propComps.length  &&  propComps[ndx][1] != comp)
                ++ndx;
            if (ndx == propComps.length) {
                ndx = 0;
            }
        }
        this.propObj = propObj;
        factory.setPropertiedObject(propObj);
        this.propertiesToDisplay = propertiesToDisplay;
        if (editor != null) {
            this.editor = editor;
            factory.setPropertiedObjectEditor(editor);
            adapter = new PropertyChangeAdapter(editor, xActionSrc);
            factory.setPropertyChangeAdapter(adapter);
        }
        if (adapter != null) {
            adapter.setPropertiedObject(propObj);
        }
        refreshDisplay(true);
        if (comp != null  &&  propObj != null) {
            if (oldPropObj == propObj  &&  ndx < propComps.length) {
                propComps[ndx][1].requestFocus();
            } else if (propComps.length > 0) {
                propComps[0][1].requestFocus();
            }
        }
    }

    /**
    Sets the PropertyChangeAdapter.  A default PropertyChangeAdapter is created automatically, so it is not necessary to call this
    method unless you wish to redirect this panel's change events to a different adapter.
    @param adapter The new PropertyChangeAdapter
    @since 2.0
    */
    public void setPropertyChangeAdapter(final PropertyChangeAdapter adapter) {
        if (factory != null) {
            factory.setPropertyChangeAdapter(adapter);
        }
        this.adapter = adapter;
    }



    /**
    Sets whether properties should be treated as read-only regardless of their actual read-only status.  Pasing false to this
    method will cause the properties' actual read-only status to be honored.
    @param readOnly True if all properties should be treated as read-only.  The default is false.
    @since 2.0
    */
    public void setReadOnlyForced(final boolean forced) {
        readOnlyForced = forced;
    }

    /**
    Sets whether the name and value column headers should be shown.
    @param showHeaders True if column headers should be shown.  The default is true.
    @since 2.0
    */
    public void setShowColumnHeaders(final boolean showHeaders) {
        if (showHeaders) {
            scroller.setColumnHeaderView(hdr);
        } else {
            scroller.setColumnHeaderView(null);
        }
    }

    /**
    Sets whether properties marked as expert should be shown.
    @param showExpert True if expert properties should be shown.  The default is false.
    @since 2.0
    */
    public void setShowExpertProperties(final boolean showExpert) {
        this.showExpert = showExpert;
    }

    /**
    Sets whether properties marked as hidden should be shown.
    @param showHidden True if hidden properties should be shown.  The default is false.
    @since 2.0
    */
    public void setShowHiddenProperties(final boolean showHidden) {
        this.showHidden = showHidden;
    }

    /**
    Sets whether properties with invalid values should be shown with a special indication.
    @param showInvalid True if invalid properties should be shown.  The default is false.
    @since 2.0
    */
    public void setShowInvalidProperties(final boolean showInvalid) {
        this.showInvalid = showInvalid;
    }

    /**
    Sets whether properties marked as required should be shown with a special indication.
    @param showRequired True if required properties should be shown.  The default is false.
    @since 2.0
    */
    public void setShowRequiredProperties(final boolean showRequired) {
        this.showRequired = showRequired;
    }

    /**
    Sets whether this panel should display ToolTips for property values.
    @param showToolTips True if ToolTips should be shown.  The default is true.
    @since 2.0
    */
    public void setShowToolTips(final boolean showToolTips) {
        this.showToolTips = showToolTips;
    }

    /**
    Attempts to resize the columns when the vertical scroll bar first appears in order to eliminate the horizontal scroll bar.
    @since 2.1
    */
    protected void sizeColumnsToFitViewport(final JScrollPane scroller) {
        final JScrollBar bar = scroller.getVerticalScrollBar();
        bar.addComponentListener(new ComponentAdapter() {
            public void componentShown(final ComponentEvent event) {
                final int wth = propsPanel.getWidth();
                final JViewport port = scroller.getViewport();
                final int portWth = port.getWidth();
                int delta = wth - portWth;
                if (delta != bar.getWidth()) {
                    return;
                }
                // Calc min column width of all columns
                final int minWth = propsPanel.getMinimumSize().width;
                // Return if can't get rid of horizontal scroll bar anyway
                if (wth - delta < minWth) {
                    return;
                }
                // Set table's size to current column width
                int hgt = propsPanel.getHeight();
                if (hgt == 0) {
                    hgt = propsPanel.getPreferredSize().height;
                }
                final Component[] comps = propsPanel.getComponents();
                Component comp;
                for (int ndx = comps.length;  --ndx >= 0;) {
                    comp = ((Container)comps[ndx]).getComponent(1);
                    comp.setSize(comp.getWidth() - delta, comp.getHeight());
                }
                propsPanel.setSize(portWth, hgt);
                valColLabel.setSize(valColLabel.getWidth() - delta, valColLabel.getHeight());
                propsPanel.revalidate();
            }
        });
    }
}
