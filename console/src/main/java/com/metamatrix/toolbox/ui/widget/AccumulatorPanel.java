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

import java.awt.Component;
import java.awt.Dimension;
import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.awt.GridLayout;
import java.awt.Insets;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.awt.event.MouseAdapter;
import java.awt.event.MouseEvent;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Enumeration;
import java.util.Iterator;
import java.util.List;

import javax.swing.BorderFactory;
import javax.swing.Box;
import javax.swing.BoxLayout;
import javax.swing.DefaultListModel;
import javax.swing.JButton;
import javax.swing.JOptionPane;
import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.ListCellRenderer;
import javax.swing.event.ListDataListener;
import javax.swing.event.ListSelectionEvent;
import javax.swing.event.ListSelectionListener;

import com.metamatrix.toolbox.ui.IconConstants;
import com.metamatrix.toolbox.ui.UIDefaults;
import com.metamatrix.toolbox.ui.widget.util.WidgetUtilities;
import com.metamatrix.toolbox.ToolboxPlugin;

/**
 * @since 2.0
 * @version 2.1
 * @author <a href="mailto:jverhaeg@metamatrix.com">John P. A. Verhaeg</a>
 */
public class AccumulatorPanel extends DialogPanel
implements IconConstants {
    //############################################################################################################################
    //# Constants                                                                                                                #
    //############################################################################################################################

    private static final String AVAILABLE_VALUES_HEADER = ToolboxPlugin.Util.getString("AccumulatorPanel.Available_Values_1"); //$NON-NLS-1$
    private static final String CURRENT_VALUES_HEADER   = ToolboxPlugin.Util.getString("AccumulatorPanel.Current_Values_2"); //$NON-NLS-1$
    
    //############################################################################################################################
    //# Instance Variables                                                                                                       #
    //############################################################################################################################
    
    private boolean useRevisedLayout = false;
    private List initAvailVals;
    private transient List initVals;
    private DefaultListModel availValModel, valModel;
    private ListWidget availValList, valList;
    private ButtonWidget rightButton, allRightButton, leftButton, allLeftButton,
                         upButton, downButton, addButton, removeButton, resetButton;
    private JButton[] additionalButtons = null;
    private boolean allowsNewVals, allowsReorderingVals;
    private Box valListButtonPanel;
    private Dimension listPrefSize;
    private LabelWidget availValHdr, valHdr;
    private int minAllowed, maxAllowed;
    private SpacerWidget spacer;
    private Comparator comparator;
     
    //############################################################################################################################
    //# Constructors                                                                                                             #
    //############################################################################################################################
    
    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since 2.0
    */
    public AccumulatorPanel() {
        this(null, null);
    }
    
    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since 2.0
    */
    public AccumulatorPanel(final List initialAvailableValues) {
        this(initialAvailableValues, null);
    }
    
    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since 2.0
    */
    public AccumulatorPanel(final List initialAvailableValues, final List initialValues) {
        initAvailVals = initialAvailableValues;
        initVals = initialValues;
        initializeAccumulatorPanel();
    }
    
    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since 3.1
    */
    public AccumulatorPanel(final List initialAvailableValues, final List initialValues, Comparator theComparator) {
        initAvailVals = initialAvailableValues;
        initVals = initialValues;
        comparator = theComparator;
        initializeAccumulatorPanel();
    }
    
    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since 4.2
    */
    public AccumulatorPanel(final List initialAvailableValues, final List initialValues, JButton[] additionalButtons,
            Comparator theComparator) {
        initAvailVals = initialAvailableValues;
        initVals = initialValues;
        this.useRevisedLayout = true;
        this.additionalButtons = additionalButtons;
        this.comparator = theComparator;
        initializeAccumulatorPanel();
    }
    //############################################################################################################################
    //# Instance Methods                                                                                                         #
    //############################################################################################################################
    
    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since 2.0
    */
    public void addAvailableValue(final Object value) {
        if (value == null) {
            return;
        }
        initAvailVals.add(value);
        if (!valModel.contains(value)) {
            if (comparator == null) {
                availValModel.addElement(value);
            } else {
                int index = modelInsertionIndex(availValModel, value);
                availValModel.add(index, value);
            }
        }
        updateButtonStatus();
    }

    private int modelInsertionIndex(DefaultListModel model, Object value) {
        //In both the interest of development time and the knowledge that to date 
        //the lists shown in this accumulator are not excessively long, we will 
        //simply do a sequential search to find the correct insertion index, not a 
        //binary search.  BWP 02/09/05
        int index = 0;
        int listSize = model.getSize();
        boolean done = false;
        while (!done) {
            if (index == listSize) {
                done = true;
            } else {
                int result = comparator.compare(value, model.get(index));
                if (result > 0) {
                    index++;
                } else {
                    done = true;
                }
            }
        }
        return index;
    }
    
    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    Adds the specified ListDataListener to the current values list.
    @param listener An implementation of ListDataListener
    @since 2.0
    */
    public void addListDataListener(final ListDataListener listener) {
       valModel.addListDataListener(listener);
    }
    
    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since 2.0
    */
    public void addValue(final Object value) {
        if (value == null) {
            return;
        }
        if (!allowsNewVals) {
            validateValue(value);
        }
        //Only bother using collator if useRevisedLayout flag set, in which case
        //the up-down arrows do not appear.
        if (useRevisedLayout && (comparator != null)) {
            int index = modelInsertionIndex(valModel, value);
            valModel.add(index, value);
        } else {
            valModel.addElement(value);
        }
        availValModel.removeElement(value);
        updateButtonStatus();
    }
    
    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since 2.0
    */
    public boolean allowsNewValues() {
        return allowsNewVals;
    }
    
    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since 2.0
    */
    public boolean allowsReorderingValues() {
        return allowsReorderingVals;
    }
    
    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since 2.0
    */
    public void clearValues() {
        while (!valModel.isEmpty()) {
            removeValue(valModel.get(0));
        }
    }
    
    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
     * @since 2.1
     */
    protected DefaultListModel createAvailableValuesListModel() {
        return createDefaultListModel();
    }
    
    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
     * @since 2.1
     */
    protected ListWidget createAvailableValuesListWidget(final DefaultListModel model) {
        return createDefaultListWidget(model);
    }
    
    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
     * @since 2.1
     */
    protected DefaultListModel createDefaultListModel() {
        return new DefaultListModel();
    }
    
    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
     * @since 2.1
     */
    protected ListWidget createDefaultListWidget(final DefaultListModel model) {
        final ListWidget list = new ListWidget(model) {
            public Dimension getPreferredScrollableViewportSize() {
                return listPrefSize;
            }
        };
        // Add controllers to list to enable/disable buttons when appropriate
        list.addListSelectionListener(new ListSelectionListener() {
            public void valueChanged(final ListSelectionEvent event) {
                updateButtonStatus();
            }
        });
        return list;
    }
    
    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
     * Need to eventually break this out into separate method for available vs current
     * @since 2.0
     */
    protected LabelWidget createHeader(final String text) {
        final LabelWidget hdr = new LabelWidget(text);
        hdr.setMaximumSize(new Dimension(Short.MAX_VALUE, hdr.getMaximumSize().height));
        hdr.setAlignmentX(0.5f);
        final UIDefaults dflts = UIDefaults.getInstance();
        final Insets margins = dflts.getInsets(ButtonWidget.MARGIN_PROPERTY);
        hdr.setBorder(BorderFactory.createCompoundBorder(dflts.getBorder(ButtonWidget.BORDER_PROPERTY),
                      BorderFactory.createEmptyBorder(margins.top, margins.left, margins.bottom, margins.right)));
        return hdr;
    }
    
    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
     * @since 2.1
     */
    protected DefaultListModel createValuesListModel() {
        return createDefaultListModel();
    }
    
    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
     * @since 2.1
     */
    protected ListWidget createValuesListWidget(final DefaultListModel model) {
        return createDefaultListWidget(model);
    }
    
    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since 2.0
    */
    public List getAvailableValues() {
        return getValues(availValModel);
    }
    
    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @return The header for the available values list
    @since 2.0
    */
    public LabelWidget getAvailableValuesHeader() {
        return availValHdr;
    }
    
    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since 2.0
    */
    public List getInitialAvailableValues() {
        return new ArrayList(initAvailVals);
    }
    
    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since 2.0
    */
    public List getInitialValues() {
        return new ArrayList(initVals);
    }
    
    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @return The maximum number of current values allowed
    @since 2.0
    */
    public int getMaximumValuesAllowed() {
        return maxAllowed;
    }
    
    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @return The minimum number of current values allowed
    @since 2.0
    */
    public int getMinimumValuesAllowed() {
        return minAllowed;
    }
    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since 3.1
    */
    public ButtonWidget getResetButton() {
    	return resetButton;
    }
    
    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since 2.0
    */
    public List getValues() {
        return getValues(valModel);
    }
    
    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since 2.0
    */
    protected List getValues(final DefaultListModel model) {
        final List vals = new ArrayList();
        final Enumeration iterator = model.elements();
        while (iterator.hasMoreElements()) {
            vals.add(iterator.nextElement());
        }
        return vals;
    }
    
    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @return The header for the current values list
    @since 2.0
    */
    public LabelWidget getValuesHeader() {
        return valHdr;
    }
    
    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since 2.0
    */
    protected void initializeAccumulatorPanel() {
        maxAllowed = Integer.MAX_VALUE;
        allowsReorderingVals = true;
        // Create widgets for available and current values
        listPrefSize = new Dimension();
        availValHdr = createHeader(AVAILABLE_VALUES_HEADER);
        availValModel = createAvailableValuesListModel();
        availValList = createAvailableValuesListWidget(availValModel);
        valHdr = createHeader(CURRENT_VALUES_HEADER);
        valModel = createValuesListModel();
        valList = createValuesListWidget(valModel);
        // Create action buttons
        final UIDefaults dflts = UIDefaults.getInstance();
        rightButton = new ButtonWidget(dflts.getIcon(RIGHT_ICON_PROPERTY));
        allRightButton = new ButtonWidget(dflts.getIcon(ALL_RIGHT_ICON_PROPERTY));
        leftButton = new ButtonWidget(dflts.getIcon(LEFT_ICON_PROPERTY));
        allLeftButton = new ButtonWidget(dflts.getIcon(ALL_LEFT_ICON_PROPERTY));
        upButton = new ButtonWidget(dflts.getIcon(UP_ICON_PROPERTY));
        downButton = new ButtonWidget(dflts.getIcon(DOWN_ICON_PROPERTY));
        addButton = new ButtonWidget(dflts.getIcon(ADD_ICON_PROPERTY));
        removeButton = new ButtonWidget(dflts.getIcon(SUBTRACT_ICON_PROPERTY));
        resetButton = new ButtonWidget(ToolboxPlugin.Util.getString("AccumulatorPanel.Reset_3")); //$NON-NLS-1$
        ((JButton)resetButton).setMnemonic('r');
        final JPanel panel = new JPanel();
        // Build panel
        if (useRevisedLayout) {
            //!!!!!!NOTE!!!!!!--
            //if useRevisedLayout is set, it is expected that upButton, downButton, 
            //addButton, removeButton, and resetButton are not used.
            GridBagLayout layout = new GridBagLayout();
            panel.setLayout(layout);
            JScrollPane availJSP = new JScrollPane(availValList);
            JScrollPane selectedJSP = new JScrollPane(valList);
            GridLayout upperButtonsLayout = new GridLayout(4, 1, 2, 0);
            JPanel upperButtonsPanel = new JPanel();
            upperButtonsPanel.setLayout(upperButtonsLayout);
            upperButtonsPanel.add(rightButton);
            upperButtonsPanel.add(allRightButton);
            upperButtonsPanel.add(leftButton);
            upperButtonsPanel.add(allLeftButton);
            JPanel lowerButtonsPanel = null;
            if (additionalButtons != null) {
                lowerButtonsPanel = new JPanel();
                GridLayout lowerButtonsLayout = new GridLayout(additionalButtons.length,
                        1, 2, 0);
                lowerButtonsPanel.setLayout(lowerButtonsLayout);
                for (int i = 0; i < additionalButtons.length; i++) {
                    lowerButtonsPanel.add(additionalButtons[i]);
                }
            }
            layout.setConstraints(availValHdr, new GridBagConstraints(0, 0, 1, 1,
                    1.0, 0.0, GridBagConstraints.CENTER, GridBagConstraints.HORIZONTAL,
                    new Insets(0, 0, 0, 0), 0, 0));
            panel.add(availValHdr);
            layout.setConstraints(availJSP, new GridBagConstraints(0, 1, 1, 2,
                    1.0, 1.0, GridBagConstraints.NORTH, GridBagConstraints.BOTH,
                    new Insets(0, 0, 0, 0), 0, 0));
            panel.add(availJSP);
            layout.setConstraints(valHdr, new GridBagConstraints(2, 0, 1, 1,
                    1.0, 0.0, GridBagConstraints.CENTER, GridBagConstraints.HORIZONTAL,
                    new Insets(0, 0, 0, 0), 0, 0));
            panel.add(valHdr);
            layout.setConstraints(selectedJSP, new GridBagConstraints(2, 1, 1, 2,
                    1.0, 1.0, GridBagConstraints.NORTH, GridBagConstraints.BOTH,
                    new Insets(0, 0, 0, 0), 0, 0));
            panel.add(selectedJSP);
            layout.setConstraints(upperButtonsPanel, new GridBagConstraints(1, 1, 1, 1,
                    0.0, 0.0, GridBagConstraints.NORTH, GridBagConstraints.NONE,
                    new Insets(0, 2, 2, 2), 0, 0));
            panel.add(upperButtonsPanel);
            if (lowerButtonsPanel != null) {
                layout.setConstraints(lowerButtonsPanel, new GridBagConstraints(1, 2, 1, 1,
                        0.0, 0.0, GridBagConstraints.SOUTH, GridBagConstraints.NONE,
                        new Insets(2, 2, 0, 2), 0, 0));
                panel.add(lowerButtonsPanel);
            }
        } else {
            WidgetUtilities.equalizeSizeConstraints(Arrays.asList(new ButtonWidget[] {rightButton, allRightButton,
                                                                                  leftButton, allLeftButton}));
            WidgetUtilities.equalizeSizeConstraints(Arrays.asList(new ButtonWidget[] {upButton, downButton,
                                                                                  addButton, removeButton}));
            panel.setLayout(new BoxLayout(panel, BoxLayout.X_AXIS));
            Box vBox = Box.createVerticalBox();
            vBox.add(availValHdr);
            vBox.add(new JScrollPane(availValList));
            panel.add(vBox);
            vBox = Box.createVerticalBox();
            vBox.add(SpacerWidget.createVerticalSpacer(availValHdr.getPreferredSize().height));
            vBox.add(rightButton);
            vBox.add(allRightButton);
            vBox.add(leftButton);
            vBox.add(allLeftButton);
            vBox.add(SpacerWidget.createVerticalExpandableSpacer());
            panel.add(vBox);
            vBox = Box.createVerticalBox();
            vBox.add(valHdr);
            vBox.add(new JScrollPane(valList));
            panel.add(vBox);
            valListButtonPanel = Box.createVerticalBox();
            valListButtonPanel.add(SpacerWidget.createVerticalSpacer(valHdr.getPreferredSize().height));
            valListButtonPanel.add(upButton);
            valListButtonPanel.add(downButton);
            valListButtonPanel.add(addButton);
            valListButtonPanel.add(removeButton);
            upButton.setVisible(allowsReorderingValues());
            downButton.setVisible(allowsReorderingValues());
            addButton.setVisible(allowsNewValues());
            removeButton.setVisible(allowsNewValues());
//          addButtonNdx = 
                    valListButtonPanel.getComponentCount();
            valListButtonPanel.add(SpacerWidget.createVerticalExpandableSpacer());
            panel.add(valListButtonPanel);
        }
        setContent(panel);
        addNavigationButton(resetButton);
        // Add controllers to buttons
        rightButton.addActionListener(new ActionListener() {
            public void actionPerformed(final ActionEvent event) {
                moveSelectedValuesRight();
            }
        });
        allRightButton.addActionListener(new ActionListener() {
            public void actionPerformed(final ActionEvent event) {
                availValList.setSelectionInterval(0, availValModel.getSize() - 1);
                moveSelectedValuesRight();
            }
        });
        leftButton.addActionListener(new ActionListener() {
            public void actionPerformed(final ActionEvent event) {
                removeSelectedValues();
            }
        });
        allLeftButton.addActionListener(new ActionListener() {
            public void actionPerformed(final ActionEvent event) {
                valList.setSelectionInterval(0, valModel.getSize() - 1);
                removeSelectedValues();
            }
        });
        resetButton.addActionListener(new ActionListener() {
            public void actionPerformed(final ActionEvent event) {
                resetLists();
            }
        });
        upButton.addActionListener(new ActionListener() {
            public void actionPerformed(final ActionEvent event) {
                final int[] indices = valList.getSelectedIndices();
                int selectedNdx;
                for (int ndx = 0;  ndx < indices.length;  ++ndx) {
                    selectedNdx = indices[ndx];
                    valModel.add(selectedNdx, valModel.remove(selectedNdx - 1));
                    indices[ndx] = selectedNdx - 1;
                }
                valList.setSelectedIndices(indices);
                updateButtonStatus();
            }
        });
        downButton.addActionListener(new ActionListener() {
            public void actionPerformed(final ActionEvent event) {
                final int[] indices = valList.getSelectedIndices();
                int selectedNdx;
                for (int ndx = indices.length;  --ndx >= 0;) {
                    selectedNdx = indices[ndx];
                    valModel.add(selectedNdx, valModel.remove(selectedNdx + 1));
                    indices[ndx] = selectedNdx + 1;
                }
                valList.setSelectedIndices(indices);
                updateButtonStatus();
            }
        });
        addButton.addActionListener(new ActionListener() {
            public void actionPerformed(final ActionEvent event) {
                final String val = JOptionPane.showInputDialog(AccumulatorPanel.this, ToolboxPlugin.Util.getString("AccumulatorPanel.Enter_a_new_value__4"), ToolboxPlugin.Util.getString("AccumulatorPanel.Input_5"), //$NON-NLS-1$ //$NON-NLS-2$
                                                               JOptionPane.INFORMATION_MESSAGE);
                addValue(val);
            }
        });
        removeButton.addActionListener(new ActionListener() {
            public void actionPerformed(final ActionEvent event) {
                removeSelectedValues();
            }
        });
        availValList.addMouseListener(new MouseAdapter() {
            public void mouseClicked(final MouseEvent event) {
                if (event.getClickCount() < 2) {
                    return;
                }
                if (valModel.size() >= maxAllowed) {
                    maximumValuesAccumulated();
                    return;
                }
                availValList.setSelectedIndex(availValList.locationToIndex(event.getPoint()));
                moveSelectedValuesRight();
            }
        });
        valList.addMouseListener(new MouseAdapter() {
            public void mouseClicked(final MouseEvent event) {
                if (event.getClickCount() < 2) {
                    return;
                }
                if (valModel.size() <= minAllowed) {
                    minimumValuesAccumulated();
                    return;
                }
                valList.setSelectedIndex(valList.locationToIndex(event.getPoint()));
                removeSelectedValues();
            }
        });
        // Initialize list widget models and affected widgets
        List vals = initAvailVals;
        initAvailVals = new ArrayList();
        setAvailableValues(vals);
        vals = initVals;
        initVals = new ArrayList();
        setInitialValues(vals);
        setValues(vals);
removeValModelItemsFromAvailValModel();        
        // Update button statuses
        updateButtonStatus();
        // Update preferred sizes of lists
        updateListPreferredSize();
        // Make cancel button reset values to initial values
        getCancelButton().addActionListener(new ActionListener() {
            public void actionPerformed(final ActionEvent event) {
                resetValues();
            }
        });
        // Save reference to spacer before cancel button in case panel is disabled
        final int ndx = getNavigationComponentIndex(getCancelButton()) - 1;
        if (ndx >= 0) {
	        final Component comp = getNavigationComponent(ndx);
	        if (comp instanceof SpacerWidget) {
	            spacer = (SpacerWidget)comp;
	        }
        }
    }
    
private void removeValModelItemsFromAvailValModel() {
 int lastIndex = availValModel.size() - 1;
 for (int i = lastIndex; i >= 0; i--) {
  Object element = availValModel.elementAt(i);
  if (valModel.contains(element)) {
   availValModel.remove(i);
  }
 }
}

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    Called whenever the user double-clicks the mouse on an available value, but the maximum number of accumulated values has
    already been reached.
    Does nothing by default.
    @since 2.0
    */
    protected void maximumValuesAccumulated() {
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    Called whenever the user double-clicks the mouse on an accumulated value, but the minimum number of accumulated values has
    already been reached.
    Does nothing by default.
    @since 2.0
    */
    protected void minimumValuesAccumulated() {
    }
    
    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since 2.0
    */
    protected void moveSelectedValuesRight() {
        final Object[] vals = availValList.getSelectedValues();
        for (int ndx = 0;  ndx < vals.length;  ++ndx) {
            addValue(vals[ndx]);
        }
        updateListPreferredSize();
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since 2.0
    */
    protected void removeSelectedValues() {
        final Object[] vals = valList.getSelectedValues();
        if( comparator != null ) {
			removeValues(vals);
        } else {
	        for (int ndx = 0;  ndx < vals.length;  ++ndx) {
	            removeValue(vals[ndx]);
	        }
        }
    }
    
    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since 2.0
    */
    public void removeAvailableValue(final Object value) {
        initAvailVals.remove(value);
        availValModel.removeElement(value);
        if (!allowsNewVals) {
            if (initVals.contains(value)) {
                initVals.remove(value);
            }
            if (valModel.contains(value)) {
                valModel.removeElement(value);
            }
        }
        updateButtonStatus();
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    Removes the specified ListDataListener from the current values list.
    @param listener An implementation of ListDataListener
    @since 2.0
    */
    public void removeListDataListener(final ListDataListener listener) {
       valModel.removeListDataListener(listener);
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since 2.0
    */
    public void removeValue(final Object value) {
        valModel.removeElement(value);
        final int valNdx = initAvailVals.indexOf(value);
        if (valNdx >= 0) {
            int addNdx = 0;
            final Enumeration iterator = availValModel.elements();
            while (iterator.hasMoreElements()) {
                if (valNdx < initAvailVals.indexOf(iterator.nextElement())) {
                    break;
                }
                ++addNdx;
            }
            availValModel.add(addNdx, value);
        }
        updateButtonStatus();
    }
    
    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since 2.0
    */
    public void removeValues(final Object[] values) {
    	List tempList = new ArrayList(availValModel.size());
    	for (int ndx = 0;  ndx < availValModel.size();  ++ndx)
    		tempList.add(availValModel.get(ndx));

    	int theIndex = 0;
    	Object nextObj = null;
    	for(theIndex=0; theIndex < values.length; theIndex++ ) {
    		nextObj = values[theIndex];
	        valModel.removeElement(nextObj);
	        final int valNdx = initAvailVals.indexOf(nextObj);
	        if (valNdx >= 0) {
	            tempList.add(nextObj);
	        }
    	}
    	
    	if( !tempList.isEmpty() ) {
    		// SORT
    		Collections.sort(tempList, comparator);
    		
    		// Clear current List, then add the new objects.
    		availValModel.clear();
    		
    		// RECONSTRUCT availValModel
    		Iterator iter = tempList.iterator();
    		theIndex = 0;
    		while( iter.hasNext() ) {
    			nextObj = iter.next();
    			availValModel.add(theIndex, nextObj);
    			++theIndex;
    		}
    	}
        updateButtonStatus();
    }
    
    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since 2.0
    */
    public void resetValues() {
//        setValues(initVals);
		// The above code was taking way too much time.
		// We know what the cached initial lists are so we should just
		// reset these lists without all this stuff.
        resetLists();
    }
    
    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    When allowsNewValues is false, does NOT remove illegal values from initial and current value lists.
    @param allowsNewValues True if values not in the initial available value list may be added to the value list.  Default is
                           false;
    @since 2.0
    */
    public void setAllowsNewValues(final boolean allowsNewValues) {
        addButton.setVisible(allowsNewValues);
        removeButton.setVisible(allowsNewValues);
        allowsNewVals = allowsNewValues;
    }
    
    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since 2.0
    */
    public void setAllowsReorderingValues(final boolean allowsReorderingValues) {
        upButton.setVisible(allowsReorderingValues);
        downButton.setVisible(allowsReorderingValues);
        allowsReorderingVals = allowsReorderingValues;
    }
    
    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since 2.0
    */
    public void setAvailableValues(final List values) {
        initAvailVals.clear();
        availValModel.clear();
        if (values != null) {
            final Iterator iterator = values.iterator();
            while (iterator.hasNext()) {
                addAvailableValue(iterator.next());
            }
        }
        allRightButton.setVisible(initAvailVals.size() <= maxAllowed);
    }

    /**
     * @since 3.0
     */
    public void setSelectionListCellRenderer(ListCellRenderer renderer) {
        valList.setCellRenderer(renderer);
    }

    /**
     * @since 3.0
     */
    public void setAvailableListCellRenderer(ListCellRenderer renderer) {
        availValList.setCellRenderer(renderer);
    }

	//////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
     * @since 2.1
     */
    public void setEnabled(final boolean enabled) {
        // Show/hide all functional buttons
    	rightButton.setVisible(enabled);
    	allRightButton.setVisible(enabled);
    	leftButton.setVisible(enabled);
		allLeftButton.setVisible(enabled);
		upButton.setVisible(enabled);
		downButton.setVisible(enabled);
		addButton.setVisible(enabled);
		removeButton.setVisible(enabled);
		// Enable/disable lists (but not scrollers)
		availValList.setEnabled(enabled);
		valList.setEnabled(enabled);
		// Show/hide cancel button and preceding spacer
		getCancelButton().setVisible(enabled);
		if (spacer != null) {
		    spacer.setVisible(enabled);
		}
        super.setEnabled(enabled);
    }
    
    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since 2.0
    */
    public void setInitialValues(final List values) {
        initVals.clear();
        if (values == null) {
            return;
        }
        if (allowsNewVals) {
            initVals.addAll(values);
        } else {
            final Iterator iterator = values.iterator();
            Object val;
            while (iterator.hasNext()) {
                val = iterator.next();
                if ( val != null ) {
                    validateValue(val);
                    initVals.add(val);
                }
            }
        }
    }
    
    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @return The maximum number of current values allowed
    @since 2.0
    */
    public void setMaximumValuesAllowed(final int maximum) {
        maxAllowed = maximum;
        allRightButton.setVisible(initAvailVals.size() <= maximum);
        updateButtonStatus();
    }
    
    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @return The minimum number of current values allowed
    @since 2.0
    */
    public void setMinimumValuesAllowed(final int minimum) {
        minAllowed = minimum;
        allLeftButton.setVisible(minimum == 0);
        updateButtonStatus();
    }
    
    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since 2.0
    */
    public void setValues(final List values) {
        clearValues();
        if (values != null) {
            final Iterator iterator = values.iterator();
            while (iterator.hasNext()) {
                addValue(iterator.next());
            }
        }
    }
    
    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since 3.1
    */ 
    protected void resetLists() {
        valModel.clear();
        if (initVals != null) {
        	Object nextObj = null;
            final Iterator iter = initVals.iterator();
            while (iter.hasNext()) {
            	nextObj = iter.next();
		        valModel.addElement(nextObj);
            }
        }
        
        availValModel.clear();
        if (initAvailVals != null) {
        	Object nextObj = null;
            final Iterator iter = initAvailVals.iterator();
            while (iter.hasNext()) {
            	nextObj = iter.next();
            	if (!valModel.contains(nextObj))
		        	availValModel.addElement(nextObj);
            }
        }

        updateButtonStatus();
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since 2.0
    */
    protected void updateButtonStatus() {
        final int availValCount = availValModel.size();
        final int valCount = valModel.size();
        rightButton.setEnabled(!availValList.isSelectionEmpty()  &&
                               availValList.getSelectedValues().length + valCount <= maxAllowed);
        final boolean removeAllowed = !valList.isSelectionEmpty()  &&
                                      valCount - valList.getSelectedValues().length >= minAllowed;
        leftButton.setEnabled(removeAllowed);
        allRightButton.setEnabled(availValCount > 0  &&  valCount + availValCount <= maxAllowed);
        allLeftButton.setEnabled(valCount > 0  &&  minAllowed <= 0);
        upButton.setEnabled(valList.getMinSelectionIndex() > 0);
        final int ndx = valList.getMaxSelectionIndex();
        downButton.setEnabled(ndx >= 0  &&  ndx < valCount - 1);
        removeButton.setEnabled(removeAllowed);
        addButton.setEnabled(valCount < maxAllowed);
        final ButtonWidget acceptButton = getAcceptButton();
        if (acceptButton != null) {
            acceptButton.setEnabled(valCount >= minAllowed  &&  valCount <= maxAllowed);
        }
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since 2.0
    */
    public void updateListPreferredSize() {
        final Dimension availValListSize = availValList.getPreferredSize();
        final Dimension valListSize = valList.getPreferredSize();
        listPrefSize.width = Math.max(Math.max(availValListSize.width, availValHdr.getPreferredSize().width),
                                  Math.max(valListSize.width, valHdr.getPreferredSize().width));
        listPrefSize.height = Math.max(availValListSize.height, valListSize.height);
    }
    
    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since 2.0
    */
    protected void validateValue(final Object value) {
        if (!initAvailVals.contains(value)) {
            throw new IllegalArgumentException(ToolboxPlugin.Util.getString("AccumulatorPanel.Value_not_in_initial_available_value_list___6") + value); //$NON-NLS-1$
        }
    }
}
