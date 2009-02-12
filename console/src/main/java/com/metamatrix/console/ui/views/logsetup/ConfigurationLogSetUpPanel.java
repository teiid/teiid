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

package com.metamatrix.console.ui.views.logsetup;

import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.awt.GridLayout;
import java.awt.Insets;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;

import javax.swing.Icon;
import javax.swing.JPanel;

import com.metamatrix.console.util.StaticQuickSorter;

import com.metamatrix.toolbox.ui.widget.ButtonWidget;
import com.metamatrix.toolbox.ui.widget.TitledBorder;

public class ConfigurationLogSetUpPanel extends JPanel
        implements MessageLevelChangeNotifyee, ContextsAccumulatorListener {
    private ConfigurationLogSetUpPanelController controller;
    private String[] messageLevelNames;
    private int initialMessageLevel;
    private java.util.List /*<String>*/ initialAvailableContexts;
    private java.util.List /*<String>*/ initialSelectedContexts;
    private String[] sourceNames;
    private Icon[] sourceIcons;
    private MessageLevelPanel messageLevelPanel;
    private ContextsAccumulatorOuterPanel outerAccumPanel;
    private ButtonWidget applyButton;
    private ButtonWidget resetButton;
    private String panelTitle;
    private boolean canModify;

    public ConfigurationLogSetUpPanel(String panelTitle, boolean canModify,
            ConfigurationLogSetUpPanelController controller,
            String[] sourceNames,
            Icon[] sourceIcons,
            String[] messageLevelNames, int initialMessageLevel,
            java.util.List /*<String>*/ initialAvailableContexts,
            java.util.List /*<String>*/ initialSelectedContexts) {
        super();
        this.panelTitle = panelTitle;
        this.canModify = canModify;
        this.controller = controller;
        this.sourceNames = sourceNames;
        this.sourceIcons = sourceIcons;
        this.messageLevelNames = messageLevelNames;
        this.initialMessageLevel = initialMessageLevel;
        this.initialAvailableContexts = initialAvailableContexts;
        this.initialSelectedContexts = initialSelectedContexts;
        init();
    }

    private void init() {
        this.setBorder(new TitledBorder(panelTitle + " Configuration")); //$NON-NLS-1$

        GridBagLayout layout = new GridBagLayout();
        this.setLayout(layout);

        applyButton = new ButtonWidget("Apply"); //$NON-NLS-1$
        applyButton.addActionListener(new ActionListener() {
            public void actionPerformed(ActionEvent ev) {
                applyPressed();
            }
        });
        resetButton = new ButtonWidget("Reset"); //$NON-NLS-1$
        resetButton.addActionListener(new ActionListener() {
            public void actionPerformed(ActionEvent ev) {
                resetPressed();
            }
        });
        JPanel buttonsPanel = new JPanel(new GridLayout(1, 2, 16, 0));
        buttonsPanel.add(applyButton);
        buttonsPanel.add(resetButton);

        messageLevelPanel = new MessageLevelPanel(controller,
                sourceNames, this, messageLevelNames,
                initialMessageLevel, canModify);
        messageLevelPanel.setBorder(new TitledBorder("Message Levels")); //$NON-NLS-1$
        outerAccumPanel = new ContextsAccumulatorOuterPanel(
                initialAvailableContexts, initialSelectedContexts, this,
                canModify, controller, sourceNames, sourceIcons);
        outerAccumPanel.setBorder(new TitledBorder("Message Contexts")); //$NON-NLS-1$

        this.add(messageLevelPanel);
        layout.setConstraints(messageLevelPanel, new GridBagConstraints(0, 0, 1, 1,
                1.0, 0.0, GridBagConstraints.CENTER, GridBagConstraints.BOTH,
                new Insets(4, 4, 4, 4), 0, 0));
        this.add(outerAccumPanel);
        layout.setConstraints(outerAccumPanel, new GridBagConstraints(0, 1, 1, 1,
                1.0, 1.0, GridBagConstraints.CENTER, GridBagConstraints.BOTH,
                new Insets(4, 4, 4, 4), 0, 0));
        if (canModify) {
            this.add(buttonsPanel);
            layout.setConstraints(buttonsPanel, new GridBagConstraints(0, 2, 1, 1,
                    0.0, 0.0, GridBagConstraints.CENTER, GridBagConstraints.NONE,
                    new Insets(4, 4, 4, 4), 0, 0));
        } else {
            applyButton.setEnabled(false);
            resetButton.setEnabled(false);
        }
        //Should set buttons to disabled or something is wrong
        checkButtonEnabling();
    }

    public int getSelectedMessageLevel() {
        return messageLevelPanel.getCurrentLevel();
    }

    public java.util.List /*<String>*/ getSelectedMessageContexts() {
        return outerAccumPanel.getSelectedContexts();
    }

    public java.util.List /*<String>*/ getAvailableMessageContexts() {
        return outerAccumPanel.getAvailableContexts();
    }

    public void messageLevelsChanged() {
        checkButtonEnabling();
    }

    public void selectedContextsChanged() {
        checkButtonEnabling();
    }

    private void checkButtonEnabling() {
        if (canModify) {
            boolean shouldBeEnabled = (hasLevelChanged() || haveContextsChanged());
            boolean isEnabled = applyButton.isEnabled();
            if (shouldBeEnabled != isEnabled) {
                applyButton.setEnabled(shouldBeEnabled);
                resetButton.setEnabled(shouldBeEnabled);
                controller.applyButtonStateChanged(panelTitle, shouldBeEnabled);
            }
        }
    }

    private boolean hasLevelChanged() {
        int curLevel = messageLevelPanel.getCurrentLevel();
        return (curLevel != initialMessageLevel);
    }

    private boolean haveContextsChanged() {
        java.util.List /*<String>*/ currentSelectedContexts =
                outerAccumPanel.getSelectedContexts();
        boolean mismatchFound = false;
        int curNumContexts = currentSelectedContexts.size();
        if (curNumContexts != initialSelectedContexts.size()) {
            mismatchFound = true;
        } else {
            int i = 0;
            while ((!mismatchFound) && (i < curNumContexts)) {
                if (!currentSelectedContexts.get(i).equals(
                        initialSelectedContexts.get(i))) {
                    mismatchFound = true;
                } else {
                    i++;
                }
            }
        }
        return mismatchFound;
    }

    private void applyPressed() {
        controller.applyButtonPressed(panelTitle,
                messageLevelPanel.getCurrentLevel(),
                outerAccumPanel.getSelectedContexts());
    }

    private void resetPressed() {
        outerAccumPanel.setContexts(initialSelectedContexts);
        messageLevelPanel.setLevel(initialMessageLevel);
        //This should set buttons to disabled or something is wrong:
        checkButtonEnabling();
    }

    public void setNewValues(java.util.List availableContexts,
            java.util.List selectedContexts, int messageLevel) {
        initialAvailableContexts = availableContexts;
        initialSelectedContexts = selectedContexts;
        initialMessageLevel = messageLevel;
        resetPressed();
    }
    
    public void setCopyButtonState(String sourceName, boolean newState) {
        messageLevelPanel.setCopyButtonState(sourceName, newState);
        outerAccumPanel.setCopyButtonState(sourceName, newState);
    }

    public boolean havePendingChanges() {
        boolean pending = (applyButton.isVisible() && applyButton.isEnabled());
        return pending;
    }

    public void doApply() {
        applyButton.doClick();
    }

    public void doReset() {
        resetButton.doClick();
    }
}//end ConfigurationLogSetUpPanel



class ContextsAccumulatorOuterPanel extends JPanel {
    private ConfigurationLogSetUpPanelController controller;
    ContextsAccumulatorPanel accumPanel;
     private String[] contextsSources;
     private Icon[] contextsIcons;
    private ButtonWidget[] copyFrom;

    public ContextsAccumulatorOuterPanel(
            java.util.List /*<String>*/ initialAvailableContexts,
            java.util.List /*<String>*/ initialSelectedContexts,
            ContextsAccumulatorListener listener, boolean canModify,
            ConfigurationLogSetUpPanelController controller,
            String[] contextsSources, Icon[] contextsIcons) {
        super();
        this.controller = controller;
        this.contextsSources = contextsSources;
        this.contextsIcons = contextsIcons;
        init(initialAvailableContexts, initialSelectedContexts, listener,
                canModify);
    }

    private void init(
            java.util.List /*<String>*/ initialAvailableContexts,
            java.util.List /*<String>*/ initialSelectedContexts,
            ContextsAccumulatorListener listener, boolean canModify) {
        GridBagLayout layout = new GridBagLayout();
        this.setLayout(layout);
        accumPanel = new ContextsAccumulatorPanel(initialAvailableContexts,
                initialSelectedContexts, listener);
        if (!canModify) {
            accumPanel.setEnabled(false);
        }
        this.add(accumPanel);
        layout.setConstraints(accumPanel, new GridBagConstraints(0, 0, 1, 1,
                1.0, 1.0, GridBagConstraints.CENTER, GridBagConstraints.BOTH,
                new Insets(0, 0, 0, 0), 0, 0));
        if (canModify) {
            JPanel buttonsPanel = new JPanel(new GridLayout(1,
                    contextsSources.length, 20, 0));
            this.add(buttonsPanel);
            layout.setConstraints(buttonsPanel, new GridBagConstraints(0, 1, 1, 1,
                    0.0, 0.0, GridBagConstraints.CENTER,
                    GridBagConstraints.NONE, new Insets(6, 0, 10, 0), 0, 0));
            copyFrom = new ButtonWidget[contextsSources.length];
            for (int i = 0; i < contextsSources.length; i++) {
                copyFrom[i] = new ButtonWidget("Copy contexts from " + //$NON-NLS-1$
                        contextsSources[i], contextsIcons[i]);
                buttonsPanel.add(copyFrom[i]);
                final int index = i;
                copyFrom[i].addActionListener(new ActionListener() {
                    public void actionPerformed(ActionEvent ev) {
                        copyButtonPressed(index);
                    }
                });
            }
        } else {
            accumPanel.setEnabled(false);
        }
    }

    private void copyButtonPressed(int index) {
        String sourceName = contextsSources[index];
        java.util.List /*<String>*/ contexts = controller.getContextsFrom(sourceName);
        setContexts(contexts);
    }

    public java.util.List /*<String>*/ getSelectedContexts() {
        return accumPanel.getValues();
    }

    public java.util.List /*<String>*/ getAvailableContexts() {
        return accumPanel.getAvailableValues();
    }
    
    public void setContexts(java.util.List /*<String>*/ selectedContexts) {
        accumPanel.setValues(selectedContexts);
    }

    public void setCopyButtonState(String sourceName, boolean newState) {
        if ((contextsSources != null) && (copyFrom != null)) {
            int index = StaticQuickSorter.unsortedStringArrayIndex(contextsSources,
                    sourceName);
            if (index >= 0) {
                copyFrom[index].setEnabled(newState);
            }
        }
    }
}//end ContextsAccumulatorOuterPanel
