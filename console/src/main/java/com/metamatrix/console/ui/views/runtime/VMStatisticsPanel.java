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

package com.metamatrix.console.ui.views.runtime;

import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.awt.GridLayout;
import java.awt.Insets;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;

import javax.swing.AbstractButton;
import javax.swing.JPanel;

import com.metamatrix.platform.admin.api.runtime.ProcessData;
import com.metamatrix.platform.vm.controller.ProcessStatistics;
import com.metamatrix.platform.vm.controller.SocketListenerStats;
import com.metamatrix.toolbox.ui.widget.ButtonWidget;

public class VMStatisticsPanel extends JPanel{
    private ProcessData processData;
    private QueueStatisticsRefreshRequestHandler controller;
    private ProcessStatistics vmStatistics;
    private ProcessVMStatisticsPanel processPanel;
    private SingleQueueStatisticsPanel queuePanel;
    private SocketVMStatisticsPanel socketPanel;
    private AbstractButton closeButton;
    
    public VMStatisticsPanel(QueueStatisticsRefreshRequestHandler ctrlr,
            ProcessData pd, ProcessStatistics vmStats) {
        super();
        controller = ctrlr;
        processData = pd;
        vmStatistics = vmStats;
        init();
    }

    private void init() {
        closeButton = new ButtonWidget("Close");
        final ButtonWidget refreshButton = new ButtonWidget("Refresh");
        final ButtonWidget runButton = new ButtonWidget("Run Garbage Collection");
        if (controller != null) {
            final ProcessData pd = processData;
            refreshButton.addActionListener(new ActionListener() {
                public void actionPerformed(ActionEvent ev) {
                    controller.refreshProcessRequested(pd);
                }
            });
            runButton.addActionListener(new ActionListener() {
                public void actionPerformed(ActionEvent ev) {
                    controller.runGarbageCollection(pd);
                    //Need to do a refresh after running garbage collector
                    controller.refreshProcessRequested(pd);
				}
            });
        } else {
            refreshButton.setVisible(false);
        }
        GridBagLayout layout = new GridBagLayout();
        this.setLayout(layout);
        
        GridBagLayout statsLayout = new GridBagLayout();
        
        JPanel statsPanel = new JPanel(statsLayout);
        processPanel = new ProcessVMStatisticsPanel(vmStatistics.name);
        processPanel.populate(vmStatistics);
        queuePanel = new SingleQueueStatisticsPanel("Socket Worker");
        queuePanel.populate(vmStatistics.processPoolStats);
        socketPanel = new SocketVMStatisticsPanel();
        socketPanel.populate(vmStatistics);
        
        statsPanel.add(processPanel);
        statsPanel.add(queuePanel);
        statsPanel.add(socketPanel);
        
        this.add(statsPanel);
        JPanel buttonsPanel = new JPanel(new GridLayout(1, 3, 40, 0));
        buttonsPanel.add(refreshButton);
        buttonsPanel.add(closeButton);
        buttonsPanel.add(runButton);
        this.add(buttonsPanel);

        
        statsLayout.setConstraints(processPanel, new GridBagConstraints(0, 0, 1, 1,
            1.0, 1.0, GridBagConstraints.CENTER, GridBagConstraints.BOTH,
            new Insets(4, 4, 4, 4), 0, 0));
        statsLayout.setConstraints(queuePanel, new GridBagConstraints(0, 1, 1, 1,
            0.0, 0.0, GridBagConstraints.CENTER, GridBagConstraints.BOTH,
            new Insets(4, 4, 4, 4), 0, 0));        
        statsLayout.setConstraints(socketPanel, new GridBagConstraints(0, 2, 1, 1,
            0.0, 0.0, GridBagConstraints.CENTER, GridBagConstraints.BOTH,
            new Insets(4, 4, 4, 4), 0, 0));

        
        layout.setConstraints(statsPanel, new GridBagConstraints(0, 0, 1, 1,
                1.0, 1.0, GridBagConstraints.CENTER, GridBagConstraints.BOTH,
                new Insets(4, 4, 4, 4), 0, 0));
        layout.setConstraints(buttonsPanel, new GridBagConstraints(0, 1, 1, 1,
                0.0, 0.0, GridBagConstraints.CENTER, GridBagConstraints.NONE,
                new Insets(4, 4, 4, 4), 0, 0));
    }

    public void repopulate(ProcessStatistics vmStat) {
        vmStatistics = vmStat;
        processPanel.populate(vmStatistics);
        queuePanel.populate(vmStatistics.processPoolStats);
        socketPanel.populate(vmStatistics);
    }

    public AbstractButton getCloseButton() {
        return closeButton;
    }
}//end QueueStatisticsPanel

class ProcessVMStatisticsPanel extends AbstractStatisticsPanel<ProcessStatistics> {
    private String processName;
    
    private static final String[] labelStrings = {
        "Total Memory:",
        "Free Memory:", 
        "Thread Count:",
    };
    
    
    public ProcessVMStatisticsPanel(String processName) {
        super();
        this.processName = processName;
        init();
    }
    
    
    /**Get title of the panel.*/
    public String getTitle() {
        return "Process: " + processName;
    }

    /**Get titles of the displayed fields.*/    
    public String[] getLabelStrings() {
        return labelStrings;
    }

    /**Populate the displayed fields from the specified VMStatistics.*/    
    public void populate(ProcessStatistics vmStats) {
        textFieldWidgets[0].setText(Long.toString(vmStats.totalMemory));
        textFieldWidgets[1].setText(Long.toString(vmStats.freeMemory));        
        textFieldWidgets[2].setText(Integer.toString(vmStats.threadCount));
    }   
}

class SocketVMStatisticsPanel extends AbstractStatisticsPanel<ProcessStatistics> {

    private static final String[] labelStrings = {
        "Message Packets Read",
        "Message Packets Written",
        "Num. Sockets",
        "Highest Num. Sockets",
    };
    
    
    public SocketVMStatisticsPanel() {
        super();
        init();
    }

    /**Get title of the panel.*/
    public String getTitle() {
        return "Communication Stats";
    }

    /**Get titles of the displayed fields.*/    
    public String[] getLabelStrings() {
        return labelStrings;
    }

    public void populate(ProcessStatistics vmStats) {        
        SocketListenerStats listenerStats = vmStats.socketListenerStats;
        textFieldWidgets[0].setText(Long.toString(listenerStats.objectsRead));
        textFieldWidgets[1].setText(Long.toString(listenerStats.objectsWritten));
        textFieldWidgets[2].setText(Integer.toString(listenerStats.sockets));
        textFieldWidgets[3].setText(Integer.toString(listenerStats.maxSockets));
    }

}