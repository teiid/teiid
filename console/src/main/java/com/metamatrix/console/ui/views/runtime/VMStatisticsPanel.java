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

import com.metamatrix.common.queue.WorkerPoolStats;
import com.metamatrix.platform.admin.api.runtime.ProcessData;
import com.metamatrix.platform.vm.controller.SocketListenerStats;
import com.metamatrix.platform.vm.controller.VMStatistics;
import com.metamatrix.toolbox.ui.widget.ButtonWidget;
import com.metamatrix.toolbox.ui.widget.LabelWidget;
import com.metamatrix.toolbox.ui.widget.TextFieldWidget;
import com.metamatrix.toolbox.ui.widget.TitledBorder;

public class VMStatisticsPanel extends JPanel{
    private ProcessData processData;
    private QueueStatisticsRefreshRequestHandler controller;
    private VMStatistics vmStatistics;
    private ProcessVMStatisticsPanel processPanel;
    private QueueVMStatisticsPanel queuePanel;
    private SocketVMStatisticsPanel socketPanel;
    private AbstractButton closeButton;
    
    public VMStatisticsPanel(QueueStatisticsRefreshRequestHandler ctrlr,
            ProcessData pd, VMStatistics vmStats) {
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
        queuePanel = new QueueVMStatisticsPanel();
        queuePanel.populate(vmStatistics);
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

    public void repopulate(VMStatistics vmStat) {
        vmStatistics = vmStat;
        processPanel.populate(vmStatistics);
        queuePanel.populate(vmStatistics);
        socketPanel.populate(vmStatistics);
    }

    public AbstractButton getCloseButton() {
        return closeButton;
    }
}//end QueueStatisticsPanel




abstract class AbstractVMStatisticsPanel extends JPanel {

    protected TextFieldWidget[] textFieldWidgets;
    
    /**Get title of the panel.*/
    public abstract String getTitle();

    /**Get titles of the displayed fields.*/    
    public abstract String[] getLabelStrings();

    /**Populate the displayed fields from the specified VMStatistics.*/    
    public abstract void populate(VMStatistics vmStats);
    
    
    public AbstractVMStatisticsPanel() {
        super();
    }

    protected void init() {
        this.setBorder(new TitledBorder(getTitle()));
        GridBagLayout layout = new GridBagLayout();
        this.setLayout(layout);
        
        String[] labelStrings = getLabelStrings();
        int nfields = labelStrings.length;

        textFieldWidgets = new TextFieldWidget[nfields];
        LabelWidget[] labelWidgets = new LabelWidget[nfields]; 
        
        
        for (int i=0; i<nfields; i++) {        
            labelWidgets[i] = new LabelWidget(labelStrings[i]);
            textFieldWidgets[i] = new TextFieldWidget(0);
            textFieldWidgets[i].setEditable(false);
            this.add(labelWidgets[i]);
        }
        
        for (int i=0; i<nfields; i++) {
            this.add(textFieldWidgets[i]);
        }
            
        for (int i=0; i<nfields; i++) {
            layout.setConstraints(labelWidgets[i], new GridBagConstraints(0, i, 1, 1,
                0.0, 0.0, GridBagConstraints.EAST, GridBagConstraints.NONE,
                new Insets(4, 4, 4, 4), 0, 0));
        }
        
        for (int i=0; i<nfields; i++) {
            layout.setConstraints(textFieldWidgets[i], new GridBagConstraints(1, i, 1, 1,
                1.0, 0.0, GridBagConstraints.CENTER, GridBagConstraints.HORIZONTAL,
                new Insets(4, 4, 4, 4), 0, 0));
        }
    }
      
}



class ProcessVMStatisticsPanel extends AbstractVMStatisticsPanel {
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
    public void populate(VMStatistics vmStats) {
        textFieldWidgets[0].setText(Long.toString(vmStats.totalMemory));
        textFieldWidgets[1].setText(Long.toString(vmStats.freeMemory));        
        textFieldWidgets[2].setText(Integer.toString(vmStats.threadCount));
    }   
}



class QueueVMStatisticsPanel extends AbstractVMStatisticsPanel {

    private static final String[] labelStrings = {
        "Current Size",
        "Highest Size",
        "Total Enqueued",
        "Total Dequeued",
        "Num. Threads",
    };
    
    
    public QueueVMStatisticsPanel() {
        super();
        init();
    }

    /**Get title of the panel.*/
    public String getTitle() {
        return "Socket Worker Queue";
    }

    /**Get titles of the displayed fields.*/    
    public String[] getLabelStrings() {
        return labelStrings;
    }

    /**Populate the displayed fields from the specified VMStatistics.*/    
        public void populate(VMStatistics vmStats) {
        WorkerPoolStats poolStats = vmStats.processPoolStats;
        textFieldWidgets[0].setText(Integer.toString(poolStats.queued));        
        textFieldWidgets[1].setText(Integer.toString(0));
        textFieldWidgets[2].setText(Long.toString(poolStats.totalSubmitted));
        textFieldWidgets[3].setText(Long.toString(poolStats.totalCompleted));        
        textFieldWidgets[4].setText(Integer.toString(poolStats.threads));
    }
}


class SocketVMStatisticsPanel extends AbstractVMStatisticsPanel {

    private static final String[] labelStrings = {
        "MetaMatrix Packets Read",
        "MetaMatrix Packets Written",
        "Num. Sockets",
        "Highest Num. Sockets",
        "Num. Client Connections",
        "Highest Num. Client Connections",
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

    public void populate(VMStatistics vmStats) {        
        SocketListenerStats listenerStats = vmStats.socketListenerStats;
        textFieldWidgets[0].setText(Long.toString(listenerStats.objectsRead));
        textFieldWidgets[1].setText(Long.toString(listenerStats.objectsWritten));
        textFieldWidgets[2].setText(Integer.toString(listenerStats.sockets));
        textFieldWidgets[3].setText(Integer.toString(listenerStats.maxSockets));
        textFieldWidgets[4].setText("0"); //$NON-NLS-1$
        textFieldWidgets[5].setText("0"); //$NON-NLS-1$
        
    }

}