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
import com.metamatrix.platform.admin.api.runtime.ServiceData;
import com.metamatrix.toolbox.ui.widget.ButtonWidget;

public class QueueStatisticsPanel extends JPanel {
    private ServiceData serviceData;
    private QueueStatisticsRefreshRequestHandler controller;
    private WorkerPoolStats[] queueStatistics;
    private SingleQueueStatisticsPanel[] subPanels;
    private AbstractButton closeButton;
    
    public QueueStatisticsPanel(QueueStatisticsRefreshRequestHandler ctrlr,
            ServiceData sd, WorkerPoolStats[] queueStat) {
        super();
        controller = ctrlr;
        serviceData = sd;
        queueStatistics = queueStat;
        init();
    }

    private void init() {
        closeButton = new ButtonWidget("Close");
        final ButtonWidget refreshButton = new ButtonWidget("Refresh");
        if (controller != null) {
            final ServiceData sd = serviceData;
            refreshButton.addActionListener(new ActionListener() {
                public void actionPerformed(ActionEvent ev) {
                    controller.refreshRequested(sd);
                }
            });
        } else {
            refreshButton.setVisible(false);
        }
        GridBagLayout layout = new GridBagLayout();
        this.setLayout(layout);
        subPanels = new SingleQueueStatisticsPanel[queueStatistics.length];
        JPanel statsPanel = new JPanel(new GridLayout(subPanels.length, 1, 0, 4));
        for (int i = 0; i < queueStatistics.length; i++) {
            subPanels[i] = new SingleQueueStatisticsPanel(
                    queueStatistics[i].getQueueName());
            subPanels[i].populate(queueStatistics[i]);
            statsPanel.add(subPanels[i]);
        }
        this.add(statsPanel);
        JPanel buttonsPanel = new JPanel(new GridLayout(1, 2, 40, 0));
        buttonsPanel.add(refreshButton);
        buttonsPanel.add(closeButton);
        this.add(buttonsPanel);

        layout.setConstraints(statsPanel, new GridBagConstraints(0, 0, 1, 1,
                1.0, 1.0, GridBagConstraints.CENTER, GridBagConstraints.BOTH,
                new Insets(4, 4, 4, 4), 0, 0));
        layout.setConstraints(buttonsPanel, new GridBagConstraints(0, 1, 1, 1,
                0.0, 0.0, GridBagConstraints.CENTER, GridBagConstraints.NONE,
                new Insets(4, 4, 4, 4), 0, 0));
    }


    public void repopulate(WorkerPoolStats[] queueStat) {
        queueStatistics = queueStat;
        for (int i = 0; i < subPanels.length; i++) {
            String subPanelQueueName = subPanels[i].getQueueName();
            int matchLoc = -1;
            int j = 0;
            while ((j < queueStatistics.length) && (matchLoc < 0)) {
                if (queueStatistics[j].getQueueName().equals(subPanelQueueName)) {
                    matchLoc = j;
                } else {
                    j++;
                }
            }
            if (matchLoc < 0) {
                subPanels[i].populate(null);
            } else {
                subPanels[i].populate(queueStatistics[matchLoc]);
            }
        }
    }

    public AbstractButton getCloseButton() {
        return closeButton;
    }
}//end QueueStatisticsPanel



class SingleQueueStatisticsPanel extends AbstractStatisticsPanel<WorkerPoolStats> {
    private String queueName;
    
    public SingleQueueStatisticsPanel(String queName) {
        super();
        queueName = queName;
        init();
    }
    
    @Override
    public String[] getLabelStrings() {
    	return new String[] { 
    			"Current size", 
    			"Highest size",
				"Total submitted", 
				"Total completed", 
				"Current threads",
				"Highest threads" };
    }
    
    @Override
    public String getTitle() {
    	return "Queue: " + queueName;
    }

    public void populate(WorkerPoolStats stats) {
        if (stats == null) {
        	for (int i = 0; i < textFieldWidgets.length; i++) {
        		textFieldWidgets[i].setText("");
        	}
        } else {
        	textFieldWidgets[0].setText(String.valueOf(stats.getQueued()));
        	textFieldWidgets[1].setText(String.valueOf(stats.getHighestQueued()));
        	textFieldWidgets[2].setText(String.valueOf(stats.getTotalSubmitted()));
        	textFieldWidgets[3].setText(String.valueOf(stats.getTotalCompleted()));
        	textFieldWidgets[4].setText(String.valueOf(stats.getActiveThreads()));
        	textFieldWidgets[5].setText(String.valueOf(stats.getHighestActiveThreads()));
        }
    }

    public String getQueueName() {
        return queueName;
    }
}//end SingleQueueStatisticsPanel
