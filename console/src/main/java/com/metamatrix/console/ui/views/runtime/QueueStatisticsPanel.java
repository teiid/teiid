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

import com.metamatrix.platform.admin.api.runtime.ServiceData;

import com.metamatrix.toolbox.ui.widget.ButtonWidget;
import com.metamatrix.toolbox.ui.widget.LabelWidget;
import com.metamatrix.toolbox.ui.widget.TextFieldWidget;
import com.metamatrix.toolbox.ui.widget.TitledBorder;

public class QueueStatisticsPanel extends JPanel {
    private ServiceData serviceData;
    private QueueStatisticsRefreshRequestHandler controller;
    private QueueStatistics[] queueStatistics;
    private SingleQueueStatisticsPanel[] subPanels;
    private AbstractButton closeButton;
    
    public QueueStatisticsPanel(QueueStatisticsRefreshRequestHandler ctrlr,
            ServiceData sd, QueueStatistics[] queueStat) {
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
            subPanels[i].populate(new Integer(queueStatistics[i].getCurrentSize()),
                    new Integer(queueStatistics[i].getHighestSize()),
                    new Integer(queueStatistics[i].getTotalEnqueued()),
                    new Integer(queueStatistics[i].getTotalDequeued()),
                    new Integer(queueStatistics[i].getNumThreads()));
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


    public void repopulate(QueueStatistics[] queueStat) {
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
                subPanels[i].populate(null, null, null, null, null);
            } else {
                subPanels[i].populate(
                        new Integer(queueStatistics[matchLoc].getCurrentSize()),
                        new Integer(queueStatistics[matchLoc].getHighestSize()),
                        new Integer(queueStatistics[matchLoc].getTotalEnqueued()),
                        new Integer(queueStatistics[matchLoc].getTotalDequeued()),
                        new Integer(queueStatistics[matchLoc].getNumThreads()));
            }
        }
    }

    public AbstractButton getCloseButton() {
        return closeButton;
    }
}//end QueueStatisticsPanel



class SingleQueueStatisticsPanel extends JPanel {
    private String queueName;
    private TextFieldWidget currentSizeTFW;
    private TextFieldWidget highestSizeTFW;
    private TextFieldWidget totalEnqueuedTFW;
    private TextFieldWidget totalDequeuedTFW;
    private TextFieldWidget numThreadsTFW;
    
    public SingleQueueStatisticsPanel(String queName) {
        super();
        queueName = queName;
        init();
    }

    private void init() {
        this.setBorder(new TitledBorder("Queue: " + queueName));
        GridBagLayout layout = new GridBagLayout();
        this.setLayout(layout);
        LabelWidget currentSizeLabel = new LabelWidget("Current size:");
        LabelWidget highestSizeLabel = new LabelWidget("Highest size:");
        LabelWidget totalEnqueuedLabel = new LabelWidget("Total enqueued:");
        LabelWidget totalDequeuedLabel = new LabelWidget("Total dequeued:");
        LabelWidget numThreadsLabel = new LabelWidget("Num. threads:");
        currentSizeTFW = new TextFieldWidget(10);
        currentSizeTFW.setEditable(false);
        highestSizeTFW = new TextFieldWidget();
        highestSizeTFW.setEditable(false);
        totalEnqueuedTFW = new TextFieldWidget();
        totalEnqueuedTFW.setEditable(false);
        totalDequeuedTFW = new TextFieldWidget();
        totalDequeuedTFW.setEditable(false);
        numThreadsTFW = new TextFieldWidget();
        numThreadsTFW.setEditable(false);
        this.add(currentSizeLabel);
        this.add(highestSizeLabel);
        this.add(totalEnqueuedLabel);
        this.add(totalDequeuedLabel);
        this.add(numThreadsLabel);
        this.add(currentSizeTFW);
        this.add(highestSizeTFW);
        this.add(totalEnqueuedTFW);
        this.add(totalDequeuedTFW);
        this.add(numThreadsTFW);
        layout.setConstraints(currentSizeLabel, new GridBagConstraints(0, 0, 1, 1,
                0.0, 0.0, GridBagConstraints.EAST, GridBagConstraints.NONE,
                new Insets(4, 4, 4, 4), 0, 0));
        layout.setConstraints(highestSizeLabel, new GridBagConstraints(0, 1, 1, 1,
                0.0, 0.0, GridBagConstraints.EAST, GridBagConstraints.NONE,
                new Insets(4, 4, 4, 4), 0, 0));
        layout.setConstraints(totalEnqueuedLabel, new GridBagConstraints(0, 2, 1, 1,
                0.0, 0.0, GridBagConstraints.EAST, GridBagConstraints.NONE,
                new Insets(4, 4, 4, 4), 0, 0));
        layout.setConstraints(totalDequeuedLabel, new GridBagConstraints(0, 3, 1, 1,
                0.0, 0.0, GridBagConstraints.EAST, GridBagConstraints.NONE,
                new Insets(4, 4, 4, 4), 0, 0));
       	layout.setConstraints(numThreadsLabel, new GridBagConstraints(0, 4, 1, 1,
       			0.0, 0.0, GridBagConstraints.EAST, GridBagConstraints.NONE,
       			new Insets(4, 4, 4, 4), 0, 0));
        layout.setConstraints(currentSizeTFW, new GridBagConstraints(1, 0, 1, 1,
                1.0, 0.0, GridBagConstraints.CENTER, GridBagConstraints.HORIZONTAL,
                new Insets(4, 4, 4, 4), 0, 0));
        layout.setConstraints(highestSizeTFW, new GridBagConstraints(1, 1, 1, 1,
                1.0, 0.0, GridBagConstraints.CENTER, GridBagConstraints.HORIZONTAL,
                new Insets(4, 4, 4, 4), 0, 0));
        layout.setConstraints(totalEnqueuedTFW, new GridBagConstraints(1, 2, 1, 1,
                1.0, 0.0, GridBagConstraints.CENTER, GridBagConstraints.HORIZONTAL,
                new Insets(4, 4, 4, 4), 0, 0));
        layout.setConstraints(totalDequeuedTFW, new GridBagConstraints(1, 3, 1, 1,
                1.0, 0.0, GridBagConstraints.CENTER, GridBagConstraints.HORIZONTAL,
                new Insets(4, 4, 4, 4), 0, 0));
        layout.setConstraints(numThreadsTFW, new GridBagConstraints(1, 4, 1, 1,
        		1.0, 0.0, GridBagConstraints.CENTER, GridBagConstraints.HORIZONTAL,
        		new Insets(4, 4, 4, 4), 0, 0));
    }

    public void populate(Integer currentSize, Integer highestSize,
            Integer totalEnqueued, Integer totalDequeued, Integer numThreads) {
        if (currentSize == null) {
            currentSizeTFW.setText("");
        } else {
            currentSizeTFW.setText(currentSize.toString());
        }
        if (highestSize == null) {
            highestSizeTFW.setText("");
        } else {
            highestSizeTFW.setText(highestSize.toString());
        }
        if (totalEnqueued == null) {
            totalEnqueuedTFW.setText("");
        } else {
            totalEnqueuedTFW.setText(totalEnqueued.toString());
        }
        if (totalDequeued == null) {
            totalDequeuedTFW.setText("");
        } else {
            totalDequeuedTFW.setText(totalDequeued.toString());
		}
        if (numThreads == null) {
        	numThreadsTFW.setText("");
        } else {
        	numThreadsTFW.setText(numThreads.toString());
        }
    }

    public String getQueueName() {
        return queueName;
    }
}//end SingleQueueStatisticsPanel
