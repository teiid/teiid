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

import java.awt.Dimension;
import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.awt.Insets;
import java.awt.Toolkit;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.awt.event.WindowAdapter;
import java.awt.event.WindowEvent;
import java.util.HashMap;
import java.util.Map;

import javax.swing.JFrame;
import javax.swing.JPanel;

import com.metamatrix.common.queue.WorkerPoolStats;
import com.metamatrix.console.ui.ViewManager;
import com.metamatrix.console.util.StaticUtilities;

import com.metamatrix.platform.admin.api.runtime.ServiceData;

public class QueueStatisticsDisplayHandler extends JPanel{
    public QueueStatisticsRefreshRequestHandler caller;
    private OperationsPanel operationsPnl;
    private QueueStatisticsFrame statisticsFrame;
    private Map /*<service name to QueueStatisticsFrame>*/ currentlyShowing =
            new HashMap();

    public QueueStatisticsDisplayHandler(
            QueueStatisticsRefreshRequestHandler cllr) {
        super();
        caller = cllr;
    }

    public boolean isServiceDisplayed(ServiceData sd) {
        return currentlyShowing.containsKey(sd);
    }

    public Map getDialogs(){
        return currentlyShowing;
    }

    
    /**
     * Create a QueueStatisticsFrame for the specified ProcessData.
     * The caller must subsequently call QueueStatisticsFrame.show() to display the frame.
     */
    public QueueStatisticsFrame startDisplayForService(String serviceDisplayName, ServiceData sd,
            WorkerPoolStats[] queueStatistics) {
        QueueStatisticsPanel panel = new QueueStatisticsPanel(caller,
                sd, queueStatistics);
        statisticsFrame = new QueueStatisticsFrame(this,
                serviceDisplayName, sd, panel);
        currentlyShowing.put(sd, statisticsFrame);
        return statisticsFrame;
    }

    public void setOperationsPnl(OperationsPanel op){
        this.operationsPnl = op;
    }

    public void refreshDisplayForService(String serviceDisplayName, ServiceData sd,
            WorkerPoolStats[] queueStatistics) {
        QueueStatisticsFrame statisticsFrame =
                (QueueStatisticsFrame)currentlyShowing.get(sd);
        QueueStatisticsPanel panel = statisticsFrame.getPanel();
        panel.repopulate(queueStatistics);
    }

    public void frameClosing(ServiceData sd) {
        currentlyShowing.remove(sd);
        operationsPnl.setEnabledShowQueues(sd);
    }
}//end QueueStatisticsDisplayHandler



class QueueStatisticsFrame extends JFrame {
    private final static float MIN_SCREEN_WIDTH_PROPORTION = (float)0.30;
    QueueStatisticsDisplayHandler caller;
    QueueStatisticsPanel panel;
    ServiceData sd;

    public QueueStatisticsFrame(QueueStatisticsDisplayHandler cllr,
            String serviceDisplayName, ServiceData sd, QueueStatisticsPanel pnl) {
        super(serviceDisplayName + " Queue Status");
        caller = cllr;
        this.sd = sd;
        panel = pnl;
        init();
    }

    private void init() {
        this.setIconImage(ViewManager.CONSOLE_ICON_IMAGE);
        GridBagLayout layout = new GridBagLayout();
        layout.setConstraints(panel, new GridBagConstraints(0, 0, 1, 1,
                0.1, 0.1, GridBagConstraints.EAST, GridBagConstraints.BOTH,
                new Insets(4, 4, 4, 4), 0, 0));
        this.getContentPane().setLayout(layout);
        this.getContentPane().add(panel);
        this.addWindowListener(new WindowAdapter(){
            public void windowClosing(WindowEvent event){
                dispose();
            }
        });
        panel.getCloseButton().addActionListener(new ActionListener() {
            public void actionPerformed(ActionEvent ev) {
                dispose();
            }
        });
        this.pack();
        Dimension screenSize = Toolkit.getDefaultToolkit().getScreenSize();
        Dimension firstSize = this.getSize();
        int height = firstSize.height;
        int width = Math.max(firstSize.width, (int)(screenSize.width *
                MIN_SCREEN_WIDTH_PROPORTION));
        this.setSize(width, height);
        this.setLocation(StaticUtilities.centerFrame(this.getSize()));
    }

    public ServiceData getServiceData(){
        return  sd;
    }

    public QueueStatisticsPanel getPanel() {
        return panel;
    }

    public void dispose() {
        caller.frameClosing(sd);
        super.dispose();
    }
}//end QueueStatisticsFrame
