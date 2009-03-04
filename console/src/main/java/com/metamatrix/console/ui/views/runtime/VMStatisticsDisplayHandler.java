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

import com.metamatrix.console.ui.ViewManager;
import com.metamatrix.console.util.StaticUtilities;

import com.metamatrix.platform.admin.api.runtime.ProcessData;
import com.metamatrix.platform.vm.controller.ProcessStatistics;

public class VMStatisticsDisplayHandler {
    public  QueueStatisticsRefreshRequestHandler caller;
    private OperationsPanel operationsPnl;
    private VMStatisticsFrame statisticsFrame;
    private Map /*<process unique identifier to QueueStatisticsFrame>*/ currentlyShowing =
            new HashMap();

    public VMStatisticsDisplayHandler(QueueStatisticsRefreshRequestHandler vmrrh) {
        super();
        caller = vmrrh;
    }

    public boolean isProcessDisplayed(ProcessData pd) {
        return currentlyShowing.containsKey(pd);
    }

    public Map getDialogs(){
        return currentlyShowing;
    }

	/**
	 * Create a VMStatisticsFrame for the specified ProcessData.
     * The caller must subsequently call VMStatisticsFrame.show() to display the frame.
     */
    public VMStatisticsFrame startDisplayForProcess(String processDisplayName, ProcessData pd,
            ProcessStatistics vmstatistics) {
        VMStatisticsPanel panel = new VMStatisticsPanel(caller, pd, vmstatistics);
        statisticsFrame = new VMStatisticsFrame(this, processDisplayName, pd, 
        		panel);
        currentlyShowing.put(pd, statisticsFrame);
        return statisticsFrame;
    }

    public void setOperationsPnl(OperationsPanel op){
        this.operationsPnl = op;
    }

    public void refreshDisplayForProcess(String processDisplayName, ProcessData pd,
            ProcessStatistics vmstatistics) {
        VMStatisticsFrame statisticsFrame =
                (VMStatisticsFrame) currentlyShowing.get(pd);
        VMStatisticsPanel panel = statisticsFrame.getPanel();
        panel.repopulate(vmstatistics);
    }

    public void frameClosing(ProcessData pd) {
        currentlyShowing.remove(pd);
        operationsPnl.setEnabledShowProcess(pd);
    }
}//end QueueStatisticsDisplayHandler




class VMStatisticsFrame extends JFrame {
    private final static float MIN_SCREEN_WIDTH_PROPORTION = (float)0.30;
    VMStatisticsDisplayHandler caller;
    VMStatisticsPanel panel;
    ProcessData pd;

    public VMStatisticsFrame(VMStatisticsDisplayHandler cllr,
            String processDisplayName, ProcessData pd, VMStatisticsPanel pnl) {
        super(processDisplayName + " Process Status");
      //  super();
        caller = cllr;
        this.pd = pd;
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

    public ProcessData getProcessData(){
        return  pd;
    }

    public VMStatisticsPanel getPanel() {
        return panel;
    }

    public void dispose() {
        caller.frameClosing(pd);
        super.dispose();
    }
}//end VMStatisticsFrame

