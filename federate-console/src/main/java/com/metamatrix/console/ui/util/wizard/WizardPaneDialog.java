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

package com.metamatrix.console.ui.util.wizard;


import java.awt.BorderLayout;
import java.awt.Dimension;
import java.awt.event.WindowAdapter;
import java.awt.event.WindowEvent;
import java.util.Iterator;

import javax.swing.JDialog;
import javax.swing.JFrame;

import com.metamatrix.console.util.StaticUtilities;

/**
 * WizardPaneDialog:  Wraps WizardPane with a real dialog as a convenience.
 *      The relationship between the WizardClient and the WizardPane is
 *      not affected by this.
 */
public class WizardPaneDialog
     extends JDialog

{


    private WizardPane wpPane               = null;
    private AbstractWizardClient wzcClient  = null;

    /**
     * Create
     */
    public WizardPaneDialog( JFrame frFrame, AbstractWizardClient wzcClient )
    {
        super( frFrame, wzcClient.getTitle(), true );

        this.wzcClient      = wzcClient;


        init();

        getWizardPane().setVisible(true);

        getWizardPane().postRealize();

    }


    /**
     * Construct all visual components for this dialog.
     */
    public void init()
    {

        getContentPane().setLayout( new BorderLayout() );

        wpPane  = new WizardPane( this, wzcClient );
        getContentPane().add( wpPane, BorderLayout.CENTER );
        //getContentPane().setBackground( Color.white );
//        setBackground( Color.white );

        addWindowListener(new WindowAdapter() {
		    public void windowClosing(WindowEvent event) {
                // WHAT METHOD DO WE CALL? Does this class also have
                //  a 'cancelClicked' method? Hmmmmmmm?
		         //cancel();
                 getWizardPane().getCancelButton().doClick();
            }
		});

        pack();

        this.setLocation(StaticUtilities.centerFrame(this.getSize()));

    }

    public WizardPane getWizardPane()
    {
        return wpPane;
    }

    // =======
    //  overrides of dialog sizing methods
    //   THIS IS A BAD IDEA!! DO NOT HAVE A getPreferredSize here
    // =======

    private java.util.List getPanelForClient(){
        return this.wzcClient.getPanels();
    }

    private Dimension calcPanelPreferrecdSize(){
//        int iCount;
        Dimension dimCompSize = null;
        WizardClientPanel wcp = null;
        int maxHeight = 0;
        int maxWidth = 0;
        java.util.List panelList = getPanelForClient();
        Iterator iter = panelList.iterator();
//        iCount = panelList.size();
        while (iter.hasNext()){
            wcp =(WizardClientPanel)iter.next();
            dimCompSize = wcp.getComponent().getPreferredSize();
            int iNewHeight =
                (int)Math.max(maxHeight, dimCompSize.getHeight());
            int iNewWidth =
                (int)Math.max(maxWidth, dimCompSize.getWidth());
            maxHeight = iNewHeight;
            maxWidth = iNewWidth;

        }

        return new Dimension(maxWidth+200,maxHeight+100);
    }

    public Dimension getPreferredSize()
    {
        
        return calcPanelPreferrecdSize();
    }
  /*
    public Dimension getMinimumSizeX()
    {
        return new Dimension( 300, 350 );
    }
*/

}
