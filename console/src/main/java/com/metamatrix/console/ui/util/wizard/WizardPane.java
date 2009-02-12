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

package com.metamatrix.console.ui.util.wizard;


import java.awt.BorderLayout;
import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.awt.Insets;
import java.awt.Window;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.awt.event.KeyEvent;
import java.util.Iterator;

import javax.swing.BoxLayout;
import javax.swing.JButton;
import javax.swing.JPanel;
import javax.swing.border.EmptyBorder;
import javax.swing.border.EtchedBorder;
import javax.swing.event.ChangeEvent;
import javax.swing.event.ChangeListener;

import com.metamatrix.toolbox.ui.widget.LabelWidget;

/**
 * WizardPane                 
 */
public class WizardPane
     extends JPanel
  implements ChangeListener

{

    // Wizard buttons, etc. constants
    public static final String BACK_BUTTON_TEXT         = "< Back"; //$NON-NLS-1$
    public static final String NEXT_BUTTON_TEXT         = "Next >"; //$NON-NLS-1$
    public static final String CANCEL_BUTTON_TEXT       = "Cancel"; //$NON-NLS-1$
    public static final String FINISH_BUTTON_TEXT       = "Finish"; //$NON-NLS-1$
    public static final int BACK_MNEMONIC               = KeyEvent.VK_B;
    public static final int NEXT_MNEMONIC               = KeyEvent.VK_N;
    public static final int CANCEL_MNEMONIC             = KeyEvent.VK_C;
    public static final int FINISH_MNEMONIC             = KeyEvent.VK_I;

//    private Dimension dialogSize = new Dimension(300, 300);

    private JPanel pnlOuter                 = null;
    private JPanel pnlExplanation           = null;
    private JPanel pnlExplanationOuter      = null;
    private LabelWidget lblExplanation           = null;
    private JPanel pnlContent               = null;
    private JPanel pnlContentOuter          = null;
    private JPanel pnlButtons               = null;

    private JButton backButton, nextButton, cancelButton;

    //private JRadioButton r1, r2, r3, r4;
    private boolean showFlag                = true;
//    private java.util.List lstPanels        = null;

//    private Dimension dimContentPanelPrefSize = new Dimension();
//    private int iStartPanel                 = -1;
    private WizardClientPanel wcpCurrentPanel = null;
    private int iCurrentPanelNumber         = -1;
    private WizardClient wzcClient          = null;
    private Window wnOwnerWindow            = null;


    /**
     * Create
     */
    public WizardPane( Window wnOwnerWindow, WizardClient wzcClient )
    {
        super();
        this.wnOwnerWindow = wnOwnerWindow;

        this.wzcClient      = wzcClient;
        //OBSOLETE: this.lstPanels      = wzcClient.getPanels();

//        this.iStartPanel    =
            wzcClient.getFirstPanelIndex();
        init();
    }

    public WizardPane( WizardClient wzcClient )
    {
        super();

        this.wzcClient      = wzcClient;
        //OBSOLETE: this.lstPanels      = wzcClient.getPanels();

//        this.iStartPanel    = 
            wzcClient.getFirstPanelIndex();
        init();
    }



    /**
     * Construct the visual components.
     */
    public void init()
    {

        setLayout( new BorderLayout() );

        pnlOuter = new JPanel();
        pnlOuter.setLayout( new BoxLayout(pnlOuter, BoxLayout.Y_AXIS) );
        pnlOuter.setBorder(new EmptyBorder(new Insets(10, 10, 20, 10)));

        add( pnlOuter, BorderLayout.CENTER );


        createExplanationPanel();
        pnlOuter.add( pnlExplanationOuter );

        createContentPanel();
        pnlOuter.add( pnlContentOuter );

        createButtonPanel();
        pnlOuter.add( pnlButtons );


        // Add listening
        addListening();

        // Add the first panel:

        processChange( iCurrentPanelNumber,
                       iCurrentPanelNumber + 1 );
        //this.validate();
    }

    private void addListening()
    {
        // add this WizardPane object as a listener to change
        //  events on all of the panels we are managing:
        WizardClientPanel wcp       = null;
        Iterator it = getWizardClient().getPanels().iterator();

        while ( it.hasNext() )
        {
            wcp =  (WizardClientPanel)it.next();
            wcp.addChangeListener( this );
        }

    }

    private WizardClient getWizardClient()
    {
        return wzcClient;
    }

    private int getPanelArraySize()
    {
        return getWizardClient().getPanels().size();
    }


    private void removeCurrentPanel()
    {
        if ( wcpCurrentPanel != null )
        {
            pnlContent.remove( (JPanel)wcpCurrentPanel );
        }
        
    }

    private void addNewPanel( WizardClientPanel wcp )
    {
        if( wcp != null )
        {
            pnlContent.add( (JPanel)wcp, BorderLayout.CENTER );

            if ( wcp.getTitle() != null ){
                lblExplanation.setText( wcp.getTitle() );
            }
            else{
                //out.println( "WizardPane.addNewPanel()...Title was null!" );
            }

        }

    }

    public void postRealize()
    {
        //calcContentPanelPreferredSize();
        packMyOwner();
    }
/***

    private void calcContentPanelPreferredSize()
    {
        WizardClientPanel wcp = null;
        Dimension dimCompSize = null;

        int iCount = getPanelArraySize();

        for( int i = 0; i < iCount; i++ )
        {
            wcp = (WizardClientPanel)getPanel( i );
            dimCompSize = wcp.getComponent().getSize();

            int iNewHeight
                = (int)Math.max( dimContentPanelPrefSize.getHeight(),
                                 dimCompSize.getHeight() );

            int iNewWidth
                = (int)Math.max( dimContentPanelPrefSize.getWidth(),
                                 dimCompSize.getWidth() );

            dimContentPanelPrefSize.setSize( iNewWidth, iNewHeight );

        // This is subject to the eternal Swing problem:
        //  "Until we have packed you, we do not know your size!!!!"
// DOES NOT WORK!!!!!!! pnlContent.setPreferredSize( dimContentPanelPrefSize );
        }

        resetContentPanelsPrefSize();

    }
***/
//    private void recalcContentPanelsPrefSize( Dimension dimNewPanelSize )
//    {
////        int iOldWidth   = (int)dimContentPanelPrefSize.getWidth();
////        int iOldHeight  = (int)dimContentPanelPrefSize.getHeight();
//
//        int iNewHeight
//            = (int)Math.max( dimContentPanelPrefSize.getHeight(),
//                             dimNewPanelSize.getHeight() );
//
//        int iNewWidth
//            = (int)Math.max( dimContentPanelPrefSize.getWidth(),
//                             dimNewPanelSize.getWidth() );
//
//        dimContentPanelPrefSize.setSize( iNewWidth, iNewHeight );
//
//        // This is subject to the eternal Swing problem:
//        //  "Until we have packed you, we do not know your size!!!!"
//// DOES NOT WORK!!!!!!! pnlContent.setPreferredSize( dimContentPanelPrefSize );
//
//    }
//
//    
//    private void resetContentPanelsPrefSize()
//    {
//        pnlContent.setPreferredSize( dimContentPanelPrefSize );
//
//    }

    public JButton getCancelButton(){
        return cancelButton;
    }
    
    private void createButtonPanel()
    {
        backButton = new JButton(BACK_BUTTON_TEXT);
        backButton.setMnemonic(BACK_MNEMONIC);
        nextButton = new JButton(NEXT_BUTTON_TEXT);
        nextButton.setMnemonic(NEXT_MNEMONIC);
        // 'Finish' needs to be put on the 'Next' button
        //  whenever we are showing the final page.  It is
        //  not its own button.

        cancelButton = new JButton(CANCEL_BUTTON_TEXT);
        cancelButton.setMnemonic(CANCEL_MNEMONIC);

        backButton.addActionListener(new ActionListener(){
            public void actionPerformed(ActionEvent e){
                backClicked();
            }
        });

        nextButton.addActionListener(new ActionListener(){
            public void actionPerformed(ActionEvent e){
                nextClicked();
            }
        });


        cancelButton.addActionListener(new ActionListener(){
            public void actionPerformed(ActionEvent e){
                cancelClicked();
            }
        });

        pnlButtons = new JPanel();
        pnlButtons.setLayout(new GridBagLayout());
        pnlButtons.add(backButton);
        pnlButtons.add(nextButton);
        pnlButtons.add(cancelButton);

        ((GridBagLayout)pnlButtons.getLayout()).setConstraints(backButton, new GridBagConstraints(
                0, 0, 1, 1, 0.1, 0.1, GridBagConstraints.EAST, GridBagConstraints.NONE,
                new Insets(0, 0, 0, 10), 0, 0) );
        ((GridBagLayout)pnlButtons.getLayout()).setConstraints(nextButton, new GridBagConstraints(
                1, 0, 1, 1, 0.1, 0.1, GridBagConstraints.CENTER, GridBagConstraints.NONE,
                new Insets(0, 5, 0, 5), 0, 0));
        ((GridBagLayout)pnlButtons.getLayout()).setConstraints(cancelButton,
                new GridBagConstraints(2, 0, 1, 1, 0.1, 0.1, GridBagConstraints.WEST,
                GridBagConstraints.NONE, new Insets(0, 10, 0, 0), 0, 0));

        pnlButtons.setBorder(new EmptyBorder(new Insets(20,10,20,10)));
//        pnlButtons.setBackground( Color.yellow );
    }


    private void createExplanationPanel()
    {

        pnlExplanation      = new JPanel( new BorderLayout() );
// future:        pnlContent.setLayout(cardLayout);
        lblExplanation = new LabelWidget(""); //$NON-NLS-1$
        pnlExplanation.add( lblExplanation );
        pnlExplanationOuter = new JPanel( new BorderLayout() );

        pnlExplanationOuter.setBorder(new EtchedBorder( EtchedBorder.LOWERED ) );
        pnlExplanation.setBorder( new EmptyBorder(new Insets(10, 10, 10, 10) ) );
        pnlExplanationOuter.add( pnlExplanation );
    }

    private void createContentPanel()
    {

        pnlContent      = new JPanel( new BorderLayout() );
// future:        pnlContent.setLayout(cardLayout);

        pnlContentOuter = new JPanel( new BorderLayout() );

        pnlContentOuter.setBorder(new EtchedBorder( EtchedBorder.LOWERED ) );
        pnlContent.setBorder( new EmptyBorder(new Insets(10, 10, 10, 10) ) );
        pnlContentOuter.add( pnlContent );

//        pnlContent.setBackground( Color.green );

    }

    private WizardClientPanel getPanel( int iIndex )
    {
        // test the arg first:
        int iTest       = iIndex;
        iTest++;
        int iPanelCount = getWizardClient().getPanels().size();

        if ( iTest > -1 && iTest <= iPanelCount )
        {

            return (WizardClientPanel)getWizardClient()
                                        .getPanels().get( iIndex );
        }
        return null;
    }

    private void setBackButtonState( boolean bEnabled )
    {
        backButton.setEnabled( bEnabled );
    }

    private void setNextButtonState( boolean bEnabled )
    {
        nextButton.setEnabled( bEnabled );
    }

    private void processChange( int iCurrPanel, int iNextPanel )
    {

        /*
            This is the heart of this class.
            1.  We need to take out the previous panel and put in
                the new panel.
            2.  Special Handling:
                    a.  If iCurrPanel is -1, we are at start, so:
                        i.   call getWizardClient().panelsChanging( int iOld,
                                                                    int iNew )
                        ii.  disable back button
                        iii. remove the current panel
                        iv. add the '0th' panel
        */
        if( iCurrPanel == -1 )
        {
            getWizardClient().panelsChanging( iCurrPanel,
                                              iNextPanel );

            // NOT NECESSARY! removeCurrentPanel();
            wcpCurrentPanel = getPanel( iNextPanel );
            addNewPanel( wcpCurrentPanel );
            iCurrentPanelNumber = iNextPanel;
        }
        else
        /*
                    b.  If iNextPanel is THE LAST PANEL, we are at end, so:
                        i.   call WizardClient.panelsChanging( int iOld,
                                                               int iNew )

                        ii.  Change 'next' button to say 'Finish'

                        iii. remove the current panel
                        iv. add the last panel
        */

        if( iNextPanel == (getPanelArraySize() - 1) )
        {
            getWizardClient().panelsChanging( iCurrPanel,
                                              iNextPanel );

            nextButton.setText( FINISH_BUTTON_TEXT );
            removeCurrentPanel();

            wcpCurrentPanel = getPanel( iNextPanel );
            addNewPanel( wcpCurrentPanel );

            iCurrentPanelNumber = iNextPanel;
            this.invalidate();
            this.repaint();
        }
        else
        /*
                    c.  Otherwise, back from the last:
                        i.   call WizardClient.panelsChanging( int iOld,
                                                               int iNew )
                        ii. remove the current panel
                        iii. add the next panel
        */
        if( iCurrPanel == (getPanelArraySize() - 1) &&
            iNextPanel < iCurrPanel )
        {
            getWizardClient().panelsChanging( iCurrPanel,
                                              iNextPanel );

            nextButton.setText( NEXT_BUTTON_TEXT );
            removeCurrentPanel();

            wcpCurrentPanel = getPanel( iNextPanel );
            addNewPanel( wcpCurrentPanel );

            iCurrentPanelNumber = iNextPanel;
            this.invalidate();
            this.repaint();
        }
        else
        /*
                    c.  Otherwise, (middle case)
                        i.   call WizardClient.panelsChanging( int iOld,
                                                               int iNew )
                        ii. remove the current panel
                        iii. add the next panel
        */
        {
            getWizardClient().panelsChanging( iCurrPanel,
                                              iNextPanel );

            nextButton.setText( NEXT_BUTTON_TEXT );
            removeCurrentPanel();

            wcpCurrentPanel = getPanel( iNextPanel );
            addNewPanel( wcpCurrentPanel );
            this.invalidate();
            this.repaint();
            iCurrentPanelNumber = iNextPanel;
        }

        // now set the enable state of the back and next buttons:
        setButtonStates( iCurrentPanelNumber );


        // Since we changed the panel, pack the owner frame or dlg:
        packMyOwner();

        // try recalcing after the pack, to give a result to use next time:
        //recalcContentPanelsPrefSize( wcpCurrentPanel.getComponent().getSize() );
        //resetContentPanelsPrefSize();



    }

    public void packMyOwner()
    {
        if ( wnOwnerWindow != null )
        {
            wnOwnerWindow.pack();

        }
    }

    public void disposeOfMyOwner()
    {

        if ( wnOwnerWindow != null )
        {
            wnOwnerWindow.dispose();
        }
    }
    /**
     * Set the enable state of the Next and Back buttons based
     *  on the value of current and next panel numbers.
     */
    public void setButtonStates( int iCurrPanel )
    {

        if( iCurrPanel == 0 )
        {
            setBackButtonState( false );
            setNextButtonState( wcpCurrentPanel.isNextButtonEnablable() );
        }
        else
        /*
                    b.  If iNextPanel is THE LAST PANEL, we are at end, so:
        */

        if( iCurrPanel == (getPanelArraySize() - 1) )
        {
            setBackButtonState( true );
            setNextButtonState( true );
        }
        else
        /*
                    d.  Otherwise, (middle case)
        */
        {
            setBackButtonState( true );
            setNextButtonState( wcpCurrentPanel.isNextButtonEnablable() );
        }

    }


    /**
     * when the file selector returns null, this frame won't show.
     */
    public boolean getShowFlag() {
        return showFlag;
    }


    /**
     * Go back to the previous panel.
     * The button panel alse changes when necessary.
     */
    private void backClicked()
    {

        processChange( iCurrentPanelNumber,
                       Math.max( iCurrentPanelNumber - 1, 0 ) );


    }

    /**
     * Go to the next panel in logical order.
     * The button panel also changes correspondingly.
     */
    private void nextClicked()
    {
        if ( nextButton.getText().equals( FINISH_BUTTON_TEXT ) )
        {
            wzcClient.finishClicked();
            disposeOfMyOwner();
        }
        else
        {
            processChange( iCurrentPanelNumber,
                           iCurrentPanelNumber + 1 );
        }
    }

    /**
     *
     */
    private void cancelClicked()
    {
        wzcClient.cancelClicked();
        disposeOfMyOwner();
    }

    // =============
    //  Method(s) for interface: ChangeListener
    // =============

    public void stateChanged( ChangeEvent e )
    {
        // called when the state of the current WizardClientPanel
        //  changes.

        if ( e.getSource() instanceof WizardClientPanel )
        {
            setNextButtonState( ((WizardClientPanel)e.getSource())
                                                        .isNextButtonEnablable() );
        }
        else
        if ( e.getSource() instanceof AbstractWizardClient )
        {
            setNextButtonState( ((WizardClientPanel)e.getSource())
                                                        .isNextButtonEnablable() );
        }


    }


    // NOT SURE IF WE NEED ANY OF THIS...MAY BE NEEDED IF WE ACTUALLY
    //  REMOVE AND READD A BUTTON...
    /**
     * Change from nextButton to importButton in the pnlButtons.
     */
     /*
    private void changeToImportButton() {
        pnlButtons.remove(nextButton);
        pnlButtons.add(importButton);
        ((GridBagLayout)pnlButtons.getLayout()).setConstraints(importButton, new GridBagConstraints(
        1, 0, 1, 1, 0.1, 0.1, GridBagConstraints.CENTER, GridBagConstraints.NONE,
        new Insets(0, 5, 0, 5), 0, 0));
    }
    */
    /**
     * Change from importButton to nextButton in the pnlButtons.
     */
/*
    private void changeToNextButton() {
        pnlButtons.remove(importButton);
        pnlButtons.add(nextButton);
        ((GridBagLayout)pnlButtons.getLayout()).setConstraints(nextButton, new GridBagConstraints(
        1, 0, 1, 1, 0.1, 0.1, GridBagConstraints.CENTER, GridBagConstraints.NONE,
        new Insets(0, 5, 0, 5), 0, 0));
    }
*/






}
