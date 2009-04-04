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

package com.metamatrix.toolbox.ui.widget;

import java.awt.Dimension;
import java.awt.GridBagLayout;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.awt.event.MouseAdapter;
import java.awt.event.MouseEvent;

import javax.swing.ButtonGroup;
import javax.swing.Icon;
import javax.swing.ImageIcon;
import javax.swing.JPanel;
import javax.swing.JPopupMenu;
import javax.swing.JRadioButtonMenuItem;

import com.metamatrix.toolbox.ui.UIDefaults;

/**
 * ComboButtonWidget is a JPanel containing two buttons butted up against each other which act together.
 * The right button (popupListButton) pops up a list of JRadioButtonMenu items which the user can select.
 * The left button (cycleButton) cycles thru the JRadioButtonMenuItems without displaying the choices
 * to the user.
 * To add listeners to the ComboButtonWidget, perform a getChoiceMenuItem() and add the listener to the
 * returned JRadioButtonMenuItem.  Hitting the cycle button will fire the event associated with the
 * appropriate JRadioButtonMenuItem.
 */
public class ComboButtonWidget extends JPanel {

    private ButtonWidget popupListButton, cycleButton;
    private JPopupMenu displayListPopupMenu;
    private Icon detailsOnIcon;
    private Icon cycleButtonIcon;
//    private Icon littleDownArrowIcon;
    private Icon popupListButtonIcon;
    private String[] choicesList;
    private String popupListButtonToolTipText = "popupListButton ToolTip text";
    private String cycleButtonToolTipText = "cycleButton ToolTip text";
    private JRadioButtonMenuItem[] choicesMenuItemArray;

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    Creates a ComboButtonWidget with the specified choices.
    @param choices The menu choices
    @since Golden Gate
    */
    public ComboButtonWidget(String[] choices) {
        super(new GridBagLayout());
        choicesList = choices;
        initializeComboButtonWidget();
        this.setMaximumSize(this.getPreferredSize());
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since Golden Gate
    */
    private void initializeComboButtonWidget() {

//        final ImageIcon cycleButtonIcon = new ImageIcon(ClassLoader.getSystemResource("com/metamatrix/toolbox/ui/widget/images/details_on.gif"));
        final ImageIcon popupListButtonIcon = new ImageIcon(ClassLoader.getSystemResource("com/metamatrix/toolbox/ui/widget/images/little_down_arrow.gif"));
//        final Icon cycleButtonIcon = ToolboxStandards.getIcon("FileChooser.detailsViewIcon");
        final Icon cycleButtonIcon = UIDefaults.getInstance().getIcon("FileChooser.listViewIcon");
//        final Icon popupListButtonIcon = ToolboxStandards.getIcon("FileChooser.upFolderIcon");

        cycleButton = new ButtonWidget(detailsOnIcon);
        cycleButton.setToolTipText(cycleButtonToolTipText);
        cycleButton.setBorderPainted(false);
        cycleButton.setIcon(cycleButtonIcon);
        cycleButton.setFocusPainted(false);
        cycleButton.addMouseListener(new MouseAdapter() {
            public void mouseEntered(MouseEvent e) {
                cycleButton.setBorderPainted(true);
                popupListButton.setBorderPainted(true);
            }
            public void mouseExited(MouseEvent e) {
                cycleButton.setBorderPainted(false);
                popupListButton.setBorderPainted(false);
            }
        });

        popupListButton = new ButtonWidget(detailsOnIcon);
        popupListButton.setToolTipText(popupListButtonToolTipText);
        popupListButton.setBorderPainted(false);
        popupListButton.setIcon(popupListButtonIcon);
        popupListButton.setFocusPainted(false);
        
        //make the height of the popupListButton match the height of the cycleButton
        int cycleButtonHeight = cycleButton.getPreferredSize().height;
        int popupListButtonWidth = popupListButton.getPreferredSize().width;
        popupListButton.setPreferredSize(new Dimension(popupListButtonWidth, cycleButtonHeight));
        
        popupListButton.addMouseListener(new MouseAdapter() {
            public void mouseEntered(MouseEvent e) {
                cycleButton.setBorderPainted(true);
                popupListButton.setBorderPainted(true);
            }
            public void mouseExited(MouseEvent e) {
                cycleButton.setBorderPainted(false);
                popupListButton.setBorderPainted(false);
            }
            public void mousePressed (MouseEvent e) {
                displayListPopupMenu.show(e.getComponent(), e.getX()-60, e.getY()+20);
            }
            public void mouseReleased (MouseEvent e) {
                displayListPopupMenu.show(e.getComponent(), e.getX()-60, e.getY()+20);
            }
        });


        choicesMenuItemArray = new JRadioButtonMenuItem[choicesList.length];
        for (int i=0; i<choicesList.length; i++){
            choicesMenuItemArray[i] = new JRadioButtonMenuItem(choicesList[i]);
            final JRadioButtonMenuItem choicesMenuItem = choicesMenuItemArray[i];

            choicesMenuItem.addActionListener(new ActionListener(){
                public void actionPerformed(ActionEvent e){
                    choicesMenuItem.setSelected(true);
                }
            });

        }

        ButtonGroup choiceMenuItemButtonGroup = new ButtonGroup();

        displayListPopupMenu = new JPopupMenu();

        for (int j=0; j<choicesList.length; j++){
            choiceMenuItemButtonGroup.add(choicesMenuItemArray[j]);
            displayListPopupMenu.add(choicesMenuItemArray[j]);
        }


        addMouseListener (new MouseAdapter() {
        public void mousePressed (MouseEvent e) {
            if (e.isPopupTrigger()) {
                displayListPopupMenu.show (e.getComponent(),
                e.getX(), e.getY());
            }
        }
        public void mouseReleased (MouseEvent e) {
            if (e.isPopupTrigger()) {
                displayListPopupMenu.show (e.getComponent(),
                e.getX(), e.getY());
            }
        }
        });

        add(cycleButton);
        add(popupListButton);

        cycleButton.addActionListener(new ActionListener(){
            public void actionPerformed(ActionEvent e){
                    repaint();
                    setVisible(true);
                    int index = -1;
                    for(int i=0; i<choicesMenuItemArray.length; i++){
                        if (choicesMenuItemArray[i].isSelected()){
                            index = i;
                        }
                    }
                    if(index+1 < choicesMenuItemArray.length){
                        choicesMenuItemArray[index+1].doClick();
                    } else {
                        choicesMenuItemArray[0].doClick();
                    }
            }
        });
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @return The desired menu item by the text it was created with
    @since Golden Gate
    */
    public JRadioButtonMenuItem getChoiceMenuItem(String choice){
        for (int i=0; i<choicesMenuItemArray.length; i++) {
            if (choicesMenuItemArray[i].getText().equals(choice)) {
                return choicesMenuItemArray[i];
            }
        }
        return null;
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since Golden Gate
    */
    public void setCycleButtonIcon(Icon image){
        cycleButtonIcon = image;
        cycleButton.setIcon(cycleButtonIcon);
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since Golden Gate
    */
    public void setCycleButtonText(String text){
        cycleButton.setText(text);
        this.setMaximumSize(this.getPreferredSize());
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since Golden Gate
    */
    public void setCycleButtonToolTipText(String toolTipText){
        cycleButtonToolTipText = toolTipText;
        cycleButton.setToolTipText(cycleButtonToolTipText);
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since Golden Gate
    */
    public void setPopupListButtonIcon(ImageIcon image){
        popupListButtonIcon = image;
        popupListButton.setIcon(popupListButtonIcon);
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since Golden Gate
    */
    public void setPopupListButtonText(String text){
        popupListButton.setText(text);
        this.setMaximumSize(this.getPreferredSize());
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since Golden Gate
    */
    public void setPopupListButtonToolTipText(String toolTipText){
        popupListButtonToolTipText = toolTipText;
        popupListButton.setToolTipText(popupListButtonToolTipText);
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since Golden Gate
    */
    public void setSelectedChoice(int index){
        choicesMenuItemArray[index].setSelected(true);
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since Golden Gate
    */
    public void setSelectedChoice(String choice){
        for (int i=0; i<choicesMenuItemArray.length; i++) {
            if (choicesMenuItemArray[i].getText().equals(choice)) {
                setSelectedChoice(i);
            }
        }
    }

}
