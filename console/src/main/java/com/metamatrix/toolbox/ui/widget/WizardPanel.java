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

//################################################################################################################################
package com.metamatrix.toolbox.ui.widget;

// System imports
import java.awt.Component;
import java.awt.Dimension;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import javax.swing.JComponent;

import com.metamatrix.toolbox.ui.widget.util.WidgetUtilities;

/**
This class is intended to be used everywhere within the application that a wizard panel needs to be displayed.  It provides the
following features:
<ul>
<li>A navigation button bar at the bottom of the dialog</li>
<li>A set of methods to work with each button</li>
<li>A default set of "back", "next", "finish", and "cancel" buttons within the bar</li>
<li>A default implementation for the functions of the back and next buttons</li>
</ul> .
The back button will not appear when the first page is displayed, and the next button will be replaced by a finish button when
the last page is displayed.
@since 2.0
@author John P. A. Verhaeg
@version 2.0
*/
public class WizardPanel extends DialogPanel {
    //############################################################################################################################
    //# Instance Variables                                                                                                       #
    //############################################################################################################################

    protected Collection pages = null;

    private ButtonWidget backButton = null;
    private ButtonWidget nextButton = null;
    private ButtonWidget finishButton = null;

    private int pgNdx = 0;  // The index of the current page, 0 = first page

    private Dimension prefSize;

    //############################################################################################################################
    //# Constructors                                                                                                             #
    //############################################################################################################################

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    Creates a panel with a BorderLayout and a navigation bar in the SOUTH region.
    @since 2.0
    */
    public WizardPanel() {
        this((Collection)null);
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    Creates a panel with a BorderLayout, a navigation bar in the SOUTH region, and the specified initial page in the CENTER
    region.
    @param page The initial page to be displayed.
    @since 2.0
    */
    public WizardPanel(final Component page) {
        this(new ArrayList(Collections.singleton(page)));
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    Creates a panel with a BorderLayout, a navigation bar in the SOUTH region, and the specified ordered list of pages,
    initially displaying the first in the CENTER region.  The pages will be traversed in order as the next button is activated.
    @param pages The pages to be displayed..
    @since 2.0
    */
    public WizardPanel(final Collection pages) {
        this.pages = pages;
        initializeWizardPanel();
    }

    //############################################################################################################################
    //# Instance Methods                                                                                                         #
    //############################################################################################################################

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    Adds the specified page to the end of the current list of pages.
    @param page The page to add
    @since 2.0
    */
    public int addPage(final Component page) {
        final int ndx = getPageCount();
        addPage(page, ndx);
        return ndx;
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    Adds the specified page to the current list of pages at the specified index.
    @param page   The page to add
    @param index  The index at which to insert the page
    @since 2.0
    */
    public void addPage(final Component page, final int index) {
        if (pages == null) {
            pages = new ArrayList();
        }
        if (pages.size() == 1) {
            addNavigationButton(backButton, getNavigationComponentIndex(finishButton));
        }
        ((List)pages).add(index, page);
        // Set panel's preferred size to maximum height and width of pages
        final Dimension size = page.getPreferredSize();
        prefSize.width = Math.max(prefSize.width, size.width);
        prefSize.height = Math.max(prefSize.height, size.height);
        // Set content to prototype component to determine panel's preferred size
        final Component currentPg = getContent();
        super.setContent(new JComponent() {
            public Dimension getPreferredSize() {
                return prefSize;
            }
        });
        setPreferredSize(null);
        setPreferredSize(getPreferredSize());
        if (index == pgNdx) {
            // Show the added page if its index is the current index
            showPage(index);
        } else {
            // Replace content with "real" page
            super.setContent(currentPg);
        }
    }
    
    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since 2.0
    */
    protected ButtonWidget createAcceptButton() {
        return WidgetFactory.createButton(FINISH_BUTTON);
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    Creates a back button with the following features:
    <ul>
    <li>A default label (as determined by the ToolboxStandards class)</li>
    <li>A default ActionListener that calls {@link #showPreviousPage}:</li>
    </ul>
    @return The back button
    @since 2.0
    */
    protected ButtonWidget createBackButton() {
        final ButtonWidget button = WidgetFactory.createButton(BACK_BUTTON);
        button.addActionListener(new ActionListener() {
            public void actionPerformed(final ActionEvent event) {
                showPreviousPage();
            }
        });
        return button;
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    Creates a next button with the following features:
    <ul>
    <li>A default label (as determined by the ToolboxStandards class)</li>
    <li>A default ActionListener that calls {@link #showNextPage}</li>
    </ul>
    @return The next button
    @since 2.0
    */
    protected ButtonWidget createNextButton() {
        final ButtonWidget button = WidgetFactory.createButton(NEXT_BUTTON);
        button.addActionListener(new ActionListener() {
            public void actionPerformed(final ActionEvent event) {
                showNextPage();
            }
        });
        return button;
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @return The back button
    @since 2.0
    */
    public ButtonWidget getBackButton() {
        return backButton;
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    Convenience method for {@link #getContent}.
    @return The currently displayed page
    @since 2.0
    */
    public final Component getCurrentPage() {
        return getContent();
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @return The index of the currently displayed page
    @since 2.0
    */
    public int getCurrentPageIndex() {
        return pgNdx;
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    Convenience method for {@link #getAcceptButton}.
    @return The finish button
    @since 2.0
    */
    public final ButtonWidget getFinishButton() {
        return getAcceptButton();
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @return The next button
    @since 2.0
    */
    public final ButtonWidget getNextButton() {
        return nextButton;
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since 2.0
    */
    public int getPageCount() {
        if (pages == null) {
            return 0;
        }
        return pages.size();
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since 2.0
    */
    protected void initializeWizardPanel() {
        finishButton = getAcceptButton();
        backButton = createBackButton();
        nextButton = createNextButton();    // Created here only to equalize button sizes
        WidgetUtilities.equalizeSizeConstraints(Arrays.asList(new ButtonWidget[] {backButton, nextButton, finishButton,
                                                getCancelButton()}));
        prefSize = new Dimension();
        if (pages != null) {
            pages = new ArrayList(pages);
            if (pages.size() > 1) {
                addNavigationButton(backButton, getNavigationComponentIndex(finishButton));
                // Set panel's preferred size to maximum height and width of pages
                final Iterator iterator = pages.iterator();
                Dimension size;
                while (iterator.hasNext()) {
                    size = ((Component)iterator.next()).getPreferredSize();
                    prefSize.width = Math.max(prefSize.width, size.width);
                    prefSize.height = Math.max(prefSize.height, size.height);
                }
                // Set content to prototype component to determine panel's preferred size
                super.setContent(new JComponent() {
                    public Dimension getPreferredSize() {
                        return prefSize;
                    }
                });
                setPreferredSize(getPreferredSize());
            }
            super.setContent((Component)((List)pages).get(0));
        }
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    Remove all internal references to the specified component being removed from the navigation bar.
    @param component The component being removed (Must not be null)
    @since 2.0
    *//*
    protected void removeInternalReferences(final Component component) {
        super.removeInternalReferences(component);
        if (component == backButton) {
            backButton = null;
        } else if (component == nextButton) {
            nextButton = null;
        }
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    Removes the specified page from the current list of pages.
    @param page The page to remove
    @since 2.0
    */
    public int removePage(final Component page) {
        final int ndx = ((List)pages).indexOf(page);
        removePage(page, ndx);
        return ndx;
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    Removes the page at the specified index from the current list of pages.
    @param index The index of the page to remove
    @since 2.0
    */
    public Component removePage(final int index) {
        final Component pg = (Component)((List)pages).get(index);
        removePage(pg, index);
        return pg;
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since 2.0
    */
    protected void removePage(final Component page, final int index) {
        ((List)pages).remove(page);
        final int pgCount = pages.size();
        if (pgCount == 0) {
            pages = null;
        } else if (pgCount == 1) {
            removeNavigationButton(backButton);
            addAcceptButton(finishButton, removeNavigationButton(nextButton));
        }
        if (index == pgNdx) {
            if (index == pgCount) {
                --pgNdx;
            }
            showPage(pgNdx);
        }
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    Displays the next page.
    @since 2.0
    */
    public void showNextPage() {
        if (pgNdx < getPageCount() - 1) {
            showPage(pgNdx + 1);
        }
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    Performs the following tasks:
    <ul>
    <li>Displays the page at the specified index (i.e., Sets the content to that page)</li>
    <li>Add a back button to the navigation bar if not present and the page is not the first</li>
    <li>Add a next button to the navigation bar if not present and the page is not the last</li>
    <li>Add a finish button to the navigation bar if not present and the page is the last</li>
    <li>Disables the back button if present and the page is the first</li>
    <li>Remove the next button if present and the page is the last</li>
    <li>Remove the finish button if present and the page is not the last</li>
    </ul>
    @param index The index of the page to be displayed
    @since 2.0
    */
    public void showPage(final int index) {
        showPage((Component)((List)pages).get(index), index);
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    Performs the following tasks:
    <ul>
    <li>Displays the specified page (i.e., Sets the content to that page)</li>
    <li>Add a back button to the navigation bar if not present and the page is not the first</li>
    <li>Add a next button to the navigation bar if not present and the page is not the last</li>
    <li>Add a finish button to the navigation bar if not present and the page is the last</li>
    <li>Disables the back button if present and the page is the first</li>
    <li>Remove the next button if present and the page is the last</li>
    <li>Remove the finish button if present and the page is not the last</li>
    </ul>
    @param page The page to be displayed
    @since 2.0
    */
    public void showPage(final Component page) {
        final Iterator iterator = pages.iterator();
        for (int ndx = 0;  iterator.hasNext();  ++ndx) {
            if (iterator.next().equals(page)){
                showPage(page, ndx);
                return;
            }
        }
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since 2.0
    */
    protected void showPage(final Component page, final int index) {
        super.setContent(page);
        pgNdx = index;
        if (getWindowAncestor() != null) {
            updateNavigationButtons();
            revalidate();
            repaint();
        }
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    Displays the previous page.
    @since 2.0
    */
    public void showPreviousPage() {
        if (pgNdx > 0) {
            showPage(pgNdx - 1);
        }
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    Performs logic tests to determine the enable/disable status of the Back/Next buttons
    @since 2.0
    */
    protected void updateNavigationButtons() {
        backButton.setEnabled(pgNdx > 0);
        final int pgCount = getPageCount();
        if (pgCount > 1) {
            if (pgNdx < pgCount - 1) {
                if (finishButton.getParent() != null) {
/*                    
                    nextButton = createNextButton();
                    final Dimension size = finishButton.getPreferredSize();
                    nextButton.setMinimumSize(size);
                    nextButton.setPreferredSize(size);
                    nextButton.setMaximumSize(size);
*/                    
                    addNavigationButton(nextButton, removeNavigationButton(finishButton));
                }
            } else if (nextButton.getParent() != null) {
                addAcceptButton(finishButton, removeNavigationButton(nextButton));
//                nextButton = null;
            }
        }
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since 2.0
    */
    public void windowAdded() {
        updateNavigationButtons();
    }
}
