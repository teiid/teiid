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

//################################################################################################################################
package com.metamatrix.toolbox.ui.widget;

// System imports
import java.awt.Dimension;
import java.awt.Font;
import java.awt.Insets;
import java.awt.Rectangle;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.awt.event.FocusEvent;
import java.awt.event.FocusListener;
import java.awt.event.KeyEvent;
import java.awt.event.MouseAdapter;
import java.awt.event.MouseEvent;
import java.awt.event.MouseListener;
import java.beans.PropertyChangeEvent;
import java.beans.PropertyChangeListener;
import java.text.ParseException;

import javax.swing.Action;
import javax.swing.BoundedRangeModel;
import javax.swing.JPopupMenu;
import javax.swing.JTextField;
import javax.swing.JToolTip;
import javax.swing.KeyStroke;
import javax.swing.SwingUtilities;
import javax.swing.text.Document;

import com.metamatrix.toolbox.ui.UIDefaults;
import com.metamatrix.toolbox.ui.widget.text.DefaultTextFieldModel;
import com.metamatrix.toolbox.ui.widget.text.TextConstants;

/**
This class is intended to be used everywhere within the application that a single-line text Textor (similar to JTextField)
needs to be displayed.  It provides the following features:
<ul>
<li>Character input validation</li>
<li>Field validation when focus is lost or the [Enter] key is hit</li>
<li>An indicator of whether the field has been modified</li>
<li>Tooltips for clipped text or error messages</li>
<li>A pop-up context menu activated by a right mouse click and containing common features such as cut/copy/paste</li>
</ul>
@since 2.0
@author John P. A. Verhaeg
@version 2.0
*/
public class TextFieldWidget extends JTextField 
implements TextConstants {
    //############################################################################################################################
    //# Constants                                                                                                                #
    //############################################################################################################################

    public static final String PROPERTY_PREFIX = "TextField.";
    public static final String BACKGROUND_COLOR_PROPERTY         = PROPERTY_PREFIX + "background";
    public static final String FOREGROUND_COLOR_PROPERTY         = PROPERTY_PREFIX + "foreground";
    public static final String INVALID_BACKGROUND_COLOR_PROPERTY = PROPERTY_PREFIX + "invalidBackgroundColor";
    public static final String INVALID_FOREGROUND_COLOR_PROPERTY = PROPERTY_PREFIX + "invalidForegroundColor";
    public static final String COLUMN_WIDTH_CHARACTER_PROPERTY   = PROPERTY_PREFIX + "columnWidthCharacter";
    
    //############################################################################################################################
    //# Instance Variables                                                                                                       #
    //############################################################################################################################
    
    private char colWthChr;
    private int colWth;
    private boolean isClipTipEnabled;
    private boolean isErrorTipEnabled;
    private JPopupMenu menu;
    private MouseListener menuCtrlr;
    private int maxLen;
    
    //############################################################################################################################
    //# Constructors                                                                                                             #
    //############################################################################################################################

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    Creates a blank field with no constraints.
    @since 2.0
    */
    public TextFieldWidget() {
        this(null, 0);
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    Creates a field populated with the specified text, and wide enough to display the text.
    @param text The initial text
    @since 2.0
    */
    public TextFieldWidget(final String text) {
        this(text, text.length());
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    Creates a blank field wide enough to display the specified number of characters.
    @param characters The width of the field
    @since 2.0
    */
    public TextFieldWidget(final int characters) {
        this(null, characters);
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    Creates a field populated with the specified text, and wide enough to display the specified number of characters.
    @param text The initial text
    @param characters The width of the field
    @since 2.0
    */
    public TextFieldWidget(final String text, final int characters) {
        super(text, characters);
        initializeTextFieldWidget();
    }

    //############################################################################################################################
    //# Instance Methods                                                                                                         #
    //############################################################################################################################

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    Builds a context menu containing a default set of options to cut, copy, and paste.
    @since 2.0
    */
    protected void buildContextMenu() {
        menu = new JPopupMenu();
        final Action[] actions = this.getActions();
        menu.add(getAction(actions, "cut"));
        menu.add(getAction(actions, "copy"));
        menu.add(getAction(actions, "paste"));
    }
    
    // Case 4294 ("paste from clipboard" on MM console does not enable "Apply" button)
    // Overrode the cut and paste methods - the expected event was not getting fired
    public void paste() {
    	super.paste();
    	fireActionPerformed();
    }
    
    public void cut() {
    	super.cut();
    	fireActionPerformed();
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    Commits any new text set on the model.
    @return True if text is valid and successfully committed
    @since 2.0
    */
    public boolean commit() {
        final Document doc = getDocument();
        if (doc instanceof DefaultTextFieldModel) {
            final DefaultTextFieldModel model = (DefaultTextFieldModel)doc;
            if (!model.isValid()) {
                return false;
            }
            model.commit();
        }
        return true;
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    Overridden to create a DefaultTextFieldModel instead of the default model created by JTextField.  Called by JTextField's
    constructor.
    @return An instance of DefaultTextFieldModel
    @since 2.0
    */
    protected Document createDefaultModel() {
        return new DefaultTextFieldModel();
    }

    public JToolTip createToolTip() {
        JToolTip tip = new MultiLineToolTip();
        tip.setComponent(this);
        return tip;
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    Retrieves an Action with the specified name from the specified list of Actions.
    @param actions    A list of Actions to search
    @param name       The name of the Action for which to search
    @return The Action if found, null otherwise
    @since 2.0
    */
    private Action getAction(final Action[] actions, final String name) {
        Action action = null;
        for (int ndx = actions.length;  --ndx >= 0;) {
            action = actions[ndx];
            if (((String)action.getValue(Action.NAME)).startsWith(name)) {
                break;
            }
        }
        return action;
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @return The width of a column as determined by the current column width character
    @since 2.0
    */
    public int getColumnWidth() {
        if (colWth == 0) {
            colWth = getFontMetrics(getFont()).stringWidth("" + colWthChr);
        }
        return colWth;
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since 2.0
    */
    public char getColumnWidthCharacter() {
        return colWthChr;
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since 2.0
    */
    public JPopupMenu getContextMenu() {
        return menu;
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    Overridden to force the field's maximum height to match its preferred height.
    @return The maximum size of the field
    @since 2.0
    */
    public Dimension getMaximumSize() {
        final Dimension size = super.getMaximumSize();
        final Dimension prefSize = getPreferredSize();
        size.height = prefSize.height;
        size.width = Math.max(size.width, prefSize.width);
        return size;
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    Overridden to force the field's minimum height to match its preferred height.
    @return The minimum size of the field
    @since 2.0
    */
    public Dimension getMinimumSize() {
        final Dimension size = super.getMinimumSize();
        size.height = getPreferredSize().height;
        return size;
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    Overridden to force the field's preferred height to be immutable.
    @return The preferred size of the field
    @since 2.0
    */
    public Dimension getPreferredSize() {
        final Dimension size = super.getPreferredSize();
        final Insets insets = getInsets();
        final int minWth = insets.left + insets.right + 2;
        final int cols = getColumns();
        if (cols > 0) {
            size.width = minWth + getColumnWidth() * cols;
        } else if (maxLen > 0) {
	        size.width = minWth + maxLen * getColumnWidth();
        } else {
            size.width = Math.max(size.width, minWth);
        }
        return size;
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    Builds a context menu containing a default set of options to cut, copy, and paste.
    @since 2.0
    */
    protected void initializeTextFieldWidget() {
        final UIDefaults dflts = UIDefaults.getInstance();
        // Make the field expandable by default
        setMaximumWidth(Short.MAX_VALUE);
        // Set the character to use to calculate the field's pixel width when width is specified in characters
        setColumnWidthCharacter(dflts.getChar(COLUMN_WIDTH_CHARACTER_PROPERTY));
        // Add listener to call any registered Validators when the [Enter] key is pressed
        addActionListener(new ActionListener() {
            public void actionPerformed(final ActionEvent event) {
                validateModel();
                commit();
                selectText();
            }
        });
        addFocusListener(new FocusListener() {
            // Add listener to select all text when the field is first entered
            public void focusGained(final FocusEvent event) {
                selectText();
            }
            // Add listener to call any registered Validators when the field loses focus
            public void focusLost(final FocusEvent event) {
                if (!event.isTemporary()) {
                    validateModel();
                    commit();
                }
            }
        });
        // Build default context menu
        setContextMenuEnabled(true);
        // Add property change listener to update clip text and colors when validity changes
        isClipTipEnabled = true;
        final Document model = getDocument();
        if (model instanceof DefaultTextFieldModel) {
            final DefaultTextFieldModel dfltModel = (DefaultTextFieldModel)model;
            dfltModel.setModified(false);
            dfltModel.addPropertyChangeListener(new PropertyChangeListener() {
                public void propertyChange(final PropertyChangeEvent event) {
                    final String prop = event.getPropertyName();
                    if (prop.equals(IS_VALID_PROPERTY)) {
                        if (!dfltModel.isValid()) {
                            // Set colors for invalid content
                            setBackground(dflts.getColor(INVALID_BACKGROUND_COLOR_PROPERTY));
                            setForeground(dflts.getColor(INVALID_FOREGROUND_COLOR_PROPERTY));
                            // Create an error tooltip if possible (& enabled)
                            if (isErrorTipEnabled) {
                                final Object validationResult = dfltModel.getValidationResult();
                                if (validationResult != null) {
                                    if (validationResult instanceof Throwable) {
                                        setToolTipText(((Throwable)validationResult).getMessage());
                                    } else {
                                        setToolTipText(validationResult.toString());
                                    }
                                }
                            }
                        } else {
                            // Set colors for valid content
                            setBackground(dflts.getColor(BACKGROUND_COLOR_PROPERTY));
                            setForeground(dflts.getColor(FOREGROUND_COLOR_PROPERTY));
                            // Re-build clip tip text if appropriate
                            setClipTipText();
                        }
                    } else if (prop.equals(MAXIMUM_LENGTH_PROPERTY)) {
                        maxLen = ((Integer)event.getNewValue()).intValue();
                        revalidate();
                    }
                }
            });
        }
        // Add ability to cancel input
        registerKeyboardAction(new ActionListener() {
            public void actionPerformed(final ActionEvent event) {
                rollback();
                validateModel();
            }
        }, KeyStroke.getKeyStroke(KeyEvent.VK_ESCAPE, 0, true), WHEN_FOCUSED);
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @return True if the clip tip feature is enabled
    @since 2.0
    */
    public boolean isClipTipEnabled() {
        return isClipTipEnabled;
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @return True if the context menu is enabled
    @since 2.0
    */
    public boolean isContextMenuEnabled() {
        return menu != null;
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @return True if the error tip feature is enabled
    @since 2.0
    */
    public boolean isErrorTipEnabled() {
        return isErrorTipEnabled;
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since 2.0
    */
    public boolean isTextValid() {
        final Document model = getDocument();
        if (model instanceof DefaultTextFieldModel) {
            return ((DefaultTextFieldModel)model).isValid();
        }
        return true;
    }
    
    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    Rollsback to the previous value in the model.
    @since 2.0
    */
    public void rollback() {
        try {
            final Document model = getDocument();
            if (model instanceof DefaultTextFieldModel) {
                ((DefaultTextFieldModel)model).rollback();
            }
            selectText();
        } catch (final Exception err) {
            // Throw BadLocationException as RuntimeException
            throw new RuntimeException(err.getMessage());
        }
    }
    
    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    Overridden to provide a tooltip containing the full text whenever the text is clipped.
    @see JTextField#scrollRectToVisible(Rectangle)
    @see #setClipTipText()
    @since 2.0
    */
    public void scrollRectToVisible(final Rectangle rectangle) {
        super.scrollRectToVisible(rectangle);
        setClipTipText();
    }
    
    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    Selects all of the current text.
    @since 2.0
    */
    public void selectText() {
        setCaretPosition(getDocument().getLength());
        moveCaretPosition(0);
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    Sets whether a tooltip will be displayed containing the full text whenever the text is clipped.
    @param isClipTipEnabled True if a tooltip should be displayed when text is clipped
    @since 2.0
    */
    public void setClipTipEnabled(final boolean isClipTipEnabled) {
        this.isClipTipEnabled = isClipTipEnabled;
    }
    
    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    Sets a tooltip containing the full text whenever the text is clipped.
    @since 2.0
    */
    protected void setClipTipText() {
        final Document model = getDocument();
        if ((isErrorTipEnabled  &&  model instanceof DefaultTextFieldModel  
            &&  (((DefaultTextFieldModel)model).getValidationResult() != null))  ||  !isClipTipEnabled) {
            return;
        }
        final BoundedRangeModel range = getHorizontalVisibility();
        if (range.getValueIsAdjusting()) {
            return;
        }
        if (range.getExtent() < range.getMaximum() - 1  ||  range.getValue() > range.getMinimum()) {
            setToolTipText(getText());
        } else {
            setToolTipText(null);
        }
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    Setting the column width to zero will cause the column width to be recalculated from the current column width character.
    @param width The preferred pixel width
    @since 2.0
    */
    public void setColumnWidth(final int width) {
        colWth = width;
        invalidate();
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    Sets the character to use when calculating pixel widths that are based upon a character count.
    @since 2.0
    */
    public void setColumnWidthCharacter(final char character) {
        colWthChr = character;
        setColumnWidth(0);
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    Sets whether a pop-up context menu will be displayed when the user right-clicks the mouse.
    @param isMenuEnabled True if a menu should be displayed
    @since 2.0
    */
    public void setContextMenuEnabled(final boolean isMenuEnabled) {
        if (isMenuEnabled) {
            // Build menu
            buildContextMenu();
            // Add listener to activate context menu
            menuCtrlr = new MouseAdapter() {
                public void mouseClicked(final MouseEvent event) {
                    // Exit if any buttons situation other than only right mouse button clicked
                    if (SwingUtilities.isLeftMouseButton(event)  ||  SwingUtilities.isMiddleMouseButton(event)
                        ||  !SwingUtilities.isRightMouseButton(event))
                        return;
                    // Select all text in field
                    setCaretPosition(getDocument().getLength());
                    moveCaretPosition(0);
                    // Display context menu
                    menu.show(TextFieldWidget.this, event.getX(), event.getY());
                }
            };
            addMouseListener(menuCtrlr);
        } else {
            menu = null;
            menuCtrlr = null;
        }
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    Sets whether a tooltip will be displayed when the text is marked as invalid.
    @param isErrorTipEnabled True if a tooltip should be displayed when text is marked as invalid
    @since 2.0
    */
    public void setErrorTipEnabled(final boolean isErrorTipEnabled) {
        this.isErrorTipEnabled = isErrorTipEnabled;
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since 2.0
    */
    public void setFont(final Font font) {
        super.setFont(font);
        colWth = 0;
    }
    
    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    A convenience method to set the list of invalid characters in the model.
    @param invalidCharacters A list of invalid characters and/or character ranges
    @throws ParseException If a syntax error was found while parsing invalidCharacters
    @see DefaultTextFieldModel#setInvalidCharacters(String)
    @since 2.0
    */
    public void setInvalidCharacters(final String invalidCharacters)
        throws ParseException {
        final Document model = getDocument();
        if (!(model instanceof DefaultTextFieldModel))
            throw new UnsupportedOperationException("Method only supported when using a DefaultTextFieldModel.");
        ((DefaultTextFieldModel)model).setInvalidCharacters(invalidCharacters);
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    Overridden to force the field's maximum height to match its preferred height.
    @see javax.swing.JComponent#setMaximumSize(Dimension)
    @since 2.0
    */
    public void setMaximumSize(final Dimension size) {
        super.setMaximumSize(new Dimension(size.width, getPreferredSize().height));
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    Sets the maximum pixel width of the field.
    @param width The maximum pixel width
    @since 2.0
    */
    public void setMaximumWidth(final int width) {
        setMaximumSize(new Dimension(width, getPreferredSize().height));
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    Overridden to force the field's minimum height to match its preferred height.
    @see javax.swing.JComponent#setMinimumSize(Dimension)
    @since 2.0
    */
    public void setMinimumSize(final Dimension size) {
        super.setMinimumSize(new Dimension(size.width, getPreferredSize().height));
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    Sets the minimum pixel width of the field.
    @param width The minimum pixel width
    @since 2.0
    */
    public void setMinimumWidth(final int width) {
        setMinimumSize(new Dimension(width, getPreferredSize().height));
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    Overridden to force the field's preferred height to be immutable.
    @see javax.swing.JComponent#setPreferredSize(Dimension)
    @since 2.0
    */
    public void setPreferredSize(final Dimension size) {
        super.setPreferredSize(new Dimension(size.width, getPreferredSize().height));
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    Sets the preferred pixel width of the field.
    @param width The preferred pixel width
    @since 2.0
    */
    public void setPreferredWidth(final int width) {
        setPreferredSize(new Dimension(width, getPreferredSize().height));
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since 2.0
    */
    public void setText(final String text) {
        super.setText(text);
        isTextValid();
    }
    
    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    A convenience method to set the list of valid characters in the model.
    @param validCharacters A list of valid characters and/or character ranges
    @throws ParseException If a syntax error was found while parsing validCharacters
    @see DefaultTextFieldModel#setValidCharacters(String)
    @since 2.0
    */
    public void setValidCharacters(final String validCharacters)
        throws ParseException {
        final Document model = getDocument();
        if (!(model instanceof DefaultTextFieldModel))
            throw new UnsupportedOperationException("Method only supported when using a DefaultTextFieldModel.");
        ((DefaultTextFieldModel)model).setValidCharacters(validCharacters);
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    @since 2.0
    */
    public String toString() {
        return getText();
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
    Determines if the model is valid.  Changes the field's colors to white on red by default and creates an error tooltip if
    not.
    @since 2.0
    */
    protected void validateModel() {
        final Document model = getDocument();
        if (model instanceof DefaultTextFieldModel)
            ((DefaultTextFieldModel)model).isValid();
    }
}
