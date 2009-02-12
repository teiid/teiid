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

import java.awt.Color;
import java.awt.Dimension;
import java.awt.Font;
import java.awt.FontMetrics;
import java.util.StringTokenizer;

import javax.swing.BorderFactory;
import javax.swing.BoxLayout;
import javax.swing.Icon;
import javax.swing.JComponent;
import javax.swing.JLabel;
import javax.swing.JPanel;

import com.metamatrix.common.properties.TextManager;
import com.metamatrix.common.util.ApplicationInfo;
import com.metamatrix.core.util.Assertion;
import com.metamatrix.toolbox.ui.UIDefaults;

/**
 * @since 2.0
 */
public class SplashPanel extends JPanel {
    //############################################################################################################################
    //# Constants                                                                                                                #
    //############################################################################################################################
    
    private static final String BANNER_PROPERTY = "Icon.splashBanner";
    private static final String IMAGE_PROPERTY  = "Icon.splashImage";
    
    public static final String PROPERTY_PREFIX = "SplashPanel.";
    public static final String BACKGROUND_COLOR_PROPERTY            = PROPERTY_PREFIX + "background";
    public static final String FOREGROUND_COLOR_PROPERTY            = PROPERTY_PREFIX + "foreground";
    public static final String NAME_FONT_PROPERTY                   = PROPERTY_PREFIX + "nameFont";
    public static final String LEGAL_TEXT_BACKGROUND_COLOR_PROPERTY = PROPERTY_PREFIX + "legalTextBackground";
    public static final String LEGAL_TEXT_FOREGROUND_COLOR_PROPERTY = PROPERTY_PREFIX + "legalTextForeground";
    public static final String COPYRIGHT_PROPERTY                   = PROPERTY_PREFIX + "copyright";
    public static final String TRADEMARK_PROPERTY                   = PROPERTY_PREFIX + "trademark";
    public static final String PENALTY_NOTICE_PROPERTY              = PROPERTY_PREFIX + "penaltyNotice";
    public static final String APPLICATION_NAME_PROPERTY            = PROPERTY_PREFIX + "applicationName";
    public static final String VERSION_NUMBER_PROPERTY              = PROPERTY_PREFIX + "versionNumber";
    public static final String BUILD_NUMBER_PROPERTY                = PROPERTY_PREFIX + "buildNumber";
    public static final String NULL_MESSAGE_PROPERTY                = PROPERTY_PREFIX + "nullMessage";
    public static final String VERSION_PROPERTY                     = PROPERTY_PREFIX + "version";
    public static final String LICENSE_PROPERTY                     = PROPERTY_PREFIX + "license";

    //############################################################################################################################
    //# Variables                                                                                                                #
    //############################################################################################################################
    
    private int wth;
        
    //############################################################################################################################
    //# Constructors                                                                                                             #
    //############################################################################################################################
    
    /**
     * @since 2.0
     */
    public SplashPanel() {
        super(null);
        initializeSplashPanel();
    }
    
    //############################################################################################################################
    //# Methods                                                                                                                  #
    //############################################################################################################################

    /**
     * @since 2.0
     */
    protected void addLabel(final JPanel panel, final String text, final Color color, final Font font) {
        final JLabel label = new JLabel(text, JLabel.CENTER);
        label.setForeground(color);
        label.setFont(font);
        label.setMaximumSize(new Dimension(Short.MAX_VALUE, label.getPreferredSize().height));
        panel.add(label);
    }

    /**
     * @since 2.0
     */
    protected JPanel addLabels(final JPanel panel, final String text, final Color foregroundColor, final Font font,
                               final int margin) {
        return addLabels(panel, text, panel.getBackground(), foregroundColor, font, margin);
    }

    /**
     * @since 2.0
     */
    protected JPanel addLabels(final JPanel panel, final String text, final Color backgroundColor, final Color foregroundColor,
                               final Font font, final int margin) {
        final JPanel subPanel = new JPanel(null);
        subPanel.setLayout(new BoxLayout(subPanel, BoxLayout.Y_AXIS));
        subPanel.setBackground(backgroundColor);
        subPanel.setBorder(BorderFactory.createEmptyBorder(margin, 0, 0, 0));
        if (text != null) {
            if (text.trim().length() == 0) {
                addLabel(subPanel, text, foregroundColor, font);
            } else {
                final FontMetrics metrics = getFontMetrics(font);
                final StringTokenizer tokens = new StringTokenizer(text, " \n", true);
                String line, newLine, token;
                boolean fits;
                if (tokens.hasMoreTokens()) {
                    token = tokens.nextToken();
                    do {
                        line = token;
                        fits = (metrics.stringWidth(line) < wth);
                        while (tokens.hasMoreTokens()  &&  fits) {
                            token = tokens.nextToken();
                            if (token.equals(" ")) {
                                continue;
                            }
                            if (token.equals("\n")) {
                                fits = false;
                                if (tokens.hasMoreTokens()) {
                                    token = tokens.nextToken();
                                }
                            } else {
                                newLine = line + ' ' + token;
                                fits = (metrics.stringWidth(newLine) < wth);
                                if (fits) {
                                    line = newLine;
                                }
                            }
                        }
                        addLabel(subPanel, line, foregroundColor, font);
                    } while (tokens.hasMoreTokens());
                }
            }
        }
        panel.add(subPanel);
        return subPanel;
    }

    /**
     * @since 2.0
     */
    public Dimension getPreferredSize() {
        final Dimension size = super.getPreferredSize();
        size.width = wth;
        return size;
    }
    
    /**
     * @since 2.0
     */
    protected void initializeSplashPanel() {
        final TextManager textMgr = TextManager.INSTANCE;
        final UIDefaults dflts = UIDefaults.getInstance();
        setLayout(new BoxLayout(this, BoxLayout.Y_AXIS));
        setBackground(dflts.getColor(BACKGROUND_COLOR_PROPERTY));
        final Icon banner = dflts.getIcon(BANNER_PROPERTY);
        wth = banner.getIconWidth();
        add(new JLabel(banner));
        final Font font = dflts.getFont("normalFont"); //$NON-NLS-1$
        int margin = getFontMetrics(font).getHeight();
        String name = "Console"; //$NON-NLS-1$
        ApplicationInfo build = ApplicationInfo.getInstance();
        validateProperty(name, APPLICATION_NAME_PROPERTY); 
        validateProperty(build.getReleaseNumber(), VERSION_NUMBER_PROPERTY);
        validateProperty(build.getBuildNumber(), BUILD_NUMBER_PROPERTY);
        validateProperty(build.getCopyright(), "Copyright"); //$NON-NLS-1$
        JPanel panel = new JPanel(null);
        panel.setLayout(new BoxLayout(panel, BoxLayout.Y_AXIS));
        panel.setBackground(getBackground());
        panel.setBorder(BorderFactory.createEmptyBorder(margin, 0, margin, 0));
        Color color = dflts.getColor(FOREGROUND_COLOR_PROPERTY);
        addLabels(panel, name, color, dflts.getFont(NAME_FONT_PROPERTY), 0);
        addLabels(panel, textMgr.getText(VERSION_PROPERTY, build.getReleaseNumber(), build), color, font, 0);

        add(panel);
        add(new JLabel(dflts.getIcon(IMAGE_PROPERTY)));
        panel = new JPanel(null);
        panel.setLayout(new BoxLayout(panel, BoxLayout.Y_AXIS));
        panel.setBackground(dflts.getColor(LEGAL_TEXT_BACKGROUND_COLOR_PROPERTY));
        color = dflts.getColor(LEGAL_TEXT_FOREGROUND_COLOR_PROPERTY);
        margin /= 2;
        panel.setBorder(BorderFactory.createEmptyBorder(margin, 0, margin, 0));
        addLabels(panel, textMgr.getText(COPYRIGHT_PROPERTY, build.getCopyright()), color,
                  font.deriveFont((float)font.getSize() - 1), 0);
        addLabels(panel, textMgr.translate(TRADEMARK_PROPERTY), color, font.deriveFont((float)font.getSize() - 2), margin);
        addLabels(panel, textMgr.translate(PENALTY_NOTICE_PROPERTY), color, font.deriveFont((float)font.getSize() - 3), margin);
        add(panel);
        for (int ndx = getComponentCount();  --ndx >= 0;) {
	        ((JComponent)getComponent(ndx)).setAlignmentX(0.5f);
        }
    }
    
    /**
     * @since 2.0
     */
    protected void validateProperty(final String property, final String name) {
        Assertion.isNotNull(property, TextManager.INSTANCE.getText(NULL_MESSAGE_PROPERTY, name));
    }
}
