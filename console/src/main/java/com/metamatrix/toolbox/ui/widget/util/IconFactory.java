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

package com.metamatrix.toolbox.ui.widget.util;

import java.awt.Image;
import java.awt.Toolkit;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;

import javax.swing.ImageIcon;

/**
 * <p>IconFactory is a static class for generating ImageIcon objects from graphics files located
 * either inside a jar file or on the file system.  IconFactory first tries to locate the
 * file in the JVM's classpath by searching the fullLocationPath.  If it is not found, IconFactory
 * then searches the file system via the relativeLocationPath from the application's current
 * default directory.  This design allows for software developers to run the application outside
 * a jar file pulling image resources from their file system with the same code as would be obtaining
 * resources from a jar archive in production.</p>
 *
 * <p>IconFactory is designed to be set upon application startup and used throughout the application.
 * The following is an example of it's use:</p>
 * <pre>
 *      // initialize the factory paths
 *      IconFactory.setFullLocationPath("/com/metamatrix/modeler/images/");
 *      IconFactory.setRelativeLocationPath("../images/");
 *      ...
 *      // use the factory
 *      ImageIcon imageIcon = IconFactory.getIconForImageFile("splash.jpg");
 *      JLabel imageLabel = new JLabel(imageIcon);
 * </pre>
 * <p>IconFactory was designed assuming all images for an application would be located in a single
 * directory.  If images are in multiple locations, you could set one location as the default, then
 * for other locations use the getIconForImageFile( ) method where the full and relative paths are
 * specified explicitly.</p>
 *
 * <p><b>Usage Note:</b> Graphics file names are <b>case sensitive</b> in jar files, but may not be
 * case sensitive on the file system.  Always use case sensitive file names.<p>
 */
public class IconFactory {

    private static String defaultJarPath = null;
    private static String defaultRelativePath = ".";

    /**
     * <p>Set the default path for the images resources as located within a jar.  Example:</p>
     * <pre> IconFactory.setDefaultJarPath("/com/metamatrix/modeler/images/"); </pre>
     */
    public static void setDefaultJarPath(String path) {
        defaultJarPath = path;
    }

    /**
     * <p>Set the default path for the images resources as located on the file system.  Example:</p>
     * <pre> IconFactory.setDefaultRelativePath("../images/"); </pre>
     * @param path relative path string.  Default value is "."
     */
    public static void setDefaultRelativePath(String path) {
        defaultRelativePath = path;
    }

    /**
     * Generate an ImageIcon from the specified graphics file in the default locations.  The setDefaultJarPath()
     * method must have been called previously within this JVM or this method will throw a RuntimeException.
     * @param fileName the name of the graphics file to be found either in the specified resource location or relative
     * path on the file system.
     * @return an ImageIcon object created from the specified file, null if the file cannot be located.
     * @throws RuntimeException if both defaultJarPath and defaultRelativePath properties are null for this
     * static class.
     */
    public static ImageIcon getIconForImageFile(String fileName) {

        if ( defaultJarPath == null || defaultRelativePath == null ) {
            throw new RuntimeException("Code Error: IconFactory paths must be set before calling getIconForImageFile");
        }

        return getIconForImageFile(fileName, defaultJarPath, defaultRelativePath);
    }

    /**
     * Generate an ImageIcon from the specified graphics file.
     * @param fileName the name of the graphics file - <b>case sensitive</b>.
     * @param jarPath the full path of the image resources.  Example: <pre> /com/metamatrix/modeler/images/ </pre>
     * @param relativePath the path on the file system relative to the current directory.  Example: <pre> ../images/ </pre>
     * @return an ImageIcon object created from the specified file, null if the file cannot be located.
     */
    public static ImageIcon getIconForImageFile(String fileName, String jarPath, String relativePath) {
        ImageIcon imageIcon = null;

        // make sure the path ends with /
        String path;
        if ( jarPath.endsWith("/") ) {
            path = jarPath;
        } else {
            path = jarPath + '/';
        }
        if ( path.startsWith("/") ) {
            path = path.substring(1);
        }

        // try getSystemResource first.  this should always work.
        java.net.URL url = ClassLoader.getSystemResource(path + fileName);
        if ( url != null ) {
            imageIcon = new ImageIcon(url);
        }

        if ( imageIcon == null ) {

            // try getResourceAsStream.
            InputStream imageStream = IconFactory.class.getResourceAsStream(path + fileName);
            if ( imageStream != null ) {

                ByteArrayOutputStream baos = new ByteArrayOutputStream();
                try {
                    int c = 0;
                    while ( (c = imageStream.read()) >= 0 ) {
                        baos.write(c);
                    }
                    Image image = Toolkit.getDefaultToolkit().createImage(baos.toByteArray());
                    imageIcon = new ImageIcon(image);
                } catch ( IOException e ) {
                    imageIcon = new ImageIcon(relativePath + fileName);
                }

            } else {

                // if not running from a jar file, try to pull it off the file system

                // make sure the path ends with /
                if ( relativePath.endsWith("/") ) {
                    path = relativePath;
                } else {
                    path = relativePath + '/';
                }

                imageIcon = new ImageIcon(path + fileName);

            }
        }
        return imageIcon;
    }

}

