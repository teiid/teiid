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

package com.metamatrix.toolbox.ui;

import java.io.InputStream;
import java.util.Properties;

/*
Goes with message.properties file and MessagePanel class.  Retrieves information stored in properties file for
message panel message and type.

*/
public class MessageStandards {

	public static final String MESSAGE_TEXT = ".text"; //$NON-NLS-1$
	public static final String MESSAGE_TYPE = ".type"; //$NON-NLS-1$

	private static final String PROPERTIES_FILE = "com/metamatrix/toolbox/message.properties"; //$NON-NLS-1$

	private static final Properties props;

	//************************************************************************
	//initializer
	static {
		props = new Properties();

		try {
			InputStream input =
				ClassLoader.getSystemResourceAsStream(PROPERTIES_FILE);

			props.load(input);

			//no other work to be done here
		} catch (final Exception e) {
			if (e instanceof RuntimeException) {
				throw (RuntimeException) e;
			}
			throw new RuntimeException(e.getMessage());
		}
		//If something goes awry loading the properties file, we still have a
		//Properties object.  Method calls to getType and getMessage will return
		//null for everything.

	}

	//************************************************************************
	//methods

	public static String getType(String id) {
		return getString(id + MESSAGE_TYPE);
	}

	public static String getMessage(String id) {
		return getString(id + MESSAGE_TEXT);
	}

	private static String getString(String s) {
		return props.getProperty(s);
	}

}
