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

package com.metamatrix.dqp.spi;

/**
 *
 */
public class TrackerLogConstants {
	public static class CMD_POINT{
		public static final short BEGIN = 1;
		public static final short END = 2;
	}
	
	public static class TXN_POINT{
		public static final short BEGIN = 1;
		public static final short END = 2;
        public static final short ERROR = 3;
		public static final short INTERMEDIATE = 4;

	}

	public static class TXN_STATUS{
		public static final short BEGIN = 1;
		public static final short COMMIT = 2;
		public static final short ROLLBACK = 3;
		public static final short SET_ROLLBACK_ONLY = 4;

        public static class PARTICIPATE {
            public static final short START = 5;
            public static final short END = 6;
            public static final short PREPARE = 7;
            public static final short COMMIT = 8;
            public static final short FORGET = 9;
            public static final short RECOVER = 10;
            public static final short ROLLBACK = 11;
        }
	}

	public static class CMD_STATUS{
		public static final short NEW = 1;
		public static final short END = 2;
		public static final short CANCEL = 3;
		public static final short ERROR = 4;
	}
}
