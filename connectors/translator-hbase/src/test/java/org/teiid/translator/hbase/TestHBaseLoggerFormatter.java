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
package org.teiid.translator.hbase;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.logging.Formatter;
import java.util.logging.LogRecord;

@SuppressWarnings("nls")
public class TestHBaseLoggerFormatter extends Formatter {
    
    SimpleDateFormat format = new SimpleDateFormat("MMM dd,yyyy HH:mm");

    @Override
    public String format(LogRecord record) {
        StringBuffer sb = new StringBuffer();
        sb.append(format.format(new Date(record.getMillis())) + " ");
        sb.append(record.getLevel() + " ");
        sb.append("[" + record.getLoggerName() + "] ");
        sb.append(record.getMessage() + "\n");
        return sb.toString();
    }

}
