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

package com.metamatrix.console.util;

import java.io.Serializable;
import java.util.Calendar;
import java.util.GregorianCalendar;

/**
 * Class to extend GregorianCalendar in order to gain public access to
 * "getTimeInMillis()" and "setTimeInMillis()".
 */
public class QCGregorianCalendar extends GregorianCalendar implements Serializable {
    public QCGregorianCalendar() {
        super();
    }

    public QCGregorianCalendar(int y, int m, int d) {
        super(y, m, d);
    }

    public QCGregorianCalendar(int y, int m, int d, int h, int min) {
        super(y, m, d, h, min);
    }

    public QCGregorianCalendar(int y, int m, int d, int h, int min, int s) {
        super(y, m, d, h, min, s);
    }

    public QCGregorianCalendar(GregorianCalendar gc) {
        this(gc.get(Calendar.YEAR), gc.get(Calendar.MONTH), gc.get(Calendar.DATE),
                gc.get(Calendar.HOUR_OF_DAY), gc.get(Calendar.MINUTE), gc.get(Calendar.SECOND));
    }

    public long getTimeInMillis() {
        return super.getTimeInMillis();
    }

    public void setTimeInMillis(long time) {
        super.setTimeInMillis(time);
    }

    /**
     * Return a time in milliseconds, given a four-digit year, month 1 through 12, day 1 through
     * 31, hour 0 through 23, minute 0 through 59, and second 0 through 59, and using the
     * default time zone.
     */
    public static long timeInMillis(int year, int month, int day, int hour, int minute,
            int second) {
        QCGregorianCalendar cal = new QCGregorianCalendar(year, month - 1, day, hour, minute,
                second);
        return cal.getTimeInMillis();
    }
}
