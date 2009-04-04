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

package com.metamatrix.console.util;

import java.io.Serializable;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;

/**
 * This class represents a date.  It was originally developed for use in charting but has some
 * static methods usable here.
 */
public class QCDate implements Serializable {
    public final static int MONDAY = 0;
    public final static int TUESDAY = 1;
    public final static int WEDNESDAY = 2;
    public final static int THURSDAY = 3;
    public final static int FRIDAY = 4;
    public final static int SATURDAY = 5;
    public final static int SUNDAY = 6;

    public final static String[] MONTH_ABBREVS = {"Jan", "Feb", "Mar", "Apr", "May", "Jun",
            "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"};

    public final static int BASE_YEAR = 1912;
        // Jan. 1 of BASE_YEAR is assigned a day number of 1, not 0.
        // Code in various methods depends on BASE_YEAR being a leap year numbered 19xx which
        // begins on day numbered 0 (MONDAY).
    public final static int MAX_YEAR = 2099;
        // Code in various methods depends on MAX_YEAR being less than 2100.

    public final static int[] NUM_DAYS_BEFORE_MONTH_NON_LEAP_YEAR = {0, 31, 59, 90, 120,
            151, 181, 212, 243, 273, 304, 334};

    private int year;
    private int month;
    private int day;

    public QCDate() {
        super();
    }

    public QCDate(int aYear, int aMonth, int aDay) {
        super();
        year = aYear;
        month = aMonth;
        day = aDay;
    }

    public QCDate(GregorianCalendar aCal) {
        this(aCal.get(Calendar.YEAR), aCal.get(Calendar.MONTH) + 1, aCal.get(Calendar.DATE));
    }

    public QCDate(Date aDate) {
        super();
        GregorianCalendar cal = new GregorianCalendar();
        cal.setTime(aDate);
        year = cal.get(Calendar.YEAR);
        month = cal.get(Calendar.MONTH) + 1;
        day = cal.get(Calendar.DATE);
    }

    public int getYear() {
        return year;
    }

    public int getMonth() {
        return month;
    }

    public int getDay() {
        return day;
    }

    public Object clone() {
        return new QCDate(year, month, day);
    }

    public boolean equals(QCDate anotherDate) {
        return ((year == anotherDate.year) && (month == anotherDate.month) &&
                (day == anotherDate.day));
    }

    public boolean isGreaterThan(QCDate anotherDate) {
        return ((year > anotherDate.year) || ((year == anotherDate.year) && (month >
                anotherDate.month)) || ((year == anotherDate.year) && (month ==
                anotherDate.month) && (day > anotherDate.day)));
    }

    public boolean isLessThan(QCDate anotherDate) {
        return ((year < anotherDate.year) || ((year == anotherDate.year) && (month <
                anotherDate.month)) || ((year == anotherDate.year) && (month ==
                anotherDate.month) && (day < anotherDate.day)));
    }

    public static int numberOfDaysInMonth(int aYear, int aMonth) {
        if ((aYear < BASE_YEAR) || (aYear > MAX_YEAR)) {
            return -1;
        }
        if (aMonth == 2) {
            if ((aYear % 4) == 0) {
                return 29;
            }
            return 28;
        }
        if ((aMonth == 4) || (aMonth == 6) || (aMonth == 9) || (aMonth == 11)) {
            return 30;
        }
        return 31;
    }

    public static boolean legalDate(int aYear, int aMonth, int aDay) {
        boolean legal = true;
        if ((aYear < BASE_YEAR) || (aYear > MAX_YEAR)) {
            legal = false;
        } else {
            if ((aMonth < 1) || (aMonth > 12)) {
                legal = false;
            } else {
                if ((aDay < 1) || (aDay > numberOfDaysInMonth(aYear, aMonth))) {
                    legal = false;
                }
            }
        }
        return legal;
    }    

    public static boolean leapYear(int aYear) {
        return ((aYear % 4) == 0);
    }

    public static int dayNumberWithinYear(int aYear, int aMonth, int aDay) {
        if (!legalDate(aYear, aMonth, aDay)) {
            return -1;
        }
        int numDaysBeforeMonth = NUM_DAYS_BEFORE_MONTH_NON_LEAP_YEAR[aMonth - 1];
        if ((aMonth > 2) && leapYear(aYear)) {
            numDaysBeforeMonth++;
        }
        int result = numDaysBeforeMonth + aDay;
        return result;
    }

    public static int dayNumberWithinYear(QCDate aDate) {
        return dayNumberWithinYear(aDate.year, aDate.month, aDate.day);
    }

    public static int dayNumber(int aYear, int aMonth, int aDay) {
        //Counts starting with Jan 01, BASE_YEAR being 1, not 0.
        if (!legalDate(aYear, aMonth, aDay)) {
            return -1;
        }
        int numDaysBeforeCurYearExclFeb29 = 365 * (aYear - BASE_YEAR);
        int numDaysCurYear = dayNumberWithinYear(aYear, aMonth, aDay);

        //Calculate number of leap days prior to current year.  (If leap day for current
        //year, it is included in numDaysCurYear.)

        int numLeapDaysPriorToCurYear = ((aYear + 3 - BASE_YEAR) / 4);
        int dayNum = numDaysBeforeCurYearExclFeb29 + numLeapDaysPriorToCurYear + 
                numDaysCurYear;
        return dayNum;
    }

    public static int dayNumber(QCDate aDate) {
        return dayNumber(aDate.year, aDate.month, aDate.day);
    }

    public static int weekdayNumber(int aYear, int aMonth, int aDay) {
        //Counts starting with Jan 01, BASE_YEAR (which must be a Monday) being 1, not 0.
        if (!legalDate(aYear, aMonth, aDay)) {
            return -1;
        }
        int numAllDays = dayNumber(aYear, aMonth, aDay);
        int numSaturdays = (numAllDays + 1) / 7;
        int numSundays = numAllDays / 7;
        return (numAllDays - (numSaturdays + numSundays));
    }

    public static int weekdayNumber(QCDate aDate) {
        return weekdayNumber(aDate.year, aDate.month, aDate.day);
    }

    public static int dayOfWeek(int aYear, int aMonth, int aDay) {
        if (!legalDate(aYear, aMonth, aDay)) {
            return -1;
        }
        return ((dayNumber(aYear, aMonth, aDay) - 1) % 7);
    }

    public static int dayOfWeek(QCDate aDate) {
        return dayOfWeek(aDate.year, aDate.month, aDate.day);
    }

    public static boolean isWeekday(QCDate aDate) {
        int dayOfWeek = QCDate.dayOfWeek(aDate);
        boolean saturday = (dayOfWeek == QCDate.SATURDAY);
        boolean sunday = (dayOfWeek == QCDate.SUNDAY);
        return (!(saturday || sunday));
    }

    public static boolean isWeekend(QCDate aDate) {
        return (!QCDate.isWeekday(aDate));
    }

    public static int weekNumber(int aYear, int aMonth, int aDay) {
        //Counts starting from week of January 1, BASE_YEAR being 1, not 0.
        if (!legalDate(aYear, aMonth, aDay)) {
            return -1;
        }
        int d = ((dayNumber(aYear, aMonth, aDay) - 1) / 7) + 1;
        return d;
    }

    public static int weekNumber(QCDate aDate) {
        return weekNumber(aDate.year, aDate.month, aDate.day);
    }

    public static int monthNumber(int aYear, int aMonth) {
        //Counts starting with January, BASE_YEAR being 1, not 0.
        if (!legalDate(aYear, aMonth, 1)) {
            return -1;
        }
        return ((aYear - BASE_YEAR) * 12 + aMonth);
    }

    public static int monthNumber(QCDate aDate) {
        return monthNumber(aDate.year, aDate.month);
    }

    public static int quarterNumber(int aYear, int aMonth) {
        //Stipulates that quarters only begin in Jan, Apr, Jul, and Oct.
        if (!legalDate(aYear, aMonth, 1)) {
            return -1;
        }
        int qtrsBeforeCurYear = 4 * (aYear - BASE_YEAR);
        int qtrOfCurYear = ((aMonth - 1) / 3) + 1;
        return (qtrsBeforeCurYear + qtrOfCurYear);
    }

    public static int quarterNumber(QCDate aDate) {
        return quarterNumber(aDate.year, aDate.month);
    }

    public static int yearNumber(int aYear) {
        //Counts BASE_YEAR as 1, not 0.
        return (aYear + 1 - BASE_YEAR);
    }

    public static int yearNumber(QCDate aDate) {
        return yearNumber(aDate.year);
    }

    public static QCDate dateOfDayNumber(int aDayNumber) {
        int DAY_NUMBER_OF_DEC_31_1998  = dayNumber(1998, 12, 31);

        if ((aDayNumber < 1) || (aDayNumber > dayNumber(MAX_YEAR, 12, 31))) {
            return null;
        }
        QCDate date = new QCDate();
        int curYear = BASE_YEAR;
        int daysAccumulated = 0;
        if (aDayNumber > DAY_NUMBER_OF_DEC_31_1998) {
            curYear = 1999;
            daysAccumulated = DAY_NUMBER_OF_DEC_31_1998;
        }
        boolean isLeapYear = leapYear(curYear);
        int numDaysInCurYear;
        if (isLeapYear) {
            numDaysInCurYear = 366;
        } else {
            numDaysInCurYear = 365;
        }
        boolean curYearAllFits = (daysAccumulated + numDaysInCurYear < aDayNumber);
        while (curYearAllFits) {
            daysAccumulated += numDaysInCurYear;
            curYear++;
            isLeapYear = leapYear(curYear);
            if (isLeapYear) {
                numDaysInCurYear = 366;
            } else {
                numDaysInCurYear = 365;
            }
            curYearAllFits = (daysAccumulated + numDaysInCurYear < aDayNumber);
        }
        date.year = curYear;
        int month = 1;
        int numDaysInMonth = numberOfDaysInMonth(curYear, month);
        boolean curMonthAllFits = (daysAccumulated + numDaysInMonth < aDayNumber);
        while (curMonthAllFits) {
            daysAccumulated += numDaysInMonth;
            month++;
            numDaysInMonth = numberOfDaysInMonth(curYear, month);
            curMonthAllFits = (daysAccumulated + numDaysInMonth < aDayNumber);
        }
        date.month = month;
        date.day = aDayNumber - daysAccumulated;
        return date;
    }

    public static QCDate dateOfWeekNumber(int aWeekNumber) {
        return dateOfDayNumber((aWeekNumber - 1) * 7 + 1);
    }

    public static QCDate dateOfMonthNumber(int aMonthNumber) {
        int year = ((aMonthNumber - 1) / 12) + BASE_YEAR;
        int month = ((aMonthNumber - 1) % 12) + 1;
        return new QCDate(year, month, 1);
    }

    public static QCDate dateOfQuarterNumber(int aQuarterNumber) {
        int year = ((aQuarterNumber - 1) / 4) + BASE_YEAR;
        int quarter = ((aQuarterNumber - 1) % 4) + 1;
        int month = 3 * (quarter - 1) + 1;
        return new QCDate(year, month, 1);
    }

    public static QCDate dateOfYearNumber(int aYearNumber) {
        return new QCDate(aYearNumber - 1 + BASE_YEAR, 1, 1);
    }

    public String toShortString() {
        int yearLastTwo = year % 100;
        String yearString = (new Integer(yearLastTwo)).toString();
        if (yearLastTwo < 10) {
            yearString = "0" + yearString;
        }
        return (month + "/" + day + "/" + yearString);
    }

    public String toDayMonthYearLastTwoString() {
        String dayString = (new Integer(day)).toString();
        if (day < 10) {
            dayString = "0" + dayString;
        }
        String yearString = (new Integer(year)).toString();
        String yearLastTwo = yearString.substring(2, 4);
        return (dayString + " " + MONTH_ABBREVS[month - 1] + " " + yearLastTwo);
    }

    public String toMonthYearString() {
        String yearString = (new Integer(year)).toString();
        String monthString = MONTH_ABBREVS[month - 1];
        return (monthString + " " + yearString);
    }

    public String toMonthYearLastTwoString() {
        String yearString = (new Integer(year)).toString();
        String yearLastTwo = yearString.substring(2, 4);
        String monthString = MONTH_ABBREVS[month - 1];
        return (monthString + " " + yearLastTwo);
    }

    public String toMonthString() {
        String monthString = MONTH_ABBREVS[month - 1];
        return monthString;
    }

    public String toMonthDayYearString() {
        String monthString = MONTH_ABBREVS[month - 1];
        String dayString = (new Integer(day)).toString();
        String yearString = (new Integer(year)).toString();
        String str = monthString + " " + dayString + ", " + yearString;
        return str;
    }

    /**
     * mm/dd/yyyy
     */
    public String toNumericMDYString() {
        String str = "";
        if (month < 10) {
            str += "0";
        }
        str += month + "/";
        if (day < 10) {
            str += "0";
        }
        str += day + "/" + year;
        return str;
    }

    public GregorianCalendar toGregorian(int hours, int minutes) {
        return new GregorianCalendar(year - 1900, month - 1, day, hours, minutes);
    }

    public GregorianCalendar toGregorian(int hours) {
        return toGregorian(hours, 0);
    }

    public QCDate nextDate() {
        int dayNumber = QCDate.dayNumber(this);
        QCDate next = QCDate.dateOfDayNumber(dayNumber + 1);
        return next;
    }

    public String toString() {
        String str = "QCDate:" + month + "/" + day + "/" + year;
        return str;
    }
}
