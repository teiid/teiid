package org.teiid.google.dataprotocol;

import java.io.IOException;
import java.io.PushbackReader;
import java.io.Reader;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.LinkedHashMap;
import java.util.List;

import org.teiid.core.util.StringUtil;
import org.teiid.translator.google.api.SpreadsheetOperationException;

/**
 * Parsing google json is a little non-standard.  They assume a js binding, so array syntax, strings, and date are used.
 * This parser supports most of the customizations except for unquoted dictionary keys.
 *
 * Assumes all numbers are properly represented by Double.
 */
public class GoogleJSONParser {

    private static final class ReaderCharSequence implements CharSequence {
        Reader r;
        int i = -1;

        @Override
        public CharSequence subSequence(int start, int end) {
            throw new UnsupportedOperationException();
        }

        @Override
        public int length() {
            return Integer.MAX_VALUE;
        }

        @Override
        public char charAt(int index) {
            if (index != ++i) {
                throw new IllegalStateException();
            }
            int result;
            try {
                result = r.read();
            } catch (IOException e) {
                throw new SpreadsheetOperationException(e);
            }
            if (result == -1) {
                throw new SpreadsheetOperationException("Read end of stream before the end of a string value");
            }
            return (char)result;
        }
    }

    private Calendar cal;
    private int[] parts = new int[7];
    private StringBuilder sb = new StringBuilder();
    private ReaderCharSequence charSequence = new ReaderCharSequence();

    public Object parseObject(Reader r, boolean wrapped) throws IOException {
        if (wrapped) {
            while (true) {
                int c = r.read();
                if (c == -1) {
                    return null;
                }
                if (c == '(') {
                    break;
                }
            }
        }
        return parseObject(new PushbackReader(r), skipWhitespace(r));
    }

    private Object parseObject(PushbackReader r, int c) throws IOException {
        switch (c) {
        case '{':
            LinkedHashMap<String, Object> map = new LinkedHashMap<String, Object>();
            while (true) {
                c = skipWhitespace(r);
                switch (c) {
                case '}':
                    return map;
                case ',':
                    //this is lenient
                    continue;
                }
                String s = parseString(r, c);
                c = skipWhitespace(r);
                if (c != ':') {
                    throw new SpreadsheetOperationException("Expected : in object name value pair");
                }
                c = skipWhitespace(r);
                Object o = parseObject(r, c);
                map.put(s, o);
            }
        case '[':
            List<Object> array = new ArrayList<Object>();
            boolean seenComma = true;
            while (true) {
                c = skipWhitespace(r);
                switch (c) {
                case ',': //special handling for google arrays
                    if (seenComma) {
                        array.add(null);
                    }
                    seenComma = true;
                    break;
                case ']':
                    return array;
                default:
                    seenComma = false;
                    Object o = parseObject(r, c);
                    array.add(o);
                }
            }
        case -1:
            return null;
        case '"':
        case '\'':
            return parseString(r, c);
        default:
            return parseLiteral(r, (char)c);
        }
    }

    private Object parseLiteral(PushbackReader r, char c) throws IOException {
        sb.setLength(0);
        do {
            sb.append(c);
            int i = r.read();
            if (i == -1) {
                break;
            }
            c = (char)i;
        } while (!Character.isWhitespace(c) && c != ',' && c != ']' && c != '}');

        //date handling
        if (areEquals(sb, "new")) { //$NON-NLS-1$
            sb.setLength(0);
            int length = 0;
            Arrays.fill(parts, 0);
            for (int i = 0; ; i++) {
                if (i > 5) { //remove " Date("
                    if (c == ',' || c == ')') {
                        if (length > 6) {
                            throw new SpreadsheetOperationException("Too many date fields");
                        }
                        parts[length++] = Integer.valueOf(sb.toString());
                        if (c == ')') {
                            break;
                        }
                        sb.setLength(0);
                    } else {
                        sb.append(c);
                    }
                }
                int chr = r.read();
                if (chr == -1) {
                    throw new SpreadsheetOperationException("Encountered end of stream in date value");
                }
                c = (char)chr;
            }
            if (length > 3) {
                Calendar calendar = getCalendar();
                calendar.set(parts[0], parts[1], parts[2], parts[3], parts[4], parts[5]);
                Timestamp ts = new Timestamp(calendar.getTimeInMillis());
                ts.setNanos(parts[6]*1000000); //convert from millis to nanos
                return ts;
            }
            Calendar calendar = getCalendar();
            calendar.set(parts[0], parts[1], parts[2]);
            return new java.sql.Date(cal.getTimeInMillis());
        }
        if (!Character.isWhitespace(c)) {
            r.unread(c); //the terminating character is still needed by the caller
            //TODO could hold this state so that a pushback reader is not needed
        }
        if (areEquals(sb, "false")) { //$NON-NLS-1$
            return Boolean.FALSE;
        } else if (areEquals(sb, "true")) { //$NON-NLS-1$
            return Boolean.TRUE;
        } else if (areEquals(sb, "null")) { //$NON-NLS-1$
            return null;
        }
        return Double.valueOf(sb.toString());
    }

    private boolean areEquals(CharSequence cs, CharSequence cs1) {
        if (cs.length() != cs1.length()) {
            return false;
        }
        for (int i = 0; i < cs.length(); i++) {
            if (cs.charAt(i) != cs1.charAt(i)) {
                return false;
            }
        }
        return true;
    }

    Calendar getCalendar() {
        if (cal == null) {
            cal = Calendar.getInstance();
        }
        cal.clear();
        return cal;
    }

    public void setCalendar(Calendar cal) {
        this.cal = cal;
    }

    private String parseString(final Reader r, int quoteChar) {
        if (quoteChar != '"' && quoteChar != '\'') {
            throw new IllegalStateException();
        }
        charSequence.i = -1;
        charSequence.r = r;
        sb.setLength(0);
        return StringUtil.unescape(charSequence, quoteChar, false, sb);
    }

    private int skipWhitespace(Reader r) throws IOException {
        while (true) {
            int c = r.read();
            if (c == -1) {
                return c;
            }
            if (!Character.isWhitespace((char)c)) {
                return c;
            }
        }
    }

}
