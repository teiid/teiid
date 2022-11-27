/*
 * Copyright Red Hat, Inc. and/or its affiliates
 * and other contributors as indicated by the @author tags and
 * the COPYRIGHT.txt file distributed with this work.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.teiid.translator.jdbc.oracle;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.teiid.core.util.StringUtil;
import org.teiid.translator.jdbc.ParseFormatFunctionModifier;

public class OracleFormatFunctionModifier extends
        ParseFormatFunctionModifier {

    static final Pattern tokenPattern = Pattern.compile("(G+|y{1,4}|M{2,4}|DD|dd|E+|a+|HH|hh|mm|ss|S+|Z+|[\\- /,.;:]+|(?:'[^'\"]*')+|[^'\"a-zA-Z]+)"); //$NON-NLS-1$

    protected boolean parse;

    public OracleFormatFunctionModifier(String prefix, boolean parse) {
        super(prefix);
        this.parse = parse;
    }

    public void setPrefix(String prefix) {
        this.prefix = prefix;
    }

    public boolean supportsLiteral(String literal) {
        try {
            translateFormat(literal);
        } catch (IllegalArgumentException e) {
            return false;
        }
        return true;
    }

    @Override
    protected Object translateFormat(String format) {
        Matcher m = tokenPattern.matcher(format);
        StringBuilder sb = new StringBuilder();
        sb.append("'"); //$NON-NLS-1$
        int end = 0;
        char previous = 0;
        while (m.find()) {
            if (m.group().length() == 0) {
                continue;
            }
            if (end == 0) {
                if (m.start() != 0) {
                    throw new IllegalArgumentException();
                }
            } else if (m.start() != end) {
                throw new IllegalArgumentException();
            }
            String group = m.group();
            if (Character.isLetter(previous) && group.charAt(0) == previous) {
                throw new IllegalArgumentException();
            }
            previous = group.charAt(0);
            sb.append(convertToken(group));
            end = m.end();
        }
        if (end != format.length()) {
            throw new IllegalArgumentException();
        }
        sb.append("'"); //$NON-NLS-1$
        return sb.toString();
    }

    protected Object convertToken(String group) {
        switch (group.charAt(0)) {
        case 'G':
            return "AD"; //$NON-NLS-1$
        case 'y':
            if (group.length() == 2) {
                return "YY"; //$NON-NLS-1$
            }
            return "YYYY"; //$NON-NLS-1$
        case 'M':
            if (group.length() == 2) {
                return "MM"; //$NON-NLS-1$
            }
            if (group.length() == 3) {
                return "Mon"; //$NON-NLS-1$
            }
            return "Month"; //$NON-NLS-1$
        case 'D':
            return "DDD"; //$NON-NLS-1$
        case 'd':
            return "DD"; //$NON-NLS-1$
        case 'E':
            if (group.length() >= 4) {
                return "Day"; //$NON-NLS-1$
            }
            return "Dy"; //$NON-NLS-1$
        case 'a':
            return "AM"; //$NON-NLS-1$
        case 'H':
            return "HH24"; //$NON-NLS-1$
        case 'h':
            if (parse) {
                return "HH24"; //$NON-NLS-1$
            }
            return "HH"; //$NON-NLS-1$
        case 'm':
            return "MI"; //$NON-NLS-1$
        case 's':
            return "SS"; //$NON-NLS-1$
        case 'S':
            return "FF" + group.length(); //$NON-NLS-1$
        case 'Z':
            return "TZHTZM";//$NON-NLS-1$
        case '\'':
            return '"' + StringUtil.replaceAll(StringUtil.replaceAll(StringUtil.replaceAll(group.substring(1, group.length() - 1), "''", "'"), "'", "''"), "\"", "\"\"") + '"'; //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$ //$NON-NLS-5$ //$NON-NLS-6$
        case ' ':
        case '-':
        case '/':
        case ',':
        case '.':
        case ';':
        case ':':
            return group;
        default:
            return '"' + group + '"';
        }
    }
}