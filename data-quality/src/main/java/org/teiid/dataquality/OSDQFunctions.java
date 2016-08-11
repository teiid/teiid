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

package org.teiid.dataquality;

import org.arrah.framework.datagen.ShuffleRTM;
import org.arrah.framework.ndtable.ResultsetToRTM;
import org.arrah.framework.util.StringCaseFormatUtil;
import org.teiid.metadata.FunctionMethod.Determinism;
import org.teiid.query.function.TeiidFunction;
import org.teiid.query.function.metadata.FunctionCategoryConstants;

/**
 * This class will be reflectively loaded in engine, so be cautioned about
 * renaming this. 
 */
public class OSDQFunctions {

    /**
     * @param a
     *  The string that need to randomize
     *   vivek singh' will become 'ihg vkeivh'
     */
    @TeiidFunction(category=FunctionCategoryConstants.MISCELLANEOUS, determinism=Determinism.COMMAND_DETERMINISTIC)
    public static String random(String sourceValue) {
        return ShuffleRTM.shuffleString(sourceValue);
    }

    /**
     * @param a
     * This function will retrun MD5 hashcode of the string
     * @return String
     */
    @TeiidFunction(category=FunctionCategoryConstants.MISCELLANEOUS)
    public static String hash(String sourceValue) {
        if (sourceValue == null)
            return "d41d8cd98f00b204e9800998ecf8427e"; // null MD5 value
        return ResultsetToRTM.getMD5(sourceValue).toString();
    }

    /**
     * @param a
     * This function will return digit characters of the string
     * @return
     * 
     */
    @TeiidFunction(category=FunctionCategoryConstants.MISCELLANEOUS)
    public static String digit(String sourceValue) {
        return StringCaseFormatUtil.digitString(sourceValue);
    }
}
