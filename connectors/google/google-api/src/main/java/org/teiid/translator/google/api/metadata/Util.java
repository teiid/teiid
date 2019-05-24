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

package org.teiid.translator.google.api.metadata;

/**
 *
 *
 */
public class Util {

    /**
     * Converts spreadsheet column name to position number.
     *
     * @param id   Name of the column
     * @return     Position of the column
     */
    public static int convertColumnIDtoInt(String id) {
        String normID=id.toUpperCase().trim();
        int result=0;
        for(int counter=0, i=normID.length()-1;i>=0;i--,counter++){
           int partial= normID.charAt(i)-64;
           result=(int)(result+(partial*Math.pow(26,counter)));
        }
        return result;
    }

    /**
     * Converts spreadsheet column position to String.
     *
     * @param id   Position of the column
     * @return     Name of the column
     */
    public static String convertColumnIDtoString(int id) {
        StringBuilder result=new StringBuilder();
        int mod;
        while(id>0){
            mod=(id%26);
            if(mod==0){
              mod=26;
              id=id-1;
            }
            result.append((char)(mod+64));
            id/=26;
        }
        return result.reverse().toString();
    }

}
