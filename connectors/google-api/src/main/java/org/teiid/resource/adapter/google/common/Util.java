package org.teiid.resource.adapter.google.common;

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
           int partial=(int)normID.charAt(i)-64;
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
