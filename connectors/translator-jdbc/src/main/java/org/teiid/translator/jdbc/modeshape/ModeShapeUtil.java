package org.teiid.translator.jdbc.modeshape;

public class ModeShapeUtil {
	
	public static final String createJCRName(String name) {
		return "[" + ModeShapeUtil.trimTics(name) + "]";
	}
	
	/**
	 * Because the Teiid Designer Import from JDBC adds tic's to a nameInSource that has special characters,
	 * they have to be removed when building the sql syntax
	 * @param name
	 * @return
	 */
	public static final String trimTics(String name) {
		String rtn = name;
		if (rtn.startsWith("'")) {
			rtn = rtn.substring(1);	
		}
		
		if (rtn.endsWith("'")) {
			rtn = rtn.substring(0, rtn.indexOf("'"));
		}
		return rtn;
	}

}
