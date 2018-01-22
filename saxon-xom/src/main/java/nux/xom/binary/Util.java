/*
 * Copyright (c) 2005, The Regents of the University of California, through
 * Lawrence Berkeley National Laboratory (subject to receipt of any required
 * approvals from the U.S. Dept. of Energy). All rights reserved.
 * 
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 * 
 * (1) Redistributions of source code must retain the above copyright notice,
 * this list of conditions and the following disclaimer.
 * 
 * (2) Redistributions in binary form must reproduce the above copyright notice,
 * this list of conditions and the following disclaimer in the documentation
 * and/or other materials provided with the distribution.
 * 
 * (3) Neither the name of the University of California, Lawrence Berkeley
 * National Laboratory, U.S. Dept. of Energy nor the names of its contributors
 * may be used to endorse or promote products derived from this software without
 * specific prior written permission.
 * 
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 * 
 * You are under no obligation whatsoever to provide any bug fixes, patches, or
 * upgrades to the features, functionality or performance of the source code
 * ("Enhancements") to anyone; however, if you choose to make your Enhancements
 * available either publicly, or directly to Lawrence Berkeley National
 * Laboratory, without imposing a separate written license agreement for such
 * Enhancements, then you hereby grant the following license: a non-exclusive,
 * royalty-free perpetual license to install, use, modify, prepare derivative
 * works, incorporate into other computer software, distribute, and sublicense
 * such enhancements or derivative works thereof, in binary and source code
 * form.
 */
package nux.xom.binary;

import nu.xom.Attribute;

/** 
 * Utilities for binary XML handling. 
 * 
 * @author whoschek.AT.lbl.DOT.gov
 * @author $Author: hoschek3 $
 * @version $Revision: 1.7 $, $Date: 2005/11/29 23:20:03 $
 */
final class Util { // not a public class!

	private Util() {} // not instantiable
	
	/** two bits (10) indicate no namespace in QName */
	public static int noNamespace(int type) {
		return type | (2 << 6);
	}
	
	/** read namespace flag */
	public static boolean hasNoNamespace(int type) {
		return ((type >>> 6) & 0x03) == 2;
	}

	public static void packOneIndex(ArrayByteList dst, int index, int type) {
		int pos = dst.size() - 1;
		if (index < 16) { 
			type = inlineIndex(type, index);
		} else {
			type |= packIndex(dst, index) << 4;
		}
		dst.set(pos, (byte)type);
	}
	
	public static void packTwoIndexes(ArrayByteList dst, int ix0, int ix1, int type) {
		int pos = dst.size() - 1;
		type |= packIndex(dst, ix0) << 4;
		type |= packIndex(dst, ix1) << 6;
		dst.set(pos, (byte)type);
	}
	
	/** Packs 4 byte integer index into 1, 2 or 4 bytes, depending on range */
	public static int packIndex(ArrayByteList dst, int index) {
		if (index < 256) { 
			dst.add((byte) index); // unsigned byte
			return 0; // bits 00
		}
		else if (index < 65536) { 
			dst.addShort((short) index); // unsigned short
			return 1; // bits 01
		}
		else {
			dst.addInt(index); // signed int
			return 3; // bits 11
		}
	}
				
	/** set bit 4 to flag inlining, four high bits for index */
	public static int inlineIndex(int type, int index) {
		type &= 0x07;
		type |= (1 << 3) | (index << 4);
		return type;
	}
	
	/** read inlining bit (bit 4) */
	public static boolean isInlinedIndex(int type) {
		return ((type >>> 3) & 0x01) != 0;
	}
	
	public static int getInlinedIndex(int type) {
		return (type >>> 4) & 0x0F;
	}
	
	/** Reuses XOM's way of representing attribute types */
	public static byte getAttributeTypeCode(Attribute attr) {
		return (byte) attr.getType().hashCode();
	}

	public static Attribute.Type getAttributeType(int typeCode) throws BinaryParsingException {
		switch (typeCode) {
			case 0 : return Attribute.Type.UNDECLARED;
			case 1 : return Attribute.Type.CDATA;
			case 2 : return Attribute.Type.ID;
			case 3 : return Attribute.Type.IDREF;
			case 4 : return Attribute.Type.IDREFS;
			case 5 : return Attribute.Type.NMTOKEN;
			case 6 : return Attribute.Type.NMTOKENS;
			case 7 : return Attribute.Type.NOTATION;
			case 8 : return Attribute.Type.ENTITY;
			case 9 : return Attribute.Type.ENTITIES;
			case 10 : return Attribute.Type.ENUMERATION;
			default: 
				throw new BinaryParsingException("Illegal attribute type code: " + typeCode);
		}
	}

	/**
	 * Returns the integer value gained when interpreting the value v as an
	 * <em>unsigned byte</em>. Example: readUnsignedByte(0xFF) == 255, rather
	 * than -1.
	 */
	public static int getUnsignedByte(byte v) {
		return v & 0xFF;
	}

	/**
	 * Returns the integer value gained when interpreting the value v as an
	 * <em>unsigned short</em>.
	 */
	public static int getUnsignedShort(short v) {
		return v & 0xFFFF;
	}
	
}
