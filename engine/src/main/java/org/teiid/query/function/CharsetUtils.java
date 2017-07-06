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

package org.teiid.query.function;

import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CharsetEncoder;
import java.nio.charset.CoderResult;
import java.util.Arrays;

import org.teiid.core.util.Base64;

public final class CharsetUtils {
	
	public static final String HEX_NAME = "HEX"; //$NON-NLS-1$
	static final char[] hex_alphabet = new char[] {'0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'A', 'B', 'C', 'D', 'E', 'F'};

	public static Charset HEX = new Charset(HEX_NAME, new String[0]) {
				
		@Override
		public CharsetEncoder newEncoder() {
			return new FixedEncoder(this, 2, .5f, 1) {
	    		char[] chars = new char[2]; 

		    	@Override
		    	protected CoderResult encode(ByteBuffer out) {
		    		this.cb.get(chars);
		    		out.put((byte)(Integer.parseInt(new String(chars), 16) & 0xff));
		    		return CoderResult.UNDERFLOW;
		    	}

		    };
		}

		@Override
		public CharsetDecoder newDecoder() {
			return new FixedDecoder(this, 1, 2, 2) {
		
				@Override
				public void decode(CharBuffer out) {
					byte b = this.bb.get();
					toHex(out, b);
				}

			};
		}
		
		@Override
		public boolean contains(Charset cs) {
			return false;
		}
	};

	public static void toHex(CharBuffer out, byte b) {
		out.put(hex_alphabet[(b & 0xf0) >> 4]);
		out.put(hex_alphabet[b & 0x0f]);
	}
	
	public static final String BASE64_NAME = "BASE64"; //$NON-NLS-1$
	public static Charset BASE64 = new Charset(BASE64_NAME, new String[0]) {
		@Override
		public CharsetEncoder newEncoder() {
			return new FixedEncoder(this, 4, .75f, 1) {

		    	@Override
		    	protected CoderResult encode(ByteBuffer out) {
		    		try {
		    			out.put(Base64.decode(cb));
		    			return CoderResult.UNDERFLOW;
		    		} catch (IllegalArgumentException e) {
		    			return CoderResult.unmappableForLength(4);
		    		}
		    	}

		    };
		}

		@Override
		public CharsetDecoder newDecoder() {
			return new FixedDecoder(this, 3, 1.25f, 3) {
		
				@Override
				public void decode(CharBuffer out) {
					if (bb.limit() == bb.array().length) {
						out.put(Base64.encodeBytes(bb.array()));
					} else {
						out.put(Base64.encodeBytes(Arrays.copyOf(bb.array(), bb.limit())));
					}
				}
			};
		}

		@Override
		public boolean contains(Charset cs) {
			return false;
		}
	};

	public static abstract class FixedEncoder extends CharsetEncoder {

    	protected CharBuffer cb;
    	
    	protected FixedEncoder(Charset cs, int encodeChars, float averageBytesPerChar, float maxBytesPerChar) {
			super(cs, averageBytesPerChar, maxBytesPerChar); 
			cb = CharBuffer.wrap(new char[encodeChars]);
		}

		@Override
    	protected CoderResult encodeLoop(CharBuffer in, ByteBuffer out) {
		    while (in.hasRemaining()) {
	    		cb.put(in.get());
	    		if (!cb.hasRemaining()) {
					if (!out.hasRemaining()) {
					    return CoderResult.OVERFLOW;
					}
					cb.flip();
					CoderResult result = encode(out);
					if (result != CoderResult.UNDERFLOW) {
						return result;
					}
					cb.clear();
	    		}
		    }
		    return CoderResult.UNDERFLOW;
    	}

		abstract protected CoderResult encode(ByteBuffer out);
		
		@Override
		protected CoderResult implFlush(ByteBuffer out) {
			if (cb.position() != 0) {
				return CoderResult.unmappableForLength(cb.position());
			}
			return super.implFlush(out);
		}
		
		@Override
		protected void implReset() {
			cb.clear();
		}
				
    }
	
	public static abstract class FixedDecoder extends CharsetDecoder {

    	protected ByteBuffer bb;
    	
		protected FixedDecoder(Charset cs, int decodeBytes,
			     float averageCharsPerByte,
			     float maxCharsPerByte) {
			super(cs, averageCharsPerByte, maxCharsPerByte);
			this.bb = ByteBuffer.wrap(new byte[decodeBytes]);
		}

		@Override
		protected CoderResult decodeLoop(ByteBuffer in, CharBuffer out) {
		    while (in.hasRemaining()) {
	    		bb.put(in.get());
	    		if (!bb.hasRemaining()) {
					if (!out.hasRemaining()) {
					    return CoderResult.OVERFLOW;
					}
					bb.flip();
					decode(out);
					bb.clear();
	    		}
		    }
		    return CoderResult.UNDERFLOW;
		}

		protected abstract void decode(CharBuffer out);
		
		@Override
		protected CoderResult implFlush(CharBuffer out) {
			if (bb.position() != 0) {
				if (!out.hasRemaining()) {
				    return CoderResult.OVERFLOW;
				}
				bb.flip();
				decode(out);
				bb.clear();
			}
			return super.implFlush(out);
		}
		
		@Override
		protected void implReset() {
			bb.clear();
		}
    }
    
}