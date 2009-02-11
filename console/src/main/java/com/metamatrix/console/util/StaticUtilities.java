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

import java.awt.Color;
import java.awt.Component;
import java.awt.Container;
import java.awt.Dimension;
import java.awt.Point;
import java.awt.Toolkit;
import java.io.File;
import java.lang.reflect.InvocationTargetException;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.StringTokenizer;
import java.util.Vector;

import javax.swing.AbstractButton;
import javax.swing.JComboBox;
import javax.swing.JComponent;
import javax.swing.JDialog;
import javax.swing.JFrame;
import javax.swing.JInternalFrame;
import javax.swing.JOptionPane;
import javax.swing.JSplitPane;
import javax.swing.SwingUtilities;
import javax.swing.text.JTextComponent;

import com.metamatrix.console.ui.ViewManager;
import com.metamatrix.console.ui.util.CenteredOptionPane;
import com.metamatrix.console.ui.util.property.PropertyProvider;

/**
 * Class containing miscellaneous useful static utility methods.
 */
public class StaticUtilities {
    public final static int PREFERRED_MODAL_DIALOG_TEXT_WIDTH = 70;
    public final static int MAX_MODAL_DIALOG_TEXT_WIDTH = 130;
    
//    private static SimpleDateFormat formatter = new SimpleDateFormat("MMM dd, yyyy hh:mm:ss a");
	private static boolean showingWaitCursor = false;

    /**
    / Returns a string representing a value rounded to a given number of decimal places.
    / Example :  roundToNumDecimalPlaces(Math.PI, 4) would return "3.1416".  Trailing zeroes
    / are always included.
    / numDecPlaces argument must be from 0 to 10, returns null if not.
    **/
    public static String roundToNumDecimalPlaces(double inputVal, int numDecPlaces) {
        int MIN_DEC_PLACES = 0;
        int MAX_DEC_PLACES = 10;
        if ((numDecPlaces < MIN_DEC_PLACES) || (numDecPlaces > MAX_DEC_PLACES)) {
            return null;
        }
        String formatString;
        if (numDecPlaces == 0) {
            formatString = "#0"; //$NON-NLS-1$
        } else {
            formatString = "#0."; //$NON-NLS-1$
            for (int i = 1; i <= numDecPlaces; i++) {
                formatString += "0"; //$NON-NLS-1$
            }
        }
        DecimalFormat df = new DecimalFormat(formatString);
        String numStr = df.format(inputVal);
        return numStr;
    }

    /**
     * Return a Point at which a frame should be located to center it on the screen, given the
     * frame size and the screen size.
     */
    public static Point centerFrame(Dimension frameSize, Dimension screenSize) {
        int x = ((screenSize.width - frameSize.width) / 2);
        int y = ((screenSize.height - frameSize.height) / 2);
        return new Point(x, y);
    }


    /**
     * Version of centerFrame() (above) to get the screen size itself.
     */
    public static Point centerFrame(Dimension frameSize) {
        Toolkit t = Toolkit.getDefaultToolkit();
        Dimension screenSize = t.getScreenSize();
        return centerFrame(frameSize, screenSize);
    }

    /**
     * Insert linefeed characters into an input string such that the resulting string has
     * no segments more than maxLineLen characters in length, and strings are never broken
     * within a word (unless this is unavoidable because a word occupies more than a whole
     * line).
     *
     * @param str input String
     * @param int preferredMaxLineLen preferred maximum allowable length of any segment in the string without inserting linebreaks
     * @param int maxMaxLineLen absolute maximum allowable length of any segment in the string without inserting linebreeaks
     * @return String the modified String
     */
    public static String insertLineBreaks(String str, int preferredMaxLineLen,
            int maxMaxLineLen) {
        int maxLineLen;
        int maxNonBlankSegment = longestNonBlankStringSegment(str);
        if (maxNonBlankSegment < preferredMaxLineLen) {
            maxLineLen = preferredMaxLineLen;
        } else if (maxNonBlankSegment > maxMaxLineLen) {
            maxLineLen = maxMaxLineLen;
        } else {
            maxLineLen = maxNonBlankSegment;
        }
        StringBuffer out = new StringBuffer(str.length() + 10);
        int lastLoc = str.length() - 1;
        int lastLocCopied = -1;
        boolean continuing = true;
        while ((lastLocCopied < lastLoc) && continuing) {
            int firstLocToCopy = lastLocCopied + 1;
            boolean firstNonBlankFound = false;
            while ((!firstNonBlankFound) && (firstLocToCopy <= lastLoc)) {
                char curChar = str.charAt(firstLocToCopy);
                if (curChar != ' ') {
                    firstNonBlankFound = true;
                } else {
                    firstLocToCopy++;
                }
            }
            if (!firstNonBlankFound) {
                continuing = false;
            }
            if (continuing) {
                if (out.length() > 0) {
                    out = out.append('\n');
                }
                
                //If character at firstLocToCopy is a linefeed then we will skip
                //it, since we have coincidentally just put a linefeed here
                //anyway.
                if (str.charAt(firstLocToCopy) == '\n') {
                	firstLocToCopy++;
                	if (firstLocToCopy > lastLoc) {
                		continuing = false;
                	}
                }
                if (continuing) {
	                int lastCopyableLoc = Math.min(lastLoc, firstLocToCopy + 
	                		maxLineLen - 1);
	                
	                //Check to see if this segment of string already has any linefeed
	                //characters in it.  If so, copy through last one, then reset
	                //firstLocToCopy to next character.
	                int curLoc = lastCopyableLoc;
	                int lastLinefeedLocInSeg = -1;
	                while ((lastLinefeedLocInSeg < 0) && (curLoc >= 
	                		firstLocToCopy)) {
	                	char curChar = str.charAt(curLoc);
	                	if (curChar == '\n') {
	                		lastLinefeedLocInSeg = curLoc;
	                	} else {
	                		curLoc--;
	                	}
	                }
	                if (lastLinefeedLocInSeg >= 0) {
	                	out = out.append(str.substring(firstLocToCopy,
	                			lastLinefeedLocInSeg + 1));
	                	firstLocToCopy = lastLinefeedLocInSeg + 1;
	                	if (firstLocToCopy > lastLoc) {
	                		continuing = false;
	                	} else {
	                		lastCopyableLoc = Math.min(lastLoc, firstLocToCopy
	                				+ maxLineLen - 1);
	                	}
	                }
	                
	                if (continuing) {
		                int origLastCopyableLoc = lastCopyableLoc;
		                char lastCopyableChar = str.charAt(lastCopyableLoc);
		                if (lastCopyableChar != ' ') {
		                    //If lastCopyableChar is the last character of a word (next character is
		                    //blank), then we are okay, else have to go back to prior to start of word
		                    boolean lastCharacterInWord = false;
		                    if (lastCopyableLoc == lastLoc) {
		                        lastCharacterInWord = true;
		                    } else {
		                        char nextChar = str.charAt(lastCopyableLoc + 1);
		                        if (nextChar == ' ') {
		                            lastCharacterInWord = true;
		                        }
		                    }
		                    if (!lastCharacterInWord) {
		                        boolean lineAllNonBlanks = false;
		                        while ((!lineAllNonBlanks) &&
		                                (str.charAt(lastCopyableLoc) != ' ')) {
		                            lastCopyableLoc--;
		                            if (lastCopyableLoc < firstLocToCopy) {
		                                lineAllNonBlanks = true;
		                                lastCopyableLoc = origLastCopyableLoc;
		                            }
		                        }
		                    }
		                }
		                while (str.charAt(lastCopyableLoc) == ' ') {
		                    lastCopyableLoc--;
		                }
		                out = out.append(str.substring(firstLocToCopy, 
		                		lastCopyableLoc + 1));
		                lastLocCopied = lastCopyableLoc;
	                }
                }
            }
        }
        return out.toString();
    }

	/**
	 * Return the segments of a string that are between linebreaks in the input
	 * string.  The linebreaks themselves are not included in any of the
	 * substrings.
	 */
	public static String[] getLineBreakSubstrings(String str) {
		java.util.List /*<String>*/ substrings = new ArrayList();
		int loc = 0;
		int strLen = str.length();
		boolean prevCharWasLinebreak = true;
		StringBuffer buf = null;
		while (loc <= strLen) {
			if (prevCharWasLinebreak) {
				buf = new StringBuffer();
			}
			char curChar;
			if (loc == strLen) {
				curChar = '\n';
			} else {
				curChar = str.charAt(loc);
			}
			if (curChar == '\n') {
				String prevStr = buf.toString();
				substrings.add(prevStr);
				prevCharWasLinebreak = true;
			} else {
				if (loc < strLen) {
					prevCharWasLinebreak = false;
					buf = buf.append(curChar);
				}
			}
			loc++;
		}
		String[] strArray = new String[substrings.size()];
		Iterator it = substrings.iterator();
		for (int i = 0; it.hasNext(); i++) {
			strArray[i] = (String)it.next();
		}
		return strArray;
	}
						
    /**
     * Display a modal dialog with an 'OK' button.
     */
    public static void displayModalDialogWithOK(Component parent,
            String header, String msg, int messageType) {
        String formattedMsg = StaticUtilities.insertLineBreaks(msg,
                PREFERRED_MODAL_DIALOG_TEXT_WIDTH, MAX_MODAL_DIALOG_TEXT_WIDTH);
        CenteredOptionPane.showMessageDialog(parent, formattedMsg, header,
                messageType, null);
    }

    /**
     * Display a modal dialog with an 'OK' button.
     */
	public static void displayModalDialogWithOK(Component parent,
			String header, String msg) {
		displayModalDialogWithOK(parent, header, msg, JOptionPane.PLAIN_MESSAGE);
	}

    /**
     * Display a modal dialog with an 'OK' button.
     */
	public static void displayModalDialogWithOK(String hdr, String msg,
			int messageType) {
		displayModalDialogWithOK(ViewManager.getMainFrame(), hdr, msg,
				messageType);
	}
			
    /**
     * Display a modal dialog with an 'OK' button.
     */
    public static void displayModalDialogWithOK(String hdr, String msg) {
        displayModalDialogWithOK(ViewManager.getMainFrame(), hdr, msg,
        		JOptionPane.PLAIN_MESSAGE);
    }
			
    public static void setDateFormat(String format) {
//        formatter = new SimpleDateFormat(format);
    }

    public static SimpleDateFormat getDefaultDateFormat(){
        PropertyProvider propProvider = new PropertyProvider("com/metamatrix/console/ui/data/common_ui"); //$NON-NLS-1$
        SimpleDateFormat formatter = (SimpleDateFormat)propProvider.getObject("date.formatter.default"); //$NON-NLS-1$
        return formatter;
    }

    /**
     * Return the longest segment between blanks within a String.  If the String is null,
     * returns -1.
     */
    public static int longestNonBlankStringSegment(String str) {
        int longest = -1;
        if (str != null) {
            longest = 0;
            char curChar;
            int lastBreakSS = -1;
            int lastSS = str.length();
            for (int curSS = 0; curSS <= lastSS; curSS++) {
                if (curSS == lastSS) {
                    curChar = ' ';
                } else {
                    curChar = str.charAt(curSS);
                }
                if (curChar == ' ') {
                    int curSegLen = curSS - lastBreakSS - 1;
                    if (curSegLen > longest) {
                        longest = curSegLen;
                    }
                    lastBreakSS = curSS;
                }
            }
        }
        return longest;
    }

	/**
	 * Return an array of tokens in a String.
	 */
	public static String[] tokenize(String str) {
		java.util.List /*<String>*/ tokens = new ArrayList();
		StringBuffer delimsBuffer = new StringBuffer();
		delimsBuffer = delimsBuffer.append(' ');
		delimsBuffer = delimsBuffer.append('\n');
		delimsBuffer = delimsBuffer.append('\r');
		String delims = delimsBuffer.toString();
		StringTokenizer tokenizer = new StringTokenizer(str, delims);
		boolean done = false;
		while (!done) {
			String token = null;
			try {
				token = tokenizer.nextToken();
			} catch (Exception ex) {
				done = true;
			}
			if (!done) {
				tokens.add(token);
			}
		}
		String[] tokenArray = new String[tokens.size()];
		Iterator it = tokens.iterator();
		for (int i = 0; it.hasNext(); i++) {
			tokenArray[i] = (String)it.next();
		}
		return tokenArray;
	}
	
    /**
     * Drop any occurrences of the indicated character from the String
     * provided, and return the resulting String.
     */
     public static String deleteChar( String sSource,  char charToRemove )
     {

        StringBuffer sbModel    = new StringBuffer( sSource );
        StringBuffer sbControl  = new StringBuffer( sbModel.toString() );

        int iLen            = sbControl.length();
        int iDeletedCnt     = 0;
        for ( int x = 0; x < iLen; x++ )
        {
            if( sbControl.charAt( x ) == charToRemove )
            {
                sbModel.deleteCharAt( x - iDeletedCnt );
                iDeletedCnt++;
            }
        }
        return sbModel.toString();
    }

    /**
     * Return a List, not necessarily in any particular order, of all descendant Components of a
     * Component.  Will use getContentPane() for JFrames and JDialogs.  The Component itself is
     * included as the first entry in the list.
     */
    public static java.util.List /*of Component*/ descendantsOfComponent(Component startComp) {
        java.util.List list = new ArrayList();
        list.add(startComp);
        int lastIndexExpanded = -1;
        while (lastIndexExpanded < list.size() - 1) {
            lastIndexExpanded++;
            Component comp = (Component)list.get(lastIndexExpanded);
            if (comp instanceof Container) {
                Component expansionComp = comp;
                if (comp instanceof JFrame) {
                    expansionComp = ((JFrame)comp).getContentPane();
                } else if (comp instanceof JDialog) {
                    expansionComp = ((JDialog)comp).getContentPane();
                }
                if (expansionComp instanceof Container) {
                    Component[] children = ((Container)expansionComp).getComponents();
                    for (int i = 0; i < children.length; i++) {
                        list.add(children[i]);
                    }
                }
            }
        }
        return list;
    }

    /**
     * Return the prime factors of a number.  If the number is <= 1, return
     * only the number itself.  Should not be used with very large numbers.
     */
    public static int[] primeFactors(int num) {
        java.util.List /*of Integer*/ factors = new ArrayList();
        if (num <= 1) {
            factors.add(new Integer(num));
        } else {
            int curValue = num;
            int curFac = 2;
            int upperLim = (int)Math.sqrt(curValue) + 1 /*just to be sure*/;
            boolean doneFactoring = false;
            while (!doneFactoring) {
                if (curFac > upperLim) {
                    doneFactoring = true;
                } else {
                    if ((curValue % curFac) == 0) {
                        factors.add(new Integer(curFac));
                        curValue = curValue / curFac;
                        upperLim = (int)Math.sqrt(curValue) + 1 /*just to be sure*/;
                    } else {
                        curFac++;
                    }
                }
            }
            factors.add(new Integer(curValue));
        }
        int[] array = new int[factors.size()];
        int loc = 0;
        Iterator it = factors.iterator();
        while (it.hasNext()) {
            array[loc] = ((Integer)it.next()).intValue();
            loc++;
        }
        return array;
    }

    /**
     * Return a Collection representing the intersection of given
     * Collections.
     */
    public static Collection intersectionOf(
            Collection /*of Collection*/ collections) {
        if (collections == null) {
            return null;
        } else if (collections.size() == 1) {
            return (Collection)collections.iterator().next();
        } else {
            java.util.List resultList = new ArrayList();
            //Translate each Collection to a List, because we will need to
            //keep track of order.
            java.util.List inputLists = new ArrayList();
            Iterator it = collections.iterator();
            while (it.hasNext()) {
                Collection curCollection = (Collection)it.next();
                Iterator it2 = curCollection.iterator();
                java.util.List curList = new ArrayList();
                while (it2.hasNext()) {
                    Object obj = it2.next();
                    curList.add(obj);
                }
                inputLists.add(curList);
            }
            //Find which list (input Collection) is the shortest.  We will
            //later cycle through using this one as the base.
            int shortestListSize = ((java.util.List)inputLists.get(0)).size();
            int shortestListLoc = 0;
            for (int i = 1; i < inputLists.size(); i++) {
                java.util.List curList = (java.util.List)inputLists.get(i);
                int curListSize = curList.size();
                if (curListSize < shortestListSize) {
                    shortestListSize = curListSize;
                    shortestListLoc = i;
                }
            }
            //Strategy is this:  We will create an auxilliary list
            //called 'markings' corresponding to the elements in each
            //set, now ordered into lists which are elements of 'inputLists'.
            //In 'markings', we will
            //initially flag each element as CELL_UNUSED.  While testing
            //elements in different lists for sameness using the equals()
            //method, we will temporarily mark the cells being tested
            //as CELL_MARKED.  If a match is found, the matching elements
            //across each list are changed to CELL_USED, so that they will
            //not be checked against again, and the matching elements will
            //be added to 'resultList'.  If no match, they are changed
            //back to CELL_UNUSED.
            int CELL_UNUSED = 0;
            int CELL_USED = 1;
            int CELL_MARKED = 2;
            java.util.List markings = new ArrayList();
            //Initialized each element to CELL_UNUSED.
            for (int i = 0; i < inputLists.size(); i++) {
                java.util.List curList = (java.util.List)inputLists.get(i);
                java.util.List curMarkedList = new ArrayList();
                for (int j = 0; j < curList.size(); j++) {
                    curMarkedList.add(new Integer(CELL_UNUSED));
                }
                markings.add(curMarkedList);
            }
//            java.util.List shortestList = (java.util.List)
            inputLists.get(shortestListLoc);
            for (int i = 0; i < shortestListSize; i++) {
                Object curElement = ((java.util.List)inputLists
                        .get(shortestListLoc)).get(i);
                ((java.util.List)markings.get(shortestListLoc)).set(i,
                        new Integer(CELL_MARKED));
                boolean notInList = false;
                int j = 0;
                while ((!notInList) && (j < inputLists.size())) {
                    if (j != shortestListLoc) {
                        java.util.List curInputList =
                                (java.util.List)inputLists.get(j);
                        boolean inList = false;
                        int k = 0;
                        while ((!inList) && (k < curInputList.size())) {
                            int curMarking = ((Integer)((java.util.List)
                                    markings.get(j)).get(k)).intValue();
                            if (curMarking == CELL_UNUSED) {
                                Object curComparisonElement =
                                        curInputList.get(k);
                                if (curElement.equals(
                                        curComparisonElement)) {
                                    inList = true;
                                    ((java.util.List)markings.get(j)).set(
                                            k, new Integer(CELL_MARKED));
                                } else {
                                    k++;
                                }
                            } else {
                                k++;
                            }
                        }
                        if (!inList) {
                            notInList = true;
                        }
                    }
                    j++;
                }
                if (notInList) {
                    resetElements(CELL_MARKED, CELL_UNUSED, markings);
                } else {
                    resultList.add(curElement);
                    resetElements(CELL_MARKED, CELL_USED, markings);
                }
            }
            return resultList;
        }
    }

    /**
     * Return a Collection representing the union of given Collections.
     */
    public static Collection unionOf(Collection /*of Collection*/ collections) {
        int CELL_UNUSED = 0;
        int CELL_MARKED = 1;
        if (collections == null) {
            return null;
        }
        java.util.List union = new ArrayList();
        java.util.List markings = new ArrayList();
        Iterator it = collections.iterator();
        while (it.hasNext()) {
            for (int i = 0; i < markings.size(); i++) {
                markings.set(i, new Integer(CELL_UNUSED));
            }
            Collection aCollection = (Collection)it.next();
            Iterator it2 = aCollection.iterator();
            while (it2.hasNext()) {
                Object curObject = it2.next();
                boolean matchFound = false;
                int i = 0;
                while ((!matchFound) && (i < union.size())) {
                    if ((((Integer)markings.get(i)).intValue() ==
                            CELL_UNUSED) && curObject.equals(
                            union.get(i))) {
                        matchFound = true;
                        markings.set(i, new Integer(CELL_MARKED));
                    } else {
                        i++;
                    }
                }
                if (!matchFound) {
                    union.add(curObject);
                    markings.add(new Integer(CELL_MARKED));
                }
            }
        }
        return union;
    }

    /**
     * Return the greatest common divisor of an array of positive integers.
     * If any of the values is not positive, returns -1.
     */
    public static int greatestCommonDivisor(int[] array) {
        for (int i = 0; i < array.length; i++) {
            if (array[i] < 1) {
                return -1;
            }
        }
        Collection /*of Collection*/ factors = new ArrayList();
        for (int j = 0; j < array.length; j++) {
            int[] fac = primeFactors(array[j]);
            Collection coll = new ArrayList();
            for (int i = 0; i < fac.length; i++) {
                coll.add(new Integer(fac[i]));
            }
            factors.add(coll);
        }
        Collection commonFactors = intersectionOf(factors);
        int product = 1;
        Iterator it = commonFactors.iterator();
        while (it.hasNext()) {
            product *= ((Integer)it.next()).intValue();
        }
        return product;
    }

    /**
     * Return the least common multiple of an array of integers.  If any
     * integer is <= 0, returns -1.
     */
    public static int leastCommonMultiple(int[] array) {
        java.util.List /*of Integer*/ values = new ArrayList();
        for (int i = 0; i < array.length; i++) {
            if (array[i] < 1) {
                return -1;
            }
            if (array[i] > 1) {
                values.add(new Integer(array[i]));
            }
        }
        Collection /*of Collection*/ collections = new ArrayList();
        Iterator it = values.iterator();
        while (it.hasNext()) {
            int val = ((Integer)it.next()).intValue();
            int[] factors = primeFactors(val);
            Collection collection = new ArrayList();
            for (int i = 0; i < factors.length; i++) {
                collection.add(new Integer(factors[i]));
            }
            collections.add(collection);
        }
        Collection union = unionOf(collections);
        int product = 1;
        it = union.iterator();
        while (it.hasNext()) {
            product *= ((Integer)it.next()).intValue();
        }
        return product;
    }

    /**
     * Attempt to force a repaint on a split pane by moving the splitter by one
     * pixel and then back again.
     */
    public static void jiggleSplitter(JSplitPane splitPane) {
        int splitterLoc = splitPane.getDividerLocation();
        int incr = -5;
        if (splitterLoc == 0) {
            incr = 5;
        }
        splitPane.setDividerLocation(splitterLoc + incr);
        splitPane.setDividerLocation(splitterLoc);
    }

    // ==============
    //  Wait cursor methods (borrowed from modeler's ViewManager)
    // ==============

    /**
     * Start the wait cursor on the frame that contains the specified
     * childComponent
     */


    public static void startWait(Component childComponent) {
		ViewManager.startBusySyncronize();
        Component parent = findParentFrame(childComponent);
         if (parent == null)
	        parent = childComponent;
        //if ( parent != null ) {
            if ( parent instanceof JFrame ) {
                ((JFrame) parent).setCursor(StaticProperties.CURSOR_WAIT);
            } else if ( parent instanceof JDialog ) {
                ((JDialog) parent).setCursor(StaticProperties.CURSOR_WAIT);
            }
            showingWaitCursor = true;
       // }
    }

    public static void startWait() {
        startWait(ViewManager.getMainFrame());
    }

    /**
     * Clear the wait cursor on the frame that contains the specified
     * childComponent
     */
    public static void endWait(Component childComponent) {
		Component parent = findParentFrame(childComponent);
        if (parent == null) {
	        parent = childComponent;
        }
        //if ( parent != null ) {
            if ( parent instanceof JFrame ) {
                ((JFrame) parent).setCursor(StaticProperties.CURSOR_DEFAULT);
            } else if ( parent instanceof JDialog ) {
                ((JDialog) parent).setCursor(StaticProperties.CURSOR_DEFAULT);
            }
            showingWaitCursor = false;
        	ViewManager.endBusySyncronize();
       // }
    }

    public static void endWait() {
        endWait(ViewManager.getMainFrame());
    }

	public static boolean isShowingWaitCursor() {
		return showingWaitCursor;
	}
	
    private static Container findParentFrame(Component child) {
        Container parent = null;
        // Check if this panel is in a dialog first...
        parent = SwingUtilities.getAncestorOfClass( JDialog.class, child );
        if ( parent == null ) {
            // Not in dialog, so check whether in internal frame...
            Component parentIntFrame = SwingUtilities.getAncestorOfClass( JInternalFrame.class, child );
            if ( parentIntFrame != null ) {
                // The following is a hack to get around a Swing bug whereby
                // the cursor can not be set on a JInternalFrame.  To outsmart
                // it, set the cursor on it's ancestor which is understood to
                // always be a JFrame in the context of this application.
                parent = SwingUtilities.getAncestorOfClass( JFrame.class, parentIntFrame );
            } else {
                // Not in dialog or internal frame, so must set on parent frame
                parent = SwingUtilities.getAncestorOfClass( JFrame.class, child );
            }
        }
        return parent;
    }

    /**
     * Return the average color of an array of colors.
     */
    public static Color averageRGBVals(Color[] colors) {
        Color avg = null;
        if (colors != null) {
            int totRed = 0;
            int totGreen = 0;
            int totBlue = 0;
            for (int i = 0; i < colors.length; i++) {
                totRed += colors[i].getRed();
                totGreen += colors[i].getGreen();
                totBlue += colors[i].getBlue();
            }
            float numColors = colors.length;
            int red = Math.round(totRed / numColors);
            int green = Math.round(totGreen / numColors);
            int blue = Math.round(totBlue / numColors);
            avg = new Color(red, green, blue);
        }
        return avg;
    }

    public static void disableComponents(Container ct){
	    Component cs[] = ct.getComponents();
	    if(cs == null)
		    return;
	    for (int ii = 0; ii < cs.length; ii++){
	    	if (
                 (cs[ii] instanceof JComboBox) ||
                 (cs[ii] instanceof AbstractButton )  ||
                 (cs[ii] instanceof JTextComponent)

                )
			    ((JComponent)cs[ii]).setEnabled(false);

		if(cs[ii] instanceof Container)
			disableComponents((Container)cs[ii]);
	    }
    }

	public static String getFileName(String fullFileName) {
    	String fileName = null;
    	if (fullFileName != null) {
    		int index = fullFileName.lastIndexOf(File.separatorChar);
    		fileName = fullFileName.substring(index + 1);
    	}
    	return fileName;
	}
	
	public static String getDirectoryName(String fullFileName) {
        String directoryName = null;
        if (fullFileName != null) {
            int index = fullFileName.lastIndexOf(File.separatorChar);
            directoryName = fullFileName.substring(0, index);
        }
        return directoryName;
	}
	
    
     
    public static String parseDelimitedStringGetRight( String sTheString,
                                                String sDelim )
    {
        String sResult              = ""; //$NON-NLS-1$
        int iDelimIndex = sTheString.indexOf( sDelim );


        if ( iDelimIndex != -1 )
            sResult = sTheString.substring( iDelimIndex + 1, sTheString.length() );
        else
            sResult = sTheString;

        return sResult;
    }

    /*
    Packages object[] in a vector
    */
    public static Vector arrayToVector(Object[] array){
        Vector result=new Vector(array.length);
        for(int i=0; i<array.length; ++i){
            result.add(array[i]);
        }
        return result;
    }

    /*
    packages object[][] into a Vector<Vector>
    */
    public static Vector doubleArrayToVector(Object[][] array){
        Vector result = new Vector(array.length);
        for(int i=0; i<array.length; ++i){
            result.add(arrayToVector(array[i]));
        }
        return result;
    }
    
    
    /**
     * Invoke the specified operation in the Swing thread, and wait until it completes.
     * If the current thread is the Swing thread, it executes the operation in the current thread.
     * If the current thread is not the Swing thread, it calls SwingUtilities.invokeAndWait().
     * @param runnable
     * @throws Execption
     * @since 4.3
     */
    public static void invokeAndWaitSafe(Runnable runnable) throws Throwable {
        if (SwingUtilities.isEventDispatchThread()) {
            runnable.run();
        } else {
            try {
                SwingUtilities.invokeAndWait(runnable);
            } catch (InvocationTargetException e) {
                throw e.getTargetException();
            } catch (InterruptedException e) {
                //do nothing
            } 
        }
    }
    
    /**
     * Invoke the specified operation in the Swing thread, and don't necessarily wait until it completes.
     * If the current thread is the Swing thread, it executes the operation in the current thread..
     * If the current thread is not the Swing thread, it calls SwingUtilities.invokeLater().
     * @param runnable
     * @throws Execption
     * @since 4.3
     */
    public static void invokeLaterSafe(Runnable runnable) {
        if (SwingUtilities.isEventDispatchThread()) {
            runnable.run();
        } else {
            SwingUtilities.invokeLater(runnable);
        }
    }
    
    

/////
// Private internal methods.
/////

    /**
    * Internal method to intersectionOf().
    */
    private static void resetElements(int oldValue, int newValue,
            java.util.List/*of List*/ markings) {
        Iterator it = markings.iterator();
        while (it.hasNext()) {
            java.util.List curList = (java.util.List)it.next();
            for (int i = 0; i < curList.size(); i++) {
                int curValue = ((Integer)curList.get(i)).intValue();
                if (curValue == oldValue) {
                    curList.set(i, new Integer(newValue));
                }
            }
        }
    }
}
