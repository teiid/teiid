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

package com.metamatrix.toolbox.ui.widget;

import java.awt.Dimension;
import java.awt.FontMetrics;
import java.awt.Graphics;
import java.awt.Graphics2D;
import java.awt.Rectangle;
import java.awt.print.PageFormat;
import java.awt.print.Printable;
import java.awt.print.PrinterException;
import java.awt.print.PrinterJob;
import java.io.BufferedReader;
import java.io.StringReader;

import javax.swing.JTextPane;
import javax.swing.RepaintManager;
import javax.swing.text.Style;
import javax.swing.text.StyleConstants;
import javax.swing.text.StyleContext;
import javax.swing.text.StyledDocument;
import javax.swing.text.View;

/**
 * Printable JTextPane
 */
public class TextPane extends JTextPane implements Printable {

    private String sPrintHeader         = "";
    private int iTitlePos               = 0;
    private LabelWidget lblTitle;
    /**
     * Constructs a new JTextPane.  A new instance of StyledEditorKit is
     * created and set, and the document model set to null.
     */
    public TextPane() {
        super();
        // make sure the text wraps before going off screen
        Dimension max = getMaximumSize();
        setMaximumSize(new Dimension(900, max.height));
    }

    /**
     * Constructs a new JTextPane, with a specified document model.
     * A new instance of javax.swing.text.StyledEditorKit is created and set.
     *
     * @param doc the document model
     */
    public TextPane(StyledDocument doc) {
        super(doc);
    }

    /**
     * Tell this panel to print it's content.
     */
    public void printContent() {

        TextPane printCopy = makePrintCopy();
        printCopy.setMaximumSize(new Dimension(800, this.getMaximumSize().height));

        PrinterJob pjob  = PrinterJob.getPrinterJob();

        pjob.pageDialog(pjob.defaultPage());

        /* to speed up performance at least a bit */
        RepaintManager.currentManager(printCopy).setDoubleBufferingEnabled(false);
        pjob.setPrintable(printCopy);

        if (pjob.printDialog()) {
            try {
                pjob.print();
                // How do we do this in Toolbox?  DialogManager.showMessageDialog("Printing Completed",
                //        "Printing Completed\n\nUser: "
                //        + pjob.getUserName() + "\nJob: " + pjob.getJobName());
            } catch (PrinterException pe) {
                // how do we do this in Toolbox??? LogManager.logError(LogContexts.GENERAL, pe, "PrinterException has occurred");
            }
        }
    }

    public void setHeaderForPrinting( String sPrintHeader ) {
        this.sPrintHeader = sPrintHeader;
        lblTitle = new LabelWidget( sPrintHeader );
    }

    public String getHeaderForPrinting() {
        return sPrintHeader;
    }

    public int print(Graphics g, PageFormat pform, int pageNumber) {

        // TODO: - print the header
        //       - make sure the calculation of the main text rectangle
        //         accounts for the header rectangle
        View   view       = this.getUI().getRootView(this);

        double pageOffset = pageNumber
                          * pform.getImageableHeight();

        if (pageOffset > view.getPreferredSpan(View.Y_AXIS)) {
            return Printable.NO_SUCH_PAGE;
        }

        if ( lblTitle != null ) {
            // drop in the header
            g.drawString( lblTitle.getText(),
                          0,
                          iTitlePos );

            //calculate the y position of the content
            int y = iTitlePos;
            g.setFont( lblTitle.getFont() );
            FontMetrics fm = g.getFontMetrics();
            y += fm.getAscent();
            int iContentPos  = y;
            iContentPos += 20;
        }

        ((Graphics2D) g).translate(0d, -pageOffset);

        Rectangle rect = new Rectangle();

        rect.setRect( pform.getImageableX(),
                      pform.getImageableY(),
                      pform.getImageableWidth(),
                      pform.getImageableHeight() + pageOffset );
        view.paint(g, rect);

        return Printable.PAGE_EXISTS;
    }

    private TextPane makePrintCopy() {

        TextPane copy = new TextPane();

        copy.setEditorKit( this.getStyledEditorKit() );
  		String sResults = this.getText();
  		BufferedReader in = new BufferedReader(new StringReader( sResults ));
  		try{
            copy.getStyledEditorKit().read(in, copy.getDocument(),0);
  		} catch (Exception e) {
  		}
  		copy.setEditable(false);

        //TODO: this doesn't work.  Find a way to make the font smaller in the printout.
        Style normal = copy.getStyledDocument().getStyle(StyleContext.DEFAULT_STYLE);
        StyleConstants.setFontSize(normal, 8);

        return copy;
    }


}




