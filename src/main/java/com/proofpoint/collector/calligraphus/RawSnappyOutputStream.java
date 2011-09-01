package com.proofpoint.collector.calligraphus;

import org.xerial.snappy.SnappyOutputStream;

import java.io.IOException;
import java.io.OutputStream;

/**
 * This class implements a stream filter for writing compressed data using
 * Snappy. The output format is the same as {@link SnappyOutputStream}
 * minus the header, which makes it compatible with
 * <a href="http://code.google.com/p/hadoop-snappy/">hadoop-snappy</a>.
 */
public class RawSnappyOutputStream
        extends SnappyOutputStream
{
    public RawSnappyOutputStream(OutputStream out)
            throws IOException
    {
        super(out, 64 * 1024);
    }

    @Override
    protected void writeHeader()
            throws IOException
    {
        // skip writing header
    }
}
