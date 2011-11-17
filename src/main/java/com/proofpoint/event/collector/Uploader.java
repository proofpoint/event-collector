package com.proofpoint.event.collector;

import java.io.File;

public interface Uploader
{
    File generateNextFilename();

    void enqueueUpload(EventPartition partition, File file);
}
