Event collector

WARNING:
  The collector buffers data locally for up to collector.max-buffer-time,
  which defaults to one minute.  Currently, the collector does not upload
  this locally buffered data after a restart, nor does it save the event
  name or partition for the files (the filename is simply a UUID).  This
  means that the buffered data is effectively lost when the server is
  stopped.  This will be fixed in a future release.
