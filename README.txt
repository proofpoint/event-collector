Event collector

  The collector buffers data locally for up to collector.max-buffer-time,
  which defaults to one minute.  The collector uploads
  this locally buffered, non uploaded data after a restart.
  Any upload failed due to errors in accessing the file, snappy errors
  or Json format errors will result in the file being moved
  to the 'failed' subdirectory of the local staging location.


API
===

Event Resource
--------------

/v2/event
  - POST (store events)

Store a list of events into the system

Examples:

curl -X POST -H'Content-type:application/json' $SERVER:$PORT/v2/event -d'
[
    {
        "type": "Test",
        "uuid": "DCD36293-3072-4AFD-B6E3-A9EB9CE1F219",
        "host": "test.local",
        "timestamp": "2011-03-30T16:10:16.000Z",
        "data": {
            "foo": "bar",
            "hello": "world"
        }
    }
]'


Spool Stats Resource
--------------------

/v1/spool/stats
  - GET (retrieve stats)
  - DELETE (clear stats)

Read or clear the spool stats.
* Counts the number of events of each type spooled to disk for S3 upload. Dropped count will
  always be 0.

Examples:

curl $SERVER:$PORT/v1/spool/stats?pretty
{
    "ScoreRequest":{
        "transferred":4,
        "lost":0
    },
    "PrsMessage":{
        "transferred":2,
        "lost":0
    },
    "HttpRequest":{
        "transferred":15,
        "lost":0
    }
}

curl -X DELETE $SERVER:$PORT/v1/spool/stats?pretty


Event Tap Stats Resource
------------------------

/v1/tap/stats
  - GET (retrieve stats)
  - DELETE (clear stats)

Read or clear the event tap stats.
* Queue stats count the number of events queued into the batch processor. Transferred count is the
  number of events enqueued. Lost count is the number of events dropped from the queue because of
  overflow. Note that, any dropped event has also been included in the transferred count.
* Flow stats count the number of events delivered to the labelled flow, or dropped due to delivery
  failure. Flows are identified by event type, flow ID and target URI.

Examples:

curl $SERVER:$PORT/v1/tap/stats?pretty
{
    "queue":{
        "ScoreRequest":{
            "transferred":4,
            "lost":0
        },
        "PrsMessage":{
            "transferred":2,
            "lost":0
        },
        "HttpRequest":{
            "transferred":12,
            "lost":0
        }
    },
    "flows":{
        "[ScoreRequest, 011fbdb3-ab97-46f8-a9cf-f45b2f2f801c, http://10.93.2.83:9000/v1/event]":{
            "transferred":0,
            "lost":4
        }
    }
}

curl -X DELETE $SERVER:$PORT/v1/tap/stats?pretty


