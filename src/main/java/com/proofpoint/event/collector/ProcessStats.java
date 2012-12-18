/*
 * Copyright 2011-2012 Proofpoint, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.proofpoint.event.collector;

import com.google.common.base.Charsets;
import com.google.common.base.Splitter;
import com.google.common.io.Files;
import com.google.inject.Inject;
import com.proofpoint.event.client.EventClient;
import com.proofpoint.event.client.EventField;
import com.proofpoint.event.client.EventType;
import com.proofpoint.log.Logger;
import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import javax.annotation.PostConstruct;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;

public class ProcessStats
{
    private static final Logger log = Logger.get(ProcessStats.class);
    private static final DateTimeFormatter DATE_FORMAT = ISODateTimeFormat.dateTimeNoMillis().withZone(DateTimeZone.UTC);
    private static final String STATS_DIR = "var/stats";
    private final String hourlyOutputFile;
    private final String currentOutputFile;
    private final ScheduledExecutorService executor;
    private final EventClient eventClient;
    private final String eventType;
    private AtomicLong counter;
    private AtomicBoolean started = new AtomicBoolean(false);

    @Inject
    public ProcessStats(ScheduledExecutorService executor, EventClient eventClient, String eventType)
    {
        this.executor = executor;
        this.eventClient = eventClient;
        this.eventType = eventType;
        hourlyOutputFile = String.format(STATS_DIR + "/%s/hourly.txt", eventType);
        currentOutputFile = String.format(STATS_DIR + "/%s/current.txt", eventType);
    }

    @PostConstruct
    public synchronized void start()
    {
        if (!started.get()) {
            File file = new File(String.format(STATS_DIR + "/%s", eventType));
            if (!file.exists()) {
                file.mkdirs();
            }
            createFileIfNotExists(currentOutputFile);
            createFileIfNotExists(hourlyOutputFile);

            counter = new AtomicLong(getLastSavedCurrentHourCount());

            DateTime date = DateTime.now(DateTimeZone.UTC);
            long nextMinuteWrite = 60 - date.getSecondOfMinute();
            long nextHourlyWrite = (60 - date.getMinuteOfHour() - 1) * 60 + nextMinuteWrite;
            long nextDailyWrite = (24 - date.getHourOfDay() - 1) * 60 * 60 + nextHourlyWrite;

            executor.scheduleAtFixedRate(new Runnable()
            {
                @Override
                public void run()
                {
                    persistCurrent(counter.get());
                }
            }, nextMinuteWrite, 60, TimeUnit.SECONDS);

            executor.scheduleAtFixedRate(new Runnable()
            {
                @Override
                public void run()
                {
                    persistHourly(counter.getAndSet(0));
                    resetCurrentFile();
                }
            }, nextHourlyWrite, 60 * 60, TimeUnit.SECONDS);

            executor.scheduleAtFixedRate(new Runnable()
            {
                @Override
                public void run()
                {
                    persistDaily();
                }
            }, nextDailyWrite, 60 * 60 * 24, TimeUnit.SECONDS);
            started.set(true);
        }
    }

    public void processed(long num)
    {
        if (!started.get()) {
            throw new IllegalStateException();
        }
        counter.addAndGet(num);
    }

    public long getProcessed()
    {
        if (!started.get()) {
            throw new IllegalStateException();
        }
        return counter.get();
    }

    private void persistCurrent(long value)
    {
        writeToDisk(DateTime.now(DateTimeZone.UTC), value, currentOutputFile, false);
    }

    private void persistHourly(long value)
    {
        writeToDisk(DateTime.now(DateTimeZone.UTC), value, hourlyOutputFile, true);
    }

    private void persistDaily()
    {
        try {
            File file = new File(hourlyOutputFile);
            List<String> lines = Files.readLines(file, Charsets.UTF_8);
            for (String line : lines) {
                Iterator<String> iterator = Splitter.on(" ").split(line).iterator();
                HourlyEventCount event = new HourlyEventCount(DATE_FORMAT.parseDateTime(iterator.next()), Long.parseLong(iterator.next()), eventType);
                eventClient.post(event);
            }
            resetHourlyFile();
        }
        catch (IOException e) {
            log.error(e, "Unable to post daily events");
        }
    }

    private void writeToDisk(DateTime dateTime, long value, String fileName, boolean append)
    {
        try {
            String date = DATE_FORMAT.print(dateTime);
            String out = String.format("%s %s\n", date, value);
            File file = new File(fileName);
            if (append) {
                Files.append(out, file, Charsets.UTF_8);
            }
            else {
                Files.write(out, file, Charsets.UTF_8);
            }
        }
        catch (IOException e) {
            logFileStatus(fileName, true);
            log.error(e, "Unable to write to file %s", fileName);
        }
    }

    private void resetCurrentFile()
    {
        File current = new File(currentOutputFile);
        if (current.exists()) {
            current.delete();
        }
        createFileIfNotExists(currentOutputFile);
    }

    private void resetHourlyFile()
    {
        File hourly = new File(hourlyOutputFile);
        if (hourly.exists()) {
            hourly.delete();
        }
        createFileIfNotExists(hourlyOutputFile);
    }

    private long getLastSavedCurrentHourCount()
    {
        try {
            File file = new File(currentOutputFile);
            String line = Files.readFirstLine(file, Charsets.UTF_8);

            if (line != null) {
                Iterator<String> iterator = Splitter.on(" ").split(line).iterator();
                String date = iterator.next();
                long value = Long.parseLong(iterator.next());

                DateTime dateTime = DATE_FORMAT.parseDateTime(date);
                DateTime currentDateTime = DateTime.now(DateTimeZone.UTC);
                if (currentDateTime.dayOfYear().get() == dateTime.dayOfYear().get() && currentDateTime.hourOfDay().get() == dateTime.hourOfDay().get()) {
                    return value;
                }
                else {
                    writeToDisk(dateTime, value, hourlyOutputFile, true);
                }
            }
            return 0;
        }
        catch (IOException e) {
            logFileStatus(currentOutputFile, false);
            log.error(e, "Unable to read the last saved value from %s. Using default 0", currentOutputFile);
            return 0;
        }
    }

    private boolean createFileIfNotExists(String fileName)
    {
        File file = new File(fileName);
        if (!file.exists()) {
            try {
                boolean created = file.createNewFile();
                if (!created) {
                    log.error("Unable to create local file %s", fileName);
                }
                return created;
            }
            catch (IOException e) {
                return false;
            }
        }
        return true;
    }

    private void logFileStatus(String fileName, boolean write)
    {
        File file = new File(fileName);
        if (!file.exists()) {
            log.debug("File %s does not exist.", fileName);
            return;
        }
        if (write && !file.canWrite()) {
            log.error("Unable to write to file %s.", fileName);
        }
        if (!write && !file.canRead()) {
            log.error("Unable to read from file %s.", fileName);
        }
    }

    @EventType("HourlyEventCount")
    public static class HourlyEventCount
    {
        private final String dateTime;
        private final long count;
        private final String eventType;

        public HourlyEventCount(DateTime dateTime, long count, String eventType)
        {
            this.dateTime = DATE_FORMAT.print(dateTime);
            this.count = count;
            this.eventType = eventType;
        }

        @EventField
        public String getDateTime()
        {
            return dateTime;
        }

        @EventField
        public long getCount()
        {
            return count;
        }

        @EventField
        public String getEventType()
        {
            return eventType;
        }
    }
}
