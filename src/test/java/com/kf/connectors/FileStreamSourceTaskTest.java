/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.kf.connectors;

import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTaskContext;
import org.apache.kafka.connect.storage.OffsetStorageReader;
import org.easymock.EasyMock;
import org.easymock.EasyMockSupport;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class FileStreamSourceTaskTest extends EasyMockSupport {

    private static final String TOPIC = "test";

    private File tempFile;
    private Map<String, String> config;
    private OffsetStorageReader offsetStorageReader;
    private SourceTaskContext context;
    private FileStreamSourceTask task;

    private boolean verifyMocks = false;

    @Before
    public void setup() throws IOException {
        tempFile = File.createTempFile("file-stream-source-task-test", null);
        config = new HashMap<>();
        config.put(FileSourceConnector.FILE_CONFIG, tempFile.getAbsolutePath());
        config.put(FileSourceConnector.TOPIC_CONFIG, TOPIC);
        task = new FileStreamSourceTask();
        offsetStorageReader = createMock(OffsetStorageReader.class);
        context = createMock(SourceTaskContext.class);
        task.initialize(context);
    }

    @After
    public void teardown() {
        tempFile.delete();

        if (verifyMocks)
            verifyAll();
    }

    private void replay() {
        replayAll();
        verifyMocks = true;
    }

    @Test
    public void testNormalLifecycle() throws InterruptedException, IOException {
        expectOffsetLookupReturnNone();
        replay();

        task.start(config);

        FileOutputStream os = new FileOutputStream(tempFile);
        assertEquals(null, task.poll());
        os.write("partial line".getBytes());
        os.flush();
        assertEquals(null, task.poll());
        os.write(" finished\n".getBytes());
        os.flush();
        List<SourceRecord> records = task.poll();
        assertEquals(1, records.size());
        assertEquals(TOPIC, records.get(0).topic());
        assertEquals("partial line finished", records.get(0).value());
        assertEquals(Collections.singletonMap(FileStreamSourceTask.FILENAME_FIELD, tempFile.getAbsolutePath()), records.get(0).sourcePartition());
        assertEquals(Collections.singletonMap(FileStreamSourceTask.POSITION_FIELD, 22L), records.get(0).sourceOffset());
        assertEquals(null, task.poll());

        // Different line endings, and make sure the final \r doesn't result in a line until we can
        // read the subsequent byte.
        os.write("line1\rline2\r\nline3\nline4\n\r".getBytes());
        os.flush();
        records = task.poll();
        assertEquals(4, records.size());
        assertEquals("line1", records.get(0).value());
        assertEquals(Collections.singletonMap(FileStreamSourceTask.FILENAME_FIELD, tempFile.getAbsolutePath()), records.get(0).sourcePartition());
        assertEquals(Collections.singletonMap(FileStreamSourceTask.POSITION_FIELD, 28L), records.get(0).sourceOffset());
        assertEquals("line2", records.get(1).value());
        assertEquals(Collections.singletonMap(FileStreamSourceTask.FILENAME_FIELD, tempFile.getAbsolutePath()), records.get(1).sourcePartition());
        assertEquals(Collections.singletonMap(FileStreamSourceTask.POSITION_FIELD, 35L), records.get(1).sourceOffset());
        assertEquals("line3", records.get(2).value());
        assertEquals(Collections.singletonMap(FileStreamSourceTask.FILENAME_FIELD, tempFile.getAbsolutePath()), records.get(2).sourcePartition());
        assertEquals(Collections.singletonMap(FileStreamSourceTask.POSITION_FIELD, 41L), records.get(2).sourceOffset());
        assertEquals("line4", records.get(3).value());
        assertEquals(Collections.singletonMap(FileStreamSourceTask.FILENAME_FIELD, tempFile.getAbsolutePath()), records.get(3).sourcePartition());
        assertEquals(Collections.singletonMap(FileStreamSourceTask.POSITION_FIELD, 47L), records.get(3).sourceOffset());

        os.write("subsequent text".getBytes());
        os.flush();
        records = task.poll();
        assertEquals(1, records.size());
        assertEquals("", records.get(0).value());
        assertEquals(Collections.singletonMap(FileStreamSourceTask.FILENAME_FIELD, tempFile.getAbsolutePath()), records.get(0).sourcePartition());
        assertEquals(Collections.singletonMap(FileStreamSourceTask.POSITION_FIELD, 48L), records.get(0).sourceOffset());

        os.close();
        task.stop();
    }

    @Test
    public void testBatchSize() throws IOException, InterruptedException {
        expectOffsetLookupReturnNone();
        replay();

        config.put(FileSourceConnector.TASK_BATCH_SIZE_CONFIG, "5000");
        task.start(config);

        FileOutputStream os = new FileOutputStream(tempFile);
        for (int i = 0; i < 10_000; i++) {
            os.write("Neque porro quisquam est qui dolorem ipsum quia dolor sit amet, consectetur, adipisci velit...\n".getBytes());
        }
        os.flush();

        List<SourceRecord> records = task.poll();
        assertEquals(5000, records.size());

        records = task.poll();
        assertEquals(5000, records.size());

        os.close();
        task.stop();
    }

    @Test(expected = ConnectException.class)
    public void testMissingTopic() throws InterruptedException {
        replay();

        config.remove(FileSourceConnector.TOPIC_CONFIG);
        task.start(config);
    }

    @Test
    public void testMissingFile() throws InterruptedException {
        replay();

        String data = "line\n";
        System.setIn(new ByteArrayInputStream(data.getBytes()));

        config.remove(FileSourceConnector.FILE_CONFIG);
        task.start(config);

        List<SourceRecord> records = task.poll();
        assertEquals(1, records.size());
        assertEquals(TOPIC, records.get(0).topic());
        assertEquals("line", records.get(0).value());

        task.stop();
    }

    public void testInvalidFile() throws InterruptedException {
        config.put(FileSourceConnector.FILE_CONFIG, "bogusfilename");
        task.start(config);
        // Currently the task retries indefinitely if the file isn't found, but shouldn't return any data.
        for (int i = 0; i < 100; i++)
            assertEquals(null, task.poll());
    }


    private void expectOffsetLookupReturnNone() {
        EasyMock.expect(context.offsetStorageReader()).andReturn(offsetStorageReader);
        EasyMock.expect(offsetStorageReader.offset(EasyMock.<Map<String, String>>anyObject())).andReturn(null);
    }
}
