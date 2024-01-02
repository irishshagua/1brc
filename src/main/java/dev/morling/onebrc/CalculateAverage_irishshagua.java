/*
 *  Copyright 2023 The original authors
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package dev.morling.onebrc;

import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayDeque;
import java.util.Map;
import java.util.concurrent.*;
import java.util.stream.Collectors;

public class CalculateAverage_irishshagua {

    private static class Measurement {
        float min;
        float mean;
        float max;
        private float sum;
        private int count;

        public Measurement(float initialValue) {
            this.min = this.mean = this.max = this.sum = initialValue;
            this.count = 1;
        }

        public Measurement aggregate(Measurement newValue) {
            this.min = Math.min(this.min, newValue.min);
            this.max = Math.max(this.min, newValue.min);
            this.count++;
            this.sum += newValue.min;
            this.mean = this.sum / this.count;

            return this;
        }

        public String toString() {
            return min + "/" + mean + "/" + max;
        }
    }

    private static final String FILE = "./measurements.txt";
    // vary for experimentation
    private static final int CHUNK_SIZE = 10_485_760;
    private static int MAX_PARALLELISM = Runtime.getRuntime().availableProcessors();
    private static volatile ArrayDeque<byte[]> DATA_BUFFER = new ArrayDeque<>();
    private static boolean READING = true;
    private static final Map<String, Measurement> RESULTS = new ConcurrentHashMap<>(500);

    public static void main(String[] args) {
        var runtime = (ThreadPoolExecutor) Executors.newFixedThreadPool(MAX_PARALLELISM);
        runtime.submit(() -> startReading(runtime));
        processTillComplete(runtime);

        String output = RESULTS.entrySet().stream()
                .sorted(Map.Entry.comparingByKey())
                .map(entry -> entry.getKey() + ";" + entry.getValue())
                .collect(Collectors.joining(", ", "{", "}"));

        System.out.println(output);
    }

    private static void startReading(ThreadPoolExecutor runtime) {
        long remaining = 0L;
        long size = 0L;
        long lookahead = 0L;
        long position = 0L;
        long fileSize = 0L;
        try (RandomAccessFile file = new RandomAccessFile(FILE, "r"); FileChannel fileChannel = file.getChannel()) {
            fileSize = fileChannel.size();
            MappedByteBuffer buffer;

            while (position < fileSize) {
                remaining = fileSize - position;
                size = Math.min(remaining, CHUNK_SIZE);
                lookahead = Math.min(size + 1_000, fileSize);
                if (position + lookahead > fileSize) {
                    buffer = fileChannel.map(FileChannel.MapMode.READ_ONLY, position, size);
                }
                else {
                    buffer = fileChannel.map(FileChannel.MapMode.READ_ONLY, position, lookahead);
                    size += extendToLineEnd(buffer, size);
                }

                // Reset the position and limit of the buffer
                buffer.position(0);
                buffer.limit((int) size);

                // Read the chunk
                byte[] bytes = new byte[buffer.remaining()];
                buffer.get(bytes);

                while (runtime.getQueue().size() > MAX_PARALLELISM * 2) {
                }
                DATA_BUFFER.offerLast(bytes);

                // Update the position
                position += size;
            }
        }
        catch (Throwable e) {
            e.printStackTrace();
        }

        READING = false;
    }

    private static void processTillComplete(ThreadPoolExecutor runtime) {
        while (READING || DATA_BUFFER.peek() != null) {
            var data = DATA_BUFFER.pollLast();
            if (data != null) {
                runtime.submit(() -> processChunk(data));
            }
        }

        runtime.shutdownNow();
    }

    private static void processChunk(byte[] data) {
        new String(data).lines().forEach((line) -> {
            var measurement = line.split(";");
            var temp = Float.parseFloat(measurement[1]);

            RESULTS.merge(measurement[0], new Measurement(temp), Measurement::aggregate);
        });
    }

    private static long extendToLineEnd(MappedByteBuffer buffer, long chunkSize) {
        long newLimit = 0;
        buffer.position((int) chunkSize); // Start from the end of the original chunk

        while (buffer.hasRemaining()) {
            byte b = buffer.get();
            newLimit++;
            if (b == '\n') { // Assuming \n as line ending
                break; // Stop at the last newline character in the buffer
            }
        }
        return newLimit;
    }
}
