/*
 * Copyright © 2024, Evolved Binary Ltd. <tech@evolvedbinary.com>
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *     * Redistributions of source code must retain the above copyright
 *       notice, this list of conditions and the following disclaimer.
 *     * Redistributions in binary form must reproduce the above copyright
 *       notice, this list of conditions and the following disclaimer in the
 *       documentation and/or other materials provided with the distribution.
 *     * Neither the name of the <organization> nor the
 *       names of its contributors may be used to endorse or promote products
 *       derived from this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
 * ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
 * WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
 * DISCLAIMED. IN NO EVENT SHALL <COPYRIGHT HOLDER> BE LIABLE FOR ANY
 * DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
 * (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
 * LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
 * ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
 * SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */
package com.evolvedbinary.j8iou.nio;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.RandomAccessFile;
import java.io.UncheckedIOException;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class MultiplexedMappedByteBufferTest {

  @TempDir
  Path tempFolder;

  @ParameterizedTest(name = "[{index}] create(initialPosition={0})")
  @ValueSource(longs = {-1, -2, Long.MIN_VALUE})
  void createNegativeInitialPosition(final long initialPosition) {
    assertThrows(IllegalArgumentException.class, () ->
      MultiplexedMappedByteBuffer.create(null, FileChannel.MapMode.READ_WRITE, 1024, 4096, 10, initialPosition)
    );
  }

  @ParameterizedTest(name = "[{index}] create(minBufferSize={0}, maxBufferSize={1}, maxBuffers={2}, initialPosition={3})")
  @CsvSource({
      "512,  2048,  5,  0",
      "1024, 16384, 10, 0",
      "2048, 4096,  20, 0",
      "512,  2048,  5,  512",
      "1024, 16384, 10, 1024",
      "2048, 4096,  20, 2048",
  })
  void createNewFile(final long minBufferSize, final long maxBufferSize, final int maxBuffers, final long initialPosition) throws IOException {
    final Path path = tempFolder.resolve("createNewFile.bin");

    try (final FileChannel fileChannel = FileChannel.open(path, StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE, StandardOpenOption.READ)) {
      try (final MultiplexedMappedByteBuffer buffer = MultiplexedMappedByteBuffer.create(fileChannel, FileChannel.MapMode.READ_WRITE, minBufferSize, maxBufferSize, maxBuffers, initialPosition)) {
        // check the file channel and positions
        assertEquals(fileChannel, buffer.fileChannel());
        assertEquals(FileChannel.MapMode.READ_WRITE, buffer.mapMode());
        // NOTE(AR) initial position in a new file will always be zero, regardless of what was requested
        assertEquals(0, buffer.fileChannel().position());
        assertEquals(initialPosition, buffer.position());
        assertEquals(initialPosition, buffer.fcPosition());
        assertEquals(initialPosition, buffer.nextFcPosition());

        // check min and max buffer sizes
        assertEquals(minBufferSize, buffer.minBufferSize());
        assertEquals(maxBufferSize, buffer.maxBufferSize());

        // check space is allocated for maxBuffers in regions
        final MultiplexedMappedByteBuffer.Region actualRegions[] = buffer.regions();
        assertEquals(maxBuffers, actualRegions.length);

        // check only the first region holds a buffer and that the rest are null
        assertEquals(1, buffer.usedRegions());
        final MultiplexedMappedByteBuffer.Region firstActualRegion = actualRegions[0];
        assertNotNull(firstActualRegion);
        assertArrayEquals(new MultiplexedMappedByteBuffer.Region[maxBuffers - 1], Arrays.copyOfRange(actualRegions, 1, maxBuffers));

        // check that the first region is set up correctly
        assertEquals(initialPosition, firstActualRegion.fcPositionStart);
        assertEquals(initialPosition + minBufferSize - 1, firstActualRegion.fcPositionEnd);
        assertEquals(0, firstActualRegion.buffer.position());
        assertEquals(minBufferSize, firstActualRegion.buffer.capacity());
        assertEquals(minBufferSize, firstActualRegion.buffer.remaining());

        // check the active region is the first region
        assertEquals(0, buffer.activeRegionIdx());
      }
    }
  }

  @ParameterizedTest(name = "[{index}] position({0})")
  @ValueSource(longs = {-1, -2, Long.MIN_VALUE})
  void negativePosition(final long position) throws IOException {
    final Path path = tempFolder.resolve("negativePosition.bin");
    try (final FileChannel fileChannel = FileChannel.open(path, StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE, StandardOpenOption.READ)) {
      try (final MultiplexedMappedByteBuffer buffer = MultiplexedMappedByteBuffer.create(fileChannel, FileChannel.MapMode.READ_WRITE, 1024, 2048, 10, 0)) {
        assertEquals(0, buffer.position());
        assertEquals(0, buffer.fcPosition());
        assertEquals(0, buffer.nextFcPosition());

        // trying to set a negative position throws an exception
        assertThrows(IllegalArgumentException.class, () ->
            buffer.position(position)
        );
      }
    }
  }

  @ParameterizedTest(name = "[{index}] position({0})")
  @ValueSource(longs = {0, 256, 512, 1024, 2048, 4096, 8192, 16384})
  void position(final long position) throws IOException {
    final Path path = tempFolder.resolve("position.bin");

    try (final FileChannel fileChannel = FileChannel.open(path, StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE, StandardOpenOption.READ)) {
      try (final MultiplexedMappedByteBuffer buffer = MultiplexedMappedByteBuffer.create(fileChannel, FileChannel.MapMode.READ_WRITE, 1024, 2048, 10, 0)) {
        assertEquals(0, buffer.position());
        assertEquals(0, buffer.fcPosition());
        assertEquals(0, buffer.nextFcPosition());

        // changing the position only records what the next position will be
        buffer.position(position);
        assertEquals(position, buffer.position());
        assertEquals(0, buffer.fcPosition());
        assertEquals(position, buffer.nextFcPosition());
      }
    }
  }

  @ParameterizedTest(name = "[{index}] {0}")
  @CsvSource({
      "requested less than min,       10, 20, 30,   20",
      "requested equal min,           20, 20, 30,   20",
      "requested between min and max, 20, 10, 30,   20",
      "requested equal max,           20, 10, 20,   20",
      "requested greater than max,    30, 10, 20,   20",
  })
  void calcBufferSize(final String name, final long requested, final long min, final long max, final long expected) {
    assertEquals(expected, MultiplexedMappedByteBuffer.calcBufferSize(requested, min, max));
  }

  @ParameterizedTest(name = "[{index}] {0}")
  @CsvSource({
      "requested less than min / read from 512,         10, 20, 30, 512,      20",
      "requested less than min / read from 1024,        10, 20, 30, 1024,     20",
      "requested equal min / read from 2048,            20, 20, 30, 2048,     20",
      "requested between min and max / read from 512,   20, 10, 30, 512,      20",
      "requested equal max / read from 1024,            20, 10, 20, 1024,     20",
      "requested greater than max / read from 2048,     30, 10, 20, 2048,     20",
  })
  void mapRegion(final String name, final long requested, final long min, final long max, final long position, final long expected) throws IOException {
    final Path path = tempFolder.resolve("mapRegion.bin");
    try (final FileChannel fileChannel = FileChannel.open(path, StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE, StandardOpenOption.READ)) {
      final long bufferSize = MultiplexedMappedByteBuffer.calcBufferSize(requested, min, max);
      final MappedByteBuffer mappedByteBuffer = MultiplexedMappedByteBuffer.mapRegion(fileChannel, FileChannel.MapMode.READ_WRITE, bufferSize, position);
      assertEquals(expected, mappedByteBuffer.capacity());
      assertEquals(0, mappedByteBuffer.position());
      assertEquals(expected, mappedByteBuffer.remaining());

      final byte empty[] = new byte[(int) expected];

      final byte data[] = Arrays.copyOf(empty, empty.length);
      mappedByteBuffer.get(data, 0, (int) expected);

      assertArrayEquals(empty, data);
    }
  }

  @ParameterizedTest(name = "[{index}] {0}")
  @CsvSource({
      "zero capacity,                             0, 10, 0,      true",
      "exact capacity,                            0, 10, 10,     false",
      "under capacity,                            0, 10, 100,    false",
      "over capacity,                             0, 100, 10,    true",
      "negative offset,                           -1, 10, 10,    true",
      "offset at capacity,                        10, 10, 10,    true",
      "offset out-of-bounds,                      11, 10, 10,    true",
      "offset out-of-bounds and over capacity,    11, 11, 10,    true",
  })
  void checkBounds(final String name, final int offset, final int length, final int capacity, final boolean exceptionExpected) {
    assertThrowsOrNot(exceptionExpected, IndexOutOfBoundsException.class, () ->
        MultiplexedMappedByteBuffer.checkBounds(offset, length, capacity)
    );
  }

  // TODO(AR) add further tests for member methods of MultiplexedMappedByteBuffer

  @ParameterizedTest(name = "[{index}] is region from position {0} with capacity {1} before file position {2} equals {3}")
  @CsvSource({
      "0, 0, 0,           false",
      "0, 0, 1,           true",
      "0, 0, 2,           true",
      "0, 0, 512,         true",
      "0, 0, 1024,        true",
      "0, 0, 2048,        true",
      "0, 1, 0,           false",
      "0, 1, 1,           true",
      "0, 1, 2,           true",
      "0, 1, 512,         true",
      "0, 1, 1024,        true",
      "0, 1, 2048,        true",
      "0, 2, 0,           false",
      "0, 2, 1,           false",
      "0, 2, 2,           true",
      "0, 2, 512,         true",
      "0, 2, 1024,        true",
      "0, 2, 2048,        true",
      "0, 512, 0,         false",
      "0, 512, 1,         false",
      "0, 512, 2,         false",
      "0, 512, 512,       true",
      "0, 512, 1024,      true",
      "0, 512, 2048,      true",
      "0, 1024, 0,        false",
      "0, 1024, 1,        false",
      "0, 1024, 2,        false",
      "0, 1024, 512,      false",
      "0, 1024, 1024,     true",
      "0, 1024, 2048,     true",
      "0, 2048, 0,        false",
      "0, 2048, 1,        false",
      "0, 2048, 2,        false",
      "0, 2048, 512,      false",
      "0, 2048, 1024,     false",
      "0, 2048, 2048,     true",

      "1, 0, 0,           false",
      "1, 0, 1,           false",
      "1, 0, 2,           true",
      "1, 0, 512,         true",
      "1, 0, 1024,        true",
      "1, 0, 2048,        true",
      "1, 1, 0,           false",
      "1, 1, 1,           false",
      "1, 1, 2,           true",
      "1, 1, 512,         true",
      "1, 1, 1024,        true",
      "1, 1, 2048,        true",
      "1, 2, 0,           false",
      "1, 2, 1,           false",
      "1, 2, 2,           false",
      "1, 2, 512,         true",
      "1, 2, 1024,        true",
      "1, 2, 2048,        true",
      "1, 512, 0,         false",
      "1, 512, 1,         false",
      "1, 512, 2,         false",
      "1, 512, 512,       false",
      "1, 512, 1024,      true",
      "1, 512, 2048,      true",
      "1, 1024, 0,        false",
      "1, 1024, 1,        false",
      "1, 1024, 2,        false",
      "1, 1024, 512,      false",
      "1, 1024, 1024,     false",
      "1, 1024, 2048,     true",
      "1, 2048, 0,        false",
      "1, 2048, 1,        false",
      "1, 2048, 2,        false",
      "1, 2048, 512,      false",
      "1, 2048, 1024,     false",
      "1, 2048, 2048,     false",

      "2, 0, 0,           false",
      "2, 0, 1,           false",
      "2, 0, 2,           false",
      "2, 0, 512,         true",
      "2, 0, 1024,        true",
      "2, 0, 2048,        true",
      "2, 1, 0,           false",
      "2, 1, 1,           false",
      "2, 1, 2,           false",
      "2, 1, 512,         true",
      "2, 1, 1024,        true",
      "2, 1, 2048,        true",
      "2, 2, 0,           false",
      "2, 2, 1,           false",
      "2, 2, 2,           false",
      "2, 2, 512,         true",
      "2, 2, 1024,        true",
      "2, 2, 2048,        true",
      "2, 512, 0,         false",
      "2, 512, 1,         false",
      "2, 512, 2,         false",
      "2, 512, 512,       false",
      "2, 512, 1024,      true",
      "2, 512, 2048,      true",
      "2, 1024, 0,        false",
      "2, 1024, 1,        false",
      "2, 1024, 2,        false",
      "2, 1024, 512,      false",
      "2, 1024, 1024,     false",
      "2, 1024, 2048,     true",
      "2, 2048, 0,        false",
      "2, 2048, 1,        false",
      "2, 2048, 2,        false",
      "2, 2048, 512,      false",
      "2, 2048, 1024,     false",
      "2, 2048, 2048,     false",

      "512, 0, 0,           false",
      "512, 0, 1,           false",
      "512, 0, 2,           false",
      "512, 0, 512,         false",
      "512, 0, 1024,        true",
      "512, 0, 2048,        true",
      "512, 1, 0,           false",
      "512, 1, 1,           false",
      "512, 1, 2,           false",
      "512, 1, 512,         false",
      "512, 1, 1024,        true",
      "512, 1, 2048,        true",
      "512, 2, 0,           false",
      "512, 2, 1,           false",
      "512, 2, 2,           false",
      "512, 2, 512,         false",
      "512, 2, 1024,        true",
      "512, 2, 2048,        true",
      "512, 512, 0,         false",
      "512, 512, 1,         false",
      "512, 512, 2,         false",
      "512, 512, 512,       false",
      "512, 512, 1024,      true",
      "512, 512, 2048,      true",
      "512, 1024, 0,        false",
      "512, 1024, 1,        false",
      "512, 1024, 2,        false",
      "512, 1024, 512,      false",
      "512, 1024, 1024,     false",
      "512, 1024, 2048,     true",
      "512, 2048, 0,        false",
      "512, 2048, 1,        false",
      "512, 2048, 2,        false",
      "512, 2048, 512,      false",
      "512, 2048, 1024,     false",
      "512, 2048, 2048,     false",

      "1024, 0, 0,           false",
      "1024, 0, 1,           false",
      "1024, 0, 2,           false",
      "1024, 0, 512,         false",
      "1024, 0, 1024,        false",
      "1024, 0, 2048,        true",
      "1024, 1, 0,           false",
      "1024, 1, 1,           false",
      "1024, 1, 2,           false",
      "1024, 1, 512,         false",
      "1024, 1, 1024,        false",
      "1024, 1, 2048,        true",
      "1024, 2, 0,           false",
      "1024, 2, 1,           false",
      "1024, 2, 2,           false",
      "1024, 2, 512,         false",
      "1024, 2, 1024,        false",
      "1024, 2, 2048,        true",
      "1024, 512, 0,         false",
      "1024, 512, 1,         false",
      "1024, 512, 2,         false",
      "1024, 512, 512,       false",
      "1024, 512, 1024,      false",
      "1024, 512, 2048,      true",
      "1024, 1024, 0,        false",
      "1024, 1024, 1,        false",
      "1024, 1024, 2,        false",
      "1024, 1024, 512,      false",
      "1024, 1024, 1024,     false",
      "1024, 1024, 2048,     true",
      "1024, 2048, 0,        false",
      "1024, 2048, 1,        false",
      "1024, 2048, 2,        false",
      "1024, 2048, 512,      false",
      "1024, 2048, 1024,     false",
      "1024, 2048, 2048,     false",

      "2046, 0, 0,           false",
      "2046, 0, 1,           false",
      "2046, 0, 2,           false",
      "2046, 0, 512,         false",
      "2046, 0, 1024,        false",
      "2046, 0, 2048,        true",
      "2046, 1, 0,           false",
      "2046, 1, 1,           false",
      "2046, 1, 2,           false",
      "2046, 1, 512,         false",
      "2046, 1, 1024,        false",
      "2046, 1, 2048,        true",
      "2046, 2, 0,           false",
      "2046, 2, 1,           false",
      "2046, 2, 2,           false",
      "2046, 2, 512,         false",
      "2046, 2, 1024,        false",
      "2046, 2, 2048,        true",
      "2046, 512, 0,         false",
      "2046, 512, 1,         false",
      "2046, 512, 2,         false",
      "2046, 512, 512,       false",
      "2046, 512, 1024,      false",
      "2046, 512, 2048,      false",
      "2046, 1024, 0,        false",
      "2046, 1024, 1,        false",
      "2046, 1024, 2,        false",
      "2046, 1024, 512,      false",
      "2046, 1024, 1024,     false",
      "2046, 1024, 2048,     false",
      "2046, 2048, 0,        false",
      "2046, 2048, 1,        false",
      "2046, 2048, 2,        false",
      "2046, 2048, 512,      false",
      "2046, 2048, 1024,     false",
      "2046, 2048, 2048,     false",

      "2047, 0, 0,           false",
      "2047, 0, 1,           false",
      "2047, 0, 2,           false",
      "2047, 0, 512,         false",
      "2047, 0, 1024,        false",
      "2047, 0, 2048,        true",
      "2047, 1, 0,           false",
      "2047, 1, 1,           false",
      "2047, 1, 2,           false",
      "2047, 1, 512,         false",
      "2047, 1, 1024,        false",
      "2047, 1, 2048,        true",
      "2047, 2, 0,           false",
      "2047, 2, 1,           false",
      "2047, 2, 2,           false",
      "2047, 2, 512,         false",
      "2047, 2, 1024,        false",
      "2047, 2, 2048,        false",
      "2047, 512, 0,         false",
      "2047, 512, 1,         false",
      "2047, 512, 2,         false",
      "2047, 512, 512,       false",
      "2047, 512, 1024,      false",
      "2047, 512, 2048,      false",
      "2047, 1024, 0,        false",
      "2047, 1024, 1,        false",
      "2047, 1024, 2,        false",
      "2047, 1024, 512,      false",
      "2047, 1024, 1024,     false",
      "2047, 1024, 2048,     false",
      "2047, 2048, 0,        false",
      "2047, 2048, 1,        false",
      "2047, 2048, 2,        false",
      "2047, 2048, 512,      false",
      "2047, 2048, 1024,     false",
      "2047, 2048, 2048,     false",

      "2048, 0, 0,           false",
      "2048, 0, 1,           false",
      "2048, 0, 2,           false",
      "2048, 0, 512,         false",
      "2048, 0, 1024,        false",
      "2048, 0, 2048,        false",
      "2048, 1, 0,           false",
      "2048, 1, 1,           false",
      "2048, 1, 2,           false",
      "2048, 1, 512,         false",
      "2048, 1, 1024,        false",
      "2048, 1, 2048,        false",
      "2048, 2, 0,           false",
      "2048, 2, 1,           false",
      "2048, 2, 2,           false",
      "2048, 2, 512,         false",
      "2048, 2, 1024,        false",
      "2048, 2, 2048,        false",
      "2048, 512, 0,         false",
      "2048, 512, 1,         false",
      "2048, 512, 2,         false",
      "2048, 512, 512,       false",
      "2048, 512, 1024,      false",
      "2048, 512, 2048,      false",
      "2048, 1024, 0,        false",
      "2048, 1024, 1,        false",
      "2048, 1024, 2,        false",
      "2048, 1024, 512,      false",
      "2048, 1024, 1024,     false",
      "2048, 1024, 2048,     false",
      "2048, 2048, 0,        false",
      "2048, 2048, 1,        false",
      "2048, 2048, 2,        false",
      "2048, 2048, 512,      false",
      "2048, 2048, 1024,     false",
      "2048, 2048, 2048,     false",

      "2049, 0, 0,           false",
      "2049, 0, 1,           false",
      "2049, 0, 2,           false",
      "2049, 0, 512,         false",
      "2049, 0, 1024,        false",
      "2049, 0, 2048,        false",
      "2049, 1, 0,           false",
      "2049, 1, 1,           false",
      "2049, 1, 2,           false",
      "2049, 1, 512,         false",
      "2049, 1, 1024,        false",
      "2049, 1, 2048,        false",
      "2049, 2, 0,           false",
      "2049, 2, 1,           false",
      "2049, 2, 2,           false",
      "2049, 2, 512,         false",
      "2049, 2, 1024,        false",
      "2049, 2, 2048,        false",
      "2049, 512, 0,         false",
      "2049, 512, 1,         false",
      "2049, 512, 2,         false",
      "2049, 512, 512,       false",
      "2049, 512, 1024,      false",
      "2049, 512, 2048,      false",
      "2049, 1024, 0,        false",
      "2049, 1024, 1,        false",
      "2049, 1024, 2,        false",
      "2049, 1024, 512,      false",
      "2049, 1024, 1024,     false",
      "2049, 1024, 2048,     false",
      "2049, 2048, 0,        false",
      "2049, 2048, 1,        false",
      "2049, 2048, 2,        false",
      "2049, 2048, 512,      false",
      "2049, 2048, 1024,     false",
      "2049, 2048, 2048,     false",

      "2050, 0, 0,           false",
      "2050, 0, 1,           false",
      "2050, 0, 2,           false",
      "2050, 0, 512,         false",
      "2050, 0, 1024,        false",
      "2050, 0, 2048,        false",
      "2050, 1, 0,           false",
      "2050, 1, 1,           false",
      "2050, 1, 2,           false",
      "2050, 1, 512,         false",
      "2050, 1, 1024,        false",
      "2050, 1, 2048,        false",
      "2050, 2, 0,           false",
      "2050, 2, 1,           false",
      "2050, 2, 2,           false",
      "2050, 2, 512,         false",
      "2050, 2, 1024,        false",
      "2050, 2, 2048,        false",
      "2050, 512, 0,         false",
      "2050, 512, 1,         false",
      "2050, 512, 2,         false",
      "2050, 512, 512,       false",
      "2050, 512, 1024,      false",
      "2050, 512, 2048,      false",
      "2050, 1024, 0,        false",
      "2050, 1024, 1,        false",
      "2050, 1024, 2,        false",
      "2050, 1024, 512,      false",
      "2050, 1024, 1024,     false",
      "2050, 1024, 2048,     false",
      "2050, 2048, 0,        false",
      "2050, 2048, 1,        false",
      "2050, 2048, 2,        false",
      "2050, 2048, 512,      false",
      "2050, 2048, 1024,     false",
      "2050, 2048, 2048,     false",
  })
  void regionIsBefore(final long regionFcPosition, final int regionCapacity, final long fcPosition, final boolean expected) {
    final MappedByteBuffer buffer = (MappedByteBuffer) ByteBuffer.allocateDirect(regionCapacity);
    final MultiplexedMappedByteBuffer.Region region = new MultiplexedMappedByteBuffer.Region(regionFcPosition, buffer);
    assertEquals(expected, region.isBefore(fcPosition));
  }

  @ParameterizedTest(name = "[{index}] is region from position {0} with capacity {1} after file position {2} equals {3}")
  @CsvSource({
      "0, 0, 0,           false",
      "0, 0, 1,           false",
      "0, 0, 2,           false",
      "0, 0, 512,         false",
      "0, 0, 1024,        false",
      "0, 0, 2048,        false",
      "0, 1, 0,           false",
      "0, 1, 1,           false",
      "0, 1, 2,           false",
      "0, 1, 512,         false",
      "0, 1, 1024,        false",
      "0, 1, 2048,        false",
      "0, 2, 0,           false",
      "0, 2, 1,           false",
      "0, 2, 2,           false",
      "0, 2, 512,         false",
      "0, 2, 1024,        false",
      "0, 2, 2048,        false",
      "0, 512, 0,         false",
      "0, 512, 1,         false",
      "0, 512, 2,         false",
      "0, 512, 512,       false",
      "0, 512, 1024,      false",
      "0, 512, 2048,      false",
      "0, 1024, 0,        false",
      "0, 1024, 1,        false",
      "0, 1024, 2,        false",
      "0, 1024, 512,      false",
      "0, 1024, 1024,     false",
      "0, 1024, 2048,     false",
      "0, 2048, 0,        false",
      "0, 2048, 1,        false",
      "0, 2048, 2,        false",
      "0, 2048, 512,      false",
      "0, 2048, 1024,     false",
      "0, 2048, 2048,     false",

      "1, 0, 0,           true",
      "1, 0, 1,           false",
      "1, 0, 2,           false",
      "1, 0, 512,         false",
      "1, 0, 1024,        false",
      "1, 0, 2048,        false",
      "1, 1, 0,           true",
      "1, 1, 1,           false",
      "1, 1, 2,           false",
      "1, 1, 512,         false",
      "1, 1, 1024,        false",
      "1, 1, 2048,        false",
      "1, 2, 0,           true",
      "1, 2, 1,           false",
      "1, 2, 2,           false",
      "1, 2, 512,         false",
      "1, 2, 1024,        false",
      "1, 2, 2048,        false",
      "1, 512, 0,         true",
      "1, 512, 1,         false",
      "1, 512, 2,         false",
      "1, 512, 512,       false",
      "1, 512, 1024,      false",
      "1, 512, 2048,      false",
      "1, 1024, 0,        true",
      "1, 1024, 1,        false",
      "1, 1024, 2,        false",
      "1, 1024, 512,      false",
      "1, 1024, 1024,     false",
      "1, 1024, 2048,     false",
      "1, 2048, 0,        true",
      "1, 2048, 1,        false",
      "1, 2048, 2,        false",
      "1, 2048, 512,      false",
      "1, 2048, 1024,     false",
      "1, 2048, 2048,     false",

      "2, 0, 0,           true",
      "2, 0, 1,           true",
      "2, 0, 2,           false",
      "2, 0, 512,         false",
      "2, 0, 1024,        false",
      "2, 0, 2048,        false",
      "2, 1, 0,           true",
      "2, 1, 1,           true",
      "2, 1, 2,           false",
      "2, 1, 512,         false",
      "2, 1, 1024,        false",
      "2, 1, 2048,        false",
      "2, 2, 0,           true",
      "2, 2, 1,           true",
      "2, 2, 2,           false",
      "2, 2, 512,         false",
      "2, 2, 1024,        false",
      "2, 2, 2048,        false",
      "2, 512, 0,         true",
      "2, 512, 1,         true",
      "2, 512, 2,         false",
      "2, 512, 512,       false",
      "2, 512, 1024,      false",
      "2, 512, 2048,      false",
      "2, 1024, 0,        true",
      "2, 1024, 1,        true",
      "2, 1024, 2,        false",
      "2, 1024, 512,      false",
      "2, 1024, 1024,     false",
      "2, 1024, 2048,     false",
      "2, 2048, 0,        true",
      "2, 2048, 1,        true",
      "2, 2048, 2,        false",
      "2, 2048, 512,      false",
      "2, 2048, 1024,     false",
      "2, 2048, 2048,     false",

      "512, 0, 0,           true",
      "512, 0, 1,           true",
      "512, 0, 2,           true",
      "512, 0, 512,         false",
      "512, 0, 1024,        false",
      "512, 0, 2048,        false",
      "512, 1, 0,           true",
      "512, 1, 1,           true",
      "512, 1, 2,           true",
      "512, 1, 512,         false",
      "512, 1, 1024,        false",
      "512, 1, 2048,        false",
      "512, 2, 0,           true",
      "512, 2, 1,           true",
      "512, 2, 2,           true",
      "512, 2, 512,         false",
      "512, 2, 1024,        false",
      "512, 2, 2048,        false",
      "512, 512, 0,         true",
      "512, 512, 1,         true",
      "512, 512, 2,         true",
      "512, 512, 512,       false",
      "512, 512, 1024,      false",
      "512, 512, 2048,      false",
      "512, 1024, 0,        true",
      "512, 1024, 1,        true",
      "512, 1024, 2,        true",
      "512, 1024, 512,      false",
      "512, 1024, 1024,     false",
      "512, 1024, 2048,     false",
      "512, 2048, 0,        true",
      "512, 2048, 1,        true",
      "512, 2048, 2,        true",
      "512, 2048, 512,      false",
      "512, 2048, 1024,     false",
      "512, 2048, 2048,     false",

      "1024, 0, 0,           true",
      "1024, 0, 1,           true",
      "1024, 0, 2,           true",
      "1024, 0, 512,         true",
      "1024, 0, 1024,        false",
      "1024, 0, 2048,        false",
      "1024, 1, 0,           true",
      "1024, 1, 1,           true",
      "1024, 1, 2,           true",
      "1024, 1, 512,         true",
      "1024, 1, 1024,        false",
      "1024, 1, 2048,        false",
      "1024, 2, 0,           true",
      "1024, 2, 1,           true",
      "1024, 2, 2,           true",
      "1024, 2, 512,         true",
      "1024, 2, 1024,        false",
      "1024, 2, 2048,        false",
      "1024, 512, 0,         true",
      "1024, 512, 1,         true",
      "1024, 512, 2,         true",
      "1024, 512, 512,       true",
      "1024, 512, 1024,      false",
      "1024, 512, 2048,      false",
      "1024, 1024, 0,        true",
      "1024, 1024, 1,        true",
      "1024, 1024, 2,        true",
      "1024, 1024, 512,      true",
      "1024, 1024, 1024,     false",
      "1024, 1024, 2048,     false",
      "1024, 2048, 0,        true",
      "1024, 2048, 1,        true",
      "1024, 2048, 2,        true",
      "1024, 2048, 512,      true",
      "1024, 2048, 1024,     false",
      "1024, 2048, 2048,     false",

      "2046, 0, 0,           true",
      "2046, 0, 1,           true",
      "2046, 0, 2,           true",
      "2046, 0, 512,         true",
      "2046, 0, 1024,        true",
      "2046, 0, 2048,        false",
      "2046, 1, 0,           true",
      "2046, 1, 1,           true",
      "2046, 1, 2,           true",
      "2046, 1, 512,         true",
      "2046, 1, 1024,        true",
      "2046, 1, 2048,        false",
      "2046, 2, 0,           true",
      "2046, 2, 1,           true",
      "2046, 2, 2,           true",
      "2046, 2, 512,         true",
      "2046, 2, 1024,        true",
      "2046, 2, 2048,        false",
      "2046, 512, 0,         true",
      "2046, 512, 1,         true",
      "2046, 512, 2,         true",
      "2046, 512, 512,       true",
      "2046, 512, 1024,      true",
      "2046, 512, 2048,      false",
      "2046, 1024, 0,        true",
      "2046, 1024, 1,        true",
      "2046, 1024, 2,        true",
      "2046, 1024, 512,      true",
      "2046, 1024, 1024,     true",
      "2046, 1024, 2048,     false",
      "2046, 2048, 0,        true",
      "2046, 2048, 1,        true",
      "2046, 2048, 2,        true",
      "2046, 2048, 512,      true",
      "2046, 2048, 1024,     true",
      "2046, 2048, 2048,     false",

      "2047, 0, 0,           true",
      "2047, 0, 1,           true",
      "2047, 0, 2,           true",
      "2047, 0, 512,         true",
      "2047, 0, 1024,        true",
      "2047, 0, 2048,        false",
      "2047, 1, 0,           true",
      "2047, 1, 1,           true",
      "2047, 1, 2,           true",
      "2047, 1, 512,         true",
      "2047, 1, 1024,        true",
      "2047, 1, 2048,        false",
      "2047, 2, 0,           true",
      "2047, 2, 1,           true",
      "2047, 2, 2,           true",
      "2047, 2, 512,         true",
      "2047, 2, 1024,        true",
      "2047, 2, 2048,        false",
      "2047, 512, 0,         true",
      "2047, 512, 1,         true",
      "2047, 512, 2,         true",
      "2047, 512, 512,       true",
      "2047, 512, 1024,      true",
      "2047, 512, 2048,      false",
      "2047, 1024, 0,        true",
      "2047, 1024, 1,        true",
      "2047, 1024, 2,        true",
      "2047, 1024, 512,      true",
      "2047, 1024, 1024,     true",
      "2047, 1024, 2048,     false",
      "2047, 2048, 0,        true",
      "2047, 2048, 1,        true",
      "2047, 2048, 2,        true",
      "2047, 2048, 512,      true",
      "2047, 2048, 1024,     true",
      "2047, 2048, 2048,     false",

      "2048, 0, 0,           true",
      "2048, 0, 1,           true",
      "2048, 0, 2,           true",
      "2048, 0, 512,         true",
      "2048, 0, 1024,        true",
      "2048, 0, 2048,        false",
      "2048, 1, 0,           true",
      "2048, 1, 1,           true",
      "2048, 1, 2,           true",
      "2048, 1, 512,         true",
      "2048, 1, 1024,        true",
      "2048, 1, 2048,        false",
      "2048, 2, 0,           true",
      "2048, 2, 1,           true",
      "2048, 2, 2,           true",
      "2048, 2, 512,         true",
      "2048, 2, 1024,        true",
      "2048, 2, 2048,        false",
      "2048, 512, 0,         true",
      "2048, 512, 1,         true",
      "2048, 512, 2,         true",
      "2048, 512, 512,       true",
      "2048, 512, 1024,      true",
      "2048, 512, 2048,      false",
      "2048, 1024, 0,        true",
      "2048, 1024, 1,        true",
      "2048, 1024, 2,        true",
      "2048, 1024, 512,      true",
      "2048, 1024, 1024,     true",
      "2048, 1024, 2048,     false",
      "2048, 2048, 0,        true",
      "2048, 2048, 1,        true",
      "2048, 2048, 2,        true",
      "2048, 2048, 512,      true",
      "2048, 2048, 1024,     true",
      "2048, 2048, 2048,     false",

      "2049, 0, 0,           true",
      "2049, 0, 1,           true",
      "2049, 0, 2,           true",
      "2049, 0, 512,         true",
      "2049, 0, 1024,        true",
      "2049, 0, 2048,        true",
      "2049, 1, 0,           true",
      "2049, 1, 1,           true",
      "2049, 1, 2,           true",
      "2049, 1, 512,         true",
      "2049, 1, 1024,        true",
      "2049, 1, 2048,        true",
      "2049, 2, 0,           true",
      "2049, 2, 1,           true",
      "2049, 2, 2,           true",
      "2049, 2, 512,         true",
      "2049, 2, 1024,        true",
      "2049, 2, 2048,        true",
      "2049, 512, 0,         true",
      "2049, 512, 1,         true",
      "2049, 512, 2,         true",
      "2049, 512, 512,       true",
      "2049, 512, 1024,      true",
      "2049, 512, 2048,      true",
      "2049, 1024, 0,        true",
      "2049, 1024, 1,        true",
      "2049, 1024, 2,        true",
      "2049, 1024, 512,      true",
      "2049, 1024, 1024,     true",
      "2049, 1024, 2048,     true",
      "2049, 2048, 0,        true",
      "2049, 2048, 1,        true",
      "2049, 2049, 2,        true",
      "2049, 2048, 512,      true",
      "2049, 2048, 1024,     true",
      "2049, 2048, 2048,     true",

      "2050, 0, 0,           true",
      "2050, 0, 1,           true",
      "2050, 0, 2,           true",
      "2050, 0, 512,         true",
      "2050, 0, 1024,        true",
      "2050, 0, 2048,        true",
      "2050, 1, 0,           true",
      "2050, 1, 1,           true",
      "2050, 1, 2,           true",
      "2050, 1, 512,         true",
      "2050, 1, 1024,        true",
      "2050, 1, 2048,        true",
      "2050, 2, 0,           true",
      "2050, 2, 1,           true",
      "2050, 2, 2,           true",
      "2050, 2, 512,         true",
      "2050, 2, 1024,        true",
      "2050, 2, 2048,        true",
      "2050, 512, 0,         true",
      "2050, 512, 1,         true",
      "2050, 512, 2,         true",
      "2050, 512, 512,       true",
      "2050, 512, 1024,      true",
      "2050, 512, 2048,      true",
      "2050, 1024, 0,        true",
      "2050, 1024, 1,        true",
      "2050, 1024, 2,        true",
      "2050, 1024, 512,      true",
      "2050, 1024, 1024,     true",
      "2050, 1024, 2048,     true",
      "2050, 2048, 0,        true",
      "2050, 2048, 1,        true",
      "2050, 2048, 2,        true",
      "2050, 2048, 512,      true",
      "2050, 2048, 1024,     true",
      "2050, 2048, 2048,     true",
  })
  void regionIsAfter(final long regionFcPosition, final int regionCapacity, final long fcPosition, final boolean expected) {
    final MappedByteBuffer buffer = (MappedByteBuffer) ByteBuffer.allocateDirect(regionCapacity);
    final MultiplexedMappedByteBuffer.Region region = new MultiplexedMappedByteBuffer.Region(regionFcPosition, buffer);
    assertEquals(expected, region.isAfter(fcPosition));
  }

  @ParameterizedTest(name = "[{index}] is region from position {0} with capacity {1} encompasses file position {2} equals {3}")
  @CsvSource({
      "0, 0, 0,           false",
      "0, 0, 1,           false",
      "0, 0, 2,           false",
      "0, 0, 512,         false",
      "0, 0, 1024,        false",
      "0, 0, 2048,        false",
      "0, 1, 0,           true",
      "0, 1, 1,           false",
      "0, 1, 2,           false",
      "0, 1, 512,         false",
      "0, 1, 1024,        false",
      "0, 1, 2048,        false",
      "0, 2, 0,           true",
      "0, 2, 1,           true",
      "0, 2, 2,           false",
      "0, 2, 512,         false",
      "0, 2, 1024,        false",
      "0, 2, 2048,        false",
      "0, 512, 0,         true",
      "0, 512, 1,         true",
      "0, 512, 2,         true",
      "0, 512, 512,       false",
      "0, 512, 1024,      false",
      "0, 512, 2048,      false",
      "0, 1024, 0,        true",
      "0, 1024, 1,        true",
      "0, 1024, 2,        true",
      "0, 1024, 512,      true",
      "0, 1024, 1024,     false",
      "0, 1024, 2048,     false",
      "0, 2048, 0,        true",
      "0, 2048, 1,        true",
      "0, 2048, 2,        true",
      "0, 2048, 512,      true",
      "0, 2048, 1024,     true",
      "0, 2048, 2048,     false",

      "1, 0, 0,           false",
      "1, 0, 1,           false",
      "1, 0, 2,           false",
      "1, 0, 512,         false",
      "1, 0, 1024,        false",
      "1, 0, 2048,        false",
      "1, 1, 0,           false",
      "1, 1, 1,           true",
      "1, 1, 2,           false",
      "1, 1, 512,         false",
      "1, 1, 1024,        false",
      "1, 1, 2048,        false",
      "1, 2, 0,           false",
      "1, 2, 1,           true",
      "1, 2, 2,           true",
      "1, 2, 512,         false",
      "1, 2, 1024,        false",
      "1, 2, 2048,        false",
      "1, 512, 0,         false",
      "1, 512, 1,         true",
      "1, 512, 2,         true",
      "1, 512, 512,       true",
      "1, 512, 1024,      false",
      "1, 512, 2048,      false",
      "1, 1024, 0,        false",
      "1, 1024, 1,        true",
      "1, 1024, 2,        true",
      "1, 1024, 512,      true",
      "1, 1024, 1024,     true",
      "1, 1024, 2048,     false",
      "1, 2048, 0,        false",
      "1, 2048, 1,        true",
      "1, 2048, 2,        true",
      "1, 2048, 512,      true",
      "1, 2048, 1024,     true",
      "1, 2048, 2048,     true",

      "2, 0, 0,           false",
      "2, 0, 1,           false",
      "2, 0, 2,           false",
      "2, 0, 512,         false",
      "2, 0, 1024,        false",
      "2, 0, 2048,        false",
      "2, 1, 0,           false",
      "2, 1, 1,           false",
      "2, 1, 2,           true",
      "2, 1, 512,         false",
      "2, 1, 1024,        false",
      "2, 1, 2048,        false",
      "2, 2, 0,           false",
      "2, 2, 1,           false",
      "2, 2, 2,           true",
      "2, 2, 512,         false",
      "2, 2, 1024,        false",
      "2, 2, 2048,        false",
      "2, 512, 0,         false",
      "2, 512, 1,         false",
      "2, 512, 2,         true",
      "2, 512, 512,       true",
      "2, 512, 1024,      false",
      "2, 512, 2048,      false",
      "2, 1024, 0,        false",
      "2, 1024, 1,        false",
      "2, 1024, 2,        true",
      "2, 1024, 512,      true",
      "2, 1024, 1024,     true",
      "2, 1024, 2048,     false",
      "2, 2048, 0,        false",
      "2, 2048, 1,        false",
      "2, 2048, 2,        true",
      "2, 2048, 512,      true",
      "2, 2048, 1024,     true",
      "2, 2048, 2048,     true",

      "512, 0, 0,           false",
      "512, 0, 1,           false",
      "512, 0, 2,           false",
      "512, 0, 512,         false",
      "512, 0, 1024,        false",
      "512, 0, 2048,        false",
      "512, 1, 0,           false",
      "512, 1, 1,           false",
      "512, 1, 2,           false",
      "512, 1, 512,         true",
      "512, 1, 1024,        false",
      "512, 1, 2048,        false",
      "512, 2, 0,           false",
      "512, 2, 1,           false",
      "512, 2, 2,           false",
      "512, 2, 512,         true",
      "512, 2, 1024,        false",
      "512, 2, 2048,        false",
      "512, 512, 0,         false",
      "512, 512, 1,         false",
      "512, 512, 2,         false",
      "512, 512, 512,       true",
      "512, 512, 1024,      false",
      "512, 512, 2048,      false",
      "512, 1024, 0,        false",
      "512, 1024, 1,        false",
      "512, 1024, 2,        false",
      "512, 1024, 512,      true",
      "512, 1024, 1024,     true",
      "512, 1024, 2048,     false",
      "512, 2048, 0,        false",
      "512, 2048, 1,        false",
      "512, 2048, 2,        false",
      "512, 2048, 512,      true",
      "512, 2048, 1024,     true",
      "512, 2048, 2048,     true",

      "1024, 0, 0,           false",
      "1024, 0, 1,           false",
      "1024, 0, 2,           false",
      "1024, 0, 512,         false",
      "1024, 0, 1024,        false",
      "1024, 0, 2048,        false",
      "1024, 1, 0,           false",
      "1024, 1, 1,           false",
      "1024, 1, 2,           false",
      "1024, 1, 512,         false",
      "1024, 1, 1024,        true",
      "1024, 1, 2048,        false",
      "1024, 2, 0,           false",
      "1024, 2, 1,           false",
      "1024, 2, 2,           false",
      "1024, 2, 512,         false",
      "1024, 2, 1024,        true",
      "1024, 2, 2048,        false",
      "1024, 512, 0,         false",
      "1024, 512, 1,         false",
      "1024, 512, 2,         false",
      "1024, 512, 512,       false",
      "1024, 512, 1024,      true",
      "1024, 512, 2048,      false",
      "1024, 1024, 0,        false",
      "1024, 1024, 1,        false",
      "1024, 1024, 2,        false",
      "1024, 1024, 512,      false",
      "1024, 1024, 1024,     true",
      "1024, 1024, 2048,     false",
      "1024, 2048, 0,        false",
      "1024, 2048, 1,        false",
      "1024, 2048, 2,        false",
      "1024, 2048, 512,      false",
      "1024, 2048, 1024,     true",
      "1024, 2048, 2048,     true",

      "2046, 0, 0,           false",
      "2046, 0, 1,           false",
      "2046, 0, 2,           false",
      "2046, 0, 512,         false",
      "2046, 0, 1024,        false",
      "2046, 0, 2048,        false",
      "2046, 1, 0,           false",
      "2046, 1, 1,           false",
      "2046, 1, 2,           false",
      "2046, 1, 512,         false",
      "2046, 1, 1024,        false",
      "2046, 1, 2048,        false",
      "2046, 2, 0,           false",
      "2046, 2, 1,           false",
      "2046, 2, 2,           false",
      "2046, 2, 512,         false",
      "2046, 2, 1024,        false",
      "2046, 2, 2048,        false",
      "2046, 512, 0,         false",
      "2046, 512, 1,         false",
      "2046, 512, 2,         false",
      "2046, 512, 512,       false",
      "2046, 512, 1024,      false",
      "2046, 512, 2048,      true",
      "2046, 1024, 0,        false",
      "2046, 1024, 1,        false",
      "2046, 1024, 2,        false",
      "2046, 1024, 512,      false",
      "2046, 1024, 1024,     false",
      "2046, 1024, 2048,     true",
      "2046, 2048, 0,        false",
      "2046, 2048, 1,        false",
      "2046, 2048, 2,        false",
      "2046, 2048, 512,      false",
      "2046, 2048, 1024,     false",
      "2046, 2048, 2048,     true",

      "2047, 0, 0,           false",
      "2047, 0, 1,           false",
      "2047, 0, 2,           false",
      "2047, 0, 512,         false",
      "2047, 0, 1024,        false",
      "2047, 0, 2048,        false",
      "2047, 1, 0,           false",
      "2047, 1, 1,           false",
      "2047, 1, 2,           false",
      "2047, 1, 512,         false",
      "2047, 1, 1024,        false",
      "2047, 1, 2048,        false",
      "2047, 2, 0,           false",
      "2047, 2, 1,           false",
      "2047, 2, 2,           false",
      "2047, 2, 512,         false",
      "2047, 2, 1024,        false",
      "2047, 2, 2048,        true",
      "2047, 512, 0,         false",
      "2047, 512, 1,         false",
      "2047, 512, 2,         false",
      "2047, 512, 512,       false",
      "2047, 512, 1024,      false",
      "2047, 512, 2048,      true",
      "2047, 1024, 0,        false",
      "2047, 1024, 1,        false",
      "2047, 1024, 2,        false",
      "2047, 1024, 512,      false",
      "2047, 1024, 1024,     false",
      "2047, 1024, 2048,     true",
      "2047, 2048, 0,        false",
      "2047, 2048, 1,        false",
      "2047, 2048, 2,        false",
      "2047, 2048, 512,      false",
      "2047, 2048, 1024,     false",
      "2047, 2048, 2048,     true",

      "2048, 0, 0,           false",
      "2048, 0, 1,           false",
      "2048, 0, 2,           false",
      "2048, 0, 512,         false",
      "2048, 0, 1024,        false",
      "2048, 0, 2048,        false",
      "2048, 1, 0,           false",
      "2048, 1, 1,           false",
      "2048, 1, 2,           false",
      "2048, 1, 512,         false",
      "2048, 1, 1024,        false",
      "2048, 1, 2048,        true",
      "2048, 2, 0,           false",
      "2048, 2, 1,           false",
      "2048, 2, 2,           false",
      "2048, 2, 512,         false",
      "2048, 2, 1024,        false",
      "2048, 2, 2048,        true",
      "2048, 512, 0,         false",
      "2048, 512, 1,         false",
      "2048, 512, 2,         false",
      "2048, 512, 512,       false",
      "2048, 512, 1024,      false",
      "2048, 512, 2048,      true",
      "2048, 1024, 0,        false",
      "2048, 1024, 1,        false",
      "2048, 1024, 2,        false",
      "2048, 1024, 512,      false",
      "2048, 1024, 1024,     false",
      "2048, 1024, 2048,     true",
      "2048, 2048, 0,        false",
      "2048, 2048, 1,        false",
      "2048, 2048, 2,        false",
      "2048, 2048, 512,      false",
      "2048, 2048, 1024,     false",
      "2048, 2048, 2048,     true",

      "2049, 0, 0,           false",
      "2049, 0, 1,           false",
      "2049, 0, 2,           false",
      "2049, 0, 512,         false",
      "2049, 0, 1024,        false",
      "2049, 0, 2048,        false",
      "2049, 1, 0,           false",
      "2049, 1, 1,           false",
      "2049, 1, 2,           false",
      "2049, 1, 512,         false",
      "2049, 1, 1024,        false",
      "2049, 1, 2048,        false",
      "2049, 2, 0,           false",
      "2049, 2, 1,           false",
      "2049, 2, 2,           false",
      "2049, 2, 512,         false",
      "2049, 2, 1024,        false",
      "2049, 2, 2048,        false",
      "2049, 512, 0,         false",
      "2049, 512, 1,         false",
      "2049, 512, 2,         false",
      "2049, 512, 512,       false",
      "2049, 512, 1024,      false",
      "2049, 512, 2048,      false",
      "2049, 1024, 0,        false",
      "2049, 1024, 1,        false",
      "2049, 1024, 2,        false",
      "2049, 1024, 512,      false",
      "2049, 1024, 1024,     false",
      "2049, 1024, 2048,     false",
      "2049, 2048, 0,        false",
      "2049, 2048, 1,        false",
      "2049, 2049, 2,        false",
      "2049, 2048, 512,      false",
      "2049, 2048, 1024,     false",
      "2049, 2048, 2048,     false",

      "2050, 0, 0,           false",
      "2050, 0, 1,           false",
      "2050, 0, 2,           false",
      "2050, 0, 512,         false",
      "2050, 0, 1024,        false",
      "2050, 0, 2048,        false",
      "2050, 1, 0,           false",
      "2050, 1, 1,           false",
      "2050, 1, 2,           false",
      "2050, 1, 512,         false",
      "2050, 1, 1024,        false",
      "2050, 1, 2048,        false",
      "2050, 2, 0,           false",
      "2050, 2, 1,           false",
      "2050, 2, 2,           false",
      "2050, 2, 512,         false",
      "2050, 2, 1024,        false",
      "2050, 2, 2048,        false",
      "2050, 512, 0,         false",
      "2050, 512, 1,         false",
      "2050, 512, 2,         false",
      "2050, 512, 512,       false",
      "2050, 512, 1024,      false",
      "2050, 512, 2048,      false",
      "2050, 1024, 0,        false",
      "2050, 1024, 1,        false",
      "2050, 1024, 2,        false",
      "2050, 1024, 512,      false",
      "2050, 1024, 1024,     false",
      "2050, 1024, 2048,     false",
      "2050, 2048, 0,        false",
      "2050, 2048, 1,        false",
      "2050, 2048, 2,        false",
      "2050, 2048, 512,      false",
      "2050, 2048, 1024,     false",
      "2050, 2048, 2048,     false",
  })
  void regionEncompasses(final long regionFcPosition, final int regionCapacity, final long fcPosition, final boolean expected) {
    final MappedByteBuffer buffer = (MappedByteBuffer) ByteBuffer.allocateDirect(regionCapacity);
    final MultiplexedMappedByteBuffer.Region region = new MultiplexedMappedByteBuffer.Region(regionFcPosition, buffer);
    assertEquals(expected, region.encompasses(fcPosition));
  }

  @Test
  void regionUseCount() {
    final ByteBuffer mockBuffer = ByteBuffer.allocateDirect(0);

    // check that useCount is initialised to zero
    final MultiplexedMappedByteBuffer.Region region = new MultiplexedMappedByteBuffer.Region(0, (MappedByteBuffer) mockBuffer);
    assertEquals(0, region.useCount());

    // check that useCount is incremented correctly
    final int count = new Random().nextInt(512);
    for (int i  = 0; i <  count; i++) {
      region.incrementUseCount();
    }
    assertEquals(count, region.useCount());

    // check that useCount cannot exceed Long.MAX_VALUE
    region.setUseCount(Long.MAX_VALUE);
    assertEquals(Long.MAX_VALUE, region.useCount());
    region.incrementUseCount();
    assertEquals(Long.MAX_VALUE, region.useCount());
  }

  @ParameterizedTest
  @CsvSource({
      "1,   0",
      "2,   0",
      "2,   1",
      "4,   0",
      "4,   1",
      "4,   2",
      "4,   3",
      "8,   0",
      "8,   1",
      "8,   2",
      "8,   3",
      "8,   4",
      "8,   5",
      "8,   6",
      "8,   7",
      "16,  0",
      "16,  1",
      "16,  2",
      "16,  3",
      "16,  4",
      "16,  5",
      "16,  6",
      "16,  7",
      "16,  8",
      "16,  9",
      "16,  10",
      "16,  11",
      "16,  12",
      "16,  13",
      "16,  14",
      "16,  15",
  })
  void getLfuRegionIndex(final int numberOfRegions, final int expectedLfuIndex) {
    final MultiplexedMappedByteBuffer.Region regions[] = constructRegionsForLfuTest(numberOfRegions, expectedLfuIndex);
    final int actualLfuIndex = MultiplexedMappedByteBuffer.getLfuRegionIndex(regions, regions.length);
    assertEquals(expectedLfuIndex, actualLfuIndex);
  }

  /**
   * Creates an array of regions, and sets one of the regions to have the smallest use-count (i.e. LFU).
   *
   * @param numberOfRegions the number of regions to construct.
   * @param lfuIndex the index of the region that should be the LFU.
   *
   * @return the constructed regions.
   */
  private MultiplexedMappedByteBuffer.Region[] constructRegionsForLfuTest(final int numberOfRegions, final int lfuIndex) {
    if (lfuIndex < 0 || lfuIndex >= numberOfRegions) {
      throw new IllegalArgumentException("lfuIndex must be within the numberOfRegions");
    }

    final MultiplexedMappedByteBuffer.Region regions[] = new MultiplexedMappedByteBuffer.Region[numberOfRegions];
    final Random random = new Random();
    int smallestUseCount = Integer.MAX_VALUE;

    // assign random use counts to each region, and record the smallest use count
    for (int i = 0; i < numberOfRegions; i++) {
      final int useCount = 1 + random.nextInt(Integer.MAX_VALUE);
      final ByteBuffer buffer = ByteBuffer.allocateDirect(0);
      regions[i] = new MultiplexedMappedByteBuffer.Region(0, (MappedByteBuffer) buffer).setUseCount(useCount);
      if (useCount < smallestUseCount) {
        smallestUseCount = useCount;
      }
    }

    // set the minimum use count at the correct index
    regions[lfuIndex].setUseCount(smallestUseCount - 1);

    return regions;
  }

  @Test
  void getBufferUnderflowException() throws IOException {
    final Path path = tempFolder.resolve("getBufferUnderflowException.bin");

    final long minBufferSize = 1024;

    try (final FileChannel fileChannel = FileChannel.open(path, StandardOpenOption.CREATE_NEW, StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE, StandardOpenOption.READ)) {
      try (final MultiplexedMappedByteBuffer buffer = MultiplexedMappedByteBuffer.create(fileChannel, FileChannel.MapMode.READ_WRITE, minBufferSize, minBufferSize, 10, 0)) {
        assertThrows(BufferUnderflowException.class, () -> {
          // file size is 1024 bytes, so trying to read 1024+1 byte into this array from the file channel should underflow
          final byte data[] = new byte[(int)minBufferSize + 1];
          buffer.get(data);
        });
      }
    }
  }

  @Test
  void getUncheckedIOException1() throws IOException {
    final FileChannel.MapMode mapMode = FileChannel.MapMode.READ_WRITE;
    final long minBufferSize = 1024;
    final long initialPosition = 0;
    final String ioExceptionMessage = "Unable to get size";

    // setup mocks
    final FileChannel mockFileChannel = mock(FileChannel.class);
    final MappedByteBuffer mockMappedByteBuffer = mock(MappedByteBuffer.class);
    when(mockFileChannel.size())
        .thenReturn(0l)
        .thenThrow(new IOException(ioExceptionMessage));
    when(mockFileChannel.map(mapMode, initialPosition, minBufferSize))
        .thenReturn(mockMappedByteBuffer);

    final MultiplexedMappedByteBuffer buffer = MultiplexedMappedByteBuffer.create(mockFileChannel, mapMode, minBufferSize, minBufferSize, 10, initialPosition);
    final UncheckedIOException actualException = assertThrows(UncheckedIOException.class, () -> {
      // second invocation to fileChannel#size() in MultiplexedMappedByteBuffer#get(byte[]) via mock should throw UncheckedIOException
      final byte data[] = new byte[(int)minBufferSize + 1];
      buffer.get(data);
    });
    assertEquals("Unable to determine size of file channel", actualException.getMessage());
    assertEquals(ioExceptionMessage, actualException.getCause().getMessage());
  }

  @Test
  void getUncheckedIOException2() throws IOException {
    final FileChannel.MapMode mapMode = FileChannel.MapMode.READ_WRITE;
    final long minBufferSize = 1024;
    final long initialPosition = 0;
    final String ioExceptionMessage = "Unable to map";

    // setup mocks
    final FileChannel mockFileChannel = mock(FileChannel.class);
    final MappedByteBuffer mockMappedByteBuffer = (MappedByteBuffer) ByteBuffer.allocateDirect((int)minBufferSize);
    when(mockFileChannel.size())
        .thenReturn(2048l);
    when(mockFileChannel.map(mapMode, initialPosition, minBufferSize))
        .thenReturn(mockMappedByteBuffer);
    when(mockFileChannel.map(mapMode, minBufferSize, minBufferSize))
        .thenThrow(new IOException(ioExceptionMessage));

    final MultiplexedMappedByteBuffer buffer = MultiplexedMappedByteBuffer.create(mockFileChannel, mapMode, minBufferSize, minBufferSize, 10, initialPosition);
    final byte data[] = new byte[(int)minBufferSize];
    buffer.get(data);
    final UncheckedIOException actualException = assertThrows(UncheckedIOException.class, () -> {
      // second invocation to fileChannel#map() (via 2nd invocation to buffer#get(byte[])) in MultiplexedMappedByteBuffer#mapRegion(FileChannel, MapMode, long, long, long, long) via mock should throw UncheckedIOException
      buffer.get(data);
    });
    assertEquals("Unable to map a region of the file channel into memory", actualException.getMessage());
    assertEquals(ioExceptionMessage, actualException.getCause().getMessage());
  }

  @Test
  void getIndexOutOfBoundsException() throws IOException {
    final FileChannel.MapMode mapMode = FileChannel.MapMode.READ_WRITE;
    final long minBufferSize = 1024;
    final long initialPosition = 0;

    // setup mocks
    final FileChannel mockFileChannel = mock(FileChannel.class);
    final MappedByteBuffer mockMappedByteBuffer = (MappedByteBuffer) ByteBuffer.allocateDirect((int)minBufferSize);
    when(mockFileChannel.size())
        .thenReturn(2048l);
    when(mockFileChannel.map(mapMode, initialPosition, minBufferSize))
        .thenReturn(mockMappedByteBuffer);

    final MultiplexedMappedByteBuffer buffer = MultiplexedMappedByteBuffer.create(mockFileChannel, mapMode, minBufferSize, minBufferSize, 10, initialPosition);
    final byte data[] = new byte[(int)minBufferSize];
    final IndexOutOfBoundsException actualException = assertThrows(IndexOutOfBoundsException.class, () -> {
      // NOTE(AR) we intentionally poison the fcPosition of the 1st region to check that the sanity check in MultiplexedMappedByteBuffer#get(byte[]) (below) works
      buffer.regions()[0] = new MultiplexedMappedByteBuffer.Region(-1l * (1l + Integer.MAX_VALUE), buffer.regions()[0].buffer);

      // should trigger the sanity check to fail and throw an IndexOutOfBoundsException
      buffer.get(data);
    });
    assertEquals("Region offset is out of bounds", actualException.getMessage());
  }

  /**
   * Creates a test file containing a sequence of length {@code sequenceLength}
   * that is repeated for n {@code iterations}; the total length of the test file
   * is {@code sequenceLength * iterations} bytes.
   *
   * We then read all bytes sequentially in a forward direction,
   * i.e. from start (offset=0) to finish (offset=file.size) from the test
   * file using our {@link MultiplexedMappedByteBuffer} and check that the bytes
   * read are the same as if we had read them using a {@FileInputStream}.
   *
   * We vary a number of parameters around the buffer size and the number of buffers to
   * have open simultaneously.
   */
  @ParameterizedTest(name = "[{index}] getAllSequentialForward(sequenceLength={0}, iterations={1}, minBufferSize={2}, maxBufferSize={3}, maxBuffers={4})")
  @CsvSource({
      "1, 1, 0, 2, 1",
      "1, 1, 1, 1, 1",
      "1, 2, 1, 1, 1",
      "1, 2, 1, 1, 2",

      "2, 2,  1, 4, 2",
      "2, 2,  2, 2, 2",
      "2, 4,  2, 2, 2",
      "2, 2,  2, 2, 4",
      "2, 2,  2, 2, 8",
      "2, 2,  2, 2, 16",
      "2, 2,  2, 2, 32",
      "2, 4,  2, 2, 4",
      "2, 4,  2, 2, 8",
      "2, 4,  2, 2, 16",
      "2, 4,  2, 2, 32",
      "2, 8,  2, 2, 4",
      "2, 8,  2, 2, 8",
      "2, 8,  2, 2, 16",
      "2, 8,  2, 2, 32",
      "2, 16, 2, 2, 4",
      "2, 16, 2, 2, 8",
      "2, 16, 2, 2, 16",
      "2, 16, 2, 2, 32",
      "2, 32, 2, 2, 4",
      "2, 32, 2, 2, 8",
      "2, 32, 2, 2, 16",
      "2, 32, 2, 2, 32",

      "4, 4,  2, 8, 4",
      "4, 4,  4, 4, 4",
      "4, 8,  4, 4, 4",
      "4, 8,  4, 4, 8",
      "4, 8,  4, 4, 16",
      "4, 8,  4, 4, 32",
      "4, 16, 4, 4, 4",
      "4, 16, 4, 4, 8",
      "4, 16, 4, 4, 16",
      "4, 16, 4, 4, 32",
      "4, 32, 4, 4, 4",
      "4, 32, 4, 4, 8",
      "4, 32, 4, 4, 16",
      "4, 32, 4, 4, 32",

      "8, 8,  4, 16, 8",
      "8, 4,  8, 8,  4",
      "8, 8,  8, 8,  4",
      "8, 16, 8, 8,  4",
      "8, 32, 8, 8,  4",
      "8, 4,  8, 8,  8",
      "8, 8,  8, 8,  8",
      "8, 8,  8, 8,  16",
      "8, 8,  8, 8,  32",
      "8, 16, 8, 8,  4",
      "8, 16, 8, 8,  8",
      "8, 16, 8, 8,  16",
      "8, 16, 8, 8,  32",
      "8, 32, 8, 8,  4",
      "8, 32, 8, 8,  8",
      "8, 32, 8, 8,  16",
      "8, 32, 8, 8,  32",

      "16, 16, 8,  32, 16",
      "16, 16, 16, 16, 4",
      "16, 16, 16, 16, 8",
      "16, 16, 16, 16, 16",
      "16, 32, 16, 16, 4",
      "16, 32, 16, 16, 8",
      "16, 32, 16, 16, 16",
      "16, 32, 16, 16, 32",

      "32, 32, 16, 64, 32",
      "32, 32, 32, 32, 32",
      "32, 64, 32, 32, 32",
      "32, 64, 32, 32, 64",
      "32, 64, 32, 32, 128",
      "32, 128, 32, 32, 32",
      "32, 128, 32, 32, 64",
      "32, 128, 32, 32, 128",

      "64, 64,  32, 128, 64",
      "64, 64,  64, 64,  64",
      "64, 128, 64, 64,  64",
      "64, 128, 64, 64,  128",
      "64, 256, 64, 64,  64",
      "64, 256, 64, 64,  128",
      "64, 256, 64, 64,  256",

      "128, 128, 64,  256, 128",
      "128, 128, 128, 128, 128",
      "128, 256, 128, 128, 128",
      "128, 256, 128, 128, 256",
      "128, 512, 128, 128, 128",
      "128, 512, 128, 128, 256",
      "128, 512, 128, 128, 512",

      "256, 256, 128, 512, 256",
      "256, 256, 256, 256, 256",
      "256, 512, 256, 256, 256",
      "256, 512, 256, 256, 512",
  })
  void getAllSequentialForward(final short sequenceLength, final int iterations, final long minBufferSize, final long maxBufferSize, final int maxBuffers) throws IOException {
    // create  a small test data file.
    final byte[] sequence = createSequenceFromZero(sequenceLength);
    final Path sourceFile = writeRepeatingPatternFile("getAllSequentialForward.bin", iterations, sequence);

    final long readBufferSize = MultiplexedMappedByteBuffer.calcBufferSize(sequence.length, minBufferSize, maxBufferSize);
    final long sourceFileSize = Files.size(sourceFile);
    final long expectedRegionSize = MultiplexedMappedByteBuffer.calcBufferSize(sourceFileSize, minBufferSize, maxBufferSize);
    if (expectedRegionSize < readBufferSize) {
      throw new UnsupportedOperationException("This test is not designed to accommodate regions that are smaller than the read buffer");
    }

    // config for MultiplexedMappedByteBuffer
    final long initialPosition = 0;  // start at the start of the file

    // counters
    long expectedTotalBytesRead = 0;
    long actualTotalBytesRead = 0;

    try (final FileChannel fileChannel = FileChannel.open(sourceFile, StandardOpenOption.READ)) {
      try (final MultiplexedMappedByteBuffer buffer = MultiplexedMappedByteBuffer.create(fileChannel, FileChannel.MapMode.READ_ONLY, minBufferSize, maxBufferSize, maxBuffers, initialPosition)) {
        // NOTE(AR) position in file channel does not change when memory-mapping regions
        assertEquals(0, buffer.fileChannel().position());
        assertEquals(initialPosition, buffer.position());
        assertEquals(initialPosition, buffer.fcPosition());
        assertEquals(initialPosition, buffer.nextFcPosition());

        final int regionToReadBufferRatio = (int)expectedRegionSize / (int)readBufferSize;
        // do we need to read more data than that provided by maxBuffers, if so buffers will be evicted and new ones mapped in as we go
        final boolean willEvictRegions = maxBuffers * expectedRegionSize < iterations * readBufferSize;

        // attempt to read blocks of actualBufferSize bytes and check they are the same as the source file
        try (final InputStream expectedIs = Files.newInputStream(sourceFile, StandardOpenOption.READ)) {

          for (int i = 0; i < iterations; i++) {
            // read expected bytes and compare to pattern
            final byte expected[] = new byte[(int) readBufferSize];
            final int expectedBytesRead = expectedIs.read(expected);
            assertEquals(readBufferSize, expectedBytesRead);
            expectedTotalBytesRead += expectedBytesRead;
            assertArrayEquals(sequence, expected);

            // read actual bytes and check position
            assertEquals(i * readBufferSize, buffer.position(), "Buffer position is wrong for iteration before read: " + i);
            final byte actual[] = new byte[(int) readBufferSize];
            buffer.get(actual);
            actualTotalBytesRead += readBufferSize;
            assertEquals((i + 1) * readBufferSize, buffer.position(), "Buffer position is wrong for iteration after read: " + i);

            // compare actual and expected
            assertArrayEquals(expected, actual);

            // check status of region within the MultiplexedMappedByteBuffer
            int expectedActiveRegionIdx = i / regionToReadBufferRatio;
            if (willEvictRegions) {
              // if we need to evict and reuse regions because maxBuffers is not enough to read all bytes without eviction, in a sequential read it will always be the last region that is evicted!
              expectedActiveRegionIdx = Math.min(expectedActiveRegionIdx, maxBuffers - 1);
            }
            assertEquals(expectedActiveRegionIdx, buffer.activeRegionIdx());
            assertEquals(expectedActiveRegionIdx + 1, buffer.usedRegions(), "The number of expected used regions is wrong for read iteration: " + i);
            final MultiplexedMappedByteBuffer.Region region = buffer.regions()[expectedActiveRegionIdx];

            final long expectedRegionStart = ((i - i % regionToReadBufferRatio) * expectedRegionSize) / regionToReadBufferRatio;
            assertEquals(expectedRegionStart, region.fcPositionStart, "The expected Region[" + expectedActiveRegionIdx + "] starts at the wrong position for read iteration: " + i);
            final long expectedRegionEnd = expectedRegionStart + expectedRegionSize - 1;
            assertEquals(expectedRegionEnd, region.fcPositionEnd, "The expected Region[" + expectedActiveRegionIdx + "] ends at the wrong position for read iteration: " + i);
            assertEquals(expectedRegionSize, region.buffer.capacity());
            final long expectedRegionPosition = readBufferSize * ((i % regionToReadBufferRatio) + 1);
            assertEquals(expectedRegionPosition, region.buffer.position(), "The expected Region[" + expectedActiveRegionIdx + "]'s buffer is at the wrong position for read iteration: " + i);
            assertEquals(expectedRegionSize - expectedRegionPosition, region.buffer.remaining());
          }
        }

        // check final status of all regions within the MultiplexedMappedByteBuffer
        int expectedUsedRegions = iterations / regionToReadBufferRatio;
        if (willEvictRegions) {
          // if we need to evict and reuse regions because maxBuffers is not enough to read all bytes without eviction, in a sequential read it will always be the last region that is evicted!
          expectedUsedRegions = Math.min(expectedUsedRegions, maxBuffers);
        }
        assertEquals(expectedUsedRegions, buffer.usedRegions());
        final int expectedActiveRegionIdx = expectedUsedRegions - 1;  // will always be last region when reading forwards through the file
        assertEquals(expectedActiveRegionIdx, buffer.activeRegionIdx());

        for (int i = 0; i < expectedUsedRegions; i++) {
          final MultiplexedMappedByteBuffer.Region region = buffer.regions()[i];
          final long expectedRegionStart;
          if (willEvictRegions && i == expectedUsedRegions - 1) {
            expectedRegionStart = (expectedRegionSize * iterations) - expectedRegionSize;
          } else {
            expectedRegionStart = i * expectedRegionSize;
          }
          assertEquals(expectedRegionStart, region.fcPositionStart, "Region[" + i + "] starts at the wrong position");
          final long expectedRegionEnd;
          if (willEvictRegions && i == expectedUsedRegions - 1) {
            expectedRegionEnd = (expectedRegionSize * iterations) - 1;
          } else {
            expectedRegionEnd = ((i + 1) * expectedRegionSize) - 1;
          }
          assertEquals(expectedRegionEnd, region.fcPositionEnd, "Region[" + i + "] ends at the wrong position");
          assertEquals(expectedRegionSize, region.buffer.capacity());
          assertEquals(expectedRegionSize, region.buffer.position());
          assertEquals(0, region.buffer.remaining());
        }
      }
    }

    // check that we read all bytes available
    assertEquals(Files.size(sourceFile), expectedTotalBytesRead);
    assertEquals(expectedTotalBytesRead, actualTotalBytesRead);
  }

  @ParameterizedTest(name = "[{index}] getAllSequentialBackward(sequenceLength={0}, iterations={1}, minBufferSize={2}, maxBufferSize={3}, maxBuffers={4})")
  @CsvSource({
//      "1, 1, 0, 2, 1",
//      "1, 1, 1, 1, 1",
//      "1, 2, 1, 1, 1",
//      "1, 2, 1, 1, 2",
//
//      "2, 2,  1, 4, 2",
//      "2, 2,  2, 2, 2",
//      "2, 4,  2, 2, 2",
//      "2, 2,  2, 2, 4",
//      "2, 2,  2, 2, 8",
//      "2, 2,  2, 2, 16",
//      "2, 2,  2, 2, 32",
//      "2, 4,  2, 2, 4",
//      "2, 4,  2, 2, 8",
//      "2, 4,  2, 2, 16",
//      "2, 4,  2, 2, 32",
//      "2, 8,  2, 2, 4",
//      "2, 8,  2, 2, 8",
//      "2, 8,  2, 2, 16",
//      "2, 8,  2, 2, 32",
//      "2, 16, 2, 2, 4",
//      "2, 16, 2, 2, 8",
//      "2, 16, 2, 2, 16",
//      "2, 16, 2, 2, 32",
//      "2, 32, 2, 2, 4",
//      "2, 32, 2, 2, 8",
//      "2, 32, 2, 2, 16",
//      "2, 32, 2, 2, 32",
//
//      "4, 4,  2, 8, 4",
//      "4, 4,  4, 4, 4",
//      "4, 8,  4, 4, 4",
//      "4, 8,  4, 4, 8",
//      "4, 8,  4, 4, 16",
//      "4, 8,  4, 4, 32",
//      "4, 16, 4, 4, 4",
//      "4, 16, 4, 4, 8",
//      "4, 16, 4, 4, 16",
//      "4, 16, 4, 4, 32",
//      "4, 32, 4, 4, 4",
//      "4, 32, 4, 4, 8",
//      "4, 32, 4, 4, 16",
//      "4, 32, 4, 4, 32",

//      "8, 8,  4, 16, 8",
      "8, 4,  8, 8,  4",
      "8, 8,  8, 8,  4",
      "8, 16, 8, 8,  4",
      "8, 32, 8, 8,  4",
      "8, 4,  8, 8,  8",
      "8, 8,  8, 8,  8",
      "8, 8,  8, 8,  16",
      "8, 8,  8, 8,  32",
      "8, 16, 8, 8,  4",
      "8, 16, 8, 8,  8",
      "8, 16, 8, 8,  16",
      "8, 16, 8, 8,  32",
      "8, 32, 8, 8,  4",
      "8, 32, 8, 8,  8",
      "8, 32, 8, 8,  16",
      "8, 32, 8, 8,  32",

//      "16, 16, 8,  32, 16",
//      "16, 16, 16, 16, 4",
//      "16, 16, 16, 16, 8",
//      "16, 16, 16, 16, 16",
//      "16, 32, 16, 16, 4",
//      "16, 32, 16, 16, 8",
//      "16, 32, 16, 16, 16",
//      "16, 32, 16, 16, 32",
//
//      "32, 32, 16, 64, 32",
//      "32, 32, 32, 32, 32",
//      "32, 64, 32, 32, 32",
//      "32, 64, 32, 32, 64",
//      "32, 64, 32, 32, 128",
//      "32, 128, 32, 32, 32",
//      "32, 128, 32, 32, 64",
//      "32, 128, 32, 32, 128",
//
//      "64, 64,  32, 128, 64",
//      "64, 64,  64, 64,  64",
//      "64, 128, 64, 64,  64",
//      "64, 128, 64, 64,  128",
//      "64, 256, 64, 64,  64",
//      "64, 256, 64, 64,  128",
//      "64, 256, 64, 64,  256",
//
//      "128, 128, 64,  256, 128",
//      "128, 128, 128, 128, 128",
//      "128, 256, 128, 128, 128",
//      "128, 256, 128, 128, 256",
//      "128, 512, 128, 128, 128",
//      "128, 512, 128, 128, 256",
//      "128, 512, 128, 128, 512",
//
//      "256, 256, 128, 512, 256",
//      "256, 256, 256, 256, 256",
//      "256, 512, 256, 256, 256",
//      "256, 512, 256, 256, 512",

      //"128, 128, 32,  32, 32",

      // TODO(AR) add tests where the region is smaller than the buffer we are reading into - so that we need to map in a new region to fill the buffer in on one read operation.
      // TODO(AR) add tests for non-multiples of two, i.e. read part from buffer and then need to read next part from a new buffer that has to be mapped in
  })
  void getAllSequentialBackward(final short sequenceLength, final int iterations, final long minBufferSize, final long maxBufferSize, final int maxBuffers) throws IOException {
    // create  a small test data file.
    final byte[] sequence = createSequenceFromZero(sequenceLength);
    final Path sourceFile = writeRepeatingPatternFile("getAllSequentialBackward.bin", iterations, sequence);

    final long readBufferSize = MultiplexedMappedByteBuffer.calcBufferSize(sequence.length, minBufferSize, maxBufferSize);
    final long sourceFileSize = Files.size(sourceFile);
    final long expectedRegionSize = MultiplexedMappedByteBuffer.calcBufferSize(sourceFileSize, minBufferSize, maxBufferSize);
    if (expectedRegionSize < readBufferSize) {
      throw new UnsupportedOperationException("This test is not designed to accommodate regions that are smaller than the read buffer");
    }

    // config for MultiplexedMappedByteBuffer
    final long initialPosition = sourceFileSize - expectedRegionSize;  // start at the end of the file less the size of the buffer we want to read

    // counters
    long expectedTotalBytesRead = 0;
    long actualTotalBytesRead = 0;

    try (final FileChannel fileChannel = FileChannel.open(sourceFile, StandardOpenOption.READ)) {
      try (final MultiplexedMappedByteBuffer buffer = MultiplexedMappedByteBuffer.create(fileChannel, FileChannel.MapMode.READ_ONLY, minBufferSize, maxBufferSize, maxBuffers, initialPosition)) {
        // NOTE(AR) position in file channel does not change when memory-mapping regions
        assertEquals(0, buffer.fileChannel().position());
        assertEquals(initialPosition, buffer.position());
        assertEquals(initialPosition, buffer.fcPosition());
        assertEquals(initialPosition, buffer.nextFcPosition());

        final int regionToReadBufferRatio = (int)expectedRegionSize / (int)readBufferSize;
        // do we need to read more data than that provided by maxBuffers, if so buffers will be evicted and new ones mapped in as we go
        final boolean willEvictRegions = maxBuffers * expectedRegionSize < iterations * readBufferSize;

        // attempt to read blocks of actualBufferSize bytes and check they are the same as the source file
        try (final RandomAccessFile expectedIs = new RandomAccessFile(sourceFile.toFile(), "r")) {

          for (int i = 0; i < iterations; i++) {
            // read expected bytes and compare to pattern
            final long expectedReadPosition = sourceFileSize - (readBufferSize * (i + 1));
            expectedIs.seek(expectedReadPosition);
            final byte expected[] = new byte[(int) readBufferSize];
            final int expectedBytesRead = expectedIs.read(expected);
            assertEquals(readBufferSize, expectedBytesRead);
//            if (expectedBytesRead == -1) {
//              break;  // exit the for-loop, we have read all the bytes
//            }
            expectedTotalBytesRead += expectedBytesRead;
            assertArrayEquals(sequence, expected);

            // read actual bytes and check position
            //final long expectedPosition = sourceFileSize - (expectedRegionSize * (i + 1));
            buffer.position(expectedReadPosition);
            assertEquals(expectedReadPosition, buffer.position(), "Buffer position is wrong for iteration before read: " + i);
            final byte actual[] = new byte[(int) readBufferSize];
            buffer.get(actual);
            actualTotalBytesRead += readBufferSize;
            assertEquals(expectedReadPosition + readBufferSize, buffer.position(), "Buffer position is wrong for iteration after read: " + i);

            // compare actual and expected
            assertArrayEquals(expected, actual);

            // check status of region within the MultiplexedMappedByteBuffer
            final int expectedActiveRegionIdx = 0;  // will always be zero when reading backwards through the file
            int expectedUsedRegions = (i / regionToReadBufferRatio) + 1;
            if (willEvictRegions) {
              expectedUsedRegions = Math.min(expectedUsedRegions, maxBuffers);
            }
            assertEquals(expectedActiveRegionIdx, buffer.activeRegionIdx());
            assertEquals(expectedUsedRegions, buffer.usedRegions(), "The number of expected used regions is wrong for read iteration: " + i);
            final MultiplexedMappedByteBuffer.Region region = buffer.regions()[expectedActiveRegionIdx];

//            final long expectedRegionStart = ((i - i % regionToReadBufferRatio) * expectedRegionSize) / regionToReadBufferRatio;
//            final long expectedRegionStart = sourceFileSize - (((i + 1 - i % regionToReadBufferRatio) * expectedRegionSize) / regionToReadBufferRatio);
            //final long expectedRegionStart = sourceFileSize - ((i + 1) * expectedRegionSize);
//            final long expectedRegionStart = sourceFileSize - ((i + 1 - (i % regionToReadBufferRatio)) * expectedRegionSize);
            final long expectedRegionStart = sourceFileSize - (((i / regionToReadBufferRatio) + 1) * expectedRegionSize);
            assertEquals(expectedRegionStart, region.fcPositionStart, "The expected Region[" + expectedActiveRegionIdx + "] starts at the wrong position for read iteration: " + i);
            final long expectedRegionEnd = expectedRegionStart + expectedRegionSize - 1;
            assertEquals(expectedRegionEnd, region.fcPositionEnd, "The expected Region[" + expectedActiveRegionIdx + "] ends at the wrong position for read iteration: " + i);
            assertEquals(expectedRegionSize, region.buffer.capacity());
            final long expectedRegionPosition = expectedRegionSize - (readBufferSize * ((i % regionToReadBufferRatio)));
            assertEquals(expectedRegionPosition, region.buffer.position(), "The expected Region[" + expectedActiveRegionIdx + "]'s buffer is at the wrong position for read iteration: " + i);
            assertEquals(expectedRegionSize - expectedRegionPosition, region.buffer.remaining());
          }
        }

        // check final status of all regions within the MultiplexedMappedByteBuffer
        int expectedUsedRegions = iterations / regionToReadBufferRatio;
        if (willEvictRegions) {
          // if we need to evict and reuse regions because maxBuffers is not enough to read all bytes without eviction, in a sequential read it will always be the last region that is evicted!
          expectedUsedRegions = Math.min(expectedUsedRegions, maxBuffers);
        }
        assertEquals(expectedUsedRegions, buffer.usedRegions());
        final int expectedActiveRegionIdx = 0;  // will always be zero when reading backwards through the file
        assertEquals(expectedActiveRegionIdx, buffer.activeRegionIdx());

        for (int i = 0; i < expectedUsedRegions; i++) {
          final MultiplexedMappedByteBuffer.Region region = buffer.regions()[i];

          // when reading the file backwards (i.e. end to start, the region cache after use will always have the regions from the start of the file to regionSize * maxBuffers
          final long expectedRegionStart = i * expectedRegionSize;
          assertEquals(expectedRegionStart, region.fcPositionStart, "Region[" + i + "] starts at the wrong position");
          final long expectedRegionEnd = ((i + 1) * expectedRegionSize) - 1;
          assertEquals(expectedRegionEnd, region.fcPositionEnd, "Region[" + i + "] ends at the wrong position");
          assertEquals(expectedRegionSize, region.buffer.capacity());
          assertEquals(expectedRegionSize, region.buffer.position());
          assertEquals(0, region.buffer.remaining());
        }
      }
    }

    // check that we read all bytes available
    assertEquals(Files.size(sourceFile), expectedTotalBytesRead);
    assertEquals(expectedTotalBytesRead, actualTotalBytesRead);
  }

  /**
   * Creates a byte array of a specific length.
   * The first array entry is 0 and, and each subsequent entry adds 1.
   *
   * @param length the length of the array.
   */
  private static byte[] createSequenceFromZero(final short length) {
    if (length < 0 || length > 256) {
      throw new IllegalArgumentException("Length must be between 0 and 256 inclusive");
    }

    final byte pattern[] = new byte[length];
    for (int i = 0; i < length; i++) {
      pattern[i] = (byte) i;
    }

    return pattern;
  }

  private Path writeRepeatingPatternFile(final String fileName, final int iterations, final byte[] pattern) throws IOException {
    final Path path = tempFolder.resolve(fileName);
    try (final OutputStream os = new BufferedOutputStream(Files.newOutputStream(path, StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE))) {
      for (int i = 0; i < iterations; i++) {
        os.write(pattern);
      }
    }
    return path;
  }

  @Test
  void createFileNonZeroPosition() throws IOException {
    final Path path = tempFolder.resolve("createFileNonZeroPosition.bin");

    try (final FileChannel fileChannel = FileChannel.open(path, StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE, StandardOpenOption.READ)) {

      try (final MultiplexedMappedByteBuffer buffer = MultiplexMappedByteBuffer.fileChannel(fileChannel)
          .mapMode(FileChannel.MapMode.READ_WRITE)
          .initialPosition(8)
          .build()) {

        // TODO(AR) implement multi-byte buf - but before that write the tests for read(byte[], int, int)
        buffer.put((byte) 'a');

        // TODO(AR) shouldn't the put above have advanced the position? check if that should be so and possibly fix it...

        assertEquals(9, buffer.position());
      }
    }
  }

  private static <T extends Throwable> void assertThrowsOrNot(final boolean mustThrow, final Class<T> expectedType, final Executable executable) {
    if (mustThrow) {
      assertThrows(expectedType, executable);
    } else {
      assertDoesNotThrow(executable);
    }
  }
}
