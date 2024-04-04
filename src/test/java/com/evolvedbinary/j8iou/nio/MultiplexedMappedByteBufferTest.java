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
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.*;

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
        assertEquals(0, buffer.position());
        assertEquals(0, buffer.fcPosition());
        assertEquals(0, buffer.nextFcPosition());

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
        assertEquals(0, firstActualRegion.fcPositionStart);
        assertEquals(minBufferSize - 1, firstActualRegion.fcPositionEnd);
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
      final MappedByteBuffer mappedByteBuffer = MultiplexedMappedByteBuffer.mapRegion(fileChannel, FileChannel.MapMode.READ_WRITE, requested, min, max, position);
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

  @Test
  void getSequentialFixedSize() throws IOException {
    // create  a small test data file.
    final byte[] pattern = {0x1, 0x2, 0x3, 0x4, 0x5, 0x6, 0x7, 0x8};
    final int iterations = 8;
    final Path sourceFile = writeRepeatingPatternFile("getSequential.bin", iterations, pattern);

    // config for MultiplexedMappedByteBuffer
    final long minBufferSize = pattern.length;
    final long maxBufferSize = pattern.length;
    final int maxBuffers = iterations;
    final int initialPosition = 0;

    final long actualBufferSize = MultiplexedMappedByteBuffer.calcBufferSize(pattern.length, minBufferSize, maxBufferSize);

    try (final FileChannel fileChannel = FileChannel.open(sourceFile, StandardOpenOption.READ)) {
      try (final MultiplexedMappedByteBuffer buffer = MultiplexedMappedByteBuffer.create(fileChannel, FileChannel.MapMode.READ_ONLY, minBufferSize, maxBufferSize, maxBuffers, initialPosition)) {
        assertEquals(0, buffer.position());
        assertEquals(0, buffer.fcPosition());
        assertEquals(0, buffer.nextFcPosition());

        // attempt to read blocks of 8 bytes and check they are the same as the source file
        try (final InputStream expectedIs = Files.newInputStream(sourceFile, StandardOpenOption.READ)) {

          for (int i = 0; i < iterations; i++) {
            // read expected bytes and compare to pattern
            final byte expected[] = new byte[(int) actualBufferSize];
            expectedIs.read(expected);
            assertArrayEquals(pattern, expected);

            // read actual bytes and check position
            final byte actual[] = new byte[(int) actualBufferSize];
            buffer.get(actual);
            assertEquals((i + 1) * actualBufferSize, buffer.position(), "Position is wrong for iteration: " + i);

            // compare actual and expected
            assertArrayEquals(expected, actual);

            // check status of region within the MultiplexedMappedByteBuffer
            assertEquals(i + 1, buffer.usedRegions());
            assertEquals(i, buffer.activeRegionIdx());
            final MultiplexedMappedByteBuffer.Region region = buffer.regions()[i];
            assertEquals(i * actualBufferSize, region.fcPositionStart);
            assertEquals(((i + 1) * actualBufferSize) - 1, region.fcPositionEnd);
            assertEquals(actualBufferSize, region.buffer.capacity());
            assertEquals(actualBufferSize, region.buffer.position());
            assertEquals(0, region.buffer.remaining());
          }
        }

        // check final status of regions within the MultiplexedMappedByteBuffer
        assertEquals(iterations, buffer.usedRegions());
        assertEquals(iterations - 1, buffer.activeRegionIdx());
        for (int i = 0; i < iterations; i++) {
          final MultiplexedMappedByteBuffer.Region region = buffer.regions()[i];
          assertEquals(i * actualBufferSize, region.fcPositionStart);
          assertEquals(((i + 1) * actualBufferSize) - 1, region.fcPositionEnd);
          assertEquals(actualBufferSize, region.buffer.capacity());
          assertEquals(actualBufferSize, region.buffer.position());
          assertEquals(0, region.buffer.remaining());
        }
      }
    }
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
