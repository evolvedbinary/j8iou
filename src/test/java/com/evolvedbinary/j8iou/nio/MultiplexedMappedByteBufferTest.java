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

import java.io.IOException;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class MultiplexedMappedByteBufferTest {

  @TempDir
  Path tempFolder;

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

  // TODO(AR) add further tests for Region and its methods

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
