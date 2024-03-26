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

import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;

import static org.junit.Assert.assertEquals;

public class MultiplexedMappedByteBufferTest {

  @ClassRule public static final TemporaryFolder TEMP_FOLDER = new TemporaryFolder();

  @Test
  public void calcBufferSize() {
    assertEquals(20, MultiplexedMappedByteBuffer.calcBufferSize(10, 20, 30));
    assertEquals(20, MultiplexedMappedByteBuffer.calcBufferSize(20, 20, 30));

    assertEquals(10, MultiplexedMappedByteBuffer.calcBufferSize(10, 5, 30));
    assertEquals(5, MultiplexedMappedByteBuffer.calcBufferSize(5, 5, 30));

    assertEquals(30, MultiplexedMappedByteBuffer.calcBufferSize(40, 20, 30));
    assertEquals(30, MultiplexedMappedByteBuffer.calcBufferSize(30, 20, 30));
  }

  @Test
  public void createFileNonZeroPosition() throws IOException {
    //final Path path = TEMP_FOLDER.getRoot().toPath().resolve("createFileNonZeroPosition.bin");
    final Path path = Paths.get("/tmp").resolve("createFileNonZeroPosition.bin");
    try (final FileChannel fileChannel = FileChannel.open(path, StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE, StandardOpenOption.READ)) {

      try (final MultiplexedMappedByteBuffer buffer = MultiplexMappedByteBuffer.fileChannel(fileChannel)
          .mapMode(FileChannel.MapMode.READ_WRITE)
          .initialPosition(8)
          .build()) {

        buffer.put((byte)'a');

        System.out.println(buffer.position());

      }
    }
    System.out.println(path);
  }
}
