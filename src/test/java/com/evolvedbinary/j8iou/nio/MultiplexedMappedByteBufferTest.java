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
