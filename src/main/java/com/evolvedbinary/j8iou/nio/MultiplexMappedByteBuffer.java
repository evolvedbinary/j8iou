package com.evolvedbinary.j8iou.nio;

import net.jcip.annotations.NotThreadSafe;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;

/**
 * Just a simple builder for {@link MultiplexedMappedByteBuffer}.
 *
 * @author <a href="adam@evolvedbinary.com">Adam Retter</a>
 */
@NotThreadSafe
public class MultiplexMappedByteBuffer {

  public static final long DEFAULT_MIN_BUFFER_SIZE = 64 * 1024 * 1024;    // 64 MB
  public static final long DEFAULT_MAX_BUFFER_SIZE = 512 * 1024 * 1024;   // 512 MB

  private final FileChannel fileChannel;
  private MapMode mapMode = MapMode.READ_ONLY;
  private long minBufferSize = DEFAULT_MIN_BUFFER_SIZE;
  private long maxBufferSize = DEFAULT_MAX_BUFFER_SIZE;
  private int maxBuffers = 16;
  private long initialPosition = 0;

  /**
   * Intentionally private constructor to facilitate builder pattern.
   *
   * @param fileChannel the file channel from which to create memory mapped buffers.
   */
  private MultiplexMappedByteBuffer(final FileChannel fileChannel) {
    this.fileChannel = fileChannel;
  }

  /**
   * Start creating a {@link MultiplexedMappedByteBuffer} for a file channel.
   *
   * @param fileChannel the file channel from which to create memory mapped buffers.
   *
   * @return the builder instance.
   */
  public static MultiplexMappedByteBuffer fileChannel(final FileChannel fileChannel) {
    return new MultiplexMappedByteBuffer(fileChannel);
  }

  /**
   * Set the mapping mode of the file.
   *
   * @param mapMode the mapping mode of the file.
   *
   * @return the builder instance.
   */
  public MultiplexMappedByteBuffer mapMode(final MapMode mapMode) {
    this.mapMode = mapMode;
    return this;
  }

  /**
   * Set the minimum size of a single memory mapped buffer.
   *
   * @param minBufferSize the minimum size of a single memory mapped buffer.
   *
   * @return the builder instance.
   */
  public MultiplexMappedByteBuffer minBufferSize(final long minBufferSize) {
    this.minBufferSize = minBufferSize;
    return this;
  }

  /**
   * Set the maximum size of a single memory mapped buffer.
   *
   * @param maxBufferSize the maximum size of a single memory mapped buffer.
   *
   * @return the builder instance.
   */
  public MultiplexMappedByteBuffer maxBufferSize(final long maxBufferSize) {
    this.maxBufferSize = maxBufferSize;
    return this;
  }

  /**
   * Set the maximum number of backing buffers to keep open at any one time.
   *
   * @param maxBuffers the maximum number of backing buffers to keep open at any one time.
   *
   * @return the builder instance.
   */
  public MultiplexMappedByteBuffer maxBuffers(final int maxBuffers) {
    this.maxBuffers = maxBuffers;
    return this;
  }

  /**
   * Set the initial position in the file to start reading/writing.
   *
   * @param initialPosition the initial position in the file to start reading/writing.
   *
   * @return the builder instance.
   */
  public MultiplexMappedByteBuffer initialPosition(final long initialPosition) {
    this.initialPosition = initialPosition;
    return this;
  }

  /**
   * Build a new MultiplexedMappedByteBuffer.
   *
   * @return a MultiplexedMappedByteBuffer.
   */
  public MultiplexedMappedByteBuffer build() throws IOException {
    return MultiplexedMappedByteBuffer.create(fileChannel, mapMode, minBufferSize, maxBufferSize, maxBuffers, initialPosition);
  }
}
