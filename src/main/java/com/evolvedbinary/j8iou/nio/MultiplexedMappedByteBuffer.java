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

import net.jcip.annotations.NotThreadSafe;
import sun.nio.ch.DirectBuffer;

import javax.annotation.Nullable;
import java.io.Closeable;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.BufferUnderflowException;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.util.Arrays;

/**
 * Provides a {@link MappedByteBuffer} like interface to
 * a file channel.
 *
 * Internally maintains one or more Regions (MappedByteBuffers) and
 * appears to provide unlimited memory-mapped access to
 * the file:
 *   1. You do not need to call {@link FileChannel#map(MapMode, long, long)}
 *     as new mappings are created dynamically for you as you read, write,
 *     and re-position.
 *  2. A cache of the most recently mapped file regions are kept open
 *     to aid performance of random-access requests.
 *
 * @author <a href="adam@evolvedbinary.com">Adam Retter</a>
 */
@NotThreadSafe
public class MultiplexedMappedByteBuffer implements Closeable {

  private @Nullable FileChannel fileChannel;
  private final MapMode mapMode;
  private final long minBufferSize;
  private final long maxBufferSize;

  // TODO(AR) consider replacing regions and activeRegionIdx with a Linked List of Region (may give before and after for free), also would it perhaps be more performant than array indexed lookup?

  /**
   * Regions are always ordered in ascending order of their position within the underlying file channel.
   */
  private final Region[] regions;
  private int usedRegions = 0;
  private int activeRegionIdx = 0;

  /**
   * Position within the underlying file channel.
   */
  private long fcPosition = 0;

  /**
   * This is the position that has been requested by the
   * user calling {@link #position(long)}.
   */
  private long nextFcPosition = 0;

  /**
   * Intentionally private constructor, use {@link #create(FileChannel, MapMode, long, long, int, long)}
   * to construct a new instance.
   *
   * @param fileChannel the file channel from which to create memory mapped buffers.
   * @param mapMode the mapping mode of the file.
   * @param minBufferSize the minimum size of a single memory mapped buffer.
   * @param maxBufferSize the maximum size of a single memory mapped buffer
   * @param maxBuffers the maximum number of backing buffers to keep open at any one time.
   * @param initialPosition the initial position of the buffer within the file channel.
   * @param initialBuffer the initial mapped byte buffer.
   */
  private MultiplexedMappedByteBuffer(final FileChannel fileChannel, final MapMode mapMode,
      final long minBufferSize, final long maxBufferSize, final int maxBuffers, final long initialPosition, final MappedByteBuffer initialBuffer) {
    this.fileChannel = fileChannel;
    this.mapMode = mapMode;
    this.minBufferSize = minBufferSize;
    this.maxBufferSize = maxBufferSize;
    this.regions = new Region[maxBuffers];
    this.activeRegionIdx = usedRegions++;
    this.regions[activeRegionIdx] = new Region(initialPosition, initialBuffer);

    this.fcPosition = initialPosition;
    this.nextFcPosition = this.fcPosition;
  }

  /**
   * Create a new multiplexed mapped byte buffer.
   *
   * @param fileChannel the file channel from which to create memory mapped buffers.
   * @param mapMode the mapping mode of the file.
   * @param minBufferSize the minimum size of a single memory mapped buffer.
   * @param maxBufferSize the maximum size of a single memory mapped buffer
   * @param maxBuffers the maximum number of backing buffers to keep open at any one time.
   * @param initialPosition the initial position in the file to start reading/writing.
   */
  public static MultiplexedMappedByteBuffer create(final FileChannel fileChannel, final MapMode mapMode,
      final long minBufferSize, final long maxBufferSize, final int maxBuffers,
      final long initialPosition) throws IOException {

    if (initialPosition < 0) {
      throw new IllegalArgumentException("Initial position must be a positive integer");
    }

    // attempt to map the first buffer from the initial position, if there is a problem, an IOException will be thrown
    final long bufferSize = calcBufferSize(fileChannel.size(), minBufferSize, maxBufferSize);
    final MappedByteBuffer initialBuffer = mapRegion(fileChannel, mapMode, bufferSize, initialPosition);

    // NOTE(AR) we use fileChannel.position() below instead of initialPosition as a new file will always start from zero, otherwise if it is an existing file this will be correct anyway
    return new MultiplexedMappedByteBuffer(fileChannel, mapMode, minBufferSize, maxBufferSize, maxBuffers, initialPosition, initialBuffer);
  }

  /**
   * Maps a region of a file channel into memory.
   *
   * @param fileChannel the file channel from which to map a region.
   * @param mapMode the mapping mode of the region.
   * @param bufferSize the size of the memory mapped region
   * @param position the position in the file that the mapped region starts from.
   *
   * @throws IOException if we cannot map the region from the file channel into memory.
   */
  static MappedByteBuffer mapRegion(final FileChannel fileChannel, final MapMode mapMode, final long bufferSize, final long position) throws IOException {
    return fileChannel.map(mapMode, position, bufferSize);
  }

  /**
   * Calculate a buffer size.
   *
   * <pre>
   * {@code
   * if (min > requested)
   *   return min;
   * else if (max < requested)
   *   return max;
   * else
   *   return requested;
   * }</pre>
   *
   * @param requested the requested size
   * @param min the minimum size
   * @param max the maximum size
   */
  static long calcBufferSize(final long requested, final long min, final long max) {
    return Math.min(Math.max(requested, min), max);
  }

  /**
   * Returns this buffer's position.
   *
   * @return The position of this buffer.
   */
  public final long position() {
    return nextFcPosition;
  }

  /**
   * Repositions the buffer within underlying file channel.
   *
   * Positioning occurs lazily on the next put/get call.
   *
   * @param newPosition The new position value; must be non-negative.
   *
   * @return This buffer.
   */
  public MultiplexedMappedByteBuffer position(final long newPosition) {
    if (newPosition < 0) {
      throw new IllegalArgumentException("New position must be a positive integer");
    }

    this.nextFcPosition = newPosition;

    return this;
  }

  /**
   * Relative bulk <i>get</i> method.
   *
   * <p> This method transfers bytes from this buffer into the given
   * destination array. An invocation of this method of the form
   * {@code src.get(a)} behaves in exactly the same way as the
   * invocation: {@code src.get(a, 0, a.length)}.
   *
   * @param dst The destination array.
   *
   * @return This buffer.
   *
   * @throws BufferUnderflowException If there are fewer than
   *     <tt>length</tt> bytes remaining in this buffer.
   */
  public MultiplexedMappedByteBuffer get(final byte[] dst) {
    return get(dst, 0, dst.length);
  }

  /**
   * Relative bulk <i>get</i> method.
   *
   * <p> This method transfers bytes from this buffer into the given
   * destination array.  If there are fewer bytes remaining in the
   * buffer than are required to satisfy the request, that is, if
   * <tt>length</tt>&nbsp;<tt>&gt;</tt>&nbsp;<tt>remaining()</tt>, then no
   * bytes are transferred and a {@link BufferUnderflowException} is
   * thrown.
   *
   * <p> Otherwise, this method copies <tt>length</tt> bytes from this
   * buffer into the given array, starting at the current position of this
   * buffer and at the given offset in the array.  The position of this
   * buffer is then incremented by <tt>length</tt>.
   *
   * <p> In other words, an invocation of this method of the form
   * <tt>src.get(dst,&nbsp;off,&nbsp;len)</tt> has exactly the same effect as
   * the loop:
   * <pre>{@code
   *     for (int i = off; i < off + len; i++)
   *         dst[i] = src.get():
   * }</pre>.
   *
   * Except that it first checks that there are sufficient bytes in
   * this buffer, and it is potentially much more efficient.
   *
   * @param dst The array into which bytes are to be written.
   * @param offset The offset within the array of the first byte to be written;
   *   must be non-negative and no larger than {@code dst.length}.
   *
   * @param length The maximum number of bytes to be written to the given array;
   *     must be non-negative and no larger than {@code dst.length - offset}.
   *
   * @return This buffer
   *
   * @throws BufferUnderflowException If there are fewer than <tt>length</tt>
   *     bytes remaining in this buffer.
   *
   * @throws IndexOutOfBoundsException If the preconditions on the <tt>offset</tt>
   *     and <tt>length</tt> parameters do not hold.
   */
  public MultiplexedMappedByteBuffer get(final byte[] dst, final int offset, final int length) {
    checkBounds(offset, length, dst.length);

    try {
      if (length > fileChannel.size() - nextFcPosition) {
        throw new BufferUnderflowException();
      }
    } catch (final IOException e) {
      throw new UncheckedIOException("Unable to determine size of file channel", e);
    }

    getInternal(dst, offset, length);

    return this;
  }

  private void getInternal(final byte[] dst, final int offset, final int length) {
    // 1. Handle re-positioning across regions (if needed)
    int regionIdx = getRegionIdxForPosition();
    if (regionIdx == -1) {
      // 2. Could not find a region encapsulating the position, map a new one...
      try {
        regionIdx = mapRegionForPosition();
      } catch (final IOException e) {
        throw new UncheckedIOException("Unable to map a region of the file channel into memory", e);
      }
    }

    // 3. Update position
    this.fcPosition = this.nextFcPosition;
    this.activeRegionIdx = regionIdx;

    // 4. Read from the buffer of the underlying region
    final Region region = regions[activeRegionIdx];
    final long regionOffset = fcPosition - region.fcPositionStart;
    // sanity check
    if (regionOffset > Integer.MAX_VALUE) {
      throw new IndexOutOfBoundsException("Region offset is out of bounds");
    }
    region.buffer.position((int) regionOffset);
    final int readLength = Math.min(region.buffer.remaining(), length);
    region.buffer.get(dst, offset, readLength);

    // 5. record the buffer read for LFU
    region.incrementUseCount();

    // 6. Advance position by number of bytes read
    final int remainingReadLength = length - readLength;
    this.nextFcPosition += (readLength - remainingReadLength);

    // 7. if we read less than length bytes from this region, read again the remaining bytes (from the next region(s))
    if (remainingReadLength > 0) {
      getInternal(dst, offset + readLength, remainingReadLength);
    }
  }

  /**
   * Find the region index for the position.
   *
   * @return the region index, or -1 if a region encapsulating
   *   the position cannot be found.
   */
  private int getRegionIdxForPosition() {
    if (nextFcPosition > fcPosition) {
      // We look forward...
      for (int i = activeRegionIdx; i < usedRegions; i++) {
        if (regions[i].encompasses(nextFcPosition)) {
          return i;
        }
      }

    } else if (nextFcPosition < fcPosition) {
      // We look backward...
      for (int i = activeRegionIdx; i > -1; i--) {
        if (regions[i].encompasses(nextFcPosition)) {
          return i;
        }
      }

    } else {
      // position has not changed, so assume the same region index
      return activeRegionIdx;
    }

    // required position not found in `regions`
    return -1;
  }

  /**
   * Map a region for the next position into memory.
   *
   * @throws IOException if we cannot map the region from the file channel into memory.
   */
  private int mapRegionForPosition() throws IOException {
    // Do we have space for a new region to be mapped?
    if (usedRegions == regions.length) {
      // no, so we need to evict the LFU region
      evictLfuRegion();
    }

    // yes, find the closest regions that occur before and after the requested position, so we can determine where to insert it and calculate the size of the new region that we need to map
    final int beforeRegionIdx = getClosestRegionIdxBeforePosition();
    final int afterRegionIdx = getClosestRegionIdxAfterPosition();

    // sanity check - before and after regions should have consecutive indexes
    if (beforeRegionIdx != -1 && afterRegionIdx != -1 && afterRegionIdx - beforeRegionIdx != 1) {
      throw new IllegalStateException("Unable to find insertion point in regions for new mapping a new region");
    }

    // calc the maximum requestable space between the needed position and the start of the next region
    final long maxRequestableSpace;
    if (afterRegionIdx == -1) {
      maxRequestableSpace = maxBufferSize;
    } else {
      maxRequestableSpace = regions[afterRegionIdx].fcPositionStart - nextFcPosition;

      // right shift each item in the regions array by one from afterRegionIdx
      final int minBeforeRegionIdx = Math.max(0, beforeRegionIdx); // NOTE(AR) this guards against beforeRegionIdx being -1 (i.e. there is no existing region before the region we want to insert)
      for (int i = usedRegions; i > minBeforeRegionIdx; i--) {
        regions[i] = regions[i - 1];
      }
    }

    // record that we now have a new region
    usedRegions++;

    // set the value of the new region
    final long min = Math.min(minBufferSize, maxRequestableSpace);
    final long bufferSize = calcBufferSize(maxRequestableSpace, minBufferSize, maxBufferSize);
    final MappedByteBuffer buffer = mapRegion(fileChannel, mapMode, bufferSize, nextFcPosition);
    final int newRegionIdx = beforeRegionIdx + 1;
    regions[newRegionIdx] = new Region(nextFcPosition, buffer);

    // return the index of the new region
    return newRegionIdx;
  }

  /**
   * Get the index of the region that is immediately before the position.
   *
   * @return the index of the closest region before the position, or -1 if no such region can be found.
   */
  private int getClosestRegionIdxBeforePosition() {
    int closestBeforeRegionIdx = -1;
    if (nextFcPosition > fcPosition) {
      // We look forward...
      for (int i = activeRegionIdx; i < usedRegions; i++) {
        if (regions[i].isBefore(nextFcPosition)) {
          closestBeforeRegionIdx = i;
        } else {
          break;
        }
      }

    } else if (nextFcPosition <= fcPosition) {
      // We look backward...
      for (int i = activeRegionIdx; i > -1; i--) {
        if (regions[i].isBefore(nextFcPosition)) {
          closestBeforeRegionIdx = i;
          break;
        }
      }
    }

    return closestBeforeRegionIdx;
  }

  /**
   * Get the index of the region that is immediately after the position.
   *
   * @return the index of the closest region after the position, or -1 if no such region can be found.
   */
  private int getClosestRegionIdxAfterPosition() {
    int closestAfterRegionIdx = -1;
    if (nextFcPosition >= fcPosition) {
      // We look forward...
      for (int i = activeRegionIdx; i < usedRegions; i++) {
        if (regions[i].isAfter(nextFcPosition)) {
          closestAfterRegionIdx = i;
          break;
        }
      }

    } else if (nextFcPosition < fcPosition) {
      // We look backward...
      for (int i = activeRegionIdx; i > -1; i--) {
        if (regions[i].isAfter(nextFcPosition)) {
          closestAfterRegionIdx = i;
        } else {
          break;
        }
      }
    }

    return closestAfterRegionIdx;
  }

  void evictLfuRegion() {
    // find the LFU Region
    final int lfuRegionIdx = getLfuRegionIndex(regions, usedRegions);
    final Region lfuRegion = regions[lfuRegionIdx];

    // ask the OS to flush the buffer of the LFU Region to disk
    lfuRegion.buffer.force();

    // de-reference the LFU Region
    regions[lfuRegionIdx] = null;

    if (activeRegionIdx == lfuRegionIdx) {
      // re-position activeRegionIdx before the lfuRegionIdx that will be removed
      activeRegionIdx = Math.max(0, lfuRegionIdx - 1);
    }

    // left shift each item in the regions array by one from minUsedRegionIdx + 1
    for (int i = lfuRegionIdx; i < usedRegions - 1; i++) {
      regions[lfuRegionIdx] = regions[lfuRegionIdx + 1];
    }

    // record that we now have a free region
    usedRegions--;

    // close the buffer of the de-referenced LFU Region
    closeMappedByteBuffer(lfuRegion.buffer);
  }

  static int getLfuRegionIndex(final Region[] regions, final int usedRegions) {
    int lfuRegionIdx = usedRegions - 1;  // default to the last region to reduce left-shifting when all regions tie-break on LFU
    long lfu = regions[lfuRegionIdx].useCount;

    // iterate right-to-left and check the use count
    for (int i = lfuRegionIdx - 1; i >= 0; i--) {
      final long useCount = regions[i].useCount();
      if (useCount < lfu) {
        lfuRegionIdx = i;
        lfu = useCount;
      }
    }
    return lfuRegionIdx;
  }

  static void checkBounds(final int off, final int len, final int size) {
    if ((off | len | (off + len) | (size - (off + len))) < 0) {
      throw new IndexOutOfBoundsException();
    }
  }


  // TODO(AR) temp for experimentation - remove this
  public void put(final byte b) {
    regions[activeRegionIdx].buffer.put(b);
  }

  @Override
  public void close() throws IOException {
    @Nullable UncheckedIOException exceptions[] = null;
    while (usedRegions > 0) {
      try {
        final MappedByteBuffer buffer = regions[usedRegions - 1].buffer;
        buffer.force();  // ask the OS to flush the buffer to disk
        regions[--usedRegions] = null;
        closeMappedByteBuffer(buffer);
      } catch (final UncheckedIOException e) {
        if (exceptions == null) {
          exceptions = new UncheckedIOException[1];
        } else {
          exceptions = Arrays.copyOf(exceptions, exceptions.length + 1);
        }
        exceptions[exceptions.length - 1] = e;
      }
    }

    if (exceptions != null) {
      final StringBuilder message = new StringBuilder();
      for (int i = 0; i < exceptions.length; i++) {
        if (message.length() > 0) {
          message.append("; ");
        }
        message.append(exceptions[i].getMessage());
      }
      throw new IOException(message.toString());
    }
  }

  private void closeMappedByteBuffer(final MappedByteBuffer mappedBuffer) {
    ((DirectBuffer) mappedBuffer).cleaner().clean();
//    Class<?> unsafeClass = Class.forName("sun.misc.Unsafe");
//    Field unsafeField = unsafeClass.getDeclaredField("theUnsafe");
//    unsafeField.setAccessible(true);
//    Object unsafe = unsafeField.get(null);
//    Method invokeCleaner = unsafeClass.getMethod("invokeCleaner", ByteBuffer.class);
//    invokeCleaner.invoke(unsafe, mappedBuffer);
  }

  static class Region {
    /**
     * The offset (inclusive) in the underlying file-channel
     * that this Region starts mapping from.
     */
    public final long fcPositionStart;

    /**
     * The offset (inclusive) in the underlying file-channel
     * that this region finishes mapping at.
     */
    public final long fcPositionEnd;

    /**
     * The memory-mapped buffer for this region.
     */
    public final MappedByteBuffer buffer;

    /**
     * Number of times this buffer has been read from or written to.
     * Used for LFU management of {@link MultiplexedMappedByteBuffer#regions}.
     */
    private long useCount;

    /**
     * Construct a new Region.
     *
     * @param fcPositionStart the offset (inclusive) in the underlying file-channel that this Region starts mapping from.
     * @param buffer the memory-mapped buffer for this region.
     */
    public Region(final long fcPositionStart, final MappedByteBuffer buffer) {
      this.fcPositionStart = fcPositionStart;
      this.fcPositionEnd = fcPositionStart + Math.max(0, buffer.capacity() - 1);
      this.buffer = buffer;
    }

    /**
     * Returns true if the extent of this region ends before the provided position.
     *
     * @param fcPosition the file channel position.
     *
     * @return true, if the extent of this region ends before the provided position, false otherwise.
     */
    public boolean isBefore(final long fcPosition) {
      return this.fcPositionEnd < fcPosition;
    }

    /**
     * Returns true if this region starts after the provided position.
     *
     * @param fcPosition the file channel position.
     *
     * @return true, if this region starts after the provided position, false otherwise.
     */
    public boolean isAfter(final long fcPosition) {
      return this.fcPositionStart > fcPosition;
    }

    /**
     * Returns true of this region encompasses the provided position.
     *
     * @param fcPosition the file channel position.
     *
     * @return true, if this region encompasses the provided position, false otherwise.
     */
    public boolean encompasses(final long fcPosition) {
      return buffer.capacity() > 0 && fcPosition >= this.fcPositionStart
          && fcPosition <= this.fcPositionEnd;
    }

    /**
     * Increment the use count of this region.
     */
    public void incrementUseCount() {
      if (useCount != Long.MAX_VALUE) {
        useCount++;
      }
    }

    /**
     * Get the use count of this region.
     *
     * @return the use count.
     */
    public long useCount() {
      return useCount;
    }

    /* Below here are package-private methods for accessing/modifying state in Unit Tests */
    Region setUseCount(final long useCount) {
      this.useCount = useCount;
      return this;
    }
  }

  /* Below here are package-private methods for accessing state in Unit Tests */
  FileChannel fileChannel() {
    return fileChannel;
  }

  MapMode mapMode() {
    return mapMode;
  }

  long minBufferSize() {
    return minBufferSize;
  }

  long maxBufferSize() {
    return maxBufferSize;
  }

  Region[] regions() {
    return regions;
  }

  int usedRegions() {
    return usedRegions;
  }

  int activeRegionIdx() {
    return activeRegionIdx;
  }

  long fcPosition() {
    return fcPosition;
  }

  long nextFcPosition() {
    return nextFcPosition;
  }
}
