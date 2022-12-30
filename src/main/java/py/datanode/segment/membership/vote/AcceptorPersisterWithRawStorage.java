/*
 * Copyright (c) 2022. PengYunNetWork
 *
 * This program is free software: you can use, redistribute, and/or modify it
 * under the terms of the GNU Affero General Public License, version 3 or later ("AGPL"),
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 *  without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 *
 *  You should have received a copy of the GNU Affero General Public License along with
 *  this program. If not, see <http://www.gnu.org/licenses/>.
 */

package py.datanode.segment.membership.vote;

import com.google.protobuf.CodedInputStream;
import com.google.protobuf.CodedOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import org.apache.commons.lang3.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.archive.segment.SegId;
import py.common.tlsf.bytebuffer.manager.TlsfByteBufferManager;
import py.common.tlsf.bytebuffer.manager.TlsfByteBufferManagerFactory;
import py.datanode.exception.ParseFailedException;
import py.datanode.segment.membership.vote.Acceptor.State;
import py.datanode.segment.membership.vote.PbVoting.PbAcceptor;
import py.exception.StorageException;
import py.proto.Broadcastlog.PbMembership;
import py.storage.Storage;

public class AcceptorPersisterWithRawStorage implements AcceptorPersister<Integer, PbMembership> {
  private static final int MAGIC = 0xACCEACCE;
  private static final int REGION_MAGIC = MAGIC + 1;
  private static final Logger logger = LoggerFactory
      .getLogger(AcceptorPersisterWithRawStorage.class);

  private final SegId segId;
  private final Storage storage;
  private final long pos;
  private final int length;
  private final int alignedSize;

  private final int regionLength;

  private final int region1Offset;
  private final int region2Offset;

  private final boolean enableTwoRegions;
  private final TlsfByteBufferManager tlsfByteBufferManager;
  private byte version = -1;
  private int currentRegionOffset;

  public AcceptorPersisterWithRawStorage(SegId segId, Storage storage, long pos, int length,
      int aligned,
      boolean enableTwoRegions) {
    this.segId = segId;
    this.storage = storage;
    this.pos = pos;
    this.length = length;
    this.enableTwoRegions = enableTwoRegions;

    int metadataLength =
        Integer.BYTES * 4 + Long.BYTES;
    this.regionLength = (length - metadataLength) / 2;

    this.region1Offset = metadataLength;
    this.region2Offset = metadataLength + regionLength;
    this.currentRegionOffset = region1Offset;
    this.alignedSize = aligned;

    Validate.isTrue(pos % aligned == 0);
    Validate.isTrue(length % aligned == 0);
    tlsfByteBufferManager = TlsfByteBufferManagerFactory.instance();
    Validate.notNull(tlsfByteBufferManager);
  }

  @Override
  public synchronized Acceptor<Integer, PbMembership> restore() throws StorageException {
    ByteBuffer buffer = tlsfByteBufferManager.allocate(length);
    try {
      storage.read(pos, buffer);
      buffer.clear();
      int magic = buffer.getInt();
      if (magic != MAGIC) {
        logger.warn("magic {} not right, uninitialized field", magic);
        return Acceptor.create(0, 0);
      }

      long volumeId = buffer.getLong();
      if (volumeId != segId.getVolumeId().getId()) {
        logger.warn("volume id {} not equal segId {}, uninitialized field", volumeId, segId);
        return Acceptor.create(0, 0);
      }
      int index = buffer.getInt();
      if (index != segId.getIndex()) {
        logger.warn("seg index {} not equal segId {}, uninitialized field", volumeId, segId);
        return Acceptor.create(0, 0);
      }

      int r1Offset = buffer.getInt();
      int r2Offset = buffer.getInt();

      if (r1Offset != region1Offset || r2Offset != region2Offset) {
        logger.error("data in disk not match with memory {} {} {} {}", r1Offset, region1Offset,
            r2Offset, region2Offset);
        return Acceptor.create(0, 0);
      }

      int magic1 = buffer.getInt(r1Offset);
      int magic2 = buffer.getInt(r2Offset);

      if (magic1 == REGION_MAGIC && magic2 == REGION_MAGIC) {
        byte version1 = buffer.get(r1Offset + Integer.BYTES);
        byte version2 = buffer.get(r2Offset + Integer.BYTES);

        if (nextVersion(version1) == version2) {
          currentRegionOffset = r2Offset;
        } else if (nextVersion(version2) == version1) {
          currentRegionOffset = r1Offset;
        } else {
          logger
              .warn("two versions are not adjacent {} {}, chose the bigger one anyway {}", version1,
                  version2, Math.max(version1, version2));
          currentRegionOffset = version1 >= version2 ? r1Offset : r2Offset;
        }
      } else if (magic1 == REGION_MAGIC) {
        currentRegionOffset = r1Offset;
      } else if (magic2 == REGION_MAGIC) {
        currentRegionOffset = r2Offset;
      } else {
        logger.error("two magic all error {} {}, expected {}", magic1, magic2, REGION_MAGIC);
        return Acceptor.create(0, 0);
      }

      buffer.position(currentRegionOffset + Integer.BYTES);
      buffer.limit(currentRegionOffset + regionLength);

      try {
        return parseRegion(buffer.slice());
      } catch (ParseFailedException e) {
        throw new StorageException(e);
      }
    } finally {
      tlsfByteBufferManager.release(buffer);
    }

  }

  private Acceptor<Integer, PbMembership> parseRegion(ByteBuffer buffer)
      throws ParseFailedException {
    this.version = buffer.get();
    int length = buffer.getInt();

    buffer.limit(buffer.position() + length);

    try {
      PbAcceptor pbAcceptor = PbAcceptor.parseFrom(CodedInputStream.newInstance(buffer));
      return Acceptor.create(pbAcceptor.getMaxN(), pbAcceptor.getLastN(),
          pbAcceptor.hasMembership() ? pbAcceptor.getMembership() : null,
          convertState(pbAcceptor.getState()));
    } catch (IOException e) {
      logger.error("parse failed", e);
      throw new ParseFailedException(e);
    }

  }

  private PbVoting.State convertState(State state) {
    switch (state) {
      case INIT:
        return PbVoting.State.INIT;
      case FROZEN:
        return PbVoting.State.FROZEN;
      case ACCEPTED:
        return PbVoting.State. ACCEPTED;
      case PROMISED:
        return PbVoting.State.PROMISED;
      default:
        throw new IllegalArgumentException("unknown state " + state);
    }
  }

  private State convertState(PbVoting.State state) {
    switch (state) {
      case INIT:
        return State.INIT;
      case FROZEN:
        return State.FROZEN;
      case ACCEPTED:
        return State.ACCEPTED;
      case PROMISED:
        return State.PROMISED;
      default:
        throw new IllegalArgumentException("unknown state " + state);
    }
  }

  private byte nextVersion(byte version) {
    byte nextVersion = version;
    nextVersion++;

    if (nextVersion < 0) {
      nextVersion = 1;
    }

    return nextVersion;
  }

  @Override
  public synchronized void persist(Acceptor<Integer, PbMembership> acceptor)
      throws StorageException {
    try {
      switchRegion();

      version = nextVersion(version);

      int writeOffset;
      ByteBuffer buffer;
      if (version == 0 || !enableTwoRegions) {
        buffer = tlsfByteBufferManager.allocate(length);
        buffer.putInt(MAGIC);
        buffer.putLong(segId.getVolumeId().getId());
        buffer.putInt(segId.getIndex());
        buffer.putInt(region1Offset);
        buffer.putInt(region2Offset);
        writeOffset = 0;
      } else {
        buffer = tlsfByteBufferManager.allocate(regionLength);
        writeOffset = currentRegionOffset;
      }

      buffer.putInt(REGION_MAGIC);
      buffer.put(version);

      PbAcceptor.Builder pbAcceptorBuilder = PbAcceptor.newBuilder().setMaxN(acceptor.getMaxN())
          .setLastN(acceptor.getLastN()).setState(convertState(acceptor.getState()));

      if (acceptor.getAcceptedProposal() != null) {
        pbAcceptorBuilder.setMembership(acceptor.getAcceptedProposal());
      }

      PbAcceptor pbAcceptor = pbAcceptorBuilder.build();
      buffer.putInt(pbAcceptor.getSerializedSize());

      try {
        CodedOutputStream out = CodedOutputStream.newInstance(buffer);
        pbAcceptor.writeTo(out);
        out.flush();
      } catch (IOException e) {
        logger.error("no way getting here !! ", e);
        throw new IllegalArgumentException(e);
      }

      if (enableTwoRegions) {
        buffer.flip();
      } else {
        buffer.clear();
      }
      try {
        writeToStorage(writeOffset, buffer);
      } finally {
        tlsfByteBufferManager.release(buffer);
      }
    } finally {
      logger.info("nothing need to do here");
    }
  }

  private void writeToStorage(int writeOffset, ByteBuffer buffer) throws StorageException {
    if (writeOffset % alignedSize == 0 && buffer.remaining() % alignedSize == 0) {
      storage.write(pos + writeOffset, buffer);
    } else {
      ByteBuffer loadBuffer = tlsfByteBufferManager.allocate(length);
      try {
        storage.read(pos, loadBuffer);
        loadBuffer.clear();

        loadBuffer.position(writeOffset);
        loadBuffer.put(buffer);

        loadBuffer.clear();
        storage.write(pos, loadBuffer);
      } finally {
        tlsfByteBufferManager.release(loadBuffer);
      }
    }
  }

  private void switchRegion() {
    if (currentRegionOffset == region1Offset) {
      currentRegionOffset = region2Offset;
    } else {
      currentRegionOffset = region1Offset;
    }
  }

}
