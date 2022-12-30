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

package py.datanode.segment.datalog;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.io.Serializable;

public class MutationLogEntryForSave implements Serializable {
  private static final long serialVersionUID = 1730923893497018535L;

  private long uuid;
  private long logId;
  private String status;
  private long offset;
  private byte[] data;
  private long checksum;
  private boolean applied;
  private boolean persisted;
  private int dataLength;
  private long archiveId;

  public MutationLogEntryForSave() {
  }

  @JsonCreator
  public MutationLogEntryForSave(@JsonProperty("uuid") long uuid, @JsonProperty("logId") long logId,
      @JsonProperty("offset") long offset, @JsonProperty("dataLength") int dataLength,
      @JsonProperty("data") byte[] data, @JsonProperty("checksum") long checksum,
      @JsonProperty("status") String status,
      @JsonProperty("applied") boolean applied, @JsonProperty("persisted") boolean persisted) {
    this.uuid = uuid;
    this.logId = logId;
    this.archiveId = 0L;
    this.offset = offset;
    this.dataLength = dataLength;
    this.data = data;
    this.checksum = checksum;
    this.status = status;
    this.applied = applied;
    this.persisted = persisted;
  }

  public static long getSerialversionuid() {
    return serialVersionUID;
  }

  public long getUuid() {
    return uuid;
  }

  public void setUuid(long uuid) {
    this.uuid = uuid;
  }

  public long getLogId() {
    return logId;
  }

  public void setLogId(long logId) {
    this.logId = logId;
  }

  public long getOffset() {
    return offset;
  }

  public void setOffset(long offset) {
    this.offset = offset;
  }

  public long getChecksum() {
    return checksum;
  }

  public void setChecksum(long checksum) {
    this.checksum = checksum;
  }

  public byte[] getData() {
    return data;
  }

  public void setData(byte[] data) {
    this.data = data;
  }

  public String getStatus() {
    return status;
  }

  public void setStatus(String status) {
    this.status = status;
  }

  public boolean isApplied() {
    return applied;
  }

  public void setApplied(boolean applied) {
    this.applied = applied;
  }

  public boolean isPersisted() {
    return persisted;
  }

  public void setPersisted(boolean persisted) {
    this.persisted = persisted;
  }

  public int getDataLength() {
    return dataLength;
  }

  public void setDataLength(int dataLength) {
    this.dataLength = dataLength;
  }

  public long getArchiveId() {
    return archiveId;
  }

  public void setArchiveId(Long archiveId) {
    this.archiveId = archiveId;
  }

}
