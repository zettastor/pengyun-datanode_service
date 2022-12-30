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

package py.datanode.archive.selection.strategy;

import com.google.common.collect.Sets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import py.common.counter.ObjectCounter;
import py.common.counter.TreeSetObjectCounter;
import py.datanode.archive.RawArchive;
import py.instance.InstanceId;
import py.membership.SegmentMembership;
import py.volume.VolumeId;

public class VolumeArchiveLayout {
  private static final InstanceId ALL = null;
  private final VolumeId volumeId;
  private final InstanceId myself;
  private final Map<CounterKey, ObjectCounter<RawArchive>> objectCounterMap;

  public VolumeArchiveLayout(VolumeId volumeId, InstanceId myself) {
    this.volumeId = volumeId;
    this.myself = myself;
    this.objectCounterMap = new HashMap<>();
  }

  public void clear() {
    objectCounterMap.clear();
  }

  private ObjectCounter<RawArchive> getCounter(CounterKey counterKey,
      Collection<RawArchive> archives) {
    ObjectCounter<RawArchive> archiveCounter = objectCounterMap.computeIfAbsent(counterKey, k -> {
      ObjectCounter<RawArchive> counter = new TreeSetObjectCounter<>();
      archives.forEach(counter::increment);
      return counter;
    });

    archives.forEach(archiveCounter::increment);
    return archiveCounter;
  }

  protected ObjectCounter<RawArchive> getArchiveCounter(Collection<RawArchive> archives) {
    CounterKey keyOfAll = new CounterKey();
    return getCounter(keyOfAll, archives);
  }

  protected ObjectCounter<RawArchive> getArchiveCounter(Collection<RawArchive> archives,
      boolean isPrimary) {
    CounterKey keyOfPrimaryOrNot = new CounterKey(isPrimary, ALL);
    return getCounter(keyOfPrimaryOrNot, archives);
  }

  protected ObjectCounter<RawArchive> getArchiveCounter(Collection<RawArchive> archives,
      boolean isPrimary,
      InstanceId instanceId) {
    CounterKey keyOfPrimaryOrNotWithOtherInstance = new CounterKey(isPrimary, instanceId);
    return getCounter(keyOfPrimaryOrNotWithOtherInstance, archives);
  }

  public Iterator<RawArchive> iterator(Collection<RawArchive> archives,
      SegmentMembership membership) {
    boolean isPrimary = membership.getPrimary().equals(myself);
    ObjectCounter<RawArchive> counterOfAll = getArchiveCounter(archives);
    ObjectCounter<RawArchive> counterOfPrimaryOrNot = getArchiveCounter(archives, isPrimary);

    List<ObjectCounter<RawArchive>> countersOfPrimaryOrNotWithInstances = new ArrayList<>();
    getMembersExcludeMe(membership).forEach(instanceId -> countersOfPrimaryOrNotWithInstances
        .add(getArchiveCounter(archives, isPrimary, instanceId)));

    Comparator<RawArchive> comparator = counterOfAll.thenComparing(counterOfPrimaryOrNot);
    for (ObjectCounter<RawArchive> counterOfInstance : countersOfPrimaryOrNotWithInstances) {
      comparator = comparator.thenComparing(counterOfInstance);
    }

    return counterOfAll.iterator(comparator);
  }

  public void increment(Collection<RawArchive> archives, SegmentMembership membership,
      RawArchive archive) {
    boolean isPrimary = membership.getPrimary().equals(myself);
    getArchiveCounter(archives).increment(archive);
    getArchiveCounter(archives, isPrimary).increment(archive);
    getMembersExcludeMe(membership)
        .forEach(
            instanceId -> getArchiveCounter(archives, isPrimary, instanceId).increment(archive));
  }

  public void decrement(SegmentMembership membership, RawArchive archive) {
    boolean isPrimary = membership.getPrimary().equals(myself);
    Collection<RawArchive> emptyList = new ArrayList<>(0);
    getArchiveCounter(emptyList).decrement(archive);
    getArchiveCounter(emptyList, isPrimary).decrement(archive);
    getMembersExcludeMe(membership)
        .forEach(
            instanceId -> getArchiveCounter(emptyList, isPrimary, instanceId).decrement(archive));
  }

  private Set<InstanceId> getMembersExcludeMe(SegmentMembership membership) {
    Set<InstanceId> membersExcludeMe = Sets.newHashSet();
    membersExcludeMe.addAll(membership.getWriteSecondaries());
    membersExcludeMe.add(membership.getPrimary());
    membersExcludeMe.remove(myself);
    return membersExcludeMe;
  }

  @Override
  public String toString() {
    return "VolumeArchiveLayout{" + "volumeId=" + volumeId + ", objectCounterMap="
        + objectCounterMap + '}';
  }

  static class CounterKey {
    CounterType counterType;
    InstanceId instanceId;
    
    CounterKey() {
      this.counterType = CounterType.All;
      this.instanceId = ALL;
    }

    CounterKey(boolean isPrimary, InstanceId instanceId) {
      this.counterType = CounterType.valueOf(isPrimary);
      this.instanceId = instanceId;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      CounterKey that = (CounterKey) o;
      return counterType == that.counterType && Objects.equals(instanceId, that.instanceId);
    }

    @Override
    public int hashCode() {
      return Objects.hash(counterType, instanceId);
    }

    @Override
    public String toString() {
      return "CounterKey{" + "counterType=" + counterType + ", instanceId=" + instanceId + '}';
    }

    enum CounterType {
      All, Primary, Secondary;

      static CounterType valueOf(boolean isPrimary) {
        return isPrimary ? Primary : Secondary;
      }
    }
  }
}
