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

package py.datanode.segment.heartbeat;

import py.datanode.segment.SegmentUnit;

public interface HostHeartbeat {
  public void addRelationBetweenPrimaryAndAllAliveSecondaries(SegmentUnit segUnit);

  public void removeRelationBetweenPrimaryAndAllAliveSecondaries(SegmentUnit segUnit);

  public void addRelationBetweenPrimaryAndAliveSecondary(SegmentUnit segunit,
      long aliveSecondaryInstanceId);

  public void removeRelationBetweenPrimaryAndAliveSecondary(SegmentUnit segunit,
      long aliveSecondaryInstanceId);

  public void start();

  public void stop();

  public void pause();

  public void restart();
}
