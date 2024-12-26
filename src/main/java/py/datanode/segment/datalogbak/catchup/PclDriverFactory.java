/**
* Copyright (C) 2013-2024 Nanjing Pengyun Network Technology Co., Ltd.
* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with the License.
* You may obtain a copy of the License at
*
*     http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/ 

package py.datanode.segment.datalogbak.catchup;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.archive.segment.SegId;
import py.client.thrift.GenericThriftClientFactory;
import py.common.struct.EndPoint;
import py.datanode.archive.RawArchiveManager;
import py.datanode.client.DataNodeServiceAsyncClientWrapper;
import py.datanode.configuration.DataNodeConfiguration;
import py.datanode.segment.SegmentUnit;
import py.datanode.segment.datalog.plal.engine.PlalEngine;
import py.instance.Instance;
import py.instance.InstanceId;
import py.instance.InstanceStore;
import py.membership.SegmentMembership;
import py.thrift.datanode.service.DataNodeService.Iface;

public class PclDriverFactory {
  private static final Logger logger = LoggerFactory.getLogger(PclDriverFactory.class);
  private final RawArchiveManager archiveManager;
  private final GenericThriftClientFactory<Iface> dataNodeClientFactory;
  private final InstanceStore instanceStore;
  private final DataNodeConfiguration cfg;
  private final InstanceId myself;
  private final PlalEngine plalEngine;
  private final DataNodeServiceAsyncClientWrapper dataNodeAsyncClient;

  public PclDriverFactory(RawArchiveManager archiveManager,
      GenericThriftClientFactory<Iface> dataNodeClientFactory,
      InstanceStore instanceStore,
      DataNodeConfiguration cfg, InstanceId myself, PlalEngine plalEngine,
      DataNodeServiceAsyncClientWrapper dataNodeAsyncClient) {
    this.archiveManager = archiveManager;
    this.dataNodeClientFactory = dataNodeClientFactory;
    this.instanceStore = instanceStore;
    this.cfg = cfg;
    this.myself = myself;
    this.plalEngine = plalEngine;
    this.dataNodeAsyncClient = dataNodeAsyncClient;
  }

  public ChainedLogDriver generate(CatchupLogContext context) {
    SegmentUnit segmentUnit = context.getSegmentUnit();
    SegId segId = context.getSegId();
    if (segmentUnit != null) {
      SegmentMembership membership = segmentUnit.getSegmentUnitMetadata().getMembership();
      EndPoint primaryEndPoint = null;
      if (membership.getPrimary() != myself) {
        Instance primaryInstance = instanceStore.get(membership.getPrimary());
        if (primaryInstance == null) {
          logger.warn("can not get the primary instance: {} for segId: {}", membership, segId);
          return null;
        }
        primaryEndPoint = primaryInstance.getEndPoint();
      }

      return new PclDriver(archiveManager, segmentUnit, segmentUnit.getSegmentLogMetadata(),
          cfg, dataNodeClientFactory, primaryEndPoint, membership, dataNodeAsyncClient, myself,
          plalEngine, instanceStore);
    } else {
      logger
          .error("Can't find the segment unit for segId: {}, segmentUnit: {}", segId, segmentUnit);
      return null;
    }
  }

  public DataNodeConfiguration getCfg() {
    return cfg;
  }

  public InstanceStore getInstanceStore() {
    return instanceStore;
  }
}
