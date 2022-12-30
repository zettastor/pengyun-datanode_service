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

package py.datanode.segment.datalogbak.catchup;

public enum PclDrivingType {
  Primary {
    @Override
    public boolean isPrimary() {
      return true;
    }
  },

  OfflinePrimary {
    @Override
    public boolean isPrimary() {
      return true;
    }
  },

  OrphanPrimary {
    @Override
    public boolean isPrimary() {
      return true;
    }
  },

  Secondary {
    @Override
    public boolean isSecondary() {
      return true;
    }
  },

  OfflineSecondary {
    @Override
    public boolean isSecondary() {
      return true;
    }
  },

  JoiningSecondary {
    @Override
    public boolean isSecondary() {
      return true;
    }
  },

  VotingSecondary {
    @Override
    public boolean isSecondary() {
      return true;
    }
  },

  NotDriving;

  public boolean isPrimary() {
    return false;
  }

  public boolean isSecondary() {
    return false;
  }

}
