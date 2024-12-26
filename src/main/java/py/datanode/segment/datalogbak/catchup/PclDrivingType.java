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
