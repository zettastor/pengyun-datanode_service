
package py.datanode.segment.membership.statemachine.checksecondaryinactive;

import py.datanode.checksecondaryinactive.CheckSecondaryInactiveByRelativeTime;

public class BogusCheckSecondary extends CheckSecondaryInactiveByRelativeTime {
  public BogusCheckSecondary() {
    super(true, 0L);
  }
}
