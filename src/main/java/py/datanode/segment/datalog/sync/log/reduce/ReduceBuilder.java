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

package py.datanode.segment.datalog.sync.log.reduce;

public interface ReduceBuilder<U, M> {
  boolean submitUnit(U unit);

  boolean submitUnit(U unit, boolean force);

  /**
   * build a instance of type M, it pack all unit of type U which submit to this builder.
   *
   */
  M build();

  /**
   * if cache has been expired, validity begins with creation. a timer will call this function.
   *
   */
  M expiredBuild();

}

