/*
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
package ru.kontur.cajrr.api;

import ru.kontur.cajrr.tools.SegmentGenerator;

import java.math.BigInteger;

public class RepairRange {

  private final BigInteger start;
  private final BigInteger end;

  public RepairRange(BigInteger start, BigInteger end) {
    this.start = start;
    this.end = end;
  }

  /**
   * Returns the size of this range
   *
   * @return size of the range, max - range, in case of wrap
   */
  public BigInteger span(BigInteger ringSize) {
    if (SegmentGenerator.greaterThanOrEqual(start, end)) {
      return end.subtract(start).add(ringSize);
    } else {
      return end.subtract(start);
    }
  }

  @Override
  public String toString() {
    return String.format("%s:%s", start.toString(), end.subtract(BigInteger.ONE).toString());
  }

}
