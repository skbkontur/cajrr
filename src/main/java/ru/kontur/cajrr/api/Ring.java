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

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.Lists;

import java.math.BigInteger;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class Ring {

    private final List<Fragment> ranges;

    private final BigInteger RANGE_MAX;
    private final BigInteger RANGE_SIZE;

    public Ring(Map<String, String> map, int slices) {
        BigInteger RANGE_MIN;
        RANGE_MIN = new BigInteger("2").pow(63).negate();
        RANGE_MAX = new BigInteger("2").pow(63).subtract(BigInteger.ONE);
        RANGE_SIZE = RANGE_MAX.subtract(RANGE_MIN).add(BigInteger.ONE);

        List<BigInteger> tokens = map.entrySet().stream().map(entry -> BigInteger.valueOf(Long.decode(entry.getKey()))).collect(Collectors.toList());
        ranges = fragment(tokens, slices);
    }

    private List<Fragment> fragment(List<BigInteger> tokens, int slices) {
        int tokenCount = tokens.size();

        List<Fragment> result = Lists.newArrayList();
        for (int i = 0; i < tokenCount; i++) {
            BigInteger start = tokens.get(i);
            BigInteger stop = tokens.get((i + 1) % tokenCount);

            BigInteger size = stop.subtract(start);

            BigInteger[] segmentCountAndRemainder =
                    size.multiply(BigInteger.valueOf(slices)).divideAndRemainder(size);
            int segmentCount = segmentCountAndRemainder[0].intValue() +
                    (segmentCountAndRemainder[1].equals(BigInteger.ZERO) ? 0 : 1);


            List<BigInteger> endpointTokens = Lists.newArrayList();
            for (int j = 0; j <= segmentCount; j++) {
                BigInteger offset = size
                        .multiply(BigInteger.valueOf(j))
                        .divide(BigInteger.valueOf(segmentCount));
                BigInteger nextToken = start.add(offset);
                // Bordered values from MAX to MIN
                if (greaterThan(nextToken, RANGE_MAX)) {
                    nextToken = nextToken.subtract(RANGE_SIZE);
                }
                endpointTokens.add(nextToken);
            }

            for (int j = 0; j < segmentCount; j++) {
                result.add(new Fragment(endpointTokens.get(j), endpointTokens.get(j + 1).subtract(BigInteger.ONE)));
            }
        }

        return result;
    }

    private static boolean greaterThan(BigInteger a, BigInteger b) {
        return a.compareTo(b) > 0;
    }

    @JsonProperty
    public List<Fragment> getRanges() {
        return ranges;
    }
}
