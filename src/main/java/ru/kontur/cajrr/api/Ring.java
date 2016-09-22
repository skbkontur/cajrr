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

import java.util.List;
import java.util.Map;

public class Ring {

    private final List<Token> tokens;

    public Ring(Map<String, String> map, int slices) {

        tokens = tokenizeMap(map);

        fragment(slices);
    }

    private void fragment(int slices) {
        for(Token t: tokens) {
            t.fragment(slices);
        }
    }

    private List<Token> tokenizeMap(Map<String, String> map) {
        Token first = null;
        Token prev = null;
        List<Token> result = Lists.newArrayList();

        for(Map.Entry<String, String> entry: map.entrySet()) {
            Token token = new Token(entry.getKey());
            if(prev==null) {
                first = token;
                prev = token;
                continue;
            }
            prev.setNext(token);
            result.add(prev);
            prev = token;
        }
        assert prev != null;
        prev.setNext(first);
        result.add(prev);
        return result;
    }

    @JsonProperty
    public List<Token> getTokens() {
        return tokens;
    }
}
