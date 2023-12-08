/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
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

#include <folly/Benchmark.h>
#include <folly/init/Init.h>

#include "velox/benchmarks/ExpressionBenchmarkBuilder.h"
#include "velox/functions/lib/Re2Functions.h"

using namespace facebook;
using namespace facebook::velox;
using namespace facebook::velox::functions;
using namespace facebook::velox::functions::test;
using namespace facebook::velox::memory;
using namespace facebook::velox;

int main(int argc, char** argv) {
  folly::Init init(&argc, &argv);

  exec::registerStatefulVectorFunction("like", likeSignatures(), makeLike);
  ExpressionBenchmarkBuilder benchmarkBuilder;
  const vector_size_t vectorSize = 1000;
  auto vectorMaker = benchmarkBuilder.vectorMaker();

  auto substringInput =
      vectorMaker.flatVector<std::string>(vectorSize, [&](auto row) {
        // Only when the number is even we make a string contains a substring
        // a_b_c.
        if (row % 2 == 0) {
          auto padding = std::string("x", row / 2 + 1);
          return fmt::format("{}a_b_c{}", padding, padding);
        } else {
          return std::string("x", row);
        }
      });

  auto prefixInput =
      vectorMaker.flatVector<std::string>(vectorSize, [&](auto row) {
        // Only when the number is even we make a string starts with a_b_c.
        if (row % 2 == 0) {
          return fmt::format("a_b_c{}", std::string("x", row));
        } else {
          return std::string("x", row);
        }
      });

  auto suffixInput =
      vectorMaker.flatVector<std::string>(vectorSize, [&](auto row) {
        // Only when the number is even we make a string ends with a_b_c.
        if (row % 2 == 0) {
          return fmt::format("{}a_b_c", std::string("x", row));
        } else {
          return std::string("x", row);
        }
      });

  benchmarkBuilder
      .addBenchmarkSet(
          "like",
          vectorMaker.rowVector(
              {"col0", "col1", "col2"},
              {substringInput, prefixInput, suffixInput}))
      .addExpression("substring", R"(like (col0, '%a\_b\_c%', '\'))")
      .addExpression("prefix", R"(like (col1, 'a\_b\_c%', '\'))")
      .addExpression("suffix", R"(like (col2, '%a\_b\_c', '\'))")
      .addExpression("generic", R"(like (col0, '%a%b%c'))")
      .disableTesting();

  benchmarkBuilder.registerBenchmarks();
  folly::runBenchmarks();
  return 0;
}
