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
#include "velox/functions/lib/Re2Functions.h"

#include <re2/re2.h>
#include <memory>
#include <optional>
#include <string>

#include "velox/expression/VectorWriters.h"
#include "velox/type/StringView.h"
#include "velox/vector/BaseVector.h"

namespace facebook::velox::functions {
namespace {

using ::facebook::velox::exec::EvalCtx;
using ::facebook::velox::exec::Expr;
using ::facebook::velox::exec::VectorFunction;
using ::facebook::velox::exec::VectorFunctionArg;
using ::re2::RE2;

static const int kMaxCompiledRegexes = 20;

std::string printTypesCsv(
    const std::vector<exec::VectorFunctionArg>& inputArgs) {
  std::string result;
  result.reserve(inputArgs.size() * 10);
  for (const auto& input : inputArgs) {
    folly::toAppend(
        result.empty() ? "" : ", ", input.type->toString(), &result);
  }
  return result;
}

template <typename T>
re2::StringPiece toStringPiece(const T& s) {
  return re2::StringPiece(s.data(), s.size());
}

// If v is a non-null constant vector, returns the constant value. Otherwise
// returns nullopt.
template <typename T>
std::optional<T> getIfConstant(const BaseVector& v) {
  if (v.encoding() == VectorEncoding::Simple::CONSTANT &&
      v.isNullAt(0) == false) {
    return v.as<ConstantVector<T>>()->valueAt(0);
  }
  return std::nullopt;
}

void checkForBadPattern(const RE2& re) {
  if (UNLIKELY(!re.ok())) {
    VELOX_USER_FAIL("invalid regular expression:{}", re.error());
  }
}

FlatVector<bool>& ensureWritableBool(
    const SelectivityVector& rows,
    EvalCtx& context,
    VectorPtr& result) {
  context.ensureWritable(rows, BOOLEAN(), result);
  return *result->as<FlatVector<bool>>();
}

FlatVector<StringView>& ensureWritableStringView(
    const SelectivityVector& rows,
    EvalCtx& context,
    VectorPtr& result) {
  context.ensureWritable(rows, VARCHAR(), result);
  auto* flat = result->as<FlatVector<StringView>>();
  flat->mutableValues(rows.end());
  return *flat;
}

bool re2FullMatch(StringView str, const RE2& re) {
  return RE2::FullMatch(toStringPiece(str), re);
}

bool re2PartialMatch(StringView str, const RE2& re) {
  return RE2::PartialMatch(toStringPiece(str), re);
}

bool re2Extract(
    FlatVector<StringView>& result,
    int row,
    const RE2& re,
    const exec::LocalDecodedVector& strs,
    std::vector<re2::StringPiece>& groups,
    int32_t groupId,
    bool emptyNoMatch) {
  const StringView str = strs->valueAt<StringView>(row);
  DCHECK_GT(groups.size(), groupId);
  if (!re.Match(
          toStringPiece(str),
          0,
          str.size(),
          RE2::UNANCHORED, // Full match not required.
          groups.data(),
          groupId + 1)) {
    if (emptyNoMatch) {
      result.setNoCopy(row, StringView(nullptr, 0));
      return true;
    } else {
      result.setNull(row, true);
      return false;
    }
  } else {
    const re2::StringPiece extracted = groups[groupId];
    result.setNoCopy(row, StringView(extracted.data(), extracted.size()));
    return !StringView::isInline(extracted.size());
  }
}

std::string likePatternToRe2(
    StringView pattern,
    std::optional<char> escapeChar,
    bool& validPattern) {
  std::string regex;
  validPattern = true;
  regex.reserve(pattern.size() * 2);
  regex.append("^");
  bool escaped = false;
  for (const char c : pattern) {
    if (escaped && !(c == '%' || c == '_' || c == escapeChar)) {
      validPattern = false;
    }
    if (!escaped && c == escapeChar) {
      escaped = true;
    } else {
      switch (c) {
        case '%':
          regex.append(escaped ? "%" : ".*");
          escaped = false;
          break;
        case '_':
          regex.append(escaped ? "_" : ".");
          escaped = false;
          break;
        // Escape all the meta characters in re2
        case '\\':
        case '|':
        case '^':
        case '$':
        case '.':
        case '*':
        case '+':
        case '?':
        case '(':
        case ')':
        case '[':
        case ']':
        case '{':
        case '}':
          regex.append("\\"); // Append the meta character after the escape.
          [[fallthrough]];
        default:
          regex.append(1, c);
          escaped = false;
      }
    }
  }
  if (escaped) {
    validPattern = false;
  }

  regex.append("$");
  return regex;
}

template <bool (*Fn)(StringView, const RE2&)>
class Re2MatchConstantPattern final : public VectorFunction {
 public:
  explicit Re2MatchConstantPattern(StringView pattern)
      : re_(toStringPiece(pattern), RE2::Quiet) {}

  void apply(
      const SelectivityVector& rows,
      std::vector<VectorPtr>& args,
      const TypePtr& /* outputType */,
      EvalCtx& context,
      VectorPtr& resultRef) const final {
    VELOX_CHECK_EQ(args.size(), 2);
    FlatVector<bool>& result = ensureWritableBool(rows, context, resultRef);
    exec::LocalDecodedVector toSearch(context, *args[0], rows);
    try {
      checkForBadPattern(re_);
    } catch (const std::exception& e) {
      context.setErrors(rows, std::current_exception());
      return;
    }

    context.applyToSelectedNoThrow(rows, [&](vector_size_t i) {
      result.set(i, Fn(toSearch->valueAt<StringView>(i), re_));
    });
  }

 private:
  RE2 re_;
};

template <bool (*Fn)(StringView, const RE2&)>
class Re2Match final : public VectorFunction {
 public:
  void apply(
      const SelectivityVector& rows,
      std::vector<VectorPtr>& args,
      const TypePtr& outputType,
      EvalCtx& context,
      VectorPtr& resultRef) const override {
    VELOX_CHECK_EQ(args.size(), 2);
    if (auto pattern = getIfConstant<StringView>(*args[1])) {
      Re2MatchConstantPattern<Fn>(*pattern).apply(
          rows, args, outputType, context, resultRef);
      return;
    }
    // General case.
    FlatVector<bool>& result = ensureWritableBool(rows, context, resultRef);
    exec::LocalDecodedVector toSearch(context, *args[0], rows);
    exec::LocalDecodedVector pattern(context, *args[1], rows);
    context.applyToSelectedNoThrow(rows, [&](vector_size_t row) {
      RE2 re(toStringPiece(pattern->valueAt<StringView>(row)), RE2::Quiet);
      checkForBadPattern(re);
      result.set(row, Fn(toSearch->valueAt<StringView>(row), re));
    });
  }
};

void checkForBadGroupId(int groupId, const RE2& re) {
  if (UNLIKELY(groupId < 0 || groupId > re.NumberOfCapturingGroups())) {
    VELOX_USER_FAIL("No group {} in regex '{}'", groupId, re.pattern());
  }
}

template <typename T>
class Re2SearchAndExtractConstantPattern final : public VectorFunction {
 public:
  explicit Re2SearchAndExtractConstantPattern(
      StringView pattern,
      bool emptyNoMatch)
      : re_(toStringPiece(pattern), RE2::Quiet), emptyNoMatch_(emptyNoMatch) {}

  void apply(
      const SelectivityVector& rows,
      std::vector<VectorPtr>& args,
      const TypePtr& /* outputType */,
      EvalCtx& context,
      VectorPtr& resultRef) const final {
    VELOX_CHECK(args.size() == 2 || args.size() == 3);
    // TODO: Potentially re-use the string vector, not just the buffer.
    FlatVector<StringView>& result =
        ensureWritableStringView(rows, context, resultRef);

    // apply() will not be invoked if the selection is empty.
    try {
      checkForBadPattern(re_);
    } catch (const std::exception& e) {
      context.setErrors(rows, std::current_exception());
      return;
    }

    exec::LocalDecodedVector toSearch(context, *args[0], rows);
    bool mustRefSourceStrings = false;
    FOLLY_DECLARE_REUSED(groups, std::vector<re2::StringPiece>);
    // Common case: constant group id.
    if (args.size() == 2) {
      groups.resize(1);
      context.applyToSelectedNoThrow(rows, [&](vector_size_t i) {
        mustRefSourceStrings |=
            re2Extract(result, i, re_, toSearch, groups, 0, emptyNoMatch_);
      });
      if (mustRefSourceStrings) {
        result.acquireSharedStringBuffers(toSearch->base());
      }
      return;
    }

    if (const auto groupId = getIfConstant<T>(*args[2])) {
      try {
        checkForBadGroupId(*groupId, re_);
      } catch (const std::exception& e) {
        context.setErrors(rows, std::current_exception());
        return;
      }

      groups.resize(*groupId + 1);
      context.applyToSelectedNoThrow(rows, [&](vector_size_t i) {
        mustRefSourceStrings |= re2Extract(
            result, i, re_, toSearch, groups, *groupId, emptyNoMatch_);
      });
      if (mustRefSourceStrings) {
        result.acquireSharedStringBuffers(toSearch->base());
      }
      return;
    }

    // Less common case: variable group id. Resize the groups vector to
    // number of capturing groups + 1.
    exec::LocalDecodedVector groupIds(context, *args[2], rows);

    groups.resize(re_.NumberOfCapturingGroups() + 1);
    context.applyToSelectedNoThrow(rows, [&](vector_size_t i) {
      T group = groupIds->valueAt<T>(i);
      checkForBadGroupId(group, re_);
      mustRefSourceStrings |=
          re2Extract(result, i, re_, toSearch, groups, group, emptyNoMatch_);
    });
    if (mustRefSourceStrings) {
      result.acquireSharedStringBuffers(toSearch->base());
    }
  }

 private:
  RE2 re_;
  const bool emptyNoMatch_;
}; // namespace

// The factory function we provide returns a unique instance for each call, so
// this is safe.
template <typename T>
class Re2SearchAndExtract final : public VectorFunction {
 public:
  explicit Re2SearchAndExtract(bool emptyNoMatch)
      : emptyNoMatch_(emptyNoMatch) {}
  void apply(
      const SelectivityVector& rows,
      std::vector<VectorPtr>& args,
      const TypePtr& outputType,
      EvalCtx& context,
      VectorPtr& resultRef) const final {
    VELOX_CHECK(args.size() == 2 || args.size() == 3);
    // Handle the common case of a constant pattern.
    if (auto pattern = getIfConstant<StringView>(*args[1])) {
      Re2SearchAndExtractConstantPattern<T>(*pattern, emptyNoMatch_)
          .apply(rows, args, outputType, context, resultRef);
      return;
    }

    // The general case. Further optimizations are possible to avoid regex
    // recompilation, but a constant pattern is by far the most common case.
    FlatVector<StringView>& result =
        ensureWritableStringView(rows, context, resultRef);
    exec::LocalDecodedVector toSearch(context, *args[0], rows);
    exec::LocalDecodedVector pattern(context, *args[1], rows);
    bool mustRefSourceStrings = false;
    FOLLY_DECLARE_REUSED(groups, std::vector<re2::StringPiece>);
    if (args.size() == 2) {
      groups.resize(1);
      context.applyToSelectedNoThrow(rows, [&](vector_size_t i) {
        RE2 re(toStringPiece(pattern->valueAt<StringView>(i)), RE2::Quiet);
        checkForBadPattern(re);
        mustRefSourceStrings |=
            re2Extract(result, i, re, toSearch, groups, 0, emptyNoMatch_);
      });
    } else {
      exec::LocalDecodedVector groupIds(context, *args[2], rows);
      context.applyToSelectedNoThrow(rows, [&](vector_size_t i) {
        const auto groupId = groupIds->valueAt<T>(i);
        RE2 re(toStringPiece(pattern->valueAt<StringView>(i)), RE2::Quiet);
        checkForBadPattern(re);
        checkForBadGroupId(groupId, re);
        groups.resize(groupId + 1);
        mustRefSourceStrings |=
            re2Extract(result, i, re, toSearch, groups, groupId, emptyNoMatch_);
      });
    }
    if (mustRefSourceStrings) {
      result.acquireSharedStringBuffers(toSearch->base());
    }
  }

 private:
  const bool emptyNoMatch_;
};

// Match string 'input' with a fixed pattern (with no wildcard characters).
bool matchExactPattern(
    StringView input,
    const std::string& pattern,
    size_t length) {
  return input.size() == pattern.size() &&
      std::memcmp(input.data(), pattern.data(), length) == 0;
}

// Match the input(from the position of start) with relaxed pattern(from the
// specified subPattern).
bool matchRelaxedFixedPattern(
    StringView input,
    const std::vector<SubPatternMetadata>& subPatterns,
    size_t length,
    size_t start,
    size_t startPatternIndex = 0) {
  // Compare the length first.
  if (input.size() - start < length) {
    return false;
  }

  auto indexInString = start;
  for (auto i = startPatternIndex; i < subPatterns.size(); i++) {
    auto& subPattern = subPatterns[i];
    if (subPattern.kind == SubPatternMetadata::SubPatternKind::kLiteralString &&
        std::memcmp(
            input.data() + indexInString,
            subPattern.pattern.data(),
            subPattern.pattern.length()) != 0) {
      return false;
    }

    indexInString += subPattern.pattern.length();
  }

  return true;
}

// Match string 'input' with a kRelaxedFixed pattern.
bool matchRelaxedFixedPattern(
    StringView input,
    const std::vector<SubPatternMetadata>& subPatterns,
    size_t length) {
  // Compare the length first.
  if (input.size() != length) {
    return false;
  }

  return matchRelaxedFixedPattern(input, subPatterns, length, 0);
}

// Match the first 'length' characters of string 'input' and prefix pattern.
bool matchPrefixPattern(
    StringView input,
    const std::string& pattern,
    size_t length) {
  return input.size() >= length &&
      std::memcmp(input.data(), pattern.data(), length) == 0;
}

bool matchRelaxedPrefixPattern(
    StringView input,
    const std::vector<SubPatternMetadata>& subPatterns,
    size_t length) {
  if (input.size() < length) {
    return false;
  }

  return matchRelaxedFixedPattern(input, subPatterns, length, 0);
}

// Match the last 'length' characters of string 'input' and suffix pattern.
bool matchSuffixPattern(
    StringView input,
    const std::string& pattern,
    size_t length) {
  return input.size() >= length &&
      std::memcmp(
          input.data() + input.size() - length,
          pattern.data() + pattern.size() - length,
          length) == 0;
}

bool matchRelaxedSuffixPattern(
    StringView input,
    const std::vector<SubPatternMetadata>& subPatterns,
    size_t length) {
  if (input.size() < length) {
    return false;
  }

  return matchRelaxedFixedPattern(
      input, subPatterns, length, input.size() - length);
}

bool matchSubstringPattern(
    const StringView& input,
    const std::string& fixedPattern) {
  return (
      std::string_view(input).find(std::string_view(fixedPattern)) !=
      std::string::npos);
}

// For pattern '___hello___', it is split into three sub-patterns:
//   - kSingleCharWildcard: ___
//   - kLiteralString: hello
//   - kSingleCharWildcard: ___
//
// A naive implementation is search the three pattern every time, a better
// implementation is: skip the length of leading kSingleCharWildcard chars(3
// here), and search the first kLiteralString string in input to start match.
bool matchRelaxedSubstringPattern(
    StringView input,
    const PatternMetadata& patternMetadata) {
  if (input.size() < patternMetadata.length()) {
    return false;
  }

  // Valid search range is: [0, end].
  auto start = patternMetadata.leadingSingleWildcardCharsCount();
  auto startPatternIndex = start > 0 ? 1 : 0;
  auto& firstFixedString =
      patternMetadata.subPatterns()[startPatternIndex].pattern;
  auto indexInString = start;
  auto end = input.size() - patternMetadata.restFixedLength() + start;

  while (indexInString <= end) {
    // Search the firstFixedString to find out where to start match the whole
    // pattern.
    auto it = strstr(input.begin() + indexInString, firstFixedString.c_str());
    // The firstFixedString is not found, no need to match anymore.
    if (!it) {
      return false;
    }

    // Match from the place where find first fixed string.
    indexInString = it - input.begin();
    if (matchRelaxedFixedPattern(
            input,
            patternMetadata.subPatterns(),
            patternMetadata.restFixedLength(),
            indexInString,
            startPatternIndex)) {
      return true;
    } else {
      // Not match the whole pattern, advance the cursor.
      indexInString++;
    }
  }

  return false;
}

template <PatternKind P>
class OptimizedLike final : public VectorFunction {
 public:
  OptimizedLike(const PatternMetadata& patternMetadata)
      : patternMetadata_(std::move(patternMetadata)) {}

  static bool match(
      const StringView& input,
      const PatternMetadata& patternMetadata) {
    switch (P) {
      case PatternKind::kExactlyN:
        return input.size() == patternMetadata.length();
      case PatternKind::kAtLeastN:
        return input.size() >= patternMetadata.length();
      case PatternKind::kFixed:
        return matchExactPattern(
            input, patternMetadata.fixedPattern(), patternMetadata.length());
      case PatternKind::kRelaxedFixed:
        return matchRelaxedFixedPattern(
            input, patternMetadata.subPatterns(), patternMetadata.length());
      case PatternKind::kPrefix:
        return matchPrefixPattern(
            input, patternMetadata.fixedPattern(), patternMetadata.length());
      case PatternKind::kRelaxedPrefix:
        return matchRelaxedPrefixPattern(
            input, patternMetadata.subPatterns(), patternMetadata.length());
      case PatternKind::kSuffix:
        return matchSuffixPattern(
            input, patternMetadata.fixedPattern(), patternMetadata.length());
      case PatternKind::kRelaxedSuffix:
        return matchRelaxedSuffixPattern(
            input, patternMetadata.subPatterns(), patternMetadata.length());
      case PatternKind::kSubstring:
        return matchSubstringPattern(input, patternMetadata.fixedPattern());
      case PatternKind::kRelaxedSubstring:
        return matchRelaxedSubstringPattern(input, patternMetadata);
    }
  }

  void apply(
      const SelectivityVector& rows,
      std::vector<VectorPtr>& args,
      const TypePtr& /* outputType */,
      EvalCtx& context,
      VectorPtr& resultRef) const final {
    VELOX_CHECK(args.size() == 2 || args.size() == 3);
    FlatVector<bool>& result = ensureWritableBool(rows, context, resultRef);
    exec::DecodedArgs decodedArgs(rows, args, context);
    auto toSearch = decodedArgs.at(0);

    if (toSearch->isIdentityMapping()) {
      auto input = toSearch->data<StringView>();
      context.applyToSelectedNoThrow(rows, [&](vector_size_t i) {
        result.set(i, match(input[i], patternMetadata_));
      });
      return;
    }
    if (toSearch->isConstantMapping()) {
      auto input = toSearch->valueAt<StringView>(0);
      bool matchResult = match(input, patternMetadata_);
      context.applyToSelectedNoThrow(
          rows, [&](vector_size_t i) { result.set(i, matchResult); });
      return;
    }

    // Since the likePattern and escapeChar (2nd and 3rd args) are both
    // constants, so the first arg is expected to be either of flat or constant
    // vector only. This code path is unreachable.
    VELOX_UNREACHABLE();
  }

 private:
  const PatternMetadata patternMetadata_;
};

// This function is used when pattern and escape are constants. And there is not
// fast path that avoids compiling the regular expression.
class LikeWithRe2 final : public VectorFunction {
 public:
  LikeWithRe2(StringView pattern, std::optional<char> escapeChar) {
    RE2::Options opt{RE2::Quiet};
    opt.set_dot_nl(true);
    re_.emplace(
        toStringPiece(likePatternToRe2(pattern, escapeChar, validPattern_)),
        opt);
  }

  void apply(
      const SelectivityVector& rows,
      std::vector<VectorPtr>& args,
      const TypePtr& /* outputType */,
      EvalCtx& context,
      VectorPtr& resultRef) const final {
    VELOX_CHECK(args.size() == 2 || args.size() == 3);

    if (!validPattern_) {
      auto error = std::make_exception_ptr(std::invalid_argument(
          "Escape character must be followed by '%', '_' or the escape character itself"));
      context.setErrors(rows, error);
      return;
    }

    // apply() will not be invoked if the selection is empty.
    try {
      checkForBadPattern(*re_);
    } catch (const std::exception& e) {
      context.setErrors(rows, std::current_exception());
      return;
    }

    FlatVector<bool>& result = ensureWritableBool(rows, context, resultRef);

    exec::DecodedArgs decodedArgs(rows, args, context);
    auto toSearch = decodedArgs.at(0);
    if (toSearch->isIdentityMapping()) {
      auto rawStrings = toSearch->data<StringView>();
      context.applyToSelectedNoThrow(rows, [&](vector_size_t i) {
        result.set(i, re2FullMatch(rawStrings[i], *re_));
      });
      return;
    }

    if (toSearch->isConstantMapping()) {
      bool match = re2FullMatch(toSearch->valueAt<StringView>(0), *re_);
      context.applyToSelectedNoThrow(
          rows, [&](vector_size_t i) { result.set(i, match); });
      return;
    }

    // Since the likePattern and escapeChar (2nd and 3rd args) are both
    // constants, so the first arg is expected to be either of flat or constant
    // vector only. This code path is unreachable.
    VELOX_UNREACHABLE();
  }

 private:
  std::optional<RE2> re_;
  bool validPattern_;
};

// This function is constructed when pattern or escape are not constants.
// It allows up to kMaxCompiledRegexes different regular expressions to be
// compiled throughout the query life per function, note that optimized regular
// expressions that are not compiled are not counted.
class LikeGeneric final : public VectorFunction {
  void apply(
      const SelectivityVector& rows,
      std::vector<VectorPtr>& args,
      const TypePtr& type,
      EvalCtx& context,
      VectorPtr& result) const final {
    VectorPtr localResult;

    auto applyWithRegex = [&](const StringView& input,
                              const StringView& pattern,
                              const std::optional<char>& escapeChar) -> bool {
      RE2::Options opt{RE2::Quiet};
      opt.set_dot_nl(true);
      bool validEscapeUsage;
      auto regex = likePatternToRe2(pattern, escapeChar, validEscapeUsage);
      VELOX_USER_CHECK(
          validEscapeUsage,
          "Escape character must be followed by '%', '_' or the escape character itself");

      auto key =
          std::pair<StringView, std::optional<char>>{pattern, escapeChar};

      auto [it, inserted] = compiledRegularExpressions_.emplace(
          key, std::make_unique<RE2>(toStringPiece(regex), opt));
      VELOX_CHECK_LE(
          compiledRegularExpressions_.size(),
          kMaxCompiledRegexes,
          "Max number of regex reached");
      checkForBadPattern(*it->second);
      return re2FullMatch(input, *it->second);
    };

    auto applyRow = [&](const StringView& input,
                        const StringView& pattern,
                        const std::optional<char>& escapeChar) -> bool {
      PatternMetadata patternMetadata =
          determinePatternKind(pattern, escapeChar);

      switch (patternMetadata.patternKind()) {
        case PatternKind::kExactlyN:
          return OptimizedLike<PatternKind::kExactlyN>::match(
              input, patternMetadata);
        case PatternKind::kAtLeastN:
          return OptimizedLike<PatternKind::kAtLeastN>::match(
              input, patternMetadata);
        case PatternKind::kFixed:
          return OptimizedLike<PatternKind::kFixed>::match(
              input, patternMetadata);
        case PatternKind::kRelaxedFixed:
          return OptimizedLike<PatternKind::kRelaxedFixed>::match(
              input, patternMetadata);
        case PatternKind::kPrefix:
          return OptimizedLike<PatternKind::kPrefix>::match(
              input, patternMetadata);
        case PatternKind::kRelaxedPrefix:
          return OptimizedLike<PatternKind::kRelaxedPrefix>::match(
              input, patternMetadata);
        case PatternKind::kSuffix:
          return OptimizedLike<PatternKind::kSuffix>::match(
              input, patternMetadata);
        case PatternKind::kRelaxedSuffix:
          return OptimizedLike<PatternKind::kRelaxedSuffix>::match(
              input, patternMetadata);
        case PatternKind::kSubstring:
          return OptimizedLike<PatternKind::kSubstring>::match(
              input, patternMetadata);
        case PatternKind::kRelaxedSubstring:
          return OptimizedLike<PatternKind::kRelaxedSubstring>::match(
              input, patternMetadata);
        default:
          return applyWithRegex(input, pattern, escapeChar);
      }
    };

    context.ensureWritable(rows, type, localResult);
    exec::VectorWriter<bool> vectorWriter;
    vectorWriter.init(*localResult->asFlatVector<bool>());
    exec::DecodedArgs decodedArgs(rows, args, context);

    exec::VectorReader<Varchar> inputReader(decodedArgs.at(0));
    exec::VectorReader<Varchar> patternReader(decodedArgs.at(1));

    if (args.size() == 2) {
      context.applyToSelectedNoThrow(rows, [&](auto row) {
        vectorWriter.setOffset(row);
        vectorWriter.current() =
            applyRow(inputReader[row], patternReader[row], std::nullopt);
        vectorWriter.commit(true);
      });
    } else {
      VELOX_CHECK_EQ(args.size(), 3);
      exec::VectorReader<Varchar> escapeReader(decodedArgs.at(2));
      context.applyToSelectedNoThrow(rows, [&](auto row) {
        vectorWriter.setOffset(row);
        auto escapeChar = escapeReader[row];
        VELOX_USER_CHECK_EQ(
            escapeChar.size(), 1, "Escape string must be a single character");
        vectorWriter.current() = applyRow(
            inputReader[row], patternReader[row], escapeChar.data()[0]);
        vectorWriter.commit(true);
      });
    }

    vectorWriter.finish();
    context.moveOrCopyResult(localResult, rows, result);
  }

 private:
  mutable folly::F14FastMap<
      std::pair<std::string, std::optional<char>>,
      std::unique_ptr<RE2>>
      compiledRegularExpressions_;
};

void re2ExtractAll(
    exec::VectorWriter<Array<Varchar>>& resultWriter,
    const RE2& re,
    const exec::LocalDecodedVector& inputStrs,
    const int row,
    std::vector<re2::StringPiece>& groups,
    int32_t groupId) {
  resultWriter.setOffset(row);

  auto& arrayWriter = resultWriter.current();

  const StringView str = inputStrs->valueAt<StringView>(row);
  const re2::StringPiece input = toStringPiece(str);
  size_t pos = 0;

  while (re.Match(
      input, pos, input.size(), RE2::UNANCHORED, groups.data(), groupId + 1)) {
    DCHECK_GT(groups.size(), groupId);

    const re2::StringPiece fullMatch = groups[0];
    const re2::StringPiece subMatch = groups[groupId];

    arrayWriter.add_item().setNoCopy(
        StringView(subMatch.data(), subMatch.size()));
    pos = fullMatch.data() + fullMatch.size() - input.data();
    if (UNLIKELY(fullMatch.size() == 0)) {
      ++pos;
    }
  }

  resultWriter.commit();
}

template <typename T>
class Re2ExtractAllConstantPattern final : public VectorFunction {
 public:
  explicit Re2ExtractAllConstantPattern(StringView pattern)
      : re_(toStringPiece(pattern), RE2::Quiet) {}

  void apply(
      const SelectivityVector& rows,
      std::vector<VectorPtr>& args,
      const TypePtr& /* outputType */,
      EvalCtx& context,
      VectorPtr& resultRef) const final {
    VELOX_CHECK(args.size() == 2 || args.size() == 3);
    try {
      checkForBadPattern(re_);
    } catch (const std::exception& e) {
      context.setErrors(rows, std::current_exception());
      return;
    }

    BaseVector::ensureWritable(
        rows, ARRAY(VARCHAR()), context.pool(), resultRef);
    exec::VectorWriter<Array<Varchar>> resultWriter;
    resultWriter.init(*resultRef->as<ArrayVector>());

    exec::LocalDecodedVector inputStrs(context, *args[0], rows);
    FOLLY_DECLARE_REUSED(groups, std::vector<re2::StringPiece>);

    if (args.size() == 2) {
      // Case 1: No groupId -- use 0 as the default groupId
      //
      groups.resize(1);
      context.applyToSelectedNoThrow(rows, [&](vector_size_t row) {
        re2ExtractAll(resultWriter, re_, inputStrs, row, groups, 0);
      });
    } else if (const auto _groupId = getIfConstant<T>(*args[2])) {
      // Case 2: Constant groupId
      //
      try {
        checkForBadGroupId(*_groupId, re_);
      } catch (const std::exception& e) {
        context.setErrors(rows, std::current_exception());
        return;
      }

      groups.resize(*_groupId + 1);
      context.applyToSelectedNoThrow(rows, [&](vector_size_t row) {
        re2ExtractAll(resultWriter, re_, inputStrs, row, groups, *_groupId);
      });
    } else {
      // Case 3: Variable groupId, so resize the groups vector to accommodate
      // number of capturing groups + 1.
      exec::LocalDecodedVector groupIds(context, *args[2], rows);

      groups.resize(re_.NumberOfCapturingGroups() + 1);
      context.applyToSelectedNoThrow(rows, [&](vector_size_t row) {
        const T groupId = groupIds->valueAt<T>(row);
        checkForBadGroupId(groupId, re_);
        re2ExtractAll(resultWriter, re_, inputStrs, row, groups, groupId);
      });
    }

    resultWriter.finish();

    resultRef->as<ArrayVector>()
        ->elements()
        ->asFlatVector<StringView>()
        ->acquireSharedStringBuffers(inputStrs->base());
  }

 private:
  RE2 re_;
};

template <typename T>
class Re2ExtractAll final : public VectorFunction {
 public:
  void apply(
      const SelectivityVector& rows,
      std::vector<VectorPtr>& args,
      const TypePtr& outputType,
      EvalCtx& context,
      VectorPtr& resultRef) const final {
    VELOX_CHECK(args.size() == 2 || args.size() == 3);
    // Use Re2ExtractAllConstantPattern if it's constant regexp pattern.
    //
    if (auto pattern = getIfConstant<StringView>(*args[1])) {
      Re2ExtractAllConstantPattern<T>(*pattern).apply(
          rows, args, outputType, context, resultRef);
      return;
    }

    BaseVector::ensureWritable(
        rows, ARRAY(VARCHAR()), context.pool(), resultRef);
    exec::VectorWriter<Array<Varchar>> resultWriter;
    resultWriter.init(*resultRef->as<ArrayVector>());

    exec::LocalDecodedVector inputStrs(context, *args[0], rows);
    exec::LocalDecodedVector pattern(context, *args[1], rows);
    FOLLY_DECLARE_REUSED(groups, std::vector<re2::StringPiece>);

    if (args.size() == 2) {
      // Case 1: No groupId -- use 0 as the default groupId
      //
      groups.resize(1);
      context.applyToSelectedNoThrow(rows, [&](vector_size_t row) {
        RE2 re(toStringPiece(pattern->valueAt<StringView>(row)), RE2::Quiet);
        checkForBadPattern(re);
        re2ExtractAll(resultWriter, re, inputStrs, row, groups, 0);
      });
    } else {
      // Case 2: Has groupId
      //
      exec::LocalDecodedVector groupIds(context, *args[2], rows);
      context.applyToSelectedNoThrow(rows, [&](vector_size_t row) {
        const T groupId = groupIds->valueAt<T>(row);
        RE2 re(toStringPiece(pattern->valueAt<StringView>(row)), RE2::Quiet);
        checkForBadPattern(re);
        checkForBadGroupId(groupId, re);
        groups.resize(groupId + 1);
        re2ExtractAll(resultWriter, re, inputStrs, row, groups, groupId);
      });
    }

    resultWriter.finish();
    resultRef->as<ArrayVector>()
        ->elements()
        ->asFlatVector<StringView>()
        ->acquireSharedStringBuffers(inputStrs->base());
  }
};

template <bool (*Fn)(StringView, const RE2&)>
std::shared_ptr<VectorFunction> makeRe2MatchImpl(
    const std::string& name,
    const std::vector<VectorFunctionArg>& inputArgs) {
  if (inputArgs.size() != 2 || !inputArgs[0].type->isVarchar() ||
      !inputArgs[1].type->isVarchar()) {
    VELOX_UNSUPPORTED(
        "{} expected (VARCHAR, VARCHAR) but got ({})",
        name,
        printTypesCsv(inputArgs));
  }

  BaseVector* constantPattern = inputArgs[1].constantValue.get();

  if (constantPattern != nullptr && !constantPattern->isNullAt(0)) {
    return std::make_shared<Re2MatchConstantPattern<Fn>>(
        constantPattern->as<ConstantVector<StringView>>()->valueAt(0));
  }
  static std::shared_ptr<Re2Match<Fn>> kMatchExpr =
      std::make_shared<Re2Match<Fn>>();
  return kMatchExpr;
}

} // namespace

std::shared_ptr<VectorFunction> makeRe2Match(
    const std::string& name,
    const std::vector<VectorFunctionArg>& inputArgs,
    const core::QueryConfig& /*config*/) {
  return makeRe2MatchImpl<re2FullMatch>(name, inputArgs);
}

std::vector<std::shared_ptr<exec::FunctionSignature>> re2MatchSignatures() {
  // varchar, varchar -> boolean
  return {exec::FunctionSignatureBuilder()
              .returnType("boolean")
              .argumentType("varchar")
              .argumentType("varchar")
              .build()};
}

std::shared_ptr<VectorFunction> makeRe2Search(
    const std::string& name,
    const std::vector<VectorFunctionArg>& inputArgs,
    const core::QueryConfig& /*config*/) {
  return makeRe2MatchImpl<re2PartialMatch>(name, inputArgs);
}

std::vector<std::shared_ptr<exec::FunctionSignature>> re2SearchSignatures() {
  // varchar, varchar -> boolean
  return {exec::FunctionSignatureBuilder()
              .returnType("boolean")
              .argumentType("varchar")
              .argumentType("varchar")
              .build()};
}

std::shared_ptr<VectorFunction> makeRe2Extract(
    const std::string& name,
    const std::vector<VectorFunctionArg>& inputArgs,
    const core::QueryConfig& /*config*/,
    const bool emptyNoMatch) {
  auto numArgs = inputArgs.size();
  VELOX_USER_CHECK(
      numArgs == 2 || numArgs == 3,
      "{} requires 2 or 3 arguments, but got {}",
      name,
      numArgs);

  VELOX_USER_CHECK(
      inputArgs[0].type->isVarchar(),
      "{} requires first argument of type VARCHAR, but got {}",
      name,
      inputArgs[0].type->toString());

  VELOX_USER_CHECK(
      inputArgs[1].type->isVarchar(),
      "{} requires second argument of type VARCHAR, but got {}",
      name,
      inputArgs[1].type->toString());

  TypeKind groupIdTypeKind = TypeKind::INTEGER;
  if (numArgs == 3) {
    groupIdTypeKind = inputArgs[2].type->kind();
    VELOX_USER_CHECK(
        groupIdTypeKind == TypeKind::INTEGER ||
            groupIdTypeKind == TypeKind::BIGINT,
        "{} requires third argument of type INTEGER or BIGINT, but got {}",
        name,
        mapTypeKindToName(groupIdTypeKind));
  }

  BaseVector* constantPattern = inputArgs[1].constantValue.get();

  if (constantPattern != nullptr && !constantPattern->isNullAt(0)) {
    auto pattern =
        constantPattern->as<ConstantVector<StringView>>()->valueAt(0);
    switch (groupIdTypeKind) {
      case TypeKind::INTEGER:
        return std::make_shared<Re2SearchAndExtractConstantPattern<int32_t>>(
            pattern, emptyNoMatch);
      case TypeKind::BIGINT:
        return std::make_shared<Re2SearchAndExtractConstantPattern<int64_t>>(
            pattern, emptyNoMatch);
      default:
        VELOX_UNREACHABLE();
    }
  }

  switch (groupIdTypeKind) {
    case TypeKind::INTEGER:
      return std::make_shared<Re2SearchAndExtract<int32_t>>(emptyNoMatch);
    case TypeKind::BIGINT:
      return std::make_shared<Re2SearchAndExtract<int64_t>>(emptyNoMatch);
    default:
      VELOX_UNREACHABLE();
  }
}

std::vector<std::shared_ptr<exec::FunctionSignature>> re2ExtractSignatures() {
  // varchar, varchar -> boolean
  // varchar, varchar, integer|bigint -> boolean
  return {
      exec::FunctionSignatureBuilder()
          .returnType("varchar")
          .argumentType("varchar")
          .argumentType("varchar")
          .build(),
      exec::FunctionSignatureBuilder()
          .returnType("varchar")
          .argumentType("varchar")
          .argumentType("varchar")
          .argumentType("bigint")
          .build(),
      exec::FunctionSignatureBuilder()
          .returnType("varchar")
          .argumentType("varchar")
          .argumentType("varchar")
          .argumentType("integer")
          .build(),
  };
}

// Return the length of the fixed part of the pattern.
size_t fixedLength(
    const std::vector<SubPatternMetadata>& subPatterns,
    size_t start = 0) {
  size_t result = 0;
  for (auto i = start; i < subPatterns.size(); i++) {
    if (subPatterns[i].kind !=
        SubPatternMetadata::SubPatternKind::kAnyCharsWildcard) {
      result += subPatterns[i].pattern.length();
    }
  }

  return result;
}

PatternMetadata PatternMetadata::generic() {
  return {PatternKind::kGeneric, 0, {}, 0, 0};
}

PatternMetadata PatternMetadata::atLeastN(size_t length) {
  return {PatternKind::kAtLeastN, length, {}, 0, 0};
}

PatternMetadata PatternMetadata::exactlyN(size_t length) {
  return {PatternKind::kExactlyN, length, {}, 0, 0};
}

PatternMetadata PatternMetadata::fixed(
    std::vector<SubPatternMetadata> subPatterns) {
  return {
      PatternKind::kFixed,
      fixedLength(subPatterns),
      std::move(subPatterns),
      0,
      0};
}

PatternMetadata PatternMetadata::relaxedFixed(
    std::vector<SubPatternMetadata> subPatterns) {
  return {
      PatternKind::kRelaxedFixed,
      fixedLength(subPatterns),
      std::move(subPatterns),
      0,
      0};
}

PatternMetadata PatternMetadata::prefix(
    std::vector<SubPatternMetadata> subPatterns) {
  return {
      PatternKind::kPrefix,
      fixedLength(subPatterns),
      std::move(subPatterns),
      0,
      0};
}

PatternMetadata PatternMetadata::relaxedPrefix(
    std::vector<SubPatternMetadata> subPatterns) {
  return {
      PatternKind::kRelaxedPrefix,
      fixedLength(subPatterns),
      std::move(subPatterns),
      0,
      0};
}

PatternMetadata PatternMetadata::suffix(
    std::vector<SubPatternMetadata> subPatterns) {
  return {
      PatternKind::kSuffix,
      fixedLength(subPatterns),
      std::move(subPatterns),
      0,
      0};
}

PatternMetadata PatternMetadata::relaxedSuffix(
    std::vector<SubPatternMetadata> subPatterns) {
  return {
      PatternKind::kRelaxedSuffix,
      fixedLength(subPatterns),
      std::move(subPatterns),
      0,
      0};
}

PatternMetadata PatternMetadata::substring(
    std::vector<SubPatternMetadata> subPatterns) {
  return {
      PatternKind::kSubstring,
      fixedLength(subPatterns),
      std::move(subPatterns),
      0,
      0};
}

PatternMetadata PatternMetadata::relaxedSubstring(
    std::vector<SubPatternMetadata> subPatterns) {
  auto leadingSingleWildcardCharsCount =
      subPatterns[0].kind == SubPatternMetadata::SubPatternKind::kLiteralString
      ? 0
      : subPatterns[0].pattern.length();
  auto restFixedLength = fixedLength(subPatterns, 1);

  return {
      PatternKind::kRelaxedSubstring,
      fixedLength(subPatterns),
      std::move(subPatterns),
      leadingSingleWildcardCharsCount,
      restFixedLength};
}

PatternMetadata::PatternMetadata(
    PatternKind patternKind,
    size_t length,
    std::vector<SubPatternMetadata> subPatterns,
    size_t leadingSingleWildcardCharsCount,
    size_t restFixedLength)
    : patternKind_{patternKind},
      length_{length},
      subPatterns_(subPatterns),
      leadingSingleWildcardCharsCount_(leadingSingleWildcardCharsCount),
      restFixedLength_(restFixedLength) {
  std::ostringstream os;
  for (auto& subPattern : subPatterns_) {
    os << subPattern.pattern;
  }
  fixedPattern_ = os.str();
}

std::string unescape(
    StringView pattern,
    size_t start,
    size_t end,
    std::optional<char> escapeChar) {
  if (!escapeChar) {
    return std::string(pattern.data(), start, end - start);
  }

  std::ostringstream os;
  auto cursor = pattern.begin() + start;
  auto endCursor = pattern.begin() + end;
  while (cursor < endCursor) {
    auto previous = cursor;

    // Find the next escape char.
    cursor = std::find(cursor, endCursor, escapeChar.value());
    if (cursor < endCursor) {
      // There are non-escape chars, append them.
      if (previous < cursor) {
        os.write(previous, cursor - previous);
      }

      // Make sure there is a following normal char.
      VELOX_USER_CHECK(
          cursor + 1 < endCursor,
          "Escape character must be followed by '%', '_' or the escape character itself");

      // Make sure the escaped char is valid.
      cursor++;
      auto current = *cursor;
      VELOX_USER_CHECK(
          current == escapeChar || current == '_' || current == '%',
          "Escape character must be followed by '%', '_' or the escape character itself");

      // Append the escaped char.
      os << current;
    } else {
      // Escape char not found, append all the non-escape chars.
      os.write(previous, endCursor - previous);
      break;
    }

    // Advance the cursor.
    cursor++;
  }

  return os.str();
}

// Iterates through a pattern string. Transparently handles escape sequences.
class PatternStringIterator {
 public:
  PatternStringIterator(StringView pattern, std::optional<char> escapeChar)
      : pattern_(pattern),
        escapeChar_(escapeChar),
        lastIndex_{pattern_.size() - 1} {}

  // Advance the cursor to next char, escape char is automatically handled.
  // Return true if the cursor is advanced successfully, false otherwise(reached
  // the end of the pattern string).
  bool next() {
    if (currentIndex_ == lastIndex_) {
      return false;
    }

    isPreviousWildcard_ =
        (charKind_ == CharKind::kSingleCharWildcard ||
         charKind_ == CharKind::kAnyCharsWildcard);

    currentIndex_++;
    auto currentChar = current();
    if (currentChar == escapeChar_) {
      // Escape char should be followed by another char.
      VELOX_USER_CHECK_LT(
          currentIndex_,
          lastIndex_,
          "Escape character must be followed by '%', '_' or the escape character itself: {}, escape {}",
          pattern_,
          escapeChar_.value())

      currentIndex_++;
      currentChar = current();
      // The char follows escapeChar can only be one of (%, _, escapeChar).
      if (currentChar == escapeChar_ || currentChar == '_' ||
          currentChar == '%') {
        charKind_ = CharKind::kNormal;
      } else {
        VELOX_USER_FAIL(
            "Escape character must be followed by '%', '_' or the escape character itself: {}, escape {}",
            pattern_,
            escapeChar_.value())
      }
    } else if (currentChar == '_') {
      charKind_ = CharKind::kSingleCharWildcard;
    } else if (currentChar == '%') {
      charKind_ = CharKind::kAnyCharsWildcard;
    } else {
      charKind_ = CharKind::kNormal;
    }

    return true;
  }

  // Current index of the cursor.
  char currentIndex() const {
    return currentIndex_;
  }

  bool isAnyCharsWildcard() const {
    return charKind_ == CharKind::kAnyCharsWildcard;
  }

  bool isSingleCharWildcard() const {
    return charKind_ == CharKind::kSingleCharWildcard;
  }

  bool isWildcard() {
    return isAnyCharsWildcard() || isSingleCharWildcard();
  }

  bool isPreviousWildcard() {
    return isPreviousWildcard_;
  }

  // Char at current cursor.
  char current() const {
    return pattern_.data()[currentIndex_];
  }

 private:
  // Represents the state of current cursor/char.
  enum class CharKind {
    // Wildcard char: %.
    // NOTE: If escape char is set as '\', for pattern '\%%', the first '%' is
    // not a wildcard, just a literal '%', the second '%' is a wildcard.
    kAnyCharsWildcard,
    // Wildcard char: _.
    // NOTE: If escape char is set as '\', for pattern '\__', the first '_' is
    // not a wildcard, just a literal '_', the second '_' is a wildcard.
    kSingleCharWildcard,
    // Chars that are not escape char & not wildcard char.
    kNormal
  };

  const StringView pattern_;
  const std::optional<char> escapeChar_;
  const size_t lastIndex_;

  int32_t currentIndex_{-1};
  CharKind charKind_{CharKind::kNormal};
  bool isPreviousWildcard_{false};
};

// Sub-pattern's stats in a top-level pattern.
struct SubPatternStats {
  SubPatternMetadata::SubPatternKind kind;
  // Count of this patterns in total.
  size_t count = 0;
  // First index of this pattern kind.
  size_t firstIndex = -1;
  // Last index of this pattern kind.
  size_t lastIndex = -1;

  void update(size_t index) {
    count++;
    if (firstIndex == -1) {
      firstIndex = index;
      lastIndex = index;
    } else {
      lastIndex = index;
    }
  }
};

PatternMetadata determinePatternKind(
    StringView pattern,
    std::optional<char> escapeChar) {
  int32_t patternLength = pattern.size();

  std::vector<SubPatternMetadata> subPatterns;
  PatternStringIterator iterator{pattern, escapeChar};

  // Iterate through the pattern string to collect the stats for the simple
  // patterns that we can optimize.
  std::ostringstream os;
  SubPatternMetadata::SubPatternKind previousKind;
  bool isFirst = true;
  while (iterator.next()) {
    SubPatternMetadata::SubPatternKind currentKind;
    if (iterator.isSingleCharWildcard()) {
      currentKind = SubPatternMetadata::SubPatternKind::kSingleCharWildcard;
    } else if (iterator.isAnyCharsWildcard()) {
      currentKind = SubPatternMetadata::SubPatternKind::kAnyCharsWildcard;
    } else {
      currentKind = SubPatternMetadata::SubPatternKind::kLiteralString;
    }

    // New sub pattern occurs.
    if (currentKind != previousKind && !isFirst) {
      subPatterns.push_back({previousKind, os.str()});

      // Reset the stream.
      os.str("");
    }

    isFirst = false;
    previousKind = currentKind;
    os << iterator.current();
  }

  // Handle the last sub-pattern.
  subPatterns.push_back({previousKind, os.str()});

  int numSubPatterns = subPatterns.size();
  SubPatternStats literalStats{
      SubPatternMetadata::SubPatternKind::kLiteralString};
  SubPatternStats singleCharWildcardStats{
      SubPatternMetadata::SubPatternKind::kSingleCharWildcard};
  SubPatternStats anyCharsWildcardStats{
      SubPatternMetadata::SubPatternKind::kAnyCharsWildcard};

  // Collect the pattern stats.
  for (auto i = 0; i < numSubPatterns; i++) {
    const auto& subPattern = subPatterns[i];
    switch (subPattern.kind) {
      case SubPatternMetadata::SubPatternKind::kLiteralString:
        literalStats.update(i);
        break;
      case SubPatternMetadata::SubPatternKind::kSingleCharWildcard:
        singleCharWildcardStats.update(i);
        break;
      case SubPatternMetadata::SubPatternKind::kAnyCharsWildcard:
        anyCharsWildcardStats.update(i);
        break;
    }
  }

  // Determine optimized pattern base on stats we have.
  SubPatternMetadata& firstSubPattern = subPatterns[0];
  SubPatternMetadata& lastSubPattern = subPatterns[numSubPatterns - 1];

  // Single sub-pattern.
  if (numSubPatterns == 1) {
    if (firstSubPattern.kind ==
        SubPatternMetadata::SubPatternKind::kSingleCharWildcard) {
      return PatternMetadata::exactlyN(firstSubPattern.pattern.size());
    } else if (
        firstSubPattern.kind ==
        SubPatternMetadata::SubPatternKind::kAnyCharsWildcard) {
      return PatternMetadata::atLeastN(0);
    } else {
      return PatternMetadata::fixed(subPatterns);
    }
  } else { // Multiple sub-patterns.
    // No kLiteralString sub-pattern.
    if (literalStats.count == 0) {
      size_t singleCharacterWildcardCount = 0;

      // Count the number of single character wildcard count.
      for (auto i = 0; i < numSubPatterns; i++) {
        SubPatternMetadata& subPattern = subPatterns[i];
        if (subPattern.kind ==
            SubPatternMetadata::SubPatternKind::kSingleCharWildcard) {
          singleCharacterWildcardCount += subPattern.pattern.size();
        }
      }
      return PatternMetadata::atLeastN(singleCharacterWildcardCount);
    } else {
      // At this point, the pattern contains at least one kLiteralString
      // sub-pattern.

      // If there are only one literal sub-pattern and several any-wildcard
      // sub-patterns.
      if (singleCharWildcardStats.count == 0 && literalStats.count == 1 &&
          anyCharsWildcardStats.count > 0) {
        if (firstSubPattern.kind ==
            SubPatternMetadata::SubPatternKind::kLiteralString) {
          return PatternMetadata::prefix({firstSubPattern});
        } else if (
            lastSubPattern.kind ==
            SubPatternMetadata::SubPatternKind::kLiteralString) {
          return PatternMetadata::suffix({lastSubPattern});
        } else if (
            subPatterns.size() == 3 &&
            firstSubPattern.kind ==
                SubPatternMetadata::SubPatternKind::kAnyCharsWildcard &&
            lastSubPattern.kind ==
                SubPatternMetadata::SubPatternKind::kAnyCharsWildcard) {
          return PatternMetadata::substring({subPatterns[1]});
        }
      }

      // No any-wildcard sub-pattern.
      if (anyCharsWildcardStats.count == 0 &&
          singleCharWildcardStats.count > 0) {
        return PatternMetadata::relaxedFixed(std::move(subPatterns));
      }

      // Pattern contains kAnyCharsWildcard, kSingleCharWildcard &
      // kLiteralString.
      if (singleCharWildcardStats.count > 0 &&
          anyCharsWildcardStats.count > 0) {
        auto firstOfLiteralOrSingleWildcard = std::min(
            literalStats.firstIndex, singleCharWildcardStats.firstIndex);
        auto lastOfLiteralOrSingleWildcard =
            std::max(literalStats.lastIndex, singleCharWildcardStats.lastIndex);

        // kSingleCharWildcard & kLiteralString sub-patterns surrounded by
        // kAnyCharsWildcard sub-pattern.
        if (firstOfLiteralOrSingleWildcard > 0 &&
            lastOfLiteralOrSingleWildcard < numSubPatterns - 1) {
          bool containsAnyWildcardBetween = false;
          for (auto i = firstOfLiteralOrSingleWildcard + 1;
               i < lastOfLiteralOrSingleWildcard;
               i++) {
            if (subPatterns[i].kind ==
                SubPatternMetadata::SubPatternKind::kAnyCharsWildcard) {
              containsAnyWildcardBetween = true;
              break;
            }
          }

          if (!containsAnyWildcardBetween) {
            std::vector<SubPatternMetadata> literalOrStringPatterns{
                subPatterns.begin() + firstOfLiteralOrSingleWildcard,
                subPatterns.begin() + lastOfLiteralOrSingleWildcard + 1};
            return PatternMetadata::relaxedSubstring(
                std::move(literalOrStringPatterns));
          }
        } else if (
            lastOfLiteralOrSingleWildcard < anyCharsWildcardStats.lastIndex) {
          std::vector<SubPatternMetadata> literalOrStringPatterns{
              subPatterns.begin() + firstOfLiteralOrSingleWildcard,
              subPatterns.begin() + lastOfLiteralOrSingleWildcard + 1};
          return PatternMetadata::relaxedPrefix(
              std::move(literalOrStringPatterns));
        } else if (
            firstOfLiteralOrSingleWildcard > anyCharsWildcardStats.lastIndex) {
          std::vector<SubPatternMetadata> literalOrStringPatterns{
              subPatterns.begin() + firstOfLiteralOrSingleWildcard,
              subPatterns.begin() + lastOfLiteralOrSingleWildcard + 1};
          return PatternMetadata::relaxedSuffix(
              std::move(literalOrStringPatterns));
        }
      }
    }
  }

  return PatternMetadata::generic();
}

std::shared_ptr<exec::VectorFunction> makeLike(
    const std::string& name,
    const std::vector<exec::VectorFunctionArg>& inputArgs,
    const core::QueryConfig& /*config*/) {
  auto numArgs = inputArgs.size();

  std::optional<char> escapeChar;
  if (numArgs == 3) {
    BaseVector* escape = inputArgs[2].constantValue.get();
    if (!escape) {
      return std::make_shared<LikeGeneric>();
    }

    auto constantEscape = escape->as<ConstantVector<StringView>>();
    if (constantEscape->isNullAt(0)) {
      return std::make_shared<exec::ApplyNeverCalled>();
    }

    try {
      VELOX_USER_CHECK_EQ(
          constantEscape->valueAt(0).size(),
          1,
          "Escape string must be a single character");
    } catch (...) {
      return std::make_shared<exec::AlwaysFailingVectorFunction>(
          std::current_exception());
    }
    escapeChar = constantEscape->valueAt(0).data()[0];
  }

  BaseVector* constantPattern = inputArgs[1].constantValue.get();
  if (!constantPattern) {
    return std::make_shared<LikeGeneric>();
  }

  if (constantPattern->isNullAt(0)) {
    return std::make_shared<exec::ApplyNeverCalled>();
  }
  auto pattern = constantPattern->as<ConstantVector<StringView>>()->valueAt(0);

  PatternMetadata patternMetadata = PatternMetadata::generic();
  try {
    patternMetadata = determinePatternKind(pattern, escapeChar);
  } catch (...) {
    return std::make_shared<exec::AlwaysFailingVectorFunction>(
        std::current_exception());
  }

  switch (patternMetadata.patternKind()) {
    case PatternKind::kExactlyN:
      return std::make_shared<OptimizedLike<PatternKind::kExactlyN>>(
          patternMetadata);
    case PatternKind::kAtLeastN:
      return std::make_shared<OptimizedLike<PatternKind::kAtLeastN>>(
          patternMetadata);
    case PatternKind::kFixed:
      return std::make_shared<OptimizedLike<PatternKind::kFixed>>(
          patternMetadata);
    case PatternKind::kRelaxedFixed:
      return std::make_shared<OptimizedLike<PatternKind::kRelaxedFixed>>(
          patternMetadata);
    case PatternKind::kPrefix:
      return std::make_shared<OptimizedLike<PatternKind::kPrefix>>(
          patternMetadata);
    case PatternKind::kRelaxedPrefix:
      return std::make_shared<OptimizedLike<PatternKind::kRelaxedPrefix>>(
          patternMetadata);
    case PatternKind::kSuffix:
      return std::make_shared<OptimizedLike<PatternKind::kSuffix>>(
          patternMetadata);
    case PatternKind::kRelaxedSuffix:
      return std::make_shared<OptimizedLike<PatternKind::kRelaxedSuffix>>(
          patternMetadata);
    case PatternKind::kSubstring:
      return std::make_shared<OptimizedLike<PatternKind::kSubstring>>(
          patternMetadata);
    case PatternKind::kRelaxedSubstring:
      return std::make_shared<OptimizedLike<PatternKind::kRelaxedSubstring>>(
          patternMetadata);
    default:
      return std::make_shared<LikeWithRe2>(pattern, escapeChar);
  }
}

std::vector<std::shared_ptr<exec::FunctionSignature>> likeSignatures() {
  // varchar, varchar -> boolean
  // varchar, varchar, varchar -> boolean
  return {
      exec::FunctionSignatureBuilder()
          .returnType("boolean")
          .argumentType("varchar")
          .constantArgumentType("varchar")
          .build(),
      exec::FunctionSignatureBuilder()
          .returnType("boolean")
          .argumentType("varchar")
          .constantArgumentType("varchar")
          .constantArgumentType("varchar")
          .build(),
  };
}

std::shared_ptr<VectorFunction> makeRe2ExtractAll(
    const std::string& name,
    const std::vector<VectorFunctionArg>& inputArgs,
    const core::QueryConfig& /*config*/) {
  auto numArgs = inputArgs.size();
  VELOX_USER_CHECK(
      numArgs == 2 || numArgs == 3,
      "{} requires 2 or 3 arguments, but got {}",
      name,
      numArgs);

  VELOX_USER_CHECK(
      inputArgs[0].type->isVarchar(),
      "{} requires first argument of type VARCHAR, but got {}",
      name,
      inputArgs[0].type->toString());

  VELOX_USER_CHECK(
      inputArgs[1].type->isVarchar(),
      "{} requires second argument of type VARCHAR, but got {}",
      name,
      inputArgs[1].type->toString());

  TypeKind groupIdTypeKind = TypeKind::INTEGER;
  if (numArgs == 3) {
    groupIdTypeKind = inputArgs[2].type->kind();
    VELOX_USER_CHECK(
        groupIdTypeKind == TypeKind::INTEGER ||
            groupIdTypeKind == TypeKind::BIGINT,
        "{} requires third argument of type INTEGER or BIGINT, but got {}",
        name,
        mapTypeKindToName(groupIdTypeKind));
  }

  BaseVector* constantPattern = inputArgs[1].constantValue.get();
  if (constantPattern != nullptr && !constantPattern->isNullAt(0)) {
    auto pattern =
        constantPattern->as<ConstantVector<StringView>>()->valueAt(0);
    switch (groupIdTypeKind) {
      case TypeKind::INTEGER:
        return std::make_shared<Re2ExtractAllConstantPattern<int32_t>>(pattern);
      case TypeKind::BIGINT:
        return std::make_shared<Re2ExtractAllConstantPattern<int64_t>>(pattern);
      default:
        VELOX_UNREACHABLE();
    }
  }

  switch (groupIdTypeKind) {
    case TypeKind::INTEGER:
      return std::make_shared<Re2ExtractAll<int32_t>>();
    case TypeKind::BIGINT:
      return std::make_shared<Re2ExtractAll<int64_t>>();
    default:
      VELOX_UNREACHABLE();
  }
}

std::vector<std::shared_ptr<exec::FunctionSignature>>
re2ExtractAllSignatures() {
  // varchar, varchar -> array<varchar>
  // varchar, varchar, integer|bigint -> array<varchar>
  return {
      exec::FunctionSignatureBuilder()
          .returnType("array(varchar)")
          .argumentType("varchar")
          .argumentType("varchar")
          .build(),
      exec::FunctionSignatureBuilder()
          .returnType("array(varchar)")
          .argumentType("varchar")
          .argumentType("varchar")
          .argumentType("bigint")
          .build(),
      exec::FunctionSignatureBuilder()
          .returnType("array(varchar)")
          .argumentType("varchar")
          .argumentType("varchar")
          .argumentType("integer")
          .build(),
  };
}

} // namespace facebook::velox::functions
