#pragma once

#include <iostream>
#include <glog/logging.h>
#include "velox/buffer/Buffer.h"
#include "velox/vector/DictionaryVector.h"
#include "velox/expression/VectorUdfTypeSystem.h"
#include "velox/functions/lib/LambdaFunctionUtil.h"
#include "velox/expression/ComplexViewTypes.h"

namespace facebook::velox::functions {

void printStartLine(const std::string title) {
  std::cout << "----------------- [" << title << "] START ----------------" << std::endl;
}

void printEndLine(const std::string title) {
  std::cout << "----------------- [" << title << "] END ----------------" << std::endl;
}

void printIndices(int size, BufferPtr indices, const std::string& title = "") {
  std::string actualTitle = title;
  if (actualTitle.empty()) {
    actualTitle = "indices";
  }
  printStartLine(actualTitle);
  auto rawIndices = indices->asMutable<vector_size_t>();

  for (auto i = 0; i < size; i++) {
    std::cout << rawIndices[i] << ", ";
  }

  std::cout << std::endl;
}

DecodedVector* decodeArrayElements(
    exec::LocalDecodedVector& arrayVectorDecoder,
    exec::LocalDecodedVector& elementsDecoder,
    const SelectivityVector& rows) {
  auto decodedVector = arrayVectorDecoder.get();
  auto baseArrayVector = arrayVectorDecoder->base()->as<ArrayVector>();

  // Decode and acquire array elements vector.
  auto elementsVector = baseArrayVector->elements();
  auto elementsSelectivityRows = toElementRows(
      elementsVector->size(), rows, baseArrayVector, decodedVector->indices());
  elementsDecoder.get()->decode(*elementsVector, elementsSelectivityRows);
  auto decodedElementsVector = elementsDecoder.get();
  return decodedElementsVector;
}

template<typename T>
void printArrayVector(ArrayVector* vector, const SelectivityVector& rows) {
  DecodedVector decoded;
  decoded.decode(*vector, rows);

  exec::VectorReader<Array<T>> reader(&decoded);

  rows.applyToSelected([&](vector_size_t row) {
    std::cout << "[" << row << "]: ";
    // Check if the row is null.
    if(!reader.isSet(row)) {
      std::cout << "<NULL>" << std::endl;
      return;
    }

    // Read the row as ArrayView. ArrayView has std::vector<std::optional<V>> interface.
    exec::ArrayView<true, T> arrayView = reader[row];


    // Elements of the array have std::map<int, std::optional<int>> interface.
    for (auto i = 0; i < arrayView.size(); i++) {
      auto val = arrayView[i];

      std::cout << val.value();

      if (i < arrayView.size() - 1) {
        std::cout << ", ";
      }
    }
    std::cout << std::endl;
  });
}

template<typename T>
void printArrayVector(ArrayVector* vector) {
  SelectivityVector rows(vector->size(), true);
  printArrayVector<T>(vector, rows);
}

template<typename T>
void printArrayVector(
    std::string title,
    exec::EvalCtx* context,
    const SelectivityVector& rows,
    exec::LocalDecodedVector& vectorDecoder) {
  printStartLine(title);
  exec::LocalDecodedVector elementsDecoder(context);
  DecodedVector* decodedElements = decodeArrayElements(vectorDecoder, elementsDecoder, rows);

  const ArrayVector* baseArrayVector = vectorDecoder.get()->base()->template as<ArrayVector>();

  rows.template applyToSelected([&](vector_size_t row){
    // 对于每一行
    auto idx = vectorDecoder.get()->index(row);
    auto size = baseArrayVector->sizeAt(idx);
    auto offset = baseArrayVector->offsetAt(idx);

    for (auto i = offset; i < offset + size; i++) {
      if (decodedElements->isNullAt(i)) {
        std::cout << "<NULL>";
      } else {
        if (std::is_same_v<T, StringView>) {
          std::cout << decodedElements->template valueAt<StringView>(i);
        } else if (std::is_same_v<T, bool>) {
          std::cout << decodedElements->template valueAt<bool>(i);
        } else {
          std::cout << "<...>";
        }
      }

      std::cout << ", ";
    }
    std::cout << std::endl;
  });
  printEndLine(title);
}

void printArrayElementsVector(
    std::string&& title,
    exec::EvalCtx* context,
    const SelectivityVector& rows,
    exec::LocalDecodedVector& arrayVectorDecoder) {
  printStartLine(title);
  exec::LocalDecodedVector elementsDecoder(context);
  DecodedVector* decodedElements =
      decodeArrayElements(arrayVectorDecoder, elementsDecoder, rows);

  for (auto i = 0; i < decodedElements->size(); i++) {
    if (i % 5 == 0) {
      std::cout << "[" << i << " - " << i + 5 << "]: ";
    }

    if (decodedElements->isNullAt(i)) {
      std::cout << "<NULL>";
    } else {
      TypeKind kind = decodedElements->base()->typeKind();
      // using T = typename TypeTraits<kind>::NativeType;
      if (kind == TypeKind::BOOLEAN) {
        bool val = decodedElements->template valueAt<bool>(i);
        std::cout << val;
      } else if (kind == TypeKind::VARCHAR) {
        StringView val = decodedElements->template valueAt<StringView>(i);
        std::cout << val;
      } else {
        std::cout << "<...>";
      }
      // std::cout << val;
    }

    std::cout << ", ";

    if (i % 5 == 4) {
      std::cout << std::endl;
    }
  }
  std::cout << std::endl;
}

// 打印一个FlatVector
void printFlatVector(std::string&& title, VectorPtr vector) {
  printStartLine(title);

  std::cout << "vector size: " << vector->size() << ", vector type: " << vector->type()->kindName() << std::endl;
  for (auto i = 0; i < vector->size(); i++) {
    if (i % 5 == 0) {
      std::cout << "[" << i << " - " << i + 5 << "]: ";
    }

    if (vector->isNullAt(i)) {
      std::cout << "<NULL>";
    } else {
      TypeKind kind = vector->typeKind();
      if (kind == TypeKind::BOOLEAN) {
        FlatVector<bool>* flatVector = vector->asFlatVector<bool>();
        bool val = flatVector->valueAt(i);
        std::cout << val;
      } else if (kind == TypeKind::VARCHAR) {
        FlatVector<StringView>* flatVector = vector->asFlatVector<StringView>();
        StringView val = flatVector->valueAt(i);
        std::cout << val;
      }
    }

    std::cout << ", ";

    if (i % 5 == 4) {
      std::cout << std::endl;
    }
  }
  std::cout << std::endl;

  printEndLine(title);
}

// 打印一个DictionaryVector
template<typename T>
void printDictVector(const std::string&& title, const DictionaryVector<T>* vector) {
  printStartLine(title);

  std::cout << "size: " << vector->size() << std::endl;
  for (auto i = 0; i < vector->size(); i++) {
    if (i % 5 == 0) {
      std::cout << "[" << i << " - " << i + 5 << "]: ";
    }

    if (vector->isNullAt(i)) {
      std::cout << "<NULL>";
    } else {
      TypeKind kind = vector->typeKind();
      T val = vector->valueAt(i);

      std::cout << typeid(T).name() << std::endl;
      if (kind == TypeKind::BOOLEAN) {
      } else if (kind == TypeKind::VARCHAR) {
        std::cout << (StringView) val;
      }
    }

    std::cout << ", ";

    if (i % 5 == 4) {
      std::cout << std::endl;
    }
  }

  std::cout << std::endl;
}

void printSelectivityVector(const std::string&& title, const SelectivityVector& selectivityVector) {
  printStartLine(title);
  Range<bool> selecteds = selectivityVector.asRange();
  for (auto it = selecteds.begin(); it < selecteds.end(); it++) {
    std::cout << "[" << it << "]: " << selecteds[it] << ",";
  }
  std::cout << std::endl;
}

void printOffsetsAndSizes(std::string&& title, int size, BufferPtr offsets, BufferPtr sizes) {
  printStartLine(title);
  auto rawOffsets = offsets->asMutable<vector_size_t>();
  auto rawSizes = sizes->asMutable<vector_size_t>();

  for (auto i = 0; i < size; i++) {
    std::cout << "[" << rawOffsets[i] << " -> " << (rawOffsets[i] + rawSizes[i]) << "), ";
  }
  std::cout << std::endl;
}
} // namespace abei::printer
