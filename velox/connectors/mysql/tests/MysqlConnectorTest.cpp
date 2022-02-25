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

#include "gtest/gtest.h"
#include "velox/connectors/mysql/MysqlConnector.h"
#include "velox/type/Type.h"
#include "velox/vector/FlatVector.h"

using namespace facebook::velox;
using namespace facebook::velox::connector::mysql;

void rowVectorEquals(RowVectorPtr expected, RowVectorPtr actual) {
  // 比较size
  ASSERT_EQ(expected->size(), actual->size());

  // 比较类型
  ASSERT_EQ(expected->type(), actual->type());

  // 比较数据
  auto rowType = expected->type()->asRow();
  for (int col = 0; col < expected->childrenSize(); col++) {
    std::vector<std::string> data;
    auto expectedChild = expected->childAt(col);
    auto actualChild = actual->childAt(col);

    for (int row = 0; row < expected->size(); row++) {
      if (!expectedChild->equalValueAt(actualChild.get(), row, row)) {
        if (rowType.childAt(col) == VARCHAR()) {
          ASSERT_EQ(expectedChild->asFlatVector<StringView>()->valueAt(row), actualChild->asFlatVector<StringView>()->valueAt(row));
        } else if (rowType.childAt(col) == INTEGER()) {
          ASSERT_EQ(expectedChild->asFlatVector<int32_t>()->valueAt(row), actualChild->asFlatVector<int32_t>()->valueAt(row));
        }
      }
    }
  }
}

class MockRecordReader : public RecordReader {
 public:
  MockRecordReader(const RowVectorPtr& data)
      : data_(data) {}

  ~MockRecordReader() {}

  bool next() override {
    current_++;
    std::cout << "current index: " << current_;
    return current_ < data_->childAt(0)->size();
  }

  bool isNull(const std::string& colName) override {
    return data_->childAt(getIndexByName(colName))->isNullAt(current_);
  }

  bool getBoolean(const std::string& colName) override {
    return data_->childAt(getIndexByName(colName))->asFlatVector<bool>()->valueAt(current_);
  }

  int getInt(const std::string& colName) override {
    return data_->childAt(getIndexByName(colName))->asFlatVector<int32_t>()->valueAt(current_);
  }

  double getDouble(const std::string& colName) override {
    return data_->childAt(getIndexByName(colName))->asFlatVector<double>()->valueAt(current_);
  }

  int getInt64(const std::string& colName) override {
    return data_->childAt(getIndexByName(colName))->asFlatVector<int64_t>()->valueAt(current_);
  }

  std::string getString(const std::string& colName) override {
    return data_->childAt(getIndexByName(colName))->asFlatVector<StringView>()->valueAt(current_);
  }

 private:
  int getIndexByName(const std::string& colName) const {
    auto rowType = data_->type()->asRow();
    auto it = std::find(rowType.names().begin(), rowType.names().end(), colName);
    if (it != rowType.names().end()) {
      return it - rowType.names().begin();
    }

    return -1;
  }
 private:
  int current_{-1};
  const std::vector<std::string> names_;
  const RowVectorPtr& data_;
};

void printRowVector(const RowVectorPtr& rowVectorPtr) {
  std::vector<VectorPtr>& children = rowVectorPtr->children();
  // print header
  std::cout << std::endl << std::endl;
  std::cout << "============================== " << std::endl;
  auto rowType = std::dynamic_pointer_cast<const RowType>(rowVectorPtr->type());
  std::cout << "[_]: ";
  for (int col = 0; col < rowType->size(); col++) {
    std::cout << rowType->nameOf(col);

    if (col < rowVectorPtr->childrenSize() - 1) {
      std::cout << ", ";
    }
  }
  std::cout << std::endl << "------------------------------ " << std::endl;

  // print data
  for (int row = 0; row < rowVectorPtr->size(); row++) {
    std::cout << "[" << row << "]: ";
    for (int col = 0; col < rowVectorPtr->childrenSize(); col++) {
      const VectorPtr vectorPtr = rowVectorPtr->childAt(col);
      if (vectorPtr->type() == INTEGER()) {
        std::cout << vectorPtr->asFlatVector<int32_t>()->valueAt(row);
      } else if (vectorPtr->type() == VARCHAR()) {
        std::cout << vectorPtr->asFlatVector<StringView>()->valueAt(row);
      }

      if (col < rowVectorPtr->childrenSize() - 1) {
        std::cout << ", ";
      }
    }
    std::cout << std::endl;
  }
  std::cout << "============================== " << std::endl << std::endl;
}

RowVectorPtr createRowVector(memory::MemoryPool* pool, RowTypePtr rowTypePtr, int size) {
  // 构造结果vector
  std::vector<VectorPtr> resultVectors;
  for (const TypePtr type : rowTypePtr->children()) {
    resultVectors.push_back(BaseVector::create(type, size, pool));
  }

  // nulls这个东西到底怎么搞？传一个 nullptr 就好了吗?
  auto result = std::make_shared<RowVector>(pool, rowTypePtr, nullptr, size, resultVectors);
  return result;
}

template<typename T>
void setRowVectorValues(const RowVectorPtr& rowVectorPtr, int col, std::vector<T> values) {
  for (int i = 0; i < values.size(); i++) {
    FlatVector<T>* rawIntVectorPtr = rowVectorPtr->childAt(col)->asFlatVector<T>();
    rawIntVectorPtr->set(i, values[i]);
  }
}

TEST(MysqlConnectorTest, readData)
{
  RowTypePtr rowTypePtr = std::make_shared<RowType>(RowType({"id", "name"}, {INTEGER(), VARCHAR()}));

  std::unique_ptr<memory::ScopedMemoryPool> pool{memory::getDefaultScopedMemoryPool()};

  // 构造输入数据
  auto input = createRowVector(pool.get(), rowTypePtr, 2);
  setRowVectorValues<int32_t>(input, 0, {1, 2});
  setRowVectorValues<StringView>(input, 1, {"hello", "james"});

  // 构造结果vector
  auto result = createRowVector(pool.get(), rowTypePtr, 2);

  // 读数据
  // 构造RecordReader
  std::shared_ptr<RecordReader> recordReaderPtr =
      std::dynamic_pointer_cast<RecordReader>(std::make_shared<MockRecordReader>(input));
  // TODO 一个size是10的vector实际上只写了5个vector就怎么样
  readData(rowTypePtr, recordReaderPtr, result, 3);

  // 打印出来看一下
  printRowVector(result);

  // assert一下
  rowVectorEquals(input, result);
  std::cout << "hello world" << std::endl;
}
