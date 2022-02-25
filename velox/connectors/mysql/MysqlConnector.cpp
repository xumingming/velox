
#include "MysqlConnector.h"
#include <mysql/jdbc.h>
#include <iostream>
#include "velox/vector/FlatVector.h"
#include "velox/vector/ComplexVector.h"

namespace facebook::velox::connector::mysql {

class MysqlRecordReader : public RecordReader {
 public:
  MysqlRecordReader(const std::shared_ptr<sql::ResultSet>& resultSet)
      : RecordReader(),
        resultSet_(resultSet){}

  ~MysqlRecordReader() {}

  bool next() override {
    return resultSet_->next();
  }

  bool isNull(const std::string& colName) override {
    return resultSet_->isNull(colName);
  }
  bool getBoolean(const std::string& colName) override {
    return resultSet_->getBoolean(colName);
  }

  int getInt(const std::string& colName) override {
    return resultSet_->getInt(colName);
  }

  int getInt64(const std::string& colName) override {
    return resultSet_->getInt64(colName);
  }

  double getDouble(const std::string& colName) override {
    return resultSet_->getDouble(colName);
  }

  std::string getString(const std::string& colName) override {
    return resultSet_->getString(colName);
  }

 private:
  const std::shared_ptr<sql::ResultSet>& resultSet_;
};

void MysqlDataSource::addSplit(std::shared_ptr<ConnectorSplit> split) {
  VELOX_CHECK(
      split_ == nullptr,
      "Previous split has not been processed yet. Call next to process the split.");
  split_ = std::dynamic_pointer_cast<MysqlSplit>(split);
  VELOX_CHECK(split_, "Wrong type of split");
}

int readData(
    RowTypePtr rowType,
    const std::shared_ptr<RecordReader> resultSet,
    const RowVectorPtr& result,
    const int& count) {
  int actualIndex = -1;
  // TODO 控制每次只读参数count指定的记录数
  while (actualIndex < count - 1 && resultSet->next()) {
    actualIndex++;
    for (int columnIndex = 0; columnIndex < rowType->size(); columnIndex++) {
      std::string colName = rowType->nameOf(columnIndex);

      VectorPtr currentVector = result->childAt(columnIndex);
      if (resultSet->isNull(colName)) {
        currentVector->setNull(actualIndex, true);
      } else {
        switch (rowType->childAt(columnIndex)->kind()) {
          case TypeKind::BOOLEAN:
            currentVector->asFlatVector<bool>()->set(
                actualIndex, resultSet->getBoolean(colName));
            break;

          case TypeKind::TINYINT:
            currentVector->asFlatVector<int8_t>()->set(
                actualIndex, resultSet->getInt(colName));
            break;

          case TypeKind::SMALLINT:
            currentVector->asFlatVector<int16_t>()->set(
                actualIndex, resultSet->getInt(colName));
            break;

          case TypeKind::INTEGER:
            currentVector->asFlatVector<int32_t>()->set(
                actualIndex, resultSet->getInt(colName));
            break;

          case TypeKind::BIGINT:
            currentVector->asFlatVector<int64_t>()->set(
                actualIndex, resultSet->getInt64(colName));
            break;

          case TypeKind::REAL:
            currentVector->asFlatVector<float>()->set(
                actualIndex, resultSet->getDouble(colName));
            break;

          case TypeKind::DOUBLE:
            currentVector->asFlatVector<double>()->set(
                actualIndex, resultSet->getDouble(colName));
            break;

          case TypeKind::VARCHAR:
            currentVector->asFlatVector<StringView>()->set(
                actualIndex, StringView(resultSet->getString(colName)));
            break;

          default:
            break;
        }
      }
    }
  }

  // we are returning count instead of index
  int actualCount = actualIndex + 1;
  result->resize(actualCount, true);

  return actualCount;
}

RowVectorPtr MysqlDataSource::next(uint64_t size) {
  VELOX_CHECK(split_ != nullptr, "No split to process. Call addSplit first.");

  if (!output_) {
    output_ = BaseVector::create(outputType_, 0, pool_);
  }

  try {
    // TODO Driver/conn这些东西不应该在next里面初始化
    sql::Driver* driver = get_driver_instance();
    sql::Connection* con =
        driver->connect(fmt::format("tcp://{}:{}", split_->host, split_->port), split_->userName, split_->password);
    std::shared_ptr<sql::Connection> conn(con);

    /* Connect to the MySQL test database */
    con->setSchema(split_->dbName);

    std::shared_ptr<sql::Statement> stmt(con->createStatement());
    // TODO 只组装实际要查的那些column， 计算下推
    std::shared_ptr<sql::ResultSet> res(stmt->executeQuery(
        fmt::format("SELECT * from {}", split_->tableName)));

    auto row = output_->as<RowVector>();
    std::vector<VectorPtr> vectors(outputType_->size());
    for (const TypePtr type : outputType_->children()) {
      vectors.emplace_back(BaseVector::create(type, size, pool_));
    }

    int recordCount = 0;
    auto ret = std::make_shared<RowVector>(
        RowVector(pool_, outputType_, nullptr, size, vectors));

    std::shared_ptr<RecordReader> recordReader = std::dynamic_pointer_cast<RecordReader>(std::make_shared<MysqlRecordReader>(res));
    readData(outputType_, recordReader, ret, recordCount);
    return ret;
  } catch (sql::SQLException& e) {
    LOG(WARNING) << "Query mysql failure failed: " << e.what();
    // TODO 这是不对的
    return std::make_shared<RowVector>(
        RowVector(pool_, outputType_, nullptr, size, {}));
  }
}

VELOX_REGISTER_CONNECTOR_FACTORY(
    std::make_shared<MysqlConnectorFactory>())
} // namespace facebook::velox::connector::mysql