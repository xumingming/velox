#pragma once

#include "velox/connectors/Connector.h"

namespace facebook::velox::connector::mysql {

// 把数据读取抽象一个接口出来，方便集成测试的时候mock
class RecordReader {
 public:
  virtual ~RecordReader() {};
  virtual bool next() = 0;
  virtual bool isNull(const std::string& colName) = 0;
  virtual bool getBoolean(const std::string& colName) = 0;
  virtual int getInt(const std::string& colName) = 0;
  virtual double getDouble(const std::string& colName) = 0;
  virtual int getInt64(const std::string& colName) = 0;
  virtual std::string getString(const std::string& colName) = 0;
};

class MysqlColumnHandle final : public ColumnHandle {
 public:
  MysqlColumnHandle(const std::string& name, TypePtr type)
      : name_(name), type_(type) {}

 private:
  const std::string name_;
  const TypePtr type_;
};

class MysqlTableHandle final : public ConnectorTableHandle {
 public:
  MysqlTableHandle() = default;
  MysqlTableHandle(
      const std::string& catalogName,
      const std::string& schemaName,
      const std::string& tableName)
      : catalogName_(catalogName),
        schemaName_(schemaName),
        tableName_(tableName) {}

  std::string catalogName() {
    return catalogName_;
  }

  std::string schemaName() {
    return schemaName_;
  }

  std::string tableName() {
    return tableName_;
  }

 private:
  std::string catalogName_;
  std::string schemaName_;
  std::string tableName_;
};

struct MysqlSplit : public ConnectorSplit {
  const std::string host;
  const int port;
  const std::string userName;
  const std::string password;
  const std::string dbName;
  const std::string tableName;

  MysqlSplit(
      const std::string& connectorId,
      const std::string& _host,
      const int& _port,
      const std::string& _userName,
      const std::string& _password,
      const std::string& _dbName,
      const std::string& _tableName)
      : ConnectorSplit(connectorId),
        host(_host),
        port(_port),
        userName(_userName),
        password(_password),
        dbName(_dbName),
        tableName(_tableName) {}
};

class MysqlDataSource final : public DataSource {
 public:
  explicit MysqlDataSource(
      const std::shared_ptr<const RowType> outputType,
      velox::memory::MemoryPool* FOLLY_NONNULL pool)
      : outputType_(outputType), pool_(pool) {}

  void addSplit(std::shared_ptr<ConnectorSplit> split) override;
  RowVectorPtr next(uint64_t size) override;
  void addDynamicFilter(
      ChannelIndex outputChannel,
      const std::shared_ptr<common::Filter>& filter) override {}
  uint64_t getCompletedRows() override {
    return 0;
  }

  uint64_t getCompletedBytes() override {
    return 0;
  }

  std::unordered_map<std::string, int64_t> runtimeStats() override {
    return {};
  }

  int64_t estimatedRowSize() override {
    return 0;
  }

 private:
  const std::shared_ptr<const RowType> outputType_;
  std::shared_ptr<MysqlSplit> split_;
  VectorPtr output_;
  velox::memory::MemoryPool* FOLLY_NONNULL pool_;
};

class MysqlConnector final : public Connector {
 public:
  MysqlConnector(
      const std::string& id,
      std::shared_ptr<const Config> properties)
      : Connector(id, properties) {}
  std::shared_ptr<DataSource> createDataSource(
      const std::shared_ptr<const RowType>& outputType,
      const std::shared_ptr<connector::ConnectorTableHandle>& tableHandle,
      const std::unordered_map<
          std::string,
          std::shared_ptr<connector::ColumnHandle>>& columnHandles,
      ConnectorQueryCtx* FOLLY_NONNULL connectorQueryCtx) override final {
    return std::make_shared<MysqlDataSource>(
        outputType, connectorQueryCtx->memoryPool());
  }

  std::shared_ptr<DataSink> createDataSink(
      std::shared_ptr<const RowType> inputType,
      std::shared_ptr<ConnectorInsertTableHandle> connectorInsertTableHandle,
      ConnectorQueryCtx* FOLLY_NONNULL connectorQueryCtx) override final {
    return nullptr;
  }
};

class MysqlConnectorFactory : public ConnectorFactory {
 public:
  static constexpr const char* FOLLY_NONNULL kMysqlConnectorName = "mysql";

  MysqlConnectorFactory() : ConnectorFactory(kMysqlConnectorName) {
  }

  MysqlConnectorFactory(const char* FOLLY_NONNULL connectorName)
      : ConnectorFactory(connectorName) {
  }

  std::shared_ptr<Connector> newConnector(
      const std::string& id,
      std::shared_ptr<const Config> properties,
      std::unique_ptr<DataCache> dataCache = nullptr,
      folly::Executor* FOLLY_NULLABLE executor = nullptr) override {
    return std::make_shared<MysqlConnector>(id, properties);
  }
};

// return the actual count read
int readData(
    RowTypePtr rowType,
    const std::shared_ptr<RecordReader> resultSet,
    const RowVectorPtr& result,
    const int& size);
} // namespace facebook::velox::connector::mysql
