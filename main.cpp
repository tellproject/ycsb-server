/*
 * (C) Copyright 2015 ETH Zurich Systems Group (http://www.systems.ethz.ch/) and others.
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
 *
 * Contributors:
 *     Markus Pilman <mpilman@inf.ethz.ch>
 *     Simon Loesing <sloesing@inf.ethz.ch>
 *     Thomas Etter <etterth@gmail.com>
 *     Kevin Bocksrocker <kevin.bocksrocker@gmail.com>
 *     Lucas Braun <braunl@inf.ethz.ch>
 */
#include <memory>
#include <array>
#include <unordered_map>
#include <unordered_set>
#include <thread>

#include <boost/asio.hpp>
#include <boost/lexical_cast.hpp>

#include <crossbow/logger.hpp>
#include <crossbow/program_options.hpp>
#include <crossbow/allocator.hpp>

#include <tellstore/ClientConfig.hpp>
#include <tellstore/ClientManager.hpp>

namespace ycsb {

using namespace boost::asio;
using error_code = boost::system::error_code;

namespace cmd {
constexpr int READ   = 1;
constexpr int SCAN   = 2;
constexpr int UPDATE = 3;
constexpr int INSERT = 4;
constexpr int DELETE = 5;
}

struct ClientBuffer {
    std::unique_ptr<char[]> buffer;
    size_t size;

    ClientBuffer(size_t size)
        : buffer(new char[size])
    {}

    void grow() {
        std::unique_ptr<char[]> nBuffer(new char[size + 1024]);
        memcpy(nBuffer.get(), buffer.get(), size);
        nBuffer.swap(buffer);
    }
};

template<class Client, class Fun>
void read_all(size_t reqSize, size_t bt, size_t readTo, Client& client, Fun f) {
    ClientBuffer& clientBuffer = client.buf();
    if (bt - readTo >= reqSize) {
        f(error_code(), bt, readTo);
        return;
    }
    // resize if necessary
    if (readTo + reqSize < clientBuffer.size) {
        clientBuffer.grow();
    }
    async_read(client.socket(), buffer(clientBuffer.buffer.get() + bt, clientBuffer.size - bt),
            [reqSize, bt, readTo, &client, f](const error_code& ec, size_t numBytes){
        if (ec) {
            f(ec, numBytes + bt, readTo);
            return;
        }
        ycsb::read_all(reqSize, bt + numBytes, readTo, client, f);
    });
}

struct ResultWriter {
    std::unique_ptr<char[]> data;
    size_t size;
    size_t offset;

    ResultWriter(size_t sz)
        : data(new char[sz])
        , size(sz)
        , offset(0)
    {}

    void grow() {
        std::unique_ptr<char[]> nData(new char[size + 1024]);
        memcpy(nData.get(), data.get(), size);
        nData.swap(data);
    }

    void write(size_t sz, const char* str) {
        while (size - offset < sz + sizeof(int32_t)) {
            grow();
        }
        *reinterpret_cast<int32_t*>(data.get() + offset) = int32_t(sz);
        offset += sizeof(int32_t);
        memcpy(data.get() + offset, str, sz);
        offset += sz;
    }

    void write(const char* str) {
        size_t len = *reinterpret_cast<const int32_t*>(str);
        write(len, str + sizeof(int32_t));
    }

    void write(const crossbow::string& str) {
        write(str.size(), str.data());
    }

    void write(int32_t value) {
        while (size - offset < sizeof(value)) {
            grow();
        }
        *reinterpret_cast<int32_t*>(data.get() + offset) = value;
        offset += sizeof(value);
    }
};

class Client : public std::enable_shared_from_this<Client> {
    ip::tcp::socket mSocket;
    tell::store::ClientManager<void>& mClientManager;
    ClientBuffer mClientBuffer;
    size_t mPrefixLength;
    std::unordered_map<crossbow::string, tell::store::Table> mTableIds;

private: // commands

    int32_t getInt(size_t& offset) {
        auto res = *reinterpret_cast<int32_t*>(mClientBuffer.buffer.get() + offset);
        offset += sizeof(int32_t);
        return res;
    }

    crossbow::string getString(size_t& offset) {
        int sz = getInt(offset);
        crossbow::string res(mClientBuffer.buffer.get() + offset, sz);
        offset += sz;
        return res;
    }

    std::unordered_set<crossbow::string> getSet(size_t& offset) {
        int num = getInt(offset);
        std::unordered_set<crossbow::string> res;
        res.reserve(num);
        for (int i = 0; i < num; ++i) {
            res.emplace(getString(offset));
        }
        return res;
    }

    std::unordered_map<crossbow::string, boost::any> getMap(size_t& offset) {
        int num = getInt(offset);
        std::unordered_map<crossbow::string, boost::any> result;
        for (int i = 0; i < num; ++i) {
            auto k = getString(offset);
            auto v = getString(offset);
            result.emplace(std::move(k), std::move(v));
        }
        return result;
    }

    tell::store::Table tableId(const crossbow::string& name, tell::store::ClientHandle& handle) {
        auto iter = mTableIds.find(name);
        if (iter != mTableIds.end()) {
            return iter->second;
        }
        auto f = handle.getTable(name);
        auto res = f->get();
        mTableIds.emplace(name, res);
        return res;
    }

    void doRead() {
        auto self = shared_from_this();
        mClientManager.execute([self](tell::store::ClientHandle& handle) {
            size_t offset = 2*sizeof(int32_t);
            auto tableName = self->getString(offset);
            auto tId = self->tableId(tableName, handle);
            auto keyStr = self->getString(offset);
            uint64_t key = boost::lexical_cast<uint64_t>(keyStr.substr(self->mPrefixLength));
            auto fields = self->getSet(offset);

            auto resF = handle.get(tId, key);
            auto res = resF->get();
            auto& rec = tId.record();

            ResultWriter result(1024);
            result.write(int32_t(fields.size()));
            for (auto& field : fields) {
                result.write(field);
                unsigned short fId;
#ifndef NDEBUG
                assert(rec.idOf(field, fId));
#else
                rec.idOf(field, fId);
#endif
                bool isNull;
                tell::store::FieldType type;
                const char* str = rec.data(res->data(), fId, isNull, &type);
                assert(type == tell::store::FieldType::TEXT);
                result.write(str);
            }
            size_t size = result.offset;
            char* resArr = result.data.release();
            self->mSocket.get_io_service().post([self, size, resArr](){
                async_write(self->mSocket, buffer(resArr, size), [self, resArr](const error_code& ec, size_t){
                    delete[] resArr;
                    if (ec) {
                        LOG_ERROR(ec.message());
                        return;
                    }
                    self->read();
                });
            });
        });
    }

    void doInsert() {
        auto self = shared_from_this();
        mClientManager.execute([self](tell::store::ClientHandle& handle) {
            size_t offset = 2*sizeof(int32_t);
            auto tableName = self->getString(offset);
            auto keyStr = self->getString(offset);
            uint64_t key = boost::lexical_cast<uint64_t>(keyStr.substr(self->mPrefixLength));
            auto values = self->getMap(offset);
            auto tId = self->tableId(tableName, handle);
            auto r = handle.insert(tId, key, std::numeric_limits<uint64_t>::max(), values);
            r->wait();
            self->mSocket.get_io_service().post([self](){
                char* res = new char[1];
                res[0] = 0;
                async_write(self->socket(), buffer(res, 1), [self, res](const error_code& ec, size_t) {
                    delete[] res;
                    if (ec) {
                        LOG_ERROR(ec.message());
                        return;
                    }
                    self->read();
                });
            });
        });
    }

    void doUpdate() {
        auto self = shared_from_this();
        mClientManager.execute([self](tell::store::ClientHandle& handle) {
            size_t offset = 2*sizeof(int32_t);
            auto tableName = self->getString(offset);
            auto keyStr = self->getString(offset);
            uint64_t key = boost::lexical_cast<uint64_t>(keyStr.substr(self->mPrefixLength));

            auto values = self->getMap(offset);
            auto tId = self->tableId(tableName, handle);
            auto resF = handle.get(tId, key);
            auto res = resF->get();
            auto& rec = tId.record();
            auto& fields = rec.schema().varSizeFields();
            tell::store::GenericTuple tuple;
            for (unsigned short i = 0; i < fields.size(); ++i) {
                if (values.count(fields[i].name()) == 0) {
                    bool isNull;
                    tell::store::FieldType type;
                    const char* str = rec.data(res->data(), i, isNull, &type);
                    values.emplace(fields[i].name(),
                            crossbow::string(str + sizeof(int32_t), *reinterpret_cast<const int32_t*>(str)));
                }
            }
            auto r = handle.update(tId, key, std::numeric_limits<uint64_t>::max(), values);
            r->wait();
            self->mSocket.get_io_service().post([self](){
                char* res = new char[1];
                res[0] = 0;
                async_write(self->socket(), buffer(res, 1), [self, res](const error_code& ec, size_t) {
                    delete[] res;
                    if (ec) {
                        LOG_ERROR(ec.message());
                        return;
                    }
                    self->read();
                });
            });
        });
    }

    void doDelete() {
        auto self = shared_from_this();
        mClientManager.execute([self](tell::store::ClientHandle& handle) {
            size_t offset = 2*sizeof(int32_t);
            auto tableName = self->getString(offset);
            auto keyStr = self->getString(offset);
            uint64_t key = boost::lexical_cast<uint64_t>(keyStr.substr(self->mPrefixLength));

            auto tId = self->tableId(tableName, handle);
            auto resp = handle.remove(tId, key, std::numeric_limits<uint64_t>::max());
            resp->wait();
            self->mSocket.get_io_service().post([self](){
                char* res = new char[1];
                res[0] = 0;
                async_write(self->socket(), buffer(res, 1), [self, res](const error_code& ec, size_t) {
                    delete[] res;
                    if (ec) {
                        LOG_ERROR(ec.message());
                        return;
                    }
                    self->read();
                });
            });
        });
    }

public:
    Client(io_service& service, tell::store::ClientManager<void>& clientManager)
        : mSocket(service)
        , mClientManager(clientManager)
        , mClientBuffer(1024)
        , mPrefixLength(4)
    {}

    ip::tcp::socket& socket() {
        return mSocket;
    }

    ClientBuffer& buf() {
        return mClientBuffer;
    }

    void read() {
        auto self = shared_from_this();
        async_read(mSocket, buffer(mClientBuffer.buffer.get(), mClientBuffer.size), [self](const error_code& ec, size_t bt){
            if (ec) {
                LOG_ERROR(ec.message());
                return;
            }
            if (bt < 4) {
                LOG_ERROR("Could not read command, read only %1% bytes", bt);
                std::terminate();
            }
            auto size = *reinterpret_cast<int32_t*>(self->mClientBuffer.buffer.get());
            ycsb::read_all(size, bt, sizeof(int32_t), *self, [self](const error_code& ec, size_t bt, size_t rT) {
                int32_t command = *reinterpret_cast<int32_t*>(self->mClientBuffer.buffer.get() + sizeof(int32_t));
                switch (command) {
                case cmd::READ:
                    self->doRead();
                    break;
                case cmd::UPDATE:
                    self->doUpdate();
                    break;
                case cmd::INSERT:
                    self->doInsert();
                    break;
                case cmd::DELETE:
                    self->doDelete();
                }
            });
        });
    }
};

void accept(io_service& service, ip::tcp::acceptor& acceptor, tell::store::ClientManager<void>& clientManager) {
    auto client = std::make_shared<Client>(service, clientManager);
    acceptor.async_accept(client->socket(), [&service, &acceptor, &clientManager, client](const error_code& ec) {
        if (ec) {
            LOG_ERROR(ec.message());
            return;
        }
        client->read();
        accept(service, acceptor, clientManager);
    });
}

} // namespace ycsb

int main(int argc, const char* argv[]) {
    using namespace crossbow::program_options;
    bool help = false;
    bool createTable = false;
    std::string host("0.0.0.0");
    std::string port("8713");
    crossbow::string logLevel("DEBUG");
    crossbow::string storageNodes;
    crossbow::string commitManager;
    tell::store::ClientConfig config;
    auto opts = create_options("tpcc_server",
            value<'h'>("help", &help, tag::description{"print help"}),
            value<'H'>("host", &host, tag::description{"Host to bind to"}),
            value<'p'>("port", &port, tag::description{"Port to bind to"}),
            value<'l'>("log-level", &logLevel, tag::description{"The log level"}),
            value<'c'>("commit-manager", &commitManager, tag::description{"Address to the commit manager"}),
            value<'C'>("create-table", &createTable, tag::description{"Client should create table on startup"}),
            value<'s'>("storage-nodes", &storageNodes, tag::description{"Semicolon-separated list of storage node addresses"}),
            value<-1>("network-threads", &config.numNetworkThreads, tag::ignore_short<true>{})
            );

    try {
        parse(opts, argc, argv);
    } catch (argument_not_found& e) {
        std::cerr << e.what() << std::endl << std::endl;
        print_help(std::cout, opts);
        return 1;
    }
    if (help) {
        print_help(std::cout, opts);
        return 0;
    }
    crossbow::allocator::init();
    crossbow::logger::logger->config.level = crossbow::logger::logLevelFromString(logLevel);
    config.commitManager = config.parseCommitManager(commitManager);
    config.tellStore = config.parseTellStore(storageNodes);
    tell::store::ClientManager<void> clientManager(config);

    if (createTable) {
        bool done = false;
        clientManager.execute([&done](tell::store::ClientHandle& handle) {
            tell::store::Schema schema;
            schema.addField(tell::store::FieldType::TEXT, "field0", true);
            schema.addField(tell::store::FieldType::TEXT, "field1", true);
            schema.addField(tell::store::FieldType::TEXT, "field2", true);
            schema.addField(tell::store::FieldType::TEXT, "field3", true);
            schema.addField(tell::store::FieldType::TEXT, "field4", true);
            schema.addField(tell::store::FieldType::TEXT, "field5", true);
            schema.addField(tell::store::FieldType::TEXT, "field6", true);
            schema.addField(tell::store::FieldType::TEXT, "field7", true);
            schema.addField(tell::store::FieldType::TEXT, "field8", true);
            schema.addField(tell::store::FieldType::TEXT, "field9", true);
            done = true;
        });
        while (!done) {
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
        }
    }

    // initialize boost::asio
    boost::asio::io_service service;
    boost::asio::io_service::work work(service);
    boost::asio::ip::tcp::acceptor a(service);
    boost::asio::ip::tcp::acceptor::reuse_address option(true);
    boost::asio::ip::tcp::resolver resolver(service);
    boost::asio::ip::tcp::resolver::iterator iter;
    if (host == "") {
        iter = resolver.resolve(boost::asio::ip::tcp::resolver::query(port));
    } else {
        iter = resolver.resolve(boost::asio::ip::tcp::resolver::query(host, port));
    }
    boost::asio::ip::tcp::resolver::iterator end;
    for (; iter != end; ++iter) {
        boost::system::error_code err;
        auto endpoint = iter->endpoint();
        auto protocol = iter->endpoint().protocol();
        a.open(protocol);
        a.set_option(option);
        a.bind(endpoint, err);
        if (err) {
            a.close();
            LOG_WARN("Bind attempt failed " + err.message());
            continue;
        }
        break;
    }
    if (!a.is_open()) {
        LOG_ERROR("Could not bind");
        return 1;
    }
    a.listen();
    // we do not need to delete this object, it will delete itself
    ycsb::accept(service, a, clientManager);
    service.run();
}

