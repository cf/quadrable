#include <optional>
#include <string>
#include <iostream>
#include <stdexcept>
#include <functional>
#include <vector>
#include <bitset>
#include <random>

#include "quadrable.h"
#include "quadrable/transport.h"
#include "quadrable/debug.h"

#include "goldilocks/goldilocks_base_field.hpp"

#include "goldilocks/goldilocks_base_field.hpp"
#include "goldilocks/goldilocks_base_field_avx.hpp"
#include "goldilocks/poseidon_goldilocks.hpp"
#include "goldilocks/poseidon_goldilocks_avx.hpp"
#include "goldilocks/ntt_goldilocks.hpp"



uint8_t* hex_string_to_buffer(std::string_view sv) {
    size_t slength = sv.length();
    if (slength != 64) // must be even
        return NULL;

    size_t dlength = slength / 2;

    uint8_t* data = (uint8_t*)std::malloc(dlength);

    std::memset(data, 0, dlength);

    size_t index = 0;
    while (index < slength) {
        char c = string[index];
        int value = 0;
        if (c >= '0' && c <= '9')
            value = (c - '0');
        else if (c >= 'A' && c <= 'F')
            value = (10 + (c - 'A'));
        else if (c >= 'a' && c <= 'f')
            value = (10 + (c - 'a'));
        else
            return NULL;

        data[(index / 2)] += value << (((index + 1) % 2) * 4);

        index++;
    }

    return data;
}
constexpr char hexmap[] = {'0', '1', '2', '3', '4', '5', '6', '7',
                           '8', '9', 'a', 'b', 'c', 'd', 'e', 'f'};

std::string buffer_to_hex_string(uint8_t * data, int len)
{
  std::string s(len * 2, ' ');
  for (int i = 0; i < len; ++i) {
    s[2 * i]     = hexmap[(data[i] & 0xF0) >> 4];
    s[2 * i + 1] = hexmap[data[i] & 0x0F];
  }
  return s;
}

/*
class Hash {
  public:
    Hash(size_t outputSize_) : outputSize(outputSize_) {
        blake2s_init(&s, outputSize);
    }

    void update(std::string_view sv) {
        if sv.length() == 32 {
        update(&s, reinterpret_cast<const uint8_t*>(sv.data()));
        }else if sv.length() == 64{
            const uint_8_t * data = hex_str_to_uint8(reinterpret_cast<const char*>(sv.data()));
            update(data, 32);
            free(data);
        }else{
            throw std::runtime_error("invalid string key");
        }
    }

    void update(const uint8_t *input, size_t length) {
        blake2s_update(&s, input, length);
    }

    void final(uint8_t *output) {
        blake2s_final(&s, output, outputSize);
    }

  private:
    blake2s_state s;
    size_t outputSize;

};
*/
namespace quadrable {

void hash_two_to_one(const uint8_t * a,const  uint8_t * b, uint8_t * result){
  const uint64_t * input_a =reinterpret_cast<const uint64_t*>(a);
  const uint64_t * input_b = reinterpret_cast<const uint64_t*>(a);
  


  Goldilocks::Element output[4];
  Goldilocks::Element input[12];
  for(int i=0;i<4;i++){
    input[i] =  Goldilocks::fromU64(input_a[i]);
  }
  for(int i=0;i<4;i++){
    input[i+4] =  Goldilocks::fromU64(input_b[i]);
  }
  for(int i=0;i<4;i++){
    input[i+8] =  Goldilocks::fromU64(0);
  }
  PoseidonGoldilocks::hash(output, input);
  uint64_t output_data[4];

  for(int i=0;i<4;i++){
    output_data[i] = Goldilocks::toU64(output[i]);
  }
   std::memcpy(result, &output_data[0], 32);
}
void hash_hex_two_to_one(std::string_view a, std::string_view b, uint8_t * result){

  uint8_t * input_a = hex_string_to_buffer(a);
  if(input_a == NULL){
    throw std::runtime_error("invalid hex string key!");
  }
  uint8_t * input_b = hex_string_to_buffer(b);
  if(input_b == NULL){
    free(input_a);
    throw std::runtime_error("invalid hex string key!");
  }
  hash_two_to_one(input_a, input_b, result);
  free(input_a);
  free(input_b);
}
void testPoseidon() {

    Goldilocks::Element a = Goldilocks::fromU64(0xFFFFFFFF00000005ULL);
    uint64_t b = Goldilocks::toU64(a);
    Goldilocks::Element c = Goldilocks::fromString("6277101731002175852863927769280199145829365870197997568000");

    std::cout << Goldilocks::toString(a) << " " << b << " " << Goldilocks::toString(c) << "\n";


    Goldilocks::Element output[4];
    Goldilocks::Element input[12];
    for (int i = 0; i < 12; i++)
    {
        input[i] = Goldilocks::fromU64(0);
    }
    input[0] = Goldilocks::fromU64(1);
    input[4] = Goldilocks::fromU64(2);
    PoseidonGoldilocks::hash(output, input);
    for(int i=0;i<4;i++){
      std::cout << "h1 result: " << Goldilocks::toString(output[i]) << "\n";
    }
    uint8_t test_out[32];
    std::string_view hex_a { "442646061a92545147092c2e0db3c18c274d85bff37c7d1640a088afa0ea22f5" };
    std::string_view hex_b { "442646061a92545147092c2e0db3c18c274d85bff37c7d1640a088afa0ea22f5" };
    hash_hex_two_to_one(hex_a, hex_b, &test_out[0]);
    std::cout << "hex result: " << buffer_to_hex_string(&test_out[0], 32) << "\n";



    

}

void doIt() {
  testPoseidon();
  
    ::system("mkdir -p testdb/ ; rm testdb/*.mdb");
    std::string dbDir = "testdb/";


    lmdb::env lmdb_env = lmdb::env::create();

    lmdb_env.set_max_dbs(64);
    lmdb_env.set_mapsize(1UL * 1024UL * 1024UL * 1024UL * 1024UL);

    lmdb_env.open(dbDir.c_str(), MDB_CREATE, 0664);

    lmdb_env.reader_check();

    quadrable::Quadrable db;

    {
        auto txn = lmdb::txn::begin(lmdb_env, nullptr, 0);
        db.init(txn);
        txn.commit();
    }



    auto txn = lmdb::txn::begin(lmdb_env, nullptr, 0);


    std::mt19937 rnd;
    rnd.seed(0);

    for (uint loopVar = 10; loopVar < 20'001; loopVar *= 2) {
        uint64_t numElems = 100000;
        uint64_t maxElem = numElems;
        uint64_t numAlterations = loopVar;

        db.checkout();

        {
            auto c = db.change();
            for (uint64_t i = 0; i < numElems; i++) {
                auto n = rnd() % maxElem;
                c.put(quadrable::Key::fromInteger(n), std::to_string(n));
            }
            c.apply(txn);
        }

        uint64_t origNodeId = db.getHeadNodeId(txn);
        db.fork(txn);

        {
            auto chg = db.change();

            for (uint64_t i = 0; i < numAlterations; i++) {
                auto n = numElems + rnd() % maxElem;
                auto action = rnd() % 2;
                if (action == 0) {
                    chg.put(quadrable::Key::fromInteger(n), "");
                } else if (action == 1) {
                    chg.del(quadrable::Key::fromInteger(n));
                }
            }

            chg.apply(txn);
        }

        uint64_t newNodeId = db.getHeadNodeId(txn);
        auto newKey = db.rootKey(txn);

        Quadrable::Sync sync(&db);
        sync.init(txn, origNodeId);

        uint64_t bytesDown = 0;
        uint64_t bytesUp = 0;
        uint64_t roundTrips = 0;

        while(1) {
            auto reqs = sync.getReqs(txn, 10000);
            uint64_t reqSize = transport::encodeSyncRequests(reqs).size();
            bytesUp += reqSize;
            if (reqs.size() == 0) break;

            auto resps = db.handleSyncRequests(txn, newNodeId, reqs, 100000);
            uint64_t respSize = transport::encodeSyncResponses(resps).size();
            bytesDown += respSize;
            sync.addResps(txn, reqs, resps);

            roundTrips++;

            std::cout << "RT: " << roundTrips << " up: " << reqSize << " down: " << respSize << std::endl;
        }

        db.checkout(sync.nodeIdShadow);
        if (db.rootKey(txn) != newKey) throw quaderr("NOT EQUAL AFTER IMPORT");

        std::cout << loopVar << "," << roundTrips << "," << bytesUp << "," << bytesDown << std::endl;
    }



    txn.abort();
}


}



int main() {
    try {
        quadrable::doIt();
    } catch (const std::runtime_error& error) {
        std::cerr << "Test failure: " << error.what() << std::endl;
        return 1;
    }

    return 0;
}
