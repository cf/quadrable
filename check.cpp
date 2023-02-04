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




namespace quadrable {

void doTests() {
    std::string dbDir = "testdb/";


    lmdb::env lmdb_env = lmdb::env::create();

    lmdb_env.set_max_dbs(64);
    lmdb_env.set_mapsize(1UL * 1024UL * 1024UL * 1024UL * 1024UL);

    lmdb_env.open(dbDir.c_str(), MDB_CREATE, 0664);

    lmdb_env.reader_check();

    quadrable::Quadrable db;

    //db.trackKeys = true;

    {
        auto txn = lmdb::txn::begin(lmdb_env, nullptr, 0);
        db.init(txn);
        txn.commit();
    }



    auto txn = lmdb::txn::begin(lmdb_env, nullptr, 0);



#   define verify(condition) do { if (!(condition)) throw quaderr(#condition, "  |  ", __FILE__, ":", __LINE__); } while(0)
#   define verifyThrow(condition, expected) { \
        bool caught = false; \
        std::string errorMsg; \
        try { condition; } \
        catch (const std::runtime_error &e) { \
            caught = true; \
            errorMsg = e.what(); \
        } \
        if (!caught) throw quaderr(#condition, " | expected error, but didn't get one (", expected, ")"); \
        if (errorMsg.find(expected) == std::string::npos) throw quaderr(#condition, " | error msg not what we expected: ", errorMsg, " (not ", expected, ")"); \
    }

    auto test = [&](std::string testName, std::function<void()> cb) {
        db.checkout();

        std::cout << "TEST: " << testName << std::endl;

        try {
            cb();
        } catch (const std::runtime_error& error) {
            throw quaderr(testName, "  |  ", error.what());
        }

        std::cout << "OK." << std::endl;
    };

    auto equivHeads = [&](std::string desc, std::function<void()> cb1, std::function<void()> cb2, bool expectEqual = true) {
        if (desc.size()) std::cout << "  - " << desc << std::endl;

        db.checkout();
        cb1();
        auto root1 = db.root(txn);

        db.checkout();
        cb2();
        auto root2 = db.root(txn);

        verify((root1 == root2) == expectEqual);
    };

    auto proofRoundtrip = [](const Proof &p) {
        return quadrable::transport::decodeProof(quadrable::transport::encodeProof(p));
    };

    auto syncRequestsRoundtrip = [](const SyncRequests &reqs) {
        return quadrable::transport::decodeSyncRequests(quadrable::transport::encodeSyncRequests(reqs));
    };

    auto syncResponsesRoundtrip = [](const SyncResponses &resps) {
        return quadrable::transport::decodeSyncResponses(quadrable::transport::encodeSyncResponses(resps));
    };

    auto dump = [&]{ quadrable::dumpDb(db, txn); };
    auto stats = [&]{ quadrable::dumpStats(db, txn); };
    auto stop = [&]{ throw quaderr("STOP"); };
    (void)dump;
    (void)stop;
    (void)stats;


    test("basic put/get", [&]{
        db.change()
          .put("hello", "world")
          .apply(txn);

        std::string_view val;
        verify(db.get(txn, "hello", val));
        verify(val == "world");

        auto stats = db.stats(txn);
        verify(stats.numLeafNodes == 1);
    });


    test("zero-length keys", [&]{
        verifyThrow(db.change().put("", "1").apply(txn), "zero-length keys not allowed");
        verifyThrow(db.change().del("").apply(txn), "zero-length keys not allowed");
    });


    test("overwriting updates before apply", [&]{
        equivHeads("double put", [&]{
            db.change().put("a", "1").apply(txn);
            db.change().put("a", "1").apply(txn);
        },[&]{
            db.change().put("a", "1").apply(txn);
        });

        equivHeads("del overwrites put", [&]{
            db.change().put("a", "1").del("a").apply(txn);
        },[&]{
        });

        equivHeads("put overwrites del overwrites put", [&]{
            db.change().put("a", "1").del("a").put("a", "2").apply(txn);
        },[&]{
            db.change().put("a", "2").apply(txn);
        });
    });

    test("saves nodeId", [&]{
        uint64_t nodeId = 999999;

        db.change()
          .put("A", "1", &nodeId)
          .apply(txn);

        verify(nodeId != 0);
        verify(nodeId == db.getHeadNodeId(txn));
        verify(Quadrable::ParsedNode(&db, txn, nodeId).leafVal() == "1");

        // Puting identical leaf returns 0

        nodeId = 999999;

        db.change()
          .put("A", "1", &nodeId)
          .apply(txn);

        verify(nodeId == 0);

        // Puting new leaf with same key but different value returns non-0

        nodeId = 999999;

        db.change()
          .put("A", "2", &nodeId)
          .apply(txn);

        verify(nodeId != 0);
        verify(nodeId == db.getHeadNodeId(txn));

        uint64_t origHead = db.getHeadNodeId(txn);


        // Deletions, etc

        uint64_t nodeId1 = 55555, nodeId2 = 44444, nodeId3 = 33333;
        uint64_t nodeIdDel1 = 8888, nodeIdDel2 = 7777;

        db.change()
          .del("A", &nodeIdDel1)
          .put("B", "2", &nodeId1)
          .put("D", "4", &nodeId2)
          .put("C", "3")
          .put("E", "5", &nodeId3)
          .del("NONE", &nodeIdDel2)
          .apply(txn);

        verify(Quadrable::ParsedNode(&db, txn, nodeId1).leafVal() == "2");
        verify(Quadrable::ParsedNode(&db, txn, nodeId2).leafVal() == "4");
        verify(Quadrable::ParsedNode(&db, txn, nodeId3).leafVal() == "5");

        verify(nodeIdDel1 == origHead);
        verify(nodeIdDel2 == 0);

        // Unchanged leaf:

        uint64_t nodeIdDup;

        db.change()
          .put("B", "2", &nodeIdDup)
          .apply(txn);

        verify(nodeIdDup == 0);
    });

    test("integer round-trips", [&]{
        for (uint64_t i = 0; i < 100'000; i++) {
            verify(quadrable::Key::fromInteger(i).toInteger() == i);
        }

        for (uint64_t i = std::numeric_limits<uint64_t>::max() - 100'000; i <= std::numeric_limits<uint64_t>::max() - 2; i++) {
            verify(quadrable::Key::fromInteger(i).toInteger() == i);
        }

        for (uint64_t i = 10; i <= 64; i++) {
            uint64_t n = (1LL << i) - 5;
            verify(quadrable::Key::fromInteger(n).toInteger() == n);
        }

        {
            uint64_t i = std::numeric_limits<uint64_t>::max() - 1;
            verifyThrow(quadrable::Key::fromInteger(i), "int range exceeded");
        }

        {
            uint64_t i = std::numeric_limits<uint64_t>::max();
            verifyThrow(quadrable::Key::fromInteger(i), "int range exceeded");
        }
    });


    test("empty heads", [&]{
        verify(Key::null() == db.root(txn));

        std::string_view val;
        verify(!db.get(txn, "hello", val));

        auto stats = db.stats(txn);
        verify(stats.numLeafNodes == 0);

        db.change().put("a", "1").apply(txn);
        verify(Key::null() != db.root(txn));

        db.change().del("a").apply(txn);
        verify(Key::null() == db.root(txn));
    });



    test("batch insert", [&]{
        db.change()
          .put("a", "1")
          .put("b", "2")
          .put("c", "3")
          .apply(txn);

        auto stats = db.stats(txn);
        verify(stats.numLeafNodes == 3);

        std::string_view val;
        verify(db.get(txn, "b", val));
        verify(val == "2");
    });


    test("getMulti", [&]{
        auto changes = db.change();
        for (int i=0; i<100; i++) {
            std::string s = std::to_string(i);
            changes.put(s, std::string("N = ") + s);
        }
        changes.apply(txn);

        auto query = db.get(txn, { "30", "31", "32", "blah", "nope", });

        verify(query["30"].exists && query["30"].val == "N = 30");
        verify(query["31"].exists && query["31"].val == "N = 31");
        verify(query["32"].exists && query["32"].val == "N = 32");

        verify(!query["blah"].exists);
        verify(!query["nope"].exists);
    });


    test("del", [&]{
        {
            auto changes = db.change();
            changes.put("a", "1");
            changes.put("b", "2");
            changes.put("c", "3");
            changes.apply(txn);
        }

        db.change()
          .del("b")
          .apply(txn);

        auto stats = db.stats(txn);
        verify(stats.numLeafNodes == 2);

        std::string_view val;
        verify(!db.get(txn, "b", val));
    });


    test("del bubble", [&]{
        equivHeads("bubble right", [&]{
            db.change().put("a", "1").put("b", "2").apply(txn);
            db.change().del("b").apply(txn);
        },[&]{
            db.change().put("a", "1").apply(txn);
        });

        equivHeads("bubble left", [&]{
            db.change().put("a", "1").put("b", "2").apply(txn);
            db.change().del("a").apply(txn);
        },[&]{
            db.change().put("b", "2").apply(txn);
        });

        equivHeads("delete both sides of a branch in same update, leaving empty node", [&]{
            db.change().put("a", "1").put("b", "2").apply(txn);
            db.change().del("a").del("b").apply(txn);
        },[&]{
        });

        equivHeads("delete both sides of a branch in same update, which causes sibling leaf to bubble up", [&]{
            db.change().put("a", "1").put("b", "2").put("c", "3").apply(txn);
            db.change().del("a").del("c").apply(txn);
        },[&]{
            db.change().put("b", "2").apply(txn);
        });

        equivHeads("delete one side of a branch and a sibling leaf in same update, which causes remaining side of branch to bubble up", [&]{
            db.change().put("a", "1").put("b", "2").put("c", "3").apply(txn);
            db.change().del("b").del("c").apply(txn);
        },[&]{
            db.change().put("a", "1").apply(txn);
        });

        equivHeads("same as previous, but other side of the branch", [&]{
            db.change().put("a", "1").put("b", "2").put("c", "3").apply(txn);
            db.change().del("b").del("a").apply(txn);
        },[&]{
            db.change().put("c", "3").apply(txn);
        });

        equivHeads("deleted neighbour node", [&]{
            db.change()
              .put(quadrable::Key::fromInteger(1), "B")
              .apply(txn);
        },[&]{
            db.change()
              .put(quadrable::Key::fromInteger(1), "A")
              .apply(txn);

            db.change()
              .del(quadrable::Key::fromInteger(2))
              .put(quadrable::Key::fromInteger(1), "B")
              .apply(txn);
        });

        equivHeads("long bubble", [&]{
            db.change()
              .put("146365204598", "A") // 11111111...
              .put("967276293879", "B") // 11111110...
              .apply(txn);

            db.change()
              .del("146365204598")
              .apply(txn);
        },[&]{
            db.change()
              .put("967276293879", "B") // 11111110...
              .apply(txn);
        });

        equivHeads("long bubble, double deletion", [&]{
            db.change()
              .put("146365204598", "A") // 11111111...
              .put("967276293879", "B") // 11111110...
              .put("948464225881", "C") // 1110...
              .apply(txn);

            db.change()
              .del("967276293879")
              .del("948464225881")
              .apply(txn);
        },[&]{
            db.change()
              .put("146365204598", "A") // 11111110...
              .apply(txn);
        });
    });


    test("mix del and put", [&]{
        equivHeads("left", [&]{
            db.change().put("a", "1").put("b", "2").put("c", "3").apply(txn);
            db.change().del("a").put("c", "4").apply(txn);
        },[&]{
            db.change().put("b", "2").put("c", "4").apply(txn);
        });

        equivHeads("right", [&]{
            db.change().put("a", "1").put("b", "2").put("c", "3").apply(txn);
            db.change().del("a").put("d", "4").apply(txn);
        },[&]{
            db.change().put("b", "2").put("c", "3").put("d", "4").apply(txn);
        });
    });

    test("del non-existent", [&]{
        equivHeads("empty root", [&]{
            db.change().del("a").apply(txn);
        },[&]{
        });

        equivHeads("simple", [&]{
            db.change().put("a", "1").put("b", "2").put("c", "3").apply(txn);
            db.change().del("d").apply(txn);
        },[&]{
            db.change().put("a", "1").put("b", "2").put("c", "3").apply(txn);
        });

        equivHeads("delete a node, and try to delete a non-existent node underneath it", [&]{
            db.change().put("a", "1").apply(txn);
            db.change().del("a").del("849686319312").apply(txn); // 01...
        },[&]{
        });

        equivHeads("same as previous, but requires bubbling", [&]{
            db.change().put("a", "1").put("b", "2").apply(txn);
            db.change().del("a").del("849686319312").apply(txn); // 01...
        },[&]{
            db.change().put("b", "2").apply(txn);
        });
    });


    test("leaf splitting while deleting/updating split leaf", [&]{
        equivHeads("first", [&]{
            db.change().put("a", "1").apply(txn); // 0...
            db.change().del("a").put("849686319312", "2").apply(txn); // 01...
        },[&]{
            db.change().put("849686319312", "2").apply(txn); // 01...
        });

        equivHeads("second", [&]{
            db.change().put("a", "1").apply(txn); // 0...
            db.change().put("a", "3").put("849686319312", "2").apply(txn); // 01...
        },[&]{
            db.change().put("a", "3").put("849686319312", "2").apply(txn); // 01...
        });
    });


    test("bunch of strings", [&]{
        int n = 1000;

        auto changes = db.change();
        for (int i=0; i<n; i++) {
            std::string s = std::to_string(i);
            changes.put(s, s+s);
        }
        changes.apply(txn); // in batch

        auto stats = db.stats(txn);
        verify(stats.numLeafNodes == (uint64_t)n);

        for (int i=0; i<n; i++) {
            std::string s = std::to_string(i);

            std::string_view val;
            verify(db.get(txn, s, val));
            verify(val == s+s);
        }


        auto origRoot = db.root(txn);



        db.checkout("bunch of ints, added one by one");
        verify(db.root(txn) == std::string(32, '\0'));

        for (int i=0; i<n; i++) {
            std::string s = std::to_string(i);
            db.put(txn, s, s+s); // not in batch
        }

        stats = db.stats(txn);
        verify(stats.numLeafNodes == (uint64_t)n);

        verify(db.root(txn) == origRoot);




        db.checkout("bunch of ints, added one by one in reverse");
        verify(db.root(txn) == std::string(32, '\0'));

        for (int i=n-1; i>=0; i--) {
            std::string s = std::to_string(i);
            db.change().put(s, s+s).apply(txn); // not in batch
        }

        stats = db.stats(txn);
        verify(stats.numLeafNodes == (uint64_t)n);

        verify(db.root(txn) == origRoot);
    });



    test("large mixed update/del", [&]{
        equivHeads("", [&]{
            auto changes = db.change();

            for (int i=0; i<600; i++) {
                std::string s = std::to_string(i);
                changes.put(s, s+s);
            }

            changes.apply(txn);

            // delete 0-99
            for (int i=0; i<100; i++) changes.del(std::to_string(i));

            // update 100-199
            for (int i=100; i<200; i++) {
                std::string s = std::to_string(i);
                changes.put(s, s+s+"updated");
            }

            // add 600-699
            for (int i=600; i<700; i++) {
                std::string s = std::to_string(i);
                changes.put(s, s+s);
            }

            changes.apply(txn);
        },[&]{
            auto changes = db.change();

            for (int i=100; i<200; i++) {
                std::string s = std::to_string(i);
                changes.put(s, s+s+"updated");
            }

            for (int i=200; i<700; i++) {
                std::string s = std::to_string(i);
                changes.put(s, s+s);
            }

            changes.apply(txn);
        });
    });



    test("back up start of iterator window", [&]{
        db.change()
          .put("a", "A")
          .put("b", "B")
          .apply(txn);

        std::string_view val;

        verify(db.get(txn, "a", val));
        verify(val == "A");

        verify(db.get(txn, "b", val));
        verify(val == "B");

        auto stats = db.stats(txn);
        verify(stats.numLeafNodes == 2);
    });



    test("fork", [&]{
        std::string_view val;

        db.change()
          .put("a", "A")
          .put("b", "B")
          .put("c", "C")
          .put("d", "D")
          .apply(txn);

        auto origNodeId = db.getHeadNodeId(txn);

        db.fork(txn);

        db.change()
          .put("e", "E")
          .apply(txn);

        auto newNodeId = db.getHeadNodeId(txn);

        verify(db.get(txn, "a", val));
        verify(val == "A");
        verify(db.get(txn, "e", val));
        verify(val == "E");

        {
            auto stats = db.stats(txn);
            verify(stats.numLeafNodes == 5);
        }

        db.checkout(origNodeId);

        verify(db.get(txn, "a", val));
        verify(val == "A");
        verify(!db.get(txn, "e", val));

        {
            auto stats = db.stats(txn);
            verify(stats.numLeafNodes == 4);
        }

        db.checkout(newNodeId);

        verify(db.get(txn, "a", val));
        verify(val == "A");
        verify(db.get(txn, "e", val));
        verify(val == "E");
    });


    test("re-use leafs", [&]{
        // Setup a simple tree

        {
            auto changes = db.change();
            for (int i=0; i<10; i++) {
                std::string s = std::to_string(i);
                changes.put(quadrable::Key::fromInteger(i), std::string("N = ") + s);
            }
            changes.apply(txn);
        }

        uint64_t origHeadNodeId = db.getHeadNodeId(txn);
        auto origRoot = db.root(txn);

        // Get the nodeId for one of the leafs to test later

        uint64_t sampleLeafNodeId = 99999999;

        {
            std::string_view val;
            db.getRaw(txn, quadrable::Key::fromInteger(6).str(), val, &sampleLeafNodeId);
        }

        // Build up a change-set that reuses all the leafs

        auto changes = db.change();

        auto it = db.iterate(txn, quadrable::Key::null());

        uint64_t newSampleNodeId = 0;

        while (!it.atEnd()) {
            changes.putReuse(txn, it.get().nodeId, it.get().key().toInteger() == 6 ? &newSampleNodeId : nullptr);
            it.next();
        }

        // Checkout a new tree and apply the changeset

        db.checkout();
        changes.apply(txn);

        // Roots are equal but node ids aren't

        verify(db.root(txn) == origRoot);
        verify(db.getHeadNodeId(txn) != origHeadNodeId);
        verify(newSampleNodeId == sampleLeafNodeId);
    });


    test("basic proof", [&]{
        auto changes = db.change();
        for (int i=0; i<100; i++) {
            std::string s = std::to_string(i);
            changes.put(s, s + "val");
        }
        changes.put("long", std::string(789, 'A')); // test varints
        changes.apply(txn);

        auto origRoot = db.root(txn);

        auto proof = proofRoundtrip(db.exportProof(txn, {
            "99",
            "68",
            "long",
            "asdf",
        }));

        db.checkout();

        db.importProof(txn, proof, origRoot);

        std::string_view val;

        verify(db.get(txn, "99", val));
        verify(val == "99val");

        verify(db.get(txn, "68", val));
        verify(val == "68val");

        verify(db.get(txn, "long", val));
        verify(val == std::string(789, 'A'));

        verify(!db.get(txn, "asdf", val));

        verifyThrow(db.get(txn, "0", val), "incomplete tree");
    });


    test("use same empty node for multiple keys", [&]{
        db.change()
          .put("735838777414", "A") // 000...
          .put("367300200150", "B") // 001...
          .apply(txn);

        auto origRoot = db.root(txn);

        auto proof = proofRoundtrip(db.exportProof(txn, {
            "582086612140", // 010
            "37481825503",  // 011
        }));

        db.checkout();

        db.importProof(txn, proof, origRoot);

        std::string_view val;
        verify(!db.get(txn, "582086612140", val));
        verify(!db.get(txn, "37481825503", val));
        verify(!db.get(txn, "915377487270", val)); // another 011... (uses empty)

        verifyThrow(db.get(txn, "735838777414", val), "incomplete tree"); // exists as a witness only
        verifyThrow(db.get(txn, "367300200150", val), "incomplete tree"); // exists as a witness only
    });


    test("more proofs", [&]{
        db.change()
          .put("983467173326", "A") // 10...
          .put("50728759955", "B")  // 11...
          .put("679040280359", "C")  // 01...
          .put("685903554406", "D")  // 000...
          .put("66727828072", "E")  // 00001...
          .apply(txn);

        auto origRoot = db.root(txn);

        auto proof = proofRoundtrip(db.exportProof(txn, {
            "983467173326",
            "50728759955",
            "836336493412", // 00..
            "826547358742", // 001..
            "231172376960", // 001..
        }));


        db.checkout();

        db.importProof(txn, proof, origRoot);

        std::string_view val;

        verify(db.get(txn, "983467173326", val));
        verify(val == "A");
        verify(db.get(txn, "50728759955", val));
        verify(val == "B");
        verifyThrow(db.get(txn, "679040280359", val), "incomplete tree"); // exists as a witness only

        verify(!db.get(txn, "826547358742", val));
        verify(!db.get(txn, "836336493412", val));
        verify(!db.get(txn, "231172376960", val));
    });


    test("big proof test", [&]{
        auto changes = db.change();
        for (int i=0; i<1000; i++) {
            std::string s = std::to_string(i);
            changes.put(s, s + "val");
        }
        changes.apply(txn);

        auto origRoot = db.root(txn);

        std::vector<std::string> keys;
        for (int i=-500; i<500; i++) {
            keys.emplace_back(std::to_string(i));
        }

        auto proof = proofRoundtrip(db.exportProof(txn, keys));


        db.checkout();

        db.importProof(txn, proof, origRoot);

        quadrable::GetMultiQuery query{};
        for (int i=-500; i<500; i++) query.emplace(std::to_string(i), GetMultiResult{});
        db.getMulti(txn, query);

        for (int i=-500; i<500; i++) {
            auto str = std::to_string(i);
            if (i < 0) {
                verify(!query[str].exists);
            } else {
                verify(query[str].exists && query[str].val == str+"val");
            }
        }
    });



    test("sub-proof test", [&]{
        std::string_view val;

        auto changes = db.change();
        for (int i=0; i<100; i++) {
            std::string s = std::to_string(i);
            changes.put(s, s + "val");
        }
        changes.apply(txn);

        auto origRoot = db.root(txn);

        quadrable::Proof proof, proof2;

        {
            std::vector<std::string> keys;
            for (int i=-50; i<50; i++) {
                keys.emplace_back(std::to_string(i));
            }

            proof = proofRoundtrip(db.exportProof(txn, keys));
        }

        db.checkout();
        db.importProof(txn, proof, origRoot);

        verify(db.get(txn, "33", val));
        verify(val == "33val");

        {
            std::vector<std::string> keys;
            for (int i=-10; i<10; i++) {
                keys.emplace_back(std::to_string(i));
            }

            proof2 = proofRoundtrip(db.exportProof(txn, keys));
        }

        db.checkout();
        db.importProof(txn, proof2, origRoot);

        quadrable::GetMultiQuery query{};
        for (int i=-10; i<10; i++) query.emplace(std::to_string(i), GetMultiResult{});
        db.getMulti(txn, query);

        for (int i=-10; i<10; i++) {
            auto str = std::to_string(i);
            if (i < 0) {
                verify(!query[str].exists);
            } else {
                verify(query[str].exists && query[str].val == str+"val");
            }
        }

        verifyThrow(db.get(txn, "33", val), "incomplete tree");
    });



    test("no unnecessary empty witnesses", [&]{
        db.change()
          .put("983467173326", "A") // 10...
          .put("50728759955", "B")  // 11...
          .apply(txn);

        auto origRoot = db.root(txn);

        auto proof = proofRoundtrip(db.exportProof(txn, {
            "983467173326",
            "14864808866", // 00...
        }));

        verify(proof.strands.size() == 1); // No separate WitnessEmpty is needed because a HashEmpty cmd is on existing node's path

        db.checkout();

        db.importProof(txn, proof, origRoot);

        std::string_view val;

        verify(db.get(txn, "983467173326", val));
        verify(val == "A");

        verifyThrow(db.get(txn, "50728759955", val), "incomplete tree");

        verify(!db.get(txn, "14864808866", val));
    });





    test("update proof", [&]{
        auto setupDb = [&]{
            db.change()
              .put("353568684874", "A")  // 01...
              .put("852771900452", "B")  // 1000...
              .put("877307249616", "C")  // 101...
              .put("640237942109", "D")  // 1001...
              .apply(txn);
        };

        quadrable::Proof proof;
        std::string origRoot, newRoot;
        std::string_view val;


        equivHeads("update leaf, fail trying to update witness", [&]{
            setupDb();

            proof = proofRoundtrip(db.exportProof(txn, {
                "353568684874",
            }));
            origRoot = db.root(txn);

            db.change().put("353568684874", "A2").apply(txn);
            newRoot = db.root(txn);
        }, [&]{
            db.importProof(txn, proof, origRoot);

            db.change().put("353568684874", "A2").apply(txn);

            verify(db.root(txn) == newRoot); // also checked by equivHeads

            verifyThrow(db.change().put("852771900452", "B2").apply(txn), "encountered witness during update");
        });


        equivHeads("update 2 leafs at different levels", [&]{
            setupDb();

            proof = proofRoundtrip(db.exportProof(txn, {
                "852771900452",
                "877307249616",
            }));
            origRoot = db.root(txn);

            db.change().put("852771900452", "B2").apply(txn);
            db.change().put("877307249616", "C2").apply(txn);
        }, [&]{
            db.importProof(txn, proof, origRoot);

            db.change().put("852771900452", "B2").apply(txn);
            db.change().put("877307249616", "C2").apply(txn);
        });


        equivHeads("split leaf", [&]{
            setupDb();

            proof = proofRoundtrip(db.exportProof(txn, {
                "852771900452",
            }));
            origRoot = db.root(txn);

            db.change().put("762909246408", "E").apply(txn); // 1000...
        }, [&]{
            db.importProof(txn, proof, origRoot);

            db.change().put("762909246408", "E").apply(txn); // 1000...
        });




        equivHeads("no change to witness leaf", [&]{
            setupDb();

            proof = proofRoundtrip(db.exportProof(txn, {
                "787934352296", // 00...
            }));
            origRoot = db.root(txn);
        }, [&]{
            db.importProof(txn, proof, origRoot);

            verifyThrow(db.get(txn, "353568684874", val), "incomplete tree");

            auto nodeId = db.getHeadNodeId(txn);
            db.change().put("353568684874", "A").apply(txn);

            verify(nodeId != db.getHeadNodeId(txn)); // must use new nodes, since upgrading WitnessLeaf to Leaf

            verify(db.get(txn, "353568684874", val));
            verify(val == "A");
        });



        equivHeads("no change to witness leaf", [&]{
            setupDb();

            proof = proofRoundtrip(db.exportProof(txn, {
                "787934352296", // 00...
            }));
            origRoot = db.root(txn);
        }, [&]{
            db.importProof(txn, proof, origRoot);

            verifyThrow(db.get(txn, "353568684874", val), "incomplete tree");

            auto nodeId = db.getHeadNodeId(txn);
            db.change().put("353568684874", "A").apply(txn);

            verify(nodeId != db.getHeadNodeId(txn)); // must use new nodes, since upgrading WitnessLeaf to Leaf

            verify(db.get(txn, "353568684874", val));
            verify(val == "A");
        });




        equivHeads("update witness leaf", [&]{
            setupDb();

            proof = proofRoundtrip(db.exportProof(txn, {
                "787934352296", // 00...
            }));
            origRoot = db.root(txn);

            db.change().put("353568684874", "A2").apply(txn);
        }, [&]{
            db.importProof(txn, proof, origRoot);

            db.change().put("353568684874", "A2").apply(txn);
        });



        equivHeads("split witness leaf", [&]{
            setupDb();

            proof = proofRoundtrip(db.exportProof(txn, {
                "787934352296", // 00...
            }));
            origRoot = db.root(txn);

            db.change().put("787934352296", "NEW").apply(txn);
        }, [&]{
            db.importProof(txn, proof, origRoot);

            db.change().put("787934352296", "NEW").apply(txn);
        });





        equivHeads("can bubble up a witnessLeaf", [&]{
            db.change().put("731156037546", "1").put("925458752084", "2").apply(txn);

            proof = proofRoundtrip(db.exportProof(txn, {
                "731156037546", // 0...
                "925458752084", // 1...
            }));
            origRoot = db.root(txn);

            db.change().del("731156037546").apply(txn);
        }, [&]{
            db.importProof(txn, proof, origRoot);

            db.change().del("731156037546").apply(txn);
        });




        equivHeads("can't bubble up a witness", [&]{
            db.change().put("a", "1").put("b", "2").apply(txn);

            proof = proofRoundtrip(db.exportProof(txn, {
                "a",
            }));

            origRoot = db.root(txn);
        }, [&]{
            db.importProof(txn, proof, origRoot);

            // FIXME: support creating a proof with enough information to do a deletion
            //db.change().del("a").apply(txn);
            verifyThrow(db.change().del("a").apply(txn), "can't bubble a witness node");
        });
    });




    test("integer proofs", [&]{
        for (uint64_t skip = 1; skip < 20; skip++) {
            db.checkout();

            uint64_t last;

            {
                auto c = db.change();
                for (uint64_t i = 1; i < 10000; i += skip) {
                    verify(quadrable::Key::fromInteger(i - 1) < quadrable::Key::fromInteger(i));
                    c.put(quadrable::Key::fromInteger(i), std::to_string(i));
                    last = i;
                }
                c.apply(txn);
            }

            auto origRoot = db.root(txn);

            // Prove presence of max element

            auto proof = proofRoundtrip(db.exportProofRaw(txn, {
                db.iterate(txn, quadrable::Key::max(), true).get().key(),
            }));

            db.checkout();

            db.importProof(txn, proof, origRoot);

            {
                auto c = db.change();
                c.put(quadrable::Key::fromInteger(last + 1), std::to_string(last + 1));
                c.apply(txn);
            }
        }
    });



    // This test shows that the size of the proof doesn't increase drastically if larger numbers are
    // used for integer keys. Even though there are more common prefix bits, most of their siblings will
    // be empty, which has a very compact proof encoding.

    test("proof sizing", [&]{
        for (uint64_t i = 1; i <= 1e12; i *= 10) {
            db.checkout();

            {
                auto c = db.change();
                c.put(quadrable::Key::fromInteger(i), "A");
                c.apply(txn);
            }

            auto proof = quadrable::transport::encodeProof(db.exportProofRaw(txn, {
                quadrable::Key::fromInteger(i),
            }));

            //std::cout << "SIZE " << i << " -> " << proof.length() << std::endl;
            verify(proof.length() <= 13);
        }
    });



    test("iterators basic", [&]{
        db.checkout();

        auto c = db.change();
        for (uint64_t i = 2; i < 20; i+=2) {
            c.put(quadrable::Key::fromInteger(i), std::to_string(i));
        }
        c.apply(txn);

        // Initial values

        {
            auto it = db.iterate(txn, quadrable::Key::fromInteger(1));
            verify(it.get().leafVal() == "2");
        }

        {
            auto it = db.iterate(txn, quadrable::Key::fromInteger(19), true);
            verify(it.get().leafVal() == "18");
        }

        // Past end values

        {
            auto it = db.iterate(txn, quadrable::Key::fromInteger(19));
            verify(it.get().nodeId == 0);
        }

        {
            auto it = db.iterate(txn, quadrable::Key::fromInteger(1), true);
            verify(it.get().nodeId == 0);
        }

        // Correct seeking behaviour

        {
            auto it = db.iterate(txn, quadrable::Key::fromInteger(11));
            verify(it.get().leafVal() == "12");
        }

        {
            auto it = db.iterate(txn, quadrable::Key::fromInteger(11), true);
            verify(it.get().leafVal() == "10");
        }

    });

    test("iterators full", [&]{
        auto go = [&](uint64_t start, uint64_t end, uint64_t skip){
            if (start < 5) throw quaderr("start too low");
            db.checkout();

            std::map<uint64_t, std::string> vals;

            auto c = db.change();
            for (uint64_t i = start; i < end; i += skip) {
                c.put(quadrable::Key::fromInteger(i), std::to_string(i));
                vals[i] = std::to_string(i);
            }
            c.apply(txn);

            for (uint64_t i = start - 5; i < end + 5; i++) {
                auto valsIt = vals.lower_bound(i);
                auto it = db.iterate(txn, quadrable::Key::fromInteger(i));

                while (!it.atEnd()) {
                    verify(valsIt != vals.end());
                    verify(it.get().leafVal() == valsIt->second);
                    it.next();
                    valsIt = std::next(valsIt);
                }

                verify(valsIt == vals.end());
            }

            for (uint64_t i = end + 5; i > start - 5; i--) {
                auto valsItFwd = vals.upper_bound(i);
                std::reverse_iterator<decltype(valsItFwd)> valsIt{valsItFwd};
                auto it = db.iterate(txn, quadrable::Key::fromInteger(i), true);

                while (!it.atEnd()) {
                    verify(valsIt != vals.rend());
                    verify(it.get().leafVal() == valsIt->second);
                    it.next();
                    valsIt = std::next(valsIt);
                }

                verify(valsIt == vals.rend());
            }
        };

        go(5, 20, 2);
        go(10, 200, 15);
        go(100, 2000, 31);
        go(4000, 5000, 82);
    });


    test("range proofs", [&]{
        db.checkout();

        {
            auto c = db.change();
            for (uint64_t i = 1; i < 10000; i++) {
                c.put(quadrable::Key::fromInteger(i), std::to_string(i));
            }
            c.apply(txn);
        }

        auto origRoot = db.root(txn);

        auto proof = proofRoundtrip(db.exportProofRange(txn, db.getHeadNodeId(txn), quadrable::Key::fromInteger(500), quadrable::Key::fromInteger(510)));

        db.checkout();

        db.importProof(txn, proof, origRoot);

        std::string_view val;

        for (uint64_t i = 500; i < 510; i++) {
            verify(db.getRaw(txn, quadrable::Key::fromInteger(i).sv(), val));
            verify(val == std::to_string(i));
        }

        verifyThrow(db.getRaw(txn, quadrable::Key::fromInteger(499).sv(), val), "incomplete tree");
        verifyThrow(db.getRaw(txn, quadrable::Key::fromInteger(511).sv(), val), "incomplete tree");
        verifyThrow(db.getRaw(txn, quadrable::Key::fromInteger(9999).sv(), val), "incomplete tree");
        verifyThrow(db.getRaw(txn, quadrable::Key::fromInteger(1).sv(), val), "incomplete tree");
    });



    test("memStore basic", [&]{
        MemStore m;

        db.withMemStore(m, [&]{
            db.checkout();
            db.writeToMemStore = true;

            db.change()
              .put("A", "res1")
              .put("B", "res2")
              .apply(txn);

            verify(db.getHeadNodeId(txn) >= firstMemStoreNodeId);

            std::string_view val;
            verify(db.get(txn, "A", val));
            verify(val == "res1");
            verify(db.get(txn, "B", val));
            verify(val == "res2");

            auto stats = db.stats(txn);
            verify(stats.numLeafNodes == 2);
        });

        db.writeToMemStore = false;
    });

    test("memStore forking from lmdb", [&]{
        MemStore m;

        db.checkout("memStore-test");

        db.change()
          .put("A", "res1")
          .put("B", "res2")
          .apply(txn);

        uint64_t origNode = db.getHeadNodeId(txn);

        db.withMemStore(m, [&]{
            db.checkout("memStore-test");
            db.writeToMemStore = true;

            verifyThrow(db.change().put("C", "res3").apply(txn), "attempted to store MemStore node into LMDB");

            db.fork(txn);
            db.change().put("C", "res3").apply(txn);

            verify(db.getHeadNodeId(txn) >= firstMemStoreNodeId);

            std::string_view val;
            verify(db.get(txn, "A", val));
            verify(val == "res1");
            verify(db.get(txn, "B", val));
            verify(val == "res2");
            verify(db.get(txn, "C", val));
            verify(val == "res3");

            auto stats = db.stats(txn);
            verify(stats.numLeafNodes == 3);
        });

        db.writeToMemStore = false;

        db.checkout(origNode);

        auto stats = db.stats(txn);
        verify(stats.numLeafNodes == 2);
    });

    test("memStore-only env", [&]{
        quadrable::Quadrable db2;
        db2.addMemStore();
        db2.writeToMemStore = true;
        lmdb::txn txn2(nullptr);

        db2.checkout();

        db2.change()
           .put("A", "res1")
           .put("B", "res2")
           .apply(txn2);

        std::string_view val;
        verify(db2.get(txn2, "A", val));
        verify(val == "res1");
        verify(db2.get(txn2, "B", val));

        auto stats = db2.stats(txn2);
        verify(stats.numLeafNodes == 2);
    });



    test("sync fuzz", [&]{
        std::mt19937 rnd;
        rnd.seed(0);

        for (uint trialIter = 0; trialIter < 500; trialIter++) {
            uint64_t numElems = rnd() % 800;
            uint64_t maxElem = 1000;
            uint64_t numAlterations = rnd() % 200;

            db.checkout();

            {
                auto c = db.change();
                for (uint64_t i = 0; i < numElems; i++) {
                    auto n = rnd() % maxElem;
                    c.put(quadrable::Key::fromInteger(n), std::to_string(n) + std::string(rnd() % 60, 'A'));
                }
                c.apply(txn);
            }

            uint64_t origNodeId = db.getHeadNodeId(txn);
            db.fork(txn);

            {
                auto chg = db.change();

                for (uint64_t i = 0; i < numAlterations; i++) {
                    auto n = rnd() % maxElem;
                    auto action = rnd() % 2;
                    if (action == 0) {
                        chg.put(quadrable::Key::fromInteger(n), std::to_string(n) + " new");
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

            db.addMemStore();
            db.writeToMemStore = true;

            std::vector<uint64_t> nodeIdsSeenDuringScan;
            std::vector<uint64_t> nodeIdsSeenDuringDiff;

            auto cb = [&](auto dt, const auto &node){
                nodeIdsSeenDuringScan.push_back(node.nodeId);
            };

            while(1) {
                auto reqs = syncRequestsRoundtrip(sync.getReqs(txn, (rnd() % 1000) + 100, cb));
                if (reqs.size() == 0) break;

                auto resps = syncResponsesRoundtrip(db.handleSyncRequests(txn, newNodeId, reqs, (rnd() % 10000) + 2000));
                sync.addResps(txn, reqs, resps);
            }

            db.writeToMemStore = false;

            db.checkout(sync.nodeIdShadow);
            if (db.rootKey(txn) != newKey) throw quaderr("NOT EQUAL AFTER IMPORT");

            db.checkout(origNodeId);
            db.fork(txn);

            {
                auto chg = db.change();

                sync.diffReset();
                sync.diff(txn, origNodeId, sync.nodeIdShadow, [&](auto dt, const auto &node){
                    nodeIdsSeenDuringDiff.push_back(node.nodeId);

                    if (dt == Quadrable::DiffType::Added) {
                        chg.put(node.key(), node.leafVal());
                    } else if (dt == Quadrable::DiffType::Changed) {
                        chg.put(node.key(), node.leafVal());
                    } else {
                        chg.del(node.key());
                    }
                });

                chg.apply(txn);
            }

            auto reconstructedKey = db.rootKey(txn);

            verify(reconstructedKey == newKey);

            db.removeMemStore();

            std::sort(nodeIdsSeenDuringScan.begin(), nodeIdsSeenDuringScan.end());
            std::sort(nodeIdsSeenDuringDiff.begin(), nodeIdsSeenDuringDiff.end());
            verify(nodeIdsSeenDuringDiff == nodeIdsSeenDuringScan);
        }
    });



    txn.abort();

    /*
    for(size_t i=0; i<=256; i++) {
        Key k = Key::existing(from_hex("ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff"));
        k.keepPrefixBits(i);
        std::cout << i << " : " << to_hex(k.str()) << std::endl;
    }
    */
}



}



int main() {
    try {
        quadrable::doTests();
    } catch (const std::runtime_error& error) {
        std::cerr << "Test failure: " << error.what() << std::endl;
        return 1;
    }

    std::cout << "\nAll tests OK" << std::endl;
    return 0;
}
