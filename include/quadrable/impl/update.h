
bool hex_string_to_buffer_c(std::string_view sv, uint8_t * data) {
    size_t slength = sv.length();
    if (slength != 64) // must be even
        return false;

    size_t dlength = slength / 2;
    std::memset(data, 0, dlength);
    const char * str_data = sv.data();

    size_t index = 0;
    while (index < slength) {
        char c = str_data[index];
        int value = 0;
        if (c >= '0' && c <= '9')
            value = (c - '0');
        else if (c >= 'A' && c <= 'F')
            value = (10 + (c - 'A'));
        else if (c >= 'a' && c <= 'f')
            value = (10 + (c - 'a'));
        else
            return false;

        data[(index / 2)] += value << (((index + 1) % 2) * 4);

        index++;
    }
    return true;
}
public:

class UpdateSet {
  friend class Quadrable;

  private:
    UpdateSetMap map;
    Quadrable *db;

  public:
    UpdateSet(Quadrable *db_) : db(db_) {}

    UpdateSet &put(std::string_view key, std::string_view val, uint64_t *outputNodeId = nullptr) {
        if (key.size() == 0) throw quaderr("zero-length keys not allowed");
        map.insert_or_assign(Key::hash(key), Update{std::string(db->trackKeys ? key : ""), std::string(val), false, outputNodeId});
        return *this;
    }

    UpdateSet &put(const Key &keyRaw, std::string_view val, uint64_t *outputNodeId = nullptr) {
        map.insert_or_assign(keyRaw, Update{"", std::string(val), false, outputNodeId});
        return *this;
    }
    UpdateSet &put(uint64_t key, uint8_t * value, uint64_t *outputNodeId = nullptr) {
        map.insert_or_assign(Key::fromUint64(key), Update{"", std::string((const char *)value, 32), false, outputNodeId});
        return *this;
    }
    UpdateSet &putBytes(uint8_t * key, uint8_t * value, uint64_t *outputNodeId = nullptr) {
        map.insert_or_assign(Key::fromBytes(key, 32), Update{"", std::string((const char *)value, 32), false, outputNodeId});
        return *this;
    }
    UpdateSet &putHex(std::string_view key, std::string_view value, uint64_t *outputNodeId = nullptr) {
        auto k = Key::fromUint64(0);

        hex_string_to_buffer_c(key, k.data);
        uint8_t v[32];
        hex_string_to_buffer_c(value, (uint8_t *)&v[0]);

        map.insert_or_assign(k, Update{"", std::string((const char *)(&v[0]), 32), false, outputNodeId});
        return *this;
    }

    UpdateSet &putReuse(const Key &keyRaw, uint64_t nodeId, uint64_t *outputNodeId = nullptr) {
        map.insert_or_assign(keyRaw, Update{"", "", false, outputNodeId, nodeId});
        return *this;
    }

    UpdateSet &putReuse(lmdb::txn &txn, uint64_t nodeId, uint64_t *outputNodeId = nullptr) {
        ParsedNode node(db, txn, nodeId);
        if (!node.isLeaf()) throw quaderr("trying to reuse a non-leaf");
        putReuse(node.key(), nodeId, outputNodeId);
        return *this;
    }

    UpdateSet &del(std::string_view key, uint64_t *outputNodeId = nullptr) {
        if (key.size() == 0) throw quaderr("zero-length keys not allowed");
        map.insert_or_assign(Key::hash(key), Update{std::string(key), "", true, outputNodeId});
        return *this;
    }

    UpdateSet &del(const Key &keyRaw, uint64_t *outputNodeId = nullptr) {
        map.insert_or_assign(keyRaw, Update{"", "", true, outputNodeId});
        return *this;
    }

    void apply(lmdb::txn &txn) {
        db->apply(txn, this);
    }

  private:
    void eraseRange(UpdateSetMap::iterator &begin, UpdateSetMap::iterator &end, std::function<bool(UpdateSetMap::iterator &)> pred) {
        if (begin == end) return;

        for (auto i = std::next(begin); i != end; ) {
            auto prev = i++;
            if (pred(prev)) map.erase(prev);
        }

        if (pred(begin)) ++begin;
    };
};




UpdateSet change() {
    return UpdateSet(this);
}



void apply(lmdb::txn &txn, UpdateSet *updatesOrig) {
    apply(txn, *updatesOrig);
}

void apply(lmdb::txn &txn, UpdateSet &updatesOrig) {
    // If exception is thrown, updatesOrig could be in inconsistent state, so ensure it's cleared by moving from it
    UpdateSet updates = std::move(updatesOrig);

    for (auto &u : updates.map) {
        if (u.second.outputNodeId) *u.second.outputNodeId = 0;
    }

    uint64_t oldNodeId = getHeadNodeId(txn);

    bool bubbleUp = false;
    auto newNode = putAux(txn, 0, oldNodeId, updates, updates.map.begin(), updates.map.end(), bubbleUp, false);

    if (newNode.nodeId != oldNodeId) setHeadNodeId(txn, newNode.nodeId);
}



void put(lmdb::txn &txn, std::string_view key, std::string_view val) {
    change().put(key, val).apply(txn);
}

void del(lmdb::txn &txn, std::string_view key) {
    change().del(key).apply(txn);
}



private:

BuiltNode putAux(lmdb::txn &txn, uint64_t depth, uint64_t nodeId, UpdateSet &updates, UpdateSetMap::iterator begin, UpdateSetMap::iterator end, bool &bubbleUp, bool deleteRightSide) {
    ParsedNode node(this, txn, nodeId);
    bool checkBubble = false;

    // recursion base cases

    if (begin == end) {
        return BuiltNode::reuse(node);
    }

    if (node.nodeType == NodeType::Witness) {
        throw quaderr("encountered witness during update: partial tree");
    } else if (node.isEmpty()) {
        updates.eraseRange(begin, end, [&](UpdateSetMap::iterator &u){ return u->second.deletion; });

        if (begin == end) {
            // All updates for this sub-tree were deletions for keys that don't exist, so do nothing.
            return BuiltNode::reuse(node);
        }

        if (std::next(begin) == end) {
            auto b = BuiltNode::newLeaf(this, txn, begin);
            if (begin->second.outputNodeId) *begin->second.outputNodeId = b.nodeId;
            return b;
        }
    } else if (node.isLeaf()) {
        if (std::next(begin) == end && begin->first == node.leafKeyHash()) {
            // Update an existing record

            if (begin->second.deletion) {
                bubbleUp = true;
                if (begin->second.outputNodeId) *begin->second.outputNodeId = node.nodeId;
                return BuiltNode::empty();
            }

            if (deleteRightSide || (node.nodeType == NodeType::Leaf && begin->second.val == node.leafVal())) {
                // No change to this leaf, so do nothing. Don't do this for WitnessLeaf nodes, since we need to upgrade them to leaves.
                return BuiltNode::reuse(node);
            }

            auto b = BuiltNode::newLeaf(this, txn, begin);
            if (begin->second.outputNodeId) *begin->second.outputNodeId = b.nodeId;
            return b;
        }

        bool deleteThisLeaf = false;

        updates.eraseRange(begin, end, [&](UpdateSetMap::iterator &u){
            if (u->second.deletion) {
                if (u->first == node.leafKeyHash()) {
                    deleteThisLeaf = true;
                    if (u->second.outputNodeId) *u->second.outputNodeId = node.nodeId;
                }
                checkBubble = true; // so we check the status of this node after handling any changes further down (may require bubbling up)
            }
            return u->second.deletion;
        });

        if (begin == end) {
            if (deleteThisLeaf) {
                // The only update for this sub-tree was to delete this key
                bubbleUp = true;
                return BuiltNode::empty();
            }
            // All updates for this sub-tree were deletions for keys that don't exist, so do nothing.
            return BuiltNode::reuse(node);
        }

        // The leaf needs to get split into a branch, so add it into our update set to get added further down (unless it itself was deleted).

        if (!deleteThisLeaf) {
            // emplace() ensures that we don't overwrite any updates to this leaf already in the UpdateSet.
            auto emplaceRes = updates.map.emplace(Key::existing(node.leafKeyHash()), Update{"", "", false, nullptr, node.nodeId});

            // If we did insert it, and it went before the start of our iterator window, back up our iterator to include it.
            //   * This happens when the leaf we are splitting is to the left of the leaf we are adding.
            //   * It's not necessary to do this for the right side since the end iterator will always point *past* any right-side nodes.
            if (emplaceRes.second && emplaceRes.first->first < begin->first) begin = emplaceRes.first;
        }
    }


    // Split into left and right groups of keys

    auto middle = begin;
    while (middle != end && !middle->first.getBit(depth)) ++middle;


    // Recurse

    assertDepth(depth);

    auto leftNode = putAux(txn, depth+1, node.leftNodeId, updates, begin, middle, checkBubble, deleteRightSide);
    auto rightNode = [&]{
        if (deleteRightSide && middle == end) {
            checkBubble = true;
            return BuiltNode::empty();
        }

        return putAux(txn, depth+1, node.rightNodeId, updates, middle, end, checkBubble, deleteRightSide);
    }();

    if (checkBubble) {
        if (leftNode.nodeType == NodeType::Witness || rightNode.nodeType == NodeType::Witness) {
            // We don't know if one of the nodes is a branch or a leaf
            throw quaderr("can't bubble a witness node");
        } else if (leftNode.isEmpty() && rightNode.isEmpty()) {
            bubbleUp = true;
            return BuiltNode::empty();
        } else if (leftNode.isLeaf() && rightNode.isEmpty()) {
            bubbleUp = true;
            ParsedNode n(this, txn, leftNode.nodeId);
            return BuiltNode::reuse(n);
        } else if (leftNode.isEmpty() && rightNode.isLeaf()) {
            bubbleUp = true;
            ParsedNode n(this, txn, rightNode.nodeId);
            return BuiltNode::reuse(n);
        }

        // One of the nodes is a branch, or both are leaves, so bubbling can stop
    }

    return BuiltNode::newBranch(this, txn, leftNode, rightNode);
}
