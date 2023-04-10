//
// Created by Li Wang on 6/1/17.
//
//3.28 ✔

#ifndef B_PLUS_TREE_LEAFNODE_H
#define B_PLUS_TREE_LEAFNODE_H

#include <sstream>
#include <iostream>
#include <string>
#include <vector>
#include "node.h"

template<typename K, typename V>
class VanillaBPlusTree;

template<typename K, typename V>
class LeafNode : public Node<K, V> {
    friend class VanillaBPlusTree<K, V>;
    struct Entry {
        K key;
        V val;

        Entry() {};

        Entry(K k, V v) : key(k), val(v) {};

        Entry &operator=(const Entry &r) {
            this->key = r.key;
            this->val = r.val;
            return *this;
        }
    };

public:

    LeafNode(int capacity) : size_(0), right_sibling_(0) {
        this->capacity_ = capacity;
 
    };




    bool insert(const K &key, const V &val) {

        int insert_position;
        const bool found = search_key_position(key, insert_position);

        if (found) {
            // update the entry.
            //相同的key已经存在，更改value
            entries_[insert_position].val = val;
            return true;
        } else {
            //该叶节点溢出，插入失败
            if (size_ >= this->capacity_) {
                return false;
            }

            // make an empty slot for new entry
            //向后腾位置
            for (int i = size_ - 1; i >= insert_position; i--) {
                entries_[i + 1] = entries_[i];
            }

            // insert the new entry.
            entries_[insert_position] = Entry(key, val);
            //entries_.insert(entries_.begin() + insert_position, Entry(key, val));
            size_++;
            return true;//插入成功
        }
    }

    //split指向新生成的分裂节点，应该放入上一层节点中
    bool insert_with_split_support(const K &key, const V &val, Split<K, V> &split) {
        int insert_position;
        //没找到的话，返回的是恰好比key大的位置
        //key应该替代insert_position，包括position在内的值都后移
        const bool found = search_key_position(key, insert_position);

        //key已经存在，需要更新
        if (found) {
            // update the entry.
            entries_[insert_position].val = val;
            return false;
        }

        //当前叶节点不会溢出
        if (size_ < this->capacity_) {
            // make an empty slot for the new entry
            for (int i = size_ - 1; i >= insert_position; i--) {
                entries_[i + 1] = entries_[i];
            }

            // insert the new entry.
            entries_[insert_position] = Entry(key, val);
            //entries_.insert(entries_.begin() + insert_position, Entry(key, val));
            size_++;
            return false;
        } else {
            //当前叶节点要分裂成两部分
            // split
            bool insert_to_first_half = insert_position < this->capacity_ / 2;

            int entry_index_for_right_node = this->capacity_ / 2;
            LeafNode<K, V> *const left = this;
            LeafNode<K, V> *const right = new LeafNode<K, V>(this->capacity_);

            //重建指针
            right->right_sibling_ = left->right_sibling_;
            left->right_sibling_ = right;

            // move entries to the right node
            for (int i = entry_index_for_right_node, j = 0; i < this->capacity_; ++i, ++j) {
                right->entries_[j] = left->entries_[i];
                //right->entries_.insert(right->entries_.begin() + j, left->entries_[i]);
            }

            const int moved = this->capacity_ - entry_index_for_right_node;
            left->size_ -= moved;
            right->size_ = moved;

            // insert
            if (insert_to_first_half)
                left->insert(key, val);
            else
                right->insert(key, val);

            split.left = left;
            split.right = right;
            split.boundary_key = right->entries_[0].key;
            return true;
        }

    }

    bool search(const K &k, V &v) {
        int position;
        const bool found = search_key_position(k, position);
        //找到
        if (found)
            v = entries_[position].val;
        return found;
    }

    bool locate_key(const K &k, Node<K, V>* &child, int &position) {
        const bool found = search_key_position(k, position);
        child = this;
        return found;
    };

    bool update(const K &k, const V &v) {
        int position;
        const bool found = search_key_position(k, position);
        if (found) {
            entries_[position].val = v;
            return true;
        } else {
            return false;
        }

    }

    bool delete_key(const K &k) {
        int position;
        const bool found = search_key_position(k, position);
        if (!found)
            return false;

        for (int i = position; i < size_ - 1; ++i) {
            entries_[i] = entries_[i + 1];
        }
        //entries_.erase(entries_.begin() + position);
        --size_;
        return true;
    }

    bool delete_key(const K &k, bool &underflow) {
        int position;
        const bool found = search_key_position(k, position);
        if (!found)
            return false;

        //删除key
        for (int i = position; i < size_ - 1; ++i) {
            entries_[i] = entries_[i + 1];
        }
        //entries_.erase(entries_.begin() + position);
        --size_;
        //如果key的数量小于总量1/2，说明当前节点不满足要求，需要平衡
        //注意：向上取整
        underflow = size_ < (this->capacity_ + 1) / 2;
        return true;
    }

    std::string toString() const {
        std::string ret;
        std::stringstream ss;
        for (int i = 0; i < size_; i++) {
            ss << "(" << entries_[i].key << "," << entries_[i].val << ")";
            if (i != size_ - 1)
                ss << " ";
        }
        return ss.str();
    }

    const K get_leftmost_key() const {
        return entries_[0].key;
    }

    bool balance(Node<K, V> *right_sibling_node, K &boundary) {
        LeafNode<K, V> *right = static_cast<LeafNode<K, V> * >(right_sibling_node);
        const int underflow_bound = UNDERFLOW_BOUND(this->capacity_);
        //调用该函数的节点是下溢节点
        //方案一：借节点
        if (size_ < underflow_bound) {
            // this node under-flows
            //右节点可以借给该节点
            if (right->size_ > underflow_bound) {

                // borrow an entry from the right sibling node
                entries_[size_] = right->entries_[0];
                //entries_.insert(entries_.begin() + size_, right->entries_[0]);
                ++size_;

                // remove the entry from the right sibling node
                for (int i = 0; i < right->size_ - 1; ++i) {
                    right->entries_[i] = right->entries_[i + 1];
                }
                //right->entries_.erase(right->entries_.begin());
                --right->size_;

                // update the boundary
                boundary = right->entries_[0].key;
                return false;
            }
        }

        //调用该函数的节点（左）借出节点
        if (right->size_ < underflow_bound) {
            // the right node under-flows
            //可以借出
            if (this->size_ > underflow_bound) {

                // make space for the entry borrowed from the left
                for (int i = right->size_ - 1; i >= 0; --i) {
                    right->entries_[i + 1] = right->entries_[i];
                }

                // copy the entry and increase the size by 1
                right->entries_[0] = this->entries_[size_ - 1];
                //right->entries_.insert(right->entries_.begin(), this->entries_[size_ - 1]);
                ++right->size_;

                // remove the entry from the left by reducing the size
                --this->size_;

                // update the boundary
                boundary = right->entries_[0].key;
                return false;
            }
        }


        // the sibling node has no additional entry to borrow. We merge the nodes.
        // move all the entries from the right to the left
        //方案二：合并
        for (int l = this->size_, r = 0; r < right->size_; ++l, ++r) {
            this->entries_[l] = right->entries_[r];
            //this->entries_.insert(this->entries_.begin() + l, right->entries_[r]);
        }
        this->size_ += right->size_;
        right->size_ = 0;
        this->right_sibling_ = right->right_sibling_;

        // delete the right
        delete right;
        return true;
    }

    virtual Node<K, V>* get_leftmost_leaf_node() {
        return this;
    }

    NodeType type() const {
        return LEAF;
    }

    int size() const {
        return size_;
    }

    friend std::ostream &operator<<(std::ostream &os, LeafNode<K, V> const &m) {
        for (int i = 0; i < m.size_; i++) {
            os << "(" << m.entries_[i].key << "," << m.entries_[i].val << ")";
            if (i != m.size_ - 1)
                os << " ";
        }
        return os;
    }

protected:
    bool getEntry(int i, K &k, V &v) const {
        if (i >= size_)
            return false;
        k = entries_[i].key;
        v = entries_[i].val;
        return true;
    }

private:

    bool search_key_position(const K &key, int &position) const {
        int l = 0, r = size_ - 1;
        int m = 0;
        bool found = false;
        while (l <= r) {
            m = (l + r) >> 1;
            if (entries_[m].key < key) {
                l = m + 1;
            } else if (entries_[m].key == key) {
                position = m;//找到
                return true;
            } else {
                r = m - 1;
            }
        }
        position = l;//没找到，返回恰好比key大的key的位置
        return false;
    }

    /**
     * TODO: store the keys and the values separately, to improve data access locality.
     */
    //std::vector<Entry> entries_;
    Entry entries_[100];
    int size_;
    LeafNode* right_sibling_;

};


#endif //B_PLUS_TREE_LEAFNODE_H
