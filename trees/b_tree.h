//
// Created by robert on 14/9/17.
//
//对B+树的操作有：插入KV对，删除key，搜索kv对，范围查找
//3.26 ✔

#ifndef B_TREE_B_TREE_H
#define B_TREE_B_TREE_H


template <typename K, typename V>
class BTree {
public:
    virtual void insert(const K &k, const V &v) = 0;
    virtual bool delete_key(const K &k) = 0;
    virtual bool search(const K &k, V &v) = 0;
    virtual void clear() = 0;
    class Iterator {
    public:
        virtual bool Valid() = 0;

        virtual bool next(K & key, V & val) {
            return false;
        };

        virtual void SeekToFirst() = 0;

        virtual void SeekToLast() = 0;

        virtual K Key() = 0;
        
        virtual V Value() = 0;

        virtual void Next() = 0;

        virtual void Prev() = 0;

        virtual void Seek(K key) = 0;

        virtual void SetForward(bool flag) = 0;

        virtual bool GetForward() = 0;
    }; 
    //virtual Iterator* range_search(const K & key_low, const K & key_high) = 0;
};
#endif //B_TREE_B_TREE_H
