//
// Created by Li Wang on 6/3/17.
//
//3.28 ✔

#ifndef B_TREE_INNER_NODE_H
#define B_TREE_INNER_NODE_H


#include <iostream>
#include <sstream>
#include <string>
#include <vector>
#include "node.h"
#include "leaf_node.h"

template<typename K, typename V>
class VanillaBPlusTree;

template<typename K, typename V>
class InnerNode : public Node<K, V> {
    friend class VanillaBPlusTree<K, V>;

public:
    InnerNode(int capacity) : size_(0) {
        this->capacity_ = capacity;
 
    };

    InnerNode(Node<K, V> *left, Node<K, V> *right, int capacity = 0) {
        //assert(left->capacity_ == )
        if(capacity == 0){
            this->capacity_ = left->get_capacity();
        }
        else{
            this->capacity_ = capacity;
        }
  
        size_ = 2;
        key_[0] = left->get_leftmost_key();
        //key_.insert(key_.begin(), left->get_leftmost_key());
        child_[0] = left;
        //child_.insert(child_.begin(), left);
        key_[1] = right->get_leftmost_key();
        //key_.insert(key_.begin() + 1, right->get_leftmost_key());
        child_[1] = right;
        //child_.insert(child_.begin() + 1, right);

    }

    ~InnerNode() {
        for (int i = 0; i < size_; ++i) {
            delete child_[i];
        }
     
    }



    //【废弃：没有考虑key小于最左边界的情况】
    bool insert(const K &key, const V &val) {
        Node<K, V> *targetNode = child_[locate_child_index(key)];
        return targetNode->insert(key, val);//直到叶节点才返回
    }

    bool search(const K &k, V &v) {
        const int index = locate_child_index(k);
        if (index < 0) return false;
        Node<K, V> *targeNode = child_[index];
        return targeNode->search(k, v);
        //最后在叶节点中搜索才能返回
    }

    //？
    bool locate_key(const K &k, Node<K, V>* &child, int &index) {
        index = locate_child_index(k);
        if (index < 0) return false;
        Node<K, V> *targeNode = child_[index];
        return targeNode->locate_key(k, child, index);
    }

    bool update(const K &k, const V &v) {
        return true;
    }

    bool delete_key(const K &k) {

    }

    bool delete_key(const K &k, bool &underflow) {
        //查找位于当前节点的哪个分支中
        int child_index = locate_child_index(k);
        if (child_index < 0)
            return false;

        Node<K, V> *child = child_[child_index];
        //一直深入到叶节点才开始返回
        bool deleted = child->delete_key(k, underflow);
        if (!deleted)//本来就不存在这个key，删除失败
            return false;

        //此处的underflow表示本节点的子节点是否溢出
        //子节点没有溢出，不会影响本节点的平衡
        //size_<2：即使溢出也没有办法合并和借节点【?】
        if (!underflow || size_ < 2)
            return true;

        //underflow
        Node<K, V> *left_child, *right_child;
        int left_child_index, right_child_index;
        int deleted_child_index = -1;
        K boundary;
        if (child_index >= 1) {
            //向左边的节点借/合并
            left_child_index = child_index - 1;
            right_child_index = child_index;
        } else {
            //向右边的节点借/合并
            left_child_index = child_index;
            right_child_index = child_index + 1;
        }
        left_child = child_[left_child_index];
        right_child = child_[right_child_index];


        // try to borrow an entry from the left. If no additional entry is available in the left, the two nodes will
        // be merged with the right one being deleted.
        bool merged = left_child->balance(right_child, boundary);

        //说明采用的是借的方法
        if (!merged) {
            // if borrowed (not merged), update the boundary
            //无论谁借谁，都是右边节点的boundary发生变化（boundary指示子节点的最小key
            key_[right_child_index] = boundary;
            underflow = false;//下溢的处理到此为止，不会再向上传递
            return true;
        }

        // merged

        // remove the reference to the deleted child, i.e., right_child
        // 右节点合并到左节点，所以要删除对右节点的应用
        // 用该节点之后的所有节点前移来覆盖
        for (int i = right_child_index; i < size_; ++i) {
            this->key_[i] = this->key_[i + 1];
            this->child_[i] = this->child_[i + 1];
        }
        //this->key_.erase(this->key_.begin() + right_child_index);
        //this->child_.erase(this->child_.begin() + right_child_index);
        --this->size_;
        //如果这个节点删除一个节点之后的数量小于下溢阈值
        //则下溢还会继续向上传递
        underflow = this->size_ < UNDERFLOW_BOUND(this->capacity_);
        return true;
    }

    //调用这个函数的可能是本身下溢的节点，也可能是被借的节点
    virtual bool balance(Node<K, V> *sibling_node, K &boundary) {
        const int underflow_bound = UNDERFLOW_BOUND(this->capacity_);
        InnerNode<K, V> *right = static_cast<InnerNode<K, V> *>(sibling_node);
        //调用这个函数的是本身就下溢的节点（也就是下溢节点是最左的情况
        if (this->size_ < underflow_bound) {
            if (right->size_ > underflow_bound) {
                // this node will borrow one child node from the right sibling node.
                //被借节点把最小子节点借过去
                this->key_[this->size_] = right->key_[0];
                this->child_[this->size_] = right->child_[0];
                //this->key_.insert(this->key_.begin() + this->size_, right->key_[0]);
                //this->child_.insert(this->child_.begin() + this->size_, right->child_[0]);
                ++this->size_;

                // remove the involved child in the right sibling node
                //删除最小子节点
                for (int i = 0; i < right->size_; ++i) {
                    right->key_[i] = right->key_[i + 1];
                    right->child_[i] = right->child_[i + 1];
                }
                //right->key_.erase(right->key_.begin());
                //right->child_.erase(right->child_.begin());
                --right->size_;

                // update the boundary
                //由于被借节点的最小子节点被借出，因此指向该节点的boundary也变化
                boundary = right->key_[0];
                return false;
            }
        }
        //否则，当前节点是要把子节点借给下溢节点的
        if (right->size_ < underflow_bound) {
            //说明可以借（左借给右
            if (this->size_ > underflow_bound) {
                // make an empty slot for the entry to borrow.
                for (int i = right->size_; i >= 0; --i) {//向后腾位置
                    right->key_[i + 1] = right->key_[i];
                    right->child_[i + 1] = right->child_[i];
                }
                ++right->size_;

                // copy the entry
                //腾出来的位置0存放借来的子节点
                right->key_[0] = this->key_[this->size_ - 1];
                right->child_[0] = this->child_[this->size_ - 1];

                //right->key_.insert(right->key_.begin(), this->key_[this->size_ - 1]);
                //right->child_.insert(right->child_.begin(), this->child_[this->size_ - 1]);
                --this->size_;

                // update the boundary
                //右边借来新节点，因此boundary（最小key）发生变化
                boundary = right->key_[0];
                return false;
            }
        }
        //说明没有多余的可借，需要合并
        //右合并到左
        for (int l = this->size_, r = 0; r < right->size_; ++l, ++r) {
            this->key_[l] = right->key_[r];
            this->child_[l] = right->child_[r];
            //this->key_.insert(this->key_.begin() + l, right->key_[r]);
            //this->child_.insert(this->child_.begin() + l, right->child_[r]);
        }
        this->size_ += right->size_;
        right->size_ = 0;
        //删除右边节点
        delete right;
        return true;
    }

    //插入一整个内部节点
    //什么情况会调用：子节点上溢，分裂，需要在父节点处添加新指针
    void insert_inner_node(Node<K, V> *innerNode, K boundary_key, int insert_position) {

        // make room for insertion.
        //腾位置
        for (int i = size_ - 1; i >= insert_position; --i) {
            key_[i + 1] = key_[i];
            child_[i + 1] = child_[i];
        }

        key_[insert_position] = boundary_key;
        child_[insert_position] = innerNode;

        //key_.insert(key_.begin() + insert_position, boundary_key);
        //child_.insert(child_.begin() + insert_position, innerNode);
        ++size_;
    }

    //返回true的时候，说明这个节点本身分裂了，待添加到上级的索引记录在split中
    bool insert_with_split_support(const K &key, const V &val, Split<K, V> &split) {
        const int target_node_index = locate_child_index(key);
        //true说明比当前最小的边界还小
        const bool exceed_left_boundary = target_node_index < 0;
        Split<K, V> local_split;

        // Insert into the target leaf node.
        bool is_split;
        //小于最左边界，需要修正整个B+树的索引
        if (exceed_left_boundary) {
            is_split = child_[0]->insert_with_split_support(key, val, local_split);
            //修改当前节点的最小边界
            key_[0] = key;
        } else {
            //插入选择好的子节点
            //is_split为true时，local_split就是要加入本节点的新索引
            is_split = child_[target_node_index]->insert_with_split_support(key, val, local_split);

        }

        // The tuple was inserted without causing leaf node split.
        //子节点没有上溢情况
        if (!is_split)
            return false;//false代表当前节点不用继续分裂

        // The leaf node was split.
        //子节点有分裂
        if (size_ < this->capacity_) {
            //当前节点还能新增指针，因此不用分裂
            insert_inner_node(local_split.right, local_split.boundary_key,
                              target_node_index + 1 + exceed_left_boundary);
                              //当exceed_left_boundary为1时，target_node_index=-1
            return false;
        }

        // leaf node was split but the current node is full. So we split the current node.
        //新节点的位置应该是target_node_index+1
        //满足以下条件，说明新节点的指针是放在当前节点的前半部分
        bool insert_to_first_half = target_node_index < (this->capacity_ / 2);

        //从start_index_for_right开始的指针要划为后半部分
        //std::cout<<"!!!!!!!!!"<<this->capacity_<<std::endl;
        int start_index_for_right = this->capacity_ / 2;
        InnerNode<K, V> *left = this;
        InnerNode<K, V> *right = new InnerNode<K, V>(this->capacity_);

        // move the keys and children to the right node
        //后半部分分裂到另一个节点
        for (int i = start_index_for_right, j = 0; i < size_; ++i, ++j) {
            right->key_[j] = key_[i];
            right->child_[j] = child_[i];
            //right->key_.insert(right->key_.begin() + j, this->key_[i]);
            //right->child_.insert(right->child_.begin() + j, this->child_[i]);
        }

        //从left移走了moved个节点
        const int moved = size_ - start_index_for_right;
        left->size_ -= moved;
        right->size_ = moved;

        // insert the new child node to the appropriate split node.
        InnerNode<K, V> *host_for_node = insert_to_first_half ? left : right;
        int inner_node_insert_position = host_for_node->locate_child_index(local_split.boundary_key);
        host_for_node->insert_inner_node(local_split.right, local_split.boundary_key, inner_node_insert_position + 1);

        // write the remaining content in the split data structure.
        split.left = left;
        split.right = right;
        split.boundary_key = right->get_leftmost_key();
        return true;
    }

    Node<K, V>* get_leftmost_leaf_node() {
        return child_[0]->get_leftmost_leaf_node();
    }

    std::string toString() const {
//        return std::to_string(this->id) + ": " + keys_to_string() + " " + nodes_to_string(); // for debug
        return keys_to_string() + " " + nodes_to_string();
    }

    std::string keys_to_string() const {
        std::stringstream ss;
        for (int i = 1; i < size_; ++i) {
            ss << key_[i];
            if (i < size_ - 1)
                ss << " ";
        }
        return ss.str();
    }

    std::string nodes_to_string() const {
        std::stringstream ss;
        for (int i = 0; i < size_; ++i) {
            ss << "[" << child_[i]->toString() << "]";
            if (i != size_ - 1) {
                ss << " ";
            }
        }
        return ss.str();
    }

    const K get_leftmost_key() const {
        return child_[0]->get_leftmost_key();
    }

    NodeType type() const {
        return INNER;
    }

    int size() const {
        return size_;
    }

    friend std::ostream &operator<<(std::ostream &os, InnerNode<K, V> const &m) {
        for (int i = 0; i < m.size_; ++i) {
            os << "[" << m.child_[i]->toString() << "]";
            if (i != m.size_ - 1)
                os << " ";
        }
        return os;
    }

protected:
    // Locate the node that might contain the particular key.
    int locate_child_index(K key) const {
        if (size_ == 0)
            return -1;
        int l = 0, r = size_ - 1;
        int m;
        bool found = false;
        while(l <= r) {
            m = (l + r) >> 1;
            if (key_[m] < key) {
                l = m + 1;
            } else if (key < key_[m]) {
                r = m - 1;
            } else {
                found = true;
                break;
            }
        }

        if (found) {
            return m;
        } else {
            //恰好小于key的boundary
            return l -  1;
        }
    }

    //std::vector<K> key_;
    //K key_[10]; // key_[0] is the smallest key for this inner node. The key boundaries start from index 1.
    K key_[100];
    //std::vector<Node<K,V>*> child_;
    Node<K, V> *child_[100];
    int size_;
};

#endif //B_TREE_INNER_NODE_H
