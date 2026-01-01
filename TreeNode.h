#ifndef TREE_NODE_H
#define TREE_NODE_H
#include <cstdlib>
#include <iostream>
template <typename T>
class TreeNode
{
public:
    TreeNode(T d);
    virtual ~TreeNode();
    T getData();
    TreeNode<T> *getLeft();
    TreeNode<T> *getRight();

    void setLeft(TreeNode<T> *left);   // Setter for left child
    void setRight(TreeNode<T> *right); // Setter for right child

    template <typename S>
    friend class LazyBST;

private:
    T m_data;
    TreeNode<T> *m_parent;
    TreeNode<T> *m_left;
    TreeNode<T> *m_right;
};

template <typename T>
TreeNode<T>::TreeNode(T d) : m_data(d), m_parent(NULL), m_left(NULL), m_right(NULL) {}

template <typename T>
TreeNode<T>::~TreeNode()
{
    // Safe delete to avoid double free if nodes are managed elsewhere
    delete m_left;
    m_left = NULL;
    delete m_right;
    m_right = NULL;
}

template <typename T>
T TreeNode<T>::getData()
{
    return m_data;
}

template <typename T>
TreeNode<T> *TreeNode<T>::getLeft()
{
    return m_left;
}

template <typename T>
TreeNode<T> *TreeNode<T>::getRight()
{
    return m_right;
}

template <typename T>
void TreeNode<T>::setLeft(TreeNode<T> *left)
{
    m_left = left;
}

template <typename T>
void TreeNode<T>::setRight(TreeNode<T> *right)
{
    m_right = right;
}

#endif
