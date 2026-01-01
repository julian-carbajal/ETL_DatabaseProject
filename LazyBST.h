/**
 * @file LazyBST.h
 * @brief Template class for a Binary Search Tree.
 * 
 * ARCHITECTURE:
 *   TreeNode - Individual node in the tree
 *       ^
 *       |
 *   LazyBST (You are here) - Binary Search Tree operations
 *       ^
 *       |
 *   DBsystem - Uses two LazyBSTs (students and faculty)
 *       ^
 *       |
 *   main.cpp - Entry point
 * 
 * BST OPERATIONS:
 * - insert: O(log n) average, O(n) worst
 * - search: O(log n) average, O(n) worst
 * - remove: O(log n) average, O(n) worst
 * - printInOrder: O(n) - prints sorted order
 * 
 * @author Julian Carbajal
 * @date Spring 2024
 */

#ifndef LazyBST_H
#define LazyBST_H

#include "TreeNode.h"

/**
 * @class LazyBST
 * @brief A Binary Search Tree for storing and retrieving data.
 * @tparam T The type of data stored (must support comparison operators).
 */
template <typename T>
class LazyBST
{
public:
    /** @brief Default constructor - creates empty tree. */
    LazyBST();
    
    /** @brief Virtual destructor. */
    virtual ~LazyBST();
    
    /** @brief Iterative search. @param d Data to find. @return True if found. */
    bool iterContains(T d);
    
    /** @brief Recursive search. @param d Data to find. @return True if found. */
    bool contains(T d);
    
    /** @brief Print all elements in sorted order. */
    void printInOrder();
    
    /** @brief Print all elements in post-order. */
    void printTreePostOrder();
    
    /** @brief Insert data into tree. @param d Data to insert. */
    void insert(T d);
    
    /** @brief Get number of elements. @return Tree size. */
    int size();
    
    /** @brief Get maximum element. @return Max value. */
    T max();
    
    /** @brief Get minimum element. @return Min value. */
    T min();
    
    /** @brief Get median element (root). @return Root value. */
    T median();
    
    /** @brief Remove element from tree. @param d Data to remove. */
    void remove(T d);
    
    /** @brief Search and return pointer to data. @param key Data to find. @return Pointer to data or NULL. */
    T* search(T key);

private:
    TreeNode<T> *m_root;  ///< Root node of the tree
    int m_size;           ///< Number of elements
    
    bool recContainsHelper(TreeNode<T> *n, T d);
    void printIOHelper(TreeNode<T> *n);
    void printTreePostOrderHelper(TreeNode<T> *subTreeRoot);
    void insertHelper(TreeNode<T> *&subTreeRoot, T &d);
    T getMaxHelper(TreeNode<T> *n);
    T getMinHelper(TreeNode<T> *n);
    void findTarget(T key, TreeNode<T> *&target, TreeNode<T> *&parent);
    TreeNode<T> *getSuccessor(TreeNode<T> *rightChild);
};

template <typename T>
LazyBST<T>::LazyBST()
{
    m_root = NULL;
    m_size = 0;
}

template <typename T>
LazyBST<T>::~LazyBST()
{
}

template <typename T>
bool LazyBST<T>::iterContains(T d)
{
    if (m_root == NULL)
    {
        return false;
    }
    if (m_root->m_data == d)
    {
        return true;
    }

    bool found = false;
    TreeNode<T> *current = m_root;
    while (!found)
    {
        if (d > current->m_data)
        {
            current = current->m_right;
        }
        else
        {
            current = current->m_left;
        }
        if (current == NULL)
        {
            found = false;
            break;
        }
        if (current->m_data == d)
        {
            found = true;
        }
    }
    return found;
}

template <typename T>
bool LazyBST<T>::contains(T d)
{
    return recContainsHelper(m_root, d);
}

template <typename T>
bool LazyBST<T>::recContainsHelper(TreeNode<T> *n, T d)
{
    if (n == NULL)
    {
        return false;
    }
    if (n->m_data == d)
    {
        return true;
    }
    if (d > n->m_data)
    {
        return recContainsHelper(n->m_right, d);
    }
    else
    {
        return recContainsHelper(n->m_left, d);
    }
}

template <typename T>
void LazyBST<T>::printInOrder()
{
    printIOHelper(m_root);
}

template <typename T>
void LazyBST<T>::printIOHelper(TreeNode<T> *n)
{
    if (n != NULL)
    {
        printIOHelper(n->m_left);
        std::cout << n->m_data << std::endl;
        printIOHelper(n->m_right);
    }
}

template <typename T>
void LazyBST<T>::printTreePostOrder()
{
    printTreePostOrderHelper(m_root);
}

template <typename T>
void LazyBST<T>::printTreePostOrderHelper(TreeNode<T> *subTreeRoot)
{
    if (subTreeRoot != NULL)
    {
        printTreePostOrderHelper(subTreeRoot->m_left);
        printTreePostOrderHelper(subTreeRoot->m_right);
        std::cout << subTreeRoot->m_data << std::endl;
    }
}

template <typename T>
void LazyBST<T>::insert(T d)
{
    insertHelper(m_root, d);
    ++m_size;
}

template <typename T>
void LazyBST<T>::insertHelper(TreeNode<T> *&subTreeRoot, T &d)
{
    if (subTreeRoot == NULL)
    {
        subTreeRoot = new TreeNode<T>(d);
    }
    else if (d > subTreeRoot->m_data)
    {
        insertHelper(subTreeRoot->m_right, d);
    }
    else
    {
        insertHelper(subTreeRoot->m_left, d);
    }
}

template <typename T>
int LazyBST<T>::size()
{
    return m_size;
}

template <typename T>
T LazyBST<T>::max()
{
    return getMaxHelper(m_root);
}

template <typename T>
T LazyBST<T>::getMaxHelper(TreeNode<T> *n)
{
    if (n->m_right == NULL)
    {
        return n->m_data;
    }
    else
    {
        return getMaxHelper(n->m_right);
    }
}

template <typename T>
T LazyBST<T>::min()
{
    return getMinHelper(m_root);
}

template <typename T>
T LazyBST<T>::getMinHelper(TreeNode<T> *n)
{
    if (n->m_left == NULL)
    {
        return n->m_data;
    }
    else
    {
        return getMinHelper(n->m_left);
    }
}

template <typename T>
T LazyBST<T>::median()
{
    return m_root->m_data;
}

template <typename T>
void LazyBST<T>::findTarget(T key, TreeNode<T> *&target, TreeNode<T> *&parent)
{
    while (target != NULL && target->m_data != key)
    {
        parent = target;
        if (key < target->m_data)
        {
            target = target->m_left;
        }
        else
        {
            target = target->m_right;
        }
    }
}

template <typename T>
TreeNode<T> *LazyBST<T>::getSuccessor(TreeNode<T> *rightChild)
{
    while (rightChild->m_left != NULL)
    {
        rightChild = rightChild->m_left;
    }
    return rightChild;
}

template <typename T>
void LazyBST<T>::remove(T d)
{
    TreeNode<T> *target = NULL;
    TreeNode<T> *parent = NULL;
    target = m_root;

    findTarget(d, target, parent);

    if (target == NULL)
    {
        return;
    }

    if (target->m_left == NULL && target->m_right == NULL)
    {
        if (target == m_root)
        {
            m_root = NULL;
        }
        else
        {
            if (target == parent->m_left)
            {
                parent->m_left = NULL;
            }
            else
            {
                parent->m_right = NULL;
            }
        }
        delete target;
        --m_size;
    }
    else if (target->m_left != NULL && target->m_right != NULL)
    {
        TreeNode<T> *suc = getSuccessor(target->m_right);
        T value = suc->m_data;
        remove(value);
        target->m_data = value;
    }
    else
    {
        TreeNode<T> *child;
        if (target->m_left != NULL)
        {
            child = target->m_left;
        }
        else
        {
            child = target->m_right;
        }

        if (target == m_root)
        {
            m_root = child;
        }
        else
        {
            if (target == parent->m_left)
            {
                parent->m_left = child;
            }
            else
            {
                parent->m_right = child;
            }
        }
        target->m_left = NULL;
        target->m_right = NULL;

        delete target;
        --m_size;
    }
}

template <typename T>
T* LazyBST<T>::search(T key)
{
    TreeNode<T> *current = m_root;
    while (current != NULL)
    {
        if (key < current->m_data)
        {
            current = current->m_left;
        }
        else if (key > current->m_data)
        {
            current = current->m_right;
        }
        else
        {
            return &(current->m_data); // Return a pointer to the found data
        }
    }
    return NULL; // Return NULL if the key is not found
}

#endif