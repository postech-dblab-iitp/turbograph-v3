/*
 * BTNode.h
 *
 *  Created on: 5 sept. 2019
 *      Author: asens
 *
 *  Known bugs:
 *    * If the parent node is longer than the combination of the children, it will crash
 *
 *  Minor problems:
 *    * Placement of '/' feels skewed to the left on words with even characters to the left of its parent
 */

#ifndef SRC_BTNODE_H_
#define SRC_BTNODE_H_

#include <string>
#include <list>
#include <vector>
#include <memory>

// Node class
template <class T>
class BTNode
{
  /**
   * Static Adaptor
   */
  public:
	// Typedefs for the getter functions
	typedef std::list<T*> (T::*childrenGetterFcn)( void);
	typedef std::string   (T::*dataGetterFcn)    ( void);
	static void initializeClass(childrenGetterFcn f1, dataGetterFcn f2);

  private:
	static childrenGetterFcn childrenGetter;
	static dataGetterFcn	 dataGetter;

	std::list<T *> getChildren();
	std::string    getData();

  /**
   * Printer class
   */
  public:
	BTNode(T* node);

	virtual ~BTNode();

	void printTree(std::shared_ptr<BTNode<T>> thisHead);

  private:
	T * _nd;
	std::list<std::shared_ptr<BTNode>> _children;
	std::size_t _depth;

	std::size_t _wself;
	std::size_t _wchild;
	std::size_t _wblock;

	std::size_t _fcp; // First  Character Position
	std::size_t _lcp; // Last   Character Position
	std::size_t _mcp; // Middle Character Position
	std::size_t _fbp; // First  Block     Position
	std::size_t _lbp; // Last   Block     Position

	void calcDepth(std::size_t initialDepth);
	std::size_t calcWidth();

	void groupNodesByDepth(std::vector<std::unique_ptr<std::list<std::shared_ptr<BTNode>>>> & levels, std::shared_ptr<BTNode<T>> thisHead);
	std::size_t assignPositionsToNodes(std::size_t blockStart);
	
	void printLine(std::size_t ln,    std::vector<std::unique_ptr<std::list<std::shared_ptr<BTNode>>>> & levels);
	void printPreline(std::size_t ln, std::vector<std::unique_ptr<std::list<std::shared_ptr<BTNode>>>> & levels);
	
	std::size_t printNodeStartingAt(std::size_t c, std::unique_ptr<std::list<std::shared_ptr<BTNode>>> & level);
	std::size_t printNodeData();
	
	bool CisParentFirstCharacter(std::size_t c, std::unique_ptr<std::list<std::shared_ptr<BTNode>>> & level);
	bool CisBetweenChildrenAndParent(std::size_t c, std::unique_ptr<std::list<std::shared_ptr<BTNode>>> & parent);
	bool CisCenterChildren(std::size_t c, std::unique_ptr<std::list<std::shared_ptr<BTNode>>> & children);
	int parentPositionRelToChildren(std::size_t c, std::unique_ptr<std::list<std::shared_ptr<BTNode>>> & parent, std::unique_ptr<std::list<std::shared_ptr<BTNode>>> & children);
	
	
	void printSpace();
	void printUnderscore();
	void printLeftChildren();
	void printRightChildren();
	void printVerticalChildren();

};

// Tree Class
template <class T>
class BTTree
{
  public:
	// Typedefs for the getter functions
	typedef std::list<T*> (T::*childrenGetterFcn)( void);
	typedef std::string   (T::*dataGetterFcn)    ( void);
	// Constructor
	BTTree(T* head, childrenGetterFcn f1, dataGetterFcn f2);
	void print();

  private:
	std::shared_ptr<BTNode<T>> _head;
};

#include "BTNode.tpp"
#endif /* SRC_BTNODE_H_ */
