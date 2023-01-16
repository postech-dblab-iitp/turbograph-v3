/*
 * BTNode.cpp
 *
 *  Created on: 5 sept. 2019
 *      Author: asens
 */

#include "BTNode.h"
#include <climits>

#define CALL_MEMBER_FN(object,ptrToMember)  ((object).*(ptrToMember))


template <class T>
typename BTNode<T>::childrenGetterFcn BTNode<T>::childrenGetter = nullptr;

template <class T>
typename BTNode<T>::dataGetterFcn     BTNode<T>::dataGetter = nullptr;

/** **************************************************
 * ***************** Static Adaptor ******************
 */ /*************************************************/
template <class T>
void BTNode<T>::initializeClass(childrenGetterFcn f1, dataGetterFcn f2)
{
	childrenGetter = f1;
	dataGetter     = f2;
}

template <class T>
std::list<T *> BTNode<T>::getChildren() {
	return CALL_MEMBER_FN(*_nd,childrenGetter)();
}

template <class T>
std::string BTNode<T>::getData() {
	return CALL_MEMBER_FN(*_nd,dataGetter)();
}


/** **************************************************
 * ****************** Printer class ******************
 */ /*************************************************/

/**
 * Node Constructor
 */
template <class T>
BTNode<T>::BTNode(T* node) {
	_nd = node;
	_depth = 0;

	_fcp = 0;
	_lcp = 0;
	_mcp = 0;

	_fbp = 0;
	_lbp = 0;

	_wself  = getData().length();
	_wchild = 0;
	_wblock = 0;

	// Create new list of nodes, imitating the structure of the passed node
	std::list<T *> ls = getChildren();
	for (auto & child: ls)
	{
		// Create unique pointer to a child
		std::shared_ptr<BTNode> newChild(new BTNode(child));
		// Move the unique pointer to the list (C++11 compatible)
		_children.push_back(std::move(newChild));
	}
}

/**
 * Destructor
 */
template <class T>
BTNode<T>::~BTNode() {
	
}

/**
 * Main Algorightm Function. Read comments for detail.
 */
template <class T>
void BTNode<T>::printTree(std::shared_ptr<BTNode<T>> thisHead) {
	// --- Preparation for the algorithm ---
	
	// Characterize main node and subnodes
	calcDepth(0); // NOTE: depth is how far down the top of the tree the node is (head is 0, its children are 1, etc)
	calcWidth();
	
	// Create a vector with the layers of the tree (layer 0 has the head, layer 1 has its children, ...)
	std::vector<std::unique_ptr<std::list<std::shared_ptr<BTNode>>>> levels;
	groupNodesByDepth(levels, thisHead);
	
	// Load the position information into the node (here's where the Magic bakes)
	assignPositionsToNodes(0);
	
	// --- Printing algorithm ---
	
	// Print line of nodes (head and some '_')
	printLine(0, levels);
	
	// Iteratively print an intermediate line (only '/', '\' and '|') followed by the next line of nodes
	for (std::size_t i = 1; i < levels.size(); i++)
	{
		printPreline(i, levels);
		printLine(i, levels);
	}
}

/** **************************************************
 * ***** Helper Functions for the Main Algorithm *****
 */ /*************************************************/

/**
 * Calculate Depth of Nodes (recursive). Assign the depth initialDepth to the node and
 * assignes a depth of initialDepth+1 to each one of the children.
 * Ideally, the head node hast depth of 0, and each row of children has increasing depth
 */
template <class T>
void BTNode<T>::calcDepth(std::size_t initialDepth)
{
	_depth = initialDepth;
	for (auto & child : _children)
	{
		child->calcDepth(initialDepth+1);
	}
}

/**
 * Calculates the max width of the node and its children (Recursive). It takes into account wether the combination
 * of children is bigger or smaller than the parent
 */
template <class T>
std::size_t BTNode<T>::calcWidth()
{
	if (_children.empty()) // If there are no children
	{
		_wchild = 0; // Width of the children is 0
	}
	else
	{
		_wchild += _children.front()->calcWidth();
		
		auto it = ++_children.begin();
		for (; it != _children.end(); ++it)
		{
			_wchild += 1;
			_wchild += (*it)->calcWidth();
		}
	}
	_wblock = std::max(_wself, _wchild);
	return _wblock;
}

/**
 * Groups nodes into a vector of layers. First layer is the thead
 * next layer are the head's direct children, etc.
 * It helps iterating through nodes at the same depth
 */
template <class T>
void BTNode<T>::groupNodesByDepth(std::vector<std::unique_ptr<std::list<std::shared_ptr<BTNode>>>> & levels, std::shared_ptr<BTNode<T>> thisHead) {
	std::list<std::shared_ptr<BTNode>> queue;
	std::shared_ptr<BTNode> nd;
	
	queue.push_back(thisHead);
	
	while (!queue.empty())
	{
		// Traverse
		nd = queue.front();
		queue.pop_front();
		for (auto & child : nd->_children)
		{
			queue.push_back(child);
		}
		// Add layers to the levels if it is not deep enough
		while (levels.size() <= nd->_depth)
		{
			// Create unique pointer to a new layer (that is: a new List of BTNode pointers)
			std::unique_ptr<std::list<std::shared_ptr<BTNode>>> layer(new std::list<std::shared_ptr<BTNode>>());
			// Move the unique pointer to the vector (C++11 compatible)
			levels.push_back(std::move(layer));
		}
		levels[nd->_depth]->push_back(nd);
	}
}

/**
 * It calculates future position to the nodes, to help place them
 * in one pass (Recursive)(Algorithm).
 */
template <class T>
std::size_t BTNode<T>::assignPositionsToNodes(std::size_t blockStart)
{
	// Block Data
	_fbp = blockStart;
	_lbp = blockStart + _wblock - 1;
	
	if (_wself >= _wchild) // if this node is longer than the children
	{
		// Do this node first
		_fcp = blockStart + (_wblock - _wself) / 2;
		_lcp = _fcp + _wself -1;
		_mcp = (_fcp + _lcp) / 2; // left aligning;
		
		// Then, do the children
		std::size_t childrenBlockStart = blockStart + (_wblock - _wchild) / 2;
		for (std::shared_ptr<BTNode> & nd : _children)
		{
			childrenBlockStart = nd->assignPositionsToNodes(childrenBlockStart);
		}
	}
	else // if the combination of children is longer than this node
	{
		// Do the children first
		std::size_t childrenBlockStart = blockStart + (_wblock - _wchild) / 2;
		for (std::shared_ptr<BTNode> & nd : _children)
		{
			childrenBlockStart = nd->assignPositionsToNodes(childrenBlockStart);
		}

		// Then, do this node
		_mcp = (_children.front()->_mcp + _children.back()->_mcp) / 2;
		_fcp = _mcp - (_wself - 1)  / 2; // left aligning;
		_lcp = _fcp + _wself -1;
	}
	
	// Return first character of the next block
	return blockStart + _wblock + 1;
}

/**
 * Print line of nodes. This also prints underscores to draw the branches
 */
template <class T>
void BTNode<T>::printLine(std::size_t ln, std::vector<std::unique_ptr<std::list<std::shared_ptr<BTNode>>>> & levels) {
	for (std::size_t c = 0; c < _wblock; c++)
	{
		if (CisParentFirstCharacter(c, levels[ln]))
		{
			c += printNodeStartingAt(c, levels[ln]) -1;
		}
		else if (CisBetweenChildrenAndParent(c, levels[ln]))
		{
			printUnderscore();
		}
		else
		{
			printSpace();
		}
	}
	std::cout << std::endl;
}

/**
 * Prints in the space between lines. This prints 3 simbols '/', '\', '|' and spaces.
 */
template <class T>
void BTNode<T>::printPreline(std::size_t ln, std::vector<std::unique_ptr<std::list<std::shared_ptr<BTNode>>>> & levels) {
	for (std::size_t c = 0; c < _wblock; c++)
	{
		if (CisCenterChildren(c, levels[ln]))
		{
			switch (parentPositionRelToChildren(c, levels[ln-1], levels[ln]))
			{
				case -1:
					printLeftChildren();
					break;
				case 0:
					printVerticalChildren();
					break;
				case 1:
					printRightChildren();
					break;
			}
		}
		else
		{
			printSpace();
		}
	}
	std::cout << std::endl;
}

/**
 * Return true if this is the first character of a node
 * TODO: add in-out (by ref) parameter to function for the parent node (to avoid iteration if true)
 */
template <class T>
bool BTNode<T>::CisParentFirstCharacter(std::size_t c, std::unique_ptr<std::list<std::shared_ptr<BTNode>>> & level)
{
	for (std::shared_ptr<BTNode> & nd : *level)
	{
		if (nd->_fcp == c)
			return true;
	}
	return false;
}

/**
 * Print the node starting at c in the layer passed and return the number of characters written.
 */
template <class T>
std::size_t BTNode<T>::printNodeStartingAt(std::size_t c, std::unique_ptr<std::list<std::shared_ptr<BTNode>>> & level)
{
	// Find corresponding node
	for (std::shared_ptr<BTNode> & nd : * level)
	{
		if (nd->_fcp == c)
		{
			// Return the number of characters printed
			return nd->printNodeData();
		}
	}
	return 0; // TODO: this should be an error
}

/**
 * Check if the character in position c is between a parent node and his childs center character.
 * If so, an underscore mus be printed
 */
template <class T>
bool BTNode<T>::CisBetweenChildrenAndParent(std::size_t c, std::unique_ptr<std::list<std::shared_ptr<BTNode>>> & parent)
{
	for (std::shared_ptr<BTNode> & nd : *parent)
	{
		if (nd->_children.empty())
		{
			continue;
		}
		std::size_t minCp = nd->_children.front()->_mcp;
		std::size_t maxCp = nd->_children.back( )->_mcp;
		if (c > minCp && c < maxCp)
			return true;
	}
	return false;
}

/**
 * Check if the character in position c is on top of a childen's center character.
 * If so, a vertical or diagonal bar (/\|) must be printed
 * TODO: add in-out (by ref) children
 */
template <class T>
bool BTNode<T>::CisCenterChildren(std::size_t c, std::unique_ptr<std::list<std::shared_ptr<BTNode>>> & children)
{
	for (std::shared_ptr<BTNode> & nd : * children)
	{
		if (nd->_mcp == c)
			return true;
	}
	return false;
}

/**
 * Determine if the character in position c joins the children underneath to its parent by means
 * of a vertical bar '|', a slash '/', or a backslash '\'. 
 */
template <class T>
int BTNode<T>::parentPositionRelToChildren(std::size_t c, std::unique_ptr<std::list<std::shared_ptr<BTNode>>> & parent, std::unique_ptr<std::list<std::shared_ptr<BTNode>>> & children)
{
	std::shared_ptr<BTNode> dad = nullptr;
	std::shared_ptr<BTNode> kid = nullptr;
	for (std::shared_ptr<BTNode> & nd : *parent) // Find Parent
	{
		if ((c >= nd->_fbp) && (c <= nd->_lbp))
		{
			dad = nd;
		}
	}
	for (auto & nd : *children) // Find children
	{
		if ((c >= nd->_fbp) && (c <= nd->_lbp))
		{
			kid = nd;
		}
	}
	// relative position of the kid w/r to the dad
	if (kid->_mcp < dad->_mcp)
		return -1;
	else if (kid->_mcp == dad->_mcp)
		return 0;
	else
		return 1;
}

/**
 * Print the node data through the imported function and return its length
 */
template <class T>
std::size_t BTNode<T>::printNodeData() {
	std::cout << getData();
	return _wself; // Return the number of characters printed
}

/*
 * Print functions
 */

template <class T>
void BTNode<T>::printSpace() {
	std::cout << " ";
}

template <class T>
void BTNode<T>::printUnderscore() {
	std::cout << "_";
}

template <class T>
void BTNode<T>::printLeftChildren() {
	std::cout << "/";
}

template <class T>
void BTNode<T>::printRightChildren() {
	std::cout << "\\";
}

template <class T>
void BTNode<T>::printVerticalChildren() {
	std::cout << "|";
}


/**
 * Tree Class Functions
 */
template <class T>
BTTree<T>::BTTree(T* head, childrenGetterFcn f1, dataGetterFcn f2)
{
	// Configure the Node class with the getter functions
	BTNode<T>::initializeClass(f1, f2);
	// Initialize the smart pointer with a new BTNode (head) 
	_head = std::shared_ptr<BTNode<T>>(new BTNode<T>(head));
}

template <class T>
void BTTree<T>::print()
{
	// Consider using the std::shared_ptr_of_this()
	_head->printTree(_head);
}
