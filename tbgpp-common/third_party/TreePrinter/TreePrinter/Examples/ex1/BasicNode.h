/*
 * MathNode.h
 *
 *  Created on: 23 ago. 2019
 *      Author: asens
 */

#ifndef BASICNODE_H_
#define BASICNODE_H_

#include <iostream>
#include <list>

class BasicNode {
public:
	BasicNode();
	BasicNode(std::string data);

	virtual ~BasicNode();

	void addChildren(BasicNode * nd);
	std::list<BasicNode *> getChildren();

	std::string getData();
	void setData(std::string data);

protected:
	void setParent(BasicNode *nd);

	std::string _data;

	std::list<BasicNode *> _children;
	BasicNode * _parent;
};

#endif /* BASICNODE_H_ */
