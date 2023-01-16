/*
 * main.cpp
 *
 *  Created on: 23 ago. 2019
 *      Author: asens
 */

#include <iostream>
#include "BasicNode.h"
#include "BTNode.h"


int main() {
	BasicNode * head     = new BasicNode("+");
	BasicNode * childL   = new BasicNode("*");
	BasicNode * childR   = new BasicNode("*");

	BasicNode * childLL  = new BasicNode("a");
	BasicNode * childLR  = new BasicNode("b");

	BasicNode * childRL  = new BasicNode("c");
	BasicNode * childRC  = new BasicNode("^");
	BasicNode * childRR  = new BasicNode("d");

	BasicNode * childRCL = new BasicNode("e");
	BasicNode * childRCC = new BasicNode("f");

	head->addChildren(childL);
	head->addChildren(childR);

	childL->addChildren(childLL);
	childL->addChildren(childLR);

	childR->addChildren(childRL);
	childR->addChildren(childRC);
	childR->addChildren(childRR);

	childRC->addChildren(childRCL);
	childRC->addChildren(childRCC);

	BTTree<BasicNode> printer(head, &BasicNode::getChildren, &BasicNode::getData);
	printer.print();

	delete head; // This deletes all subnodes
}

