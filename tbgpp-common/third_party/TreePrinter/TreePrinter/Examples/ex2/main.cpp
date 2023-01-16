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
	BasicNode * head     = new BasicNode("me");
	BasicNode * childL   = new BasicNode("Paul");
	BasicNode * childCL  = new BasicNode("Anna");
	BasicNode * childCC  = new BasicNode("Sarah");
	BasicNode * childCR  = new BasicNode("Emma");
	BasicNode * childR   = new BasicNode("Tina");

	BasicNode * childLL  = new BasicNode("Kevin");
	BasicNode * childLR  = new BasicNode("Elloine");

	BasicNode * childCLC = new BasicNode("Anthony");

	BasicNode * childRL  = new BasicNode("Susan");
	BasicNode * childRR  = new BasicNode("Jason");

	head->addChildren(childL);
	head->addChildren(childCL);
	head->addChildren(childCC);
	head->addChildren(childCR);
	head->addChildren(childR);

	childCL->addChildren(childCLC);


	childL->addChildren(childLL);
	childL->addChildren(childLR);

	childR->addChildren(childRL);
	childR->addChildren(childRR);

	BTTree<BasicNode> printer(head, &BasicNode::getChildren, &BasicNode::getData);
	printer.print();

	delete head; // This deletes all subnodes
}

