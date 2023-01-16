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
	BasicNode * childCL  = new BasicNode("Mary Jane");
	BasicNode * childCC  = new BasicNode("Sarah");
	BasicNode * childCR  = new BasicNode("Emma");
	BasicNode * childR   = new BasicNode("Bartholomew III");

	BasicNode * childLL  = new BasicNode("Kevin");
	BasicNode * childLC  = new BasicNode("Antoine");
	BasicNode * childLR  = new BasicNode("Elloine");

	BasicNode * childLRL  = new BasicNode("Hector");
	BasicNode * childLRR  = new BasicNode("Tom");

	BasicNode * childLRLL   = new BasicNode("Charlie");
	BasicNode * childLRLCL  = new BasicNode("Jennifer");
	BasicNode * childLRLCR  = new BasicNode("Margaret");
	BasicNode * childLRLR   = new BasicNode("Megan");

	BasicNode * childCLC = new BasicNode("Anthony");

	BasicNode * childCLCL = new BasicNode("Edison");
	BasicNode * childCLCR = new BasicNode("Chaplin");

	BasicNode * childCRC  = new BasicNode("Eva");

	BasicNode * childCRCL  = new BasicNode("Oliver");
	BasicNode * childCRCR  = new BasicNode("Harry");

	BasicNode * childRL  = new BasicNode("Susan");
	BasicNode * childRR  = new BasicNode("Jason");

	head->addChildren(childL);
	head->addChildren(childCL);
	head->addChildren(childCC);
	head->addChildren(childCR);
	head->addChildren(childR);

	childCL->addChildren(childCLC);

	childCLC->addChildren(childCLCL);
	childCLC->addChildren(childCLCR);

	childCR->addChildren(childCRC);

	childCRC->addChildren(childCRCL);
	childCRC->addChildren(childCRCR);

	childL->addChildren(childLL);
	childL->addChildren(childLC);
	childL->addChildren(childLR);

	childLR->addChildren(childLRL);
	childLR->addChildren(childLRR);

	childLRL->addChildren(childLRLL);
	childLRL->addChildren(childLRLCL);
	childLRL->addChildren(childLRLCR);
	childLRL->addChildren(childLRLR);

	childR->addChildren(childRL);
	childR->addChildren(childRR);

	BTTree<BasicNode> printer(head, &BasicNode::getChildren, &BasicNode::getData);
	printer.print();

	delete head; // This deletes all subnodes
}

