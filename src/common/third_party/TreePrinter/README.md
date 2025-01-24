# TreePrinter
C++ Console Printer for Trees with Any Number of Children

## History
After searching a little bit on the Internet and not finding anything that would do the job I decided to do it my self (and later decided to share it with the community).

I hope there is not any flagrant errors on my code (I am Aerospace major, not CS, altough I took some CS classes).

## Idea
To create a superclass that embraces the head node of a tree (where nodes can have an arbitrary number of children).

By doing so, it enables to print the whole tree on the console without messing the original node class.

It uses a templated class and a set-up function to avoid the hustle of diving into the internals.

## How to use it
### Step 1
Copy the BTNode Class, from the `src` folder to your project folder.

### Step 2
Your node class needs 3 things. You most likely have them already, or it is trivial to add them if you are building this kind of tree.
1. A pointer to the main node of your tree (aka `YourClass * ptrToHeadNodeOfYourTree`)
2. A _getChildren_ member function of your node class that
    1. takes no parameters, and
    2. returns a standard list of pointer to its children (aka `std::list<YourClass *> YourClass::youGetChildrenFunction()`)
3. A _getData_ member function of your node class that
    1. takes no parameters, and
    2. returns a standard string of its content (aka `std::string YourClass::yourGetStringFunction()`)

### Step 3
Initialize the templated class in *your code* with a pointer to the main node of your tree, and the two functions above:
```cpp
BTTree<YourClass> printer(ptrToHeadNodeOfYourTree, &YourClass::youGetChildrenFunction, &YourClass::yourGetStringFunction);
```

### Step 4
Print the tree:
```cpp
printer.print();
```

## Results
Here are the output of the examples provided.
Example 1:
```text
  __+__    
 /     \   
 *   __*__ 
/ \ /  |  \
a b c  ^  d
      / \  
      e f 
```
Example 2
```text
      _______________me_______________
     /           /      \    \        \
   _Paul_       Anna  Sarah Emma    _Tina
  /      \       |                 /     \
Kevin Elloine Anthony            Susan Jason
```
Example 3
```text
                     __________________________________me__________________________________        
                    /                                   \          \        \              \       
   ________________Paul_______________              Mary Jane    Sarah     Emma     Bartholomew III
  /      /                            \                 |                   |           /     \    
Kevin Antoine                 _____Elloine_____      Anthony              _Eva__      Susan Jason  
                             /                 \    /       \            /      \                  
                  _________Hector_________    Tom Edison Chaplin       Oliver Harry                
                 /       /        \       \                                                        
              Charlie Jennifer Margaret Megan                                                      
```
