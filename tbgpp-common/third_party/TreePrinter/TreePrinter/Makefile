EX1 = exam1
EX2 = exam2
EX3 = exam3

EX_LIST = $(EX1) $(EX2) $(EX3)

SRC_DIR = src

EX1_SRC_DIR = Examples/ex1
EX2_SRC_DIR = Examples/ex2
EX3_SRC_DIR = Examples/ex3

EX1_OBJS_DIR = .objs1
EX2_OBJS_DIR = .objs2
EX3_OBJS_DIR = .objs3

EX1_OBJS = BasicNode.o main.o
EX2_OBJS = BasicNode.o main.o
EX3_OBJS = BasicNode.o main.o

CXX = g++
LD = g++

WARNINGS = -pedantic -Wall -Werror -Wfatal-errors -Wextra -Wno-unused-parameter -Wno-unused-variable
CXXFLAGS = -std=c++1y -g3 -O0 $(WARNINGS) -c -fmessage-length=0
INCLUDES = -I./$(SRC_DIR)
LDFLAGS = -std=c++1y 

	
all: $(EX1) $(EX2) $(EX3)

# Pattern rules for each example's object files
$(EX1_OBJS_DIR)/%.o: $(EX1_SRC_DIR)/%.cpp | $(EX1_OBJS_DIR)
	$(CXX) $(CXXFLAGS) $(INCLUDES) -I./$(EX1_SRC_DIR) $< -o $@

$(EX2_OBJS_DIR)/%.o:  $(EX2_SRC_DIR)/%.cpp | $(EX2_OBJS_DIR)
	$(CXX) $(CXXFLAGS) $(INCLUDES) -I./$(EX2_SRC_DIR) $< -o $@

$(EX3_OBJS_DIR)/%.o:  $(EX3_SRC_DIR)/%.cpp | $(EX3_OBJS_DIR)
	$(CXX) $(CXXFLAGS) $(INCLUDES) -I./$(EX3_SRC_DIR) $< -o $@

# Create directories for each example files
$(EX1_OBJS_DIR):
	@mkdir -p $(EX1_OBJS_DIR)
	
$(EX2_OBJS_DIR):
	@mkdir -p $(EX2_OBJS_DIR)
	
$(EX3_OBJS_DIR):
	@mkdir -p $(EX3_OBJS_DIR)


# Compile Example 1
$(EX1):
	$(LD) $(LDFLAGS) $^ -o $@

$(EX1): $(patsubst %.o, $(EX1_OBJS_DIR)/%.o, $(EX1_OBJS))


$(EX2):
	$(LD) $(LDFLAGS) $^ -o $@

$(EX2): $(patsubst %.o, $(EX2_OBJS_DIR)/%.o, $(EX2_OBJS))


$(EX3):
	$(LD) $(LDFLAGS) $^ -o $@

$(EX3): $(patsubst %.o, $(EX3_OBJS_DIR)/%.o, $(EX3_OBJS))

.PHONY: clean

clean:
	rm -rf $(EX_LIST) $(EX1_OBJS_DIR) $(EX2_OBJS_DIR) $(EX3_OBJS_DIR)
