# include gporca library (managed by separate cmakefile)
include_directories(src/backend/gporca)

# add libraries
# add_library(gpdb STATIC)
# target_include_directories(gpdb PUBLIC 
# 	# gpdb libraries
# 	"src/include/gpopt"
# 	# gporca
# 	"src/backend/gporca/libgpdbcost/include"
# 	"src/backend/gporca/libgpopt/include"
# 	"src/backend/gporca/libgpos/include"
# 	"src/backend/gporca/libnaucrates/include"
# )

# # add sources - exclude gporca/ from sources.
# file (GLOB_RECURSE SOURCES "src/backend/gpopt/*.c*")
# target_sources(gpdb PUBLIC ${SOURCES})

# target_link_libraries(gpdb PUBLIC gporca)