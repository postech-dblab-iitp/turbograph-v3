# include gporca library (managed by separate cmakefile)
include_directories(src/backend/gporca)

# add libraries
add_library(gpdb STATIC)
target_include_directories(gpdb PUBLIC 
	# gpdb libraries
	"src/include"
	"src/include/cdb"
	"src/include/gpopt"
	# gporca
	"src/backend/gporca/libgpdbcost/include"
	"src/backend/gporca/libgpopt/include"
	"src/backend/gporca/libgpos/include"
	"src/backend/gporca/libnaucrates/include"
	# s62-graph-store
	"${PROJECT_SOURCE_DIR}/../s62-graph-store/include"
)

# # add sources - exclude gporca/ from sources.
file (GLOB_RECURSE SOURCES "src/backend/gpopt/*.c*" "src/backend/nodes/*.c*" "src/backend/utils/*.c*")
target_sources(gpdb PUBLIC ${SOURCES})

target_link_libraries(gpdb PUBLIC gpos naucrates gpdbcost gpopt s62-main s62-common-lib s62-graph-store-lib)
