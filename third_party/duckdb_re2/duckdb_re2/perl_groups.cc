// GENERATED BY make_perl_groups.pl; DO NOT EDIT.
// make_perl_groups.pl >perl_groups.cc

#include "duckdb_re2/unicode_groups.h"

namespace duckdb_re2 {

static const URange16 code1[] = {  /* \d */
	{ 0x30, 0x39 },
};
static const URange16 code2[] = {  /* \s */
	{ 0x9, 0xa },
	{ 0xc, 0xd },
	{ 0x20, 0x20 },
};
static const URange16 code3[] = {  /* \w */
	{ 0x30, 0x39 },
	{ 0x41, 0x5a },
	{ 0x5f, 0x5f },
	{ 0x61, 0x7a },
};
const UGroup perl_groups[] = {
	{ "\\d", +1, code1, 1 , nullptr, 0},
	{ "\\D", -1, code1, 1 , nullptr, 0},
	{ "\\s", +1, code2, 3 , nullptr, 0},
	{ "\\S", -1, code2, 3 , nullptr, 0},
	{ "\\w", +1, code3, 4 , nullptr, 0},
	{ "\\W", -1, code3, 4 , nullptr, 0},
};
const int num_perl_groups = 6;
static const URange16 code4[] = {  /* [:alnum:] */
	{ 0x30, 0x39 },
	{ 0x41, 0x5a },
	{ 0x61, 0x7a },
};
static const URange16 code5[] = {  /* [:alpha:] */
	{ 0x41, 0x5a },
	{ 0x61, 0x7a },
};
static const URange16 code6[] = {  /* [:ascii:] */
	{ 0x0, 0x7f },
};
static const URange16 code7[] = {  /* [:blank:] */
	{ 0x9, 0x9 },
	{ 0x20, 0x20 },
};
static const URange16 code8[] = {  /* [:cntrl:] */
	{ 0x0, 0x1f },
	{ 0x7f, 0x7f },
};
static const URange16 code9[] = {  /* [:digit:] */
	{ 0x30, 0x39 },
};
static const URange16 code10[] = {  /* [:graph:] */
	{ 0x21, 0x7e },
};
static const URange16 code11[] = {  /* [:lower:] */
	{ 0x61, 0x7a },
};
static const URange16 code12[] = {  /* [:print:] */
	{ 0x20, 0x7e },
};
static const URange16 code13[] = {  /* [:punct:] */
	{ 0x21, 0x2f },
	{ 0x3a, 0x40 },
	{ 0x5b, 0x60 },
	{ 0x7b, 0x7e },
};
static const URange16 code14[] = {  /* [:space:] */
	{ 0x9, 0xd },
	{ 0x20, 0x20 },
};
static const URange16 code15[] = {  /* [:upper:] */
	{ 0x41, 0x5a },
};
static const URange16 code16[] = {  /* [:word:] */
	{ 0x30, 0x39 },
	{ 0x41, 0x5a },
	{ 0x5f, 0x5f },
	{ 0x61, 0x7a },
};
static const URange16 code17[] = {  /* [:xdigit:] */
	{ 0x30, 0x39 },
	{ 0x41, 0x46 },
	{ 0x61, 0x66 },
};
const UGroup posix_groups[] = {
	{ "[:alnum:]", +1, code4, 3 , nullptr, 0},
	{ "[:^alnum:]", -1, code4, 3 , nullptr, 0},
	{ "[:alpha:]", +1, code5, 2 , nullptr, 0},
	{ "[:^alpha:]", -1, code5, 2 , nullptr, 0},
	{ "[:ascii:]", +1, code6, 1 , nullptr, 0},
	{ "[:^ascii:]", -1, code6, 1 , nullptr, 0},
	{ "[:blank:]", +1, code7, 2 , nullptr, 0},
	{ "[:^blank:]", -1, code7, 2 , nullptr, 0},
	{ "[:cntrl:]", +1, code8, 2 , nullptr, 0},
	{ "[:^cntrl:]", -1, code8, 2 , nullptr, 0},
	{ "[:digit:]", +1, code9, 1 , nullptr, 0},
	{ "[:^digit:]", -1, code9, 1 , nullptr, 0},
	{ "[:graph:]", +1, code10, 1 , nullptr, 0},
	{ "[:^graph:]", -1, code10, 1 , nullptr, 0},
	{ "[:lower:]", +1, code11, 1 , nullptr, 0},
	{ "[:^lower:]", -1, code11, 1 , nullptr, 0},
	{ "[:print:]", +1, code12, 1 , nullptr, 0},
	{ "[:^print:]", -1, code12, 1 , nullptr, 0},
	{ "[:punct:]", +1, code13, 4 , nullptr, 0},
	{ "[:^punct:]", -1, code13, 4 , nullptr, 0},
	{ "[:space:]", +1, code14, 2 , nullptr, 0},
	{ "[:^space:]", -1, code14, 2 , nullptr, 0},
	{ "[:upper:]", +1, code15, 1 , nullptr, 0},
	{ "[:^upper:]", -1, code15, 1, nullptr, 0},
	{ "[:word:]", +1, code16, 4 , nullptr, 0},
	{ "[:^word:]", -1, code16, 4 , nullptr, 0},
	{ "[:xdigit:]", +1, code17, 3 , nullptr, 0},
	{ "[:^xdigit:]", -1, code17, 3 , nullptr, 0},
};
const int num_posix_groups = 28;

}  // namespace duckdb_re2
