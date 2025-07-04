export SHELL=bash
CHECK_TERARK_FSA_LIB_UPDATE ?= 1
ROCKSDB_HOME ?= ../../../..
ifneq (,$(wildcard ../../../topling-core))
TERARK_HOME ?= ../../../topling-core
else ifneq (,$(wildcard ../../../topling-zip))
TERARK_HOME ?= ../../../topling-zip
else
$(error "Fatal: not found topling-zip or topling-core")
endif
BOOST_INC ?= -I${TERARK_HOME}/boost-include
MARCH ?= $(shell uname -m)
ifeq "${MARCH}" "x86_64"
WITH_BMI2 ?= $(shell ${TERARK_HOME}/cpu_has_bmi2.sh)
else
# not available
WITH_BMI2 ?= na
endif

CURRENT_MAKEFILE := $(lastword $(MAKEFILE_LIST))

ifeq "$(origin CXX)" "default"
  ifeq "$(shell test -e /opt/bin/g++ && echo 1)" "1"
    CXX := /opt/bin/g++
  else
    ifeq "$(shell test -e ${HOME}/opt/bin/g++ && echo 1)" "1"
      CXX := ${HOME}/opt/bin/g++
    endif
  endif
endif

ifeq "$(origin LD)" "default"
  LD := ${CXX}
endif

INCS := -I${TERARK_HOME}/src ${INCS} ${BOOST_INC}
INCS += -I${ROCKSDB_HOME} -I${ROCKSDB_HOME}/include
INCS += -I../../src
INCS += -I${ROCKSDB_HOME}/sideplugin/rockside/src

#CXXFLAGS += -pipe
CXXFLAGS += -g3
CXXFLAGS += -Wall -Wextra
CXXFLAGS += -Wno-unused-parameter
CXXFLAGS += -DROCKSDB_PLATFORM_POSIX
CXXFLAGS += -D_GNU_SOURCE
CXXFLAGS += -D_FILE_OFFSET_BITS=64 -D_LARGEFILE_SOURCE -D_LARGEFILE64_SOURCE
#CXXFLAGS += -Wno-unused-variable
#CXXFLAGS += -Wconversion -Wno-sign-conversion

#CXXFLAGS += -Wfatal-errors
#DBG_ASAN ?= -fsanitize=address
#AFR_ASAN ?= -fsanitize=address
#RLS_ASAN ?=
AFR_FLAGS = -O1 ${AFR_ASAN}
DBG_FLAGS = -O0 ${DBG_ASAN}
RLS_FLAGS = -Ofast -DNDEBUG ${RLS_ASAN}

tmpfile := $(shell mktemp -u compiler-XXXXXX)
COMPILER := $(shell ${CXX} ${TERARK_HOME}/tools/configure/compiler.cpp -o ${tmpfile}.exe && ./${tmpfile}.exe; rm -f ${tmpfile}*)
UNAME_MachineSystem := $(shell uname -m -s | sed 's:[ /]:-:g')
UNAME_System := $(shell uname | sed 's/^\([0-9a-zA-Z]*\).*/\1/')

ifeq (${UNAME_System},Linux)
  LIBS += -lrt
endif
LIBS += -lpthread

ifeq "$(shell a=${COMPILER};echo $${a:0:5})" "clang"
  CXXFLAGS += -fcolor-diagnostics
endif

ifeq "$(shell a=${COMPILER};echo $${a:0:3})" "g++"
  ifeq ($(shell uname), Darwin)
    CXXFLAGS += -Wa,-q
  endif
  CXXFLAGS += -time
  ifeq "${MARCH}" "x86_64"
    CXXFLAGS += -mcx16
  endif
#  CXXFLAGS += -fmax-errors=5
  #CXXFLAGS += -fmax-errors=2
endif

# icc or icpc
ifeq "$(shell a=${COMPILER};echo $${a:0:2})" "ic"
  CXXFLAGS += -xHost -fasm-blocks
  CPU ?= -xHost
else
  ifeq "${MARCH}" "x86_64"
    ifeq (${WITH_BMI2},1)
      CPU ?= -march=haswell
    endif
  endif
  COMMON_C_FLAGS  += -Wno-deprecated-declarations
  ifeq "$(shell a=${COMPILER};echo $${a:0:5})" "clang"
    COMMON_C_FLAGS  += -fstrict-aliasing
  else
    COMMON_C_FLAGS  += -Wstrict-aliasing=3
  endif
endif

ifeq (${WITH_BMI2},1)
  CPU += -mbmi -mbmi2
else ifeq (${WITH_BMI2},0)
  CPU += -mno-bmi -mno-bmi2
endif

CXXFLAGS += ${CPU}

ifneq (${WITH_TBB},)
  CXXFLAGS += -DTERARK_WITH_TBB=${WITH_TBB}
  LIBS += -ltbb
endif

ifeq "$(shell a=${COMPILER};echo $${a:0:3})" "g++"
  #ifeq (Linux, ${UNAME_System})
  #  LDFLAGS += -rdynamic
  #endif
  CXXFLAGS += -time
endif

CXX_STD := -std=gnu++17

CXXFLAGS += ${CXX_STD}

ifeq (CYGWIN, ${UNAME_System})
  FPIC =
  # lazy expansion
  CYGWIN_LDFLAGS = -Wl,--out-implib=$@ \
				   -Wl,--export-all-symbols \
				   -Wl,--enable-auto-import
  DLL_SUFFIX = .dll.a
  CYG_DLL_FILE = $(shell echo $@ | sed 's:\(.*\)/lib\([^/]*\)\.a$$:\1/cyg\2:')
else
  ifeq (Darwin,${UNAME_System})
    DLL_SUFFIX = .dylib
  else
    DLL_SUFFIX = .so
  endif
  FPIC = -fPIC
  CYG_DLL_FILE = $@
endif
#CXXFLAGS += ${FPIC}

ifeq (Darwin,${UNAME_System})
  DLL_PATH_VAR = DYLD_LIBRARY_PATH
else
  DLL_PATH_VAR =   LD_LIBRARY_PATH
endif

# rocksdb must have been built before this make
include ${ROCKSDB_HOME}/make_config.mk
ifeq (${JEMALOC},1)
  INC += ${JEMALLOC_INCLUDE}
  LIBS := ${JEMALLOC_LIB} ${LIBS}
endif
ifeq (${WITH_JEMALLOC_FLAG},1)
  LIBS := -ljemalloc ${LIBS}
endif

BUILD_NAME := ${UNAME_MachineSystem}-${COMPILER}-bmi2-${WITH_BMI2}
BUILD_ROOT := build/${BUILD_NAME}

AFR_DIR := ${BUILD_ROOT}/afr
DBG_DIR := ${BUILD_ROOT}/dbg
RLS_DIR := ${BUILD_ROOT}/rls

EXE_SRCS ?= $(wildcard *.cpp)
EXE_OBJS_A := $(addprefix ${AFR_DIR}/,$(addsuffix .o,$(basename ${EXE_SRCS})))
EXE_OBJS_R := $(addprefix ${RLS_DIR}/,$(addsuffix .o,$(basename ${EXE_SRCS})))
EXE_OBJS_D := $(addprefix ${DBG_DIR}/,$(addsuffix .o,$(basename ${EXE_SRCS})))
EXE_BINS_A := $(addsuffix .exe,$(basename ${EXE_OBJS_A}))
EXE_BINS_D := $(addsuffix .exe,$(basename ${EXE_OBJS_D}))
EXE_BINS_R := $(addsuffix .exe,$(basename ${EXE_OBJS_R}))

DLL_SRCS += $(wildcard *.cxx)
DLL_OBJS_A := $(addprefix ${AFR_DIR}/,$(addsuffix .o,$(basename ${DLL_SRCS})))
DLL_OBJS_R := $(addprefix ${RLS_DIR}/,$(addsuffix .o,$(basename ${DLL_SRCS})))
DLL_OBJS_D := $(addprefix ${DBG_DIR}/,$(addsuffix .o,$(basename ${DLL_SRCS})))
DLL_BINS_A := $(addsuffix ${DLL_SUFFIX},$(basename ${DLL_OBJS_A}))
DLL_BINS_D := $(addsuffix ${DLL_SUFFIX},$(basename ${DLL_OBJS_D}))
DLL_BINS_R := $(addsuffix ${DLL_SUFFIX},$(basename ${DLL_OBJS_R}))

ALL_OBJ := ${EXE_OBJS_A} ${EXE_OBJS_D} ${EXE_OBJS_R} ${DLL_OBJS_A} ${DLL_OBJS_D} ${DLL_OBJS_R}
ALL_EXE := ${EXE_BINS_A} ${EXE_BINS_D} ${EXE_BINS_R}
ALL_DLL := ${DLL_BINS_A} ${DLL_BINS_D} ${DLL_BINS_R}
ALL_BIN := ${ALL_EXE} ${ALL_DLL}

LINK_ROCKSDB_R := -L${ROCKSDB_HOME} -lrocksdb
LINK_ROCKSDB_A := -L${ROCKSDB_HOME} -lrocksdb_debug_1
LINK_ROCKSDB_D := -L${ROCKSDB_HOME} -lrocksdb_debug

ext_ldflags = $(strip $(shell sed -n 's,.*//Makefile\s*:\s*LDFLAGS\s*:\s*\(.*\),\1,p' $(subst .exe,.cpp,$(subst ${AFR_DIR}/,,$(subst ${RLS_DIR}/,,$(subst ${DBG_DIR}/,,$@))))))
ext_cxxflags = $(strip $(shell sed -n 's,.*//Makefile\s*:\s*CXXFLAGS\s*:\s*\(.*\),\1,p' $<))
ext_cxxflags_delay = $$(strip $$(shell sed -n 's,.*//Makefile\s*:\s*CXXFLAGS\s*:\s*\(.*\),\1,p' $$<))

.PHONY : all clean link

all : link
link: ${ALL_BIN}
	@echo ALL_BIN = ${ALL_BIN}
	@mkdir -p dbg; cd dbg; \
	for f in `find ../${DBG_DIR} -name '*.exe' -o -name '*'${DLL_SUFFIX}`; do \
		ln -sf $$f .; \
	done; cd ..
	@mkdir -p rls; cd rls; \
	for f in `find ../${RLS_DIR} -name '*.exe' -o -name '*'${DLL_SUFFIX}`; do \
		ln -sf $$f .; \
	done; cd ..
	@mkdir -p afr; cd afr; \
	for f in `find ../${AFR_DIR} -name '*.exe' -o -name '*'${DLL_SUFFIX}`; do \
		ln -sf $$f .; \
	done; cd ..

ifeq (${TERARK_BIN_USE_STATIC_LIB},1)
  LIB_DIR := ${TERARK_HOME}/${BUILD_ROOT}/lib_static
  LIB_SUF := .a
  ifneq (Darwin,${UNAME_System})
    LIBS += -laio
    LIBS += -lgomp
  endif
else
  LIB_DIR := ${TERARK_HOME}/${BUILD_ROOT}/lib_shared
  LIB_SUF := ${DLL_SUFFIX}
  export   LD_LIBRARY_PATH=${LIB_DIR}
  export DYLD_LIBRARY_PATH=${LIB_DIR}  # for Mac
endif

ifeq (${CHECK_TERARK_FSA_LIB_UPDATE},1)
LIB_SRCS := $(shell find ${TERARK_HOME}/src -type f -name '*pp') \
            ${CURRENT_MAKEFILE} ${TERARK_HOME}/Makefile
#$(warning $(shell ( echo LIB_SRCS:  ; ls -l ${LIB_SRCS}   ) >&2))
#$(warning $(shell ( echo LIB_LIBS_D:; ls -l ${LIB_LIBS_D} ) >&2))
#$(warning $(shell ( echo LIB_LIBS_R:; ls -l ${LIB_LIBS_R} ) >&2))
#$(warning LIB_LIBS_D: $(subst ${TERARK_HOME}/,,${LIB_LIBS_D}))
#$(warning LIB_LIBS_R: $(subst ${TERARK_HOME}/,,${LIB_LIBS_R}))

LIB_LIBS_A := $(shell echo ${LIB_DIR}/libterark-{core,fsa,zbs}-${COMPILER}-a${LIB_SUF})
LIB_LIBS_D := $(shell echo ${LIB_DIR}/libterark-{core,fsa,zbs}-${COMPILER}-d${LIB_SUF})
LIB_LIBS_R := $(shell echo ${LIB_DIR}/libterark-{core,fsa,zbs}-${COMPILER}-r${LIB_SUF})

${LIB_LIBS_A}: ${LIB_SRCS}
${LIB_LIBS_D}: ${LIB_SRCS}
${LIB_LIBS_R}: ${LIB_SRCS}

%zbs-${COMPILER}-a${LIB_SUF}:
	+$(MAKE) -C ${TERARK_HOME} $(subst ${TERARK_HOME}/,,${LIB_LIBS_A})

%zbs-${COMPILER}-d${LIB_SUF}:
	+$(MAKE) -C ${TERARK_HOME} $(subst ${TERARK_HOME}/,,${LIB_LIBS_D})

%zbs-${COMPILER}-r${LIB_SUF}:
	+$(MAKE) -C ${TERARK_HOME} $(subst ${TERARK_HOME}/,,${LIB_LIBS_R})

${EXE_BINS_A}: ${LIB_LIBS_A}
${EXE_BINS_D}: ${LIB_LIBS_D}
${EXE_BINS_R}: ${LIB_LIBS_R}
endif

ifneq (${DLL_SRCS},)
${DLL_BINS_A} : LDFLAGS := ${AFR_ASAN} ${LDFLAGS}
${DLL_BINS_D} : LDFLAGS := ${DBG_ASAN} ${LDFLAGS}
${DLL_BINS_R} : LDFLAGS := ${RLS_ASAN} ${LDFLAGS}
endif

${EXE_BINS_A} : LDFLAGS := ${AFR_ASAN} ${LDFLAGS}
${EXE_BINS_D} : LDFLAGS := ${DBG_ASAN} ${LDFLAGS}
${EXE_BINS_R} : LDFLAGS := ${RLS_ASAN} ${LDFLAGS}

${EXE_BINS_A} : LIBS := ${LINK_ROCKSDB_A} -L${LIB_DIR} -lterark-{zbs,fsa,core}-${COMPILER}-a ${LIBS}
${EXE_BINS_D} : LIBS := ${LINK_ROCKSDB_D} -L${LIB_DIR} -lterark-{zbs,fsa,core}-${COMPILER}-d ${LIBS}
${EXE_BINS_R} : LIBS := ${LINK_ROCKSDB_R} -L${LIB_DIR} -lterark-{zbs,fsa,core}-${COMPILER}-r ${LIBS}

clean :
	rm -rf ${BUILD_ROOT} dbg rls afr

.PHONY : afr
.PHONY : dbg
.PHONY : rls
afr: ${EXE_BINS_A}
dbg: ${EXE_BINS_D}
rls: ${EXE_BINS_R}

UNIT_TESTS_DBG := $(addsuffix .test_dbg,${EXE_BINS_D})
UNIT_TESTS_AFR := $(addsuffix .test_afr,${EXE_BINS_A})
UNIT_TESTS_RLS := $(addsuffix .test_rls,${EXE_BINS_R})

.PHONY : test
.PHONY : test_dbg
.PHONY : test_afr
.PHONY : test_rls
test : test_dbg test_afr test_rls

test_dbg : ${UNIT_TESTS_DBG}
test_afr : ${UNIT_TESTS_AFR}
test_rls : ${UNIT_TESTS_RLS}

${UNIT_TESTS_DBG} : %.test_dbg : %
	env ${DLL_PATH_VAR}=${LIB_DIR} $<

${UNIT_TESTS_AFR} : %.test_afr : %
	env ${DLL_PATH_VAR}=${LIB_DIR} $<

${UNIT_TESTS_RLS} : %.test_rls : %
	env ${DLL_PATH_VAR}=${LIB_DIR} $<

#@param ${1} file name suffix: cpp | cxx | cc
#@PARAM ${2} build dir       : ddir | rdir | adir
#@param ${3} debug flag      : DBG_FLAGS | RLS_FLAGS | AFR_FLAGS
define COMPILE_CXX
${2}/%.o : %.${1}
	@echo file: $$< "->" $$@
	@echo TERARK_INC=$${TERARK_INC} BOOST_INC=$${BOOST_INC} BOOST_SUFFIX=$${BOOST_SUFFIX}
	@mkdir -p $$(dir $$@)
	$${CXX} $${CXX_STD} $${CPU} ${3} $${CXXFLAGS} $${INCS} ${ext_cxxflags_delay} $$< -o $$@ -c
${2}/%.s : %.${1}
	@echo file: $$< "->" $$@
	@mkdir -p $$(dir $$@)
	$${CXX} $${CXX_STD} $${CPU} ${3} $${CXXFLAGS} $${INCS} ${ext_cxxflags_delay} $$< -o $$@ -S -fverbose-asm
${2}/%.dep : %.${1}
	@echo file: $$< "->" $$@
	@echo INCS = $${INCS}
	@mkdir -p $$(dir $$@)
	-$${CXX} $${CXX_STD} ${3} -M -MT $$(basename $$@).o $${INCS} ${ext_cxxflags_delay} $$< > $$@
endef

$(eval $(call COMPILE_CXX,cpp,${AFR_DIR},${AFR_FLAGS}))
$(eval $(call COMPILE_CXX,cpp,${DBG_DIR},${DBG_FLAGS}))
$(eval $(call COMPILE_CXX,cpp,${RLS_DIR},${RLS_FLAGS}))

$(eval $(call COMPILE_CXX,cxx,${AFR_DIR},${AFR_FLAGS}))
$(eval $(call COMPILE_CXX,cxx,${DBG_DIR},${DBG_FLAGS}))
$(eval $(call COMPILE_CXX,cxx,${RLS_DIR},${RLS_FLAGS}))

%.exe : %.o
	@echo Linking ... $@
	${LD} ${LDFLAGS} -o $@ $< ${LIBS} $(ext_ldflags)

%${DLL_SUFFIX}: %.o
	@echo "----------------------------------------------------------------------------------"
	@echo "Creating dynamic library: $@"
	@echo BOOST_INC=${BOOST_INC} BOOST_SUFFIX=${BOOST_SUFFIX}
	@echo -e "OBJS:" $(addprefix "\n  ",$(sort $(filter %.o,$^)))
	@echo -e "LIBS:" $(addprefix "\n  ",${LIBS})
	@rm -f $@
	@rm -f $(subst -${COMPILER},, $@)
	@${LD} -shared $(sort $(filter %.o,$^)) ${LDFLAGS} ${LIBS} -o ${CYG_DLL_FILE} ${CYGWIN_LDFLAGS}
ifeq (CYGWIN, ${UNAME_System})
	@cp -l -f ${CYG_DLL_FILE} /usr/bin
endif

ifneq ($(MAKECMDGOALS),cleanall)
ifneq ($(MAKECMDGOALS),clean)
alldep := ${ALL_OBJ:.o=.dep}
-include ${alldep}
endif
endif
