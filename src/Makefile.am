AUTOMAKE_OPTIONS=foreign
bin_PROGRAMS=emeralddb edb hello
emeralddb_SOURCES=\
   oss/ossSocket.cpp \
   oss/ossPrimitiveFileOp.cpp \
   pmd/pmdEDU.cpp \
   pmd/pmdEDUMgr.cpp \
   pmd/pmdAgent.cpp \
   pmd/pmdTcpListener.cpp \
   bson/src/bsonobj.cpp bson/src/util/json.cpp bson/src/oid.cpp \
   bson/src/lib/base64.cpp bson/src/lib/md5.cpp bson/src/lib/nonce.cpp \
   msg/msg.cpp \
   pd/pd.cpp pmd/pmdOptions.cpp pmd/pmd.cpp \
   pmd/pmdMain.cpp

edb_SOURCES=\
   client/edb.cpp client/command.cpp client/commandFactory.cpp \
   bson/src/bsonobj.cpp bson/src/util/json.cpp bson/src/oid.cpp \
   bson/src/lib/base64.cpp bson/src/lib/md5.cpp bson/src/lib/nonce.cpp \
   oss/ossSocket.cpp \
   oss/ossPrimitiveFileOp.cpp \
   msg/msg.cpp \
   pd/pd.cpp 

hello_SOURCES=\
   bson/src/bsonobj.cpp bson/src/util/json.cpp bson/src/oid.cpp \
   bson/src/lib/base64.cpp bson/src/lib/md5.cpp bson/src/lib/nonce.cpp \
   oss/ossPrimitiveFileOp.cpp \
   pd/pd.cpp \
   pmd/pmdHello.cpp

emeralddb_CXXFLAGS=-I../boost -Ibson/src -Iinclude -D_FILE_OFFSET_BITS=64 -ggdb -Wall -O0
emeralddb_LDADD=-lpthread -lm -lboost_system -lboost_thread -lboost_program_options -lrt
emeralddb_LDFLAGS=-fPIC -rdynamic -L../boost/stage/lib -pthread

edb_CXXFLAGS=-I../boost -Ibson/src -Iinclude -D_FILE_OFFSET_BITS=64 -ggdb -Wall -O0
edb_LDADD=-lm -lboost_system -lboost_thread -lrt
edb_LDFLAGS=-fPIC -rdynamic -L../boost/stage/lib

hello_CXXFLAGS=-I../boost -Ibson/src -Iinclude -D_FILE_OFFSET_BITS=64 -ggdb -Wall -O0
hello_LDADD=-lm -lboost_system -lboost_thread -lrt
hello_LDFLAGS=-fPIC -rdynamic -L../boost/stage/lib
