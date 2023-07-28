


# make zip --- cleans and produces a zip file

# Add files you want to go into your client library here.
MRDFS_CLI_FILES= MRdfs_client.cpp
MRDFS_CLI_OBJS= MRdfs_client.o

# Add files you want to go into your server here.
MRDFS_SERVER_FILES = MRdfs_server.cpp
MRDFS_SERVER_OBJS = MRdfs_server.o
# E.g. for A3 add rw_lock.c and rw_lock.o to the

CXX = g++

# Add the required fuse library includes.
CXXFLAGS += $(shell pkg-config --cflags fuse)
CXXFLAGS += -g -Wall -std=c++1y -MMD
# If you want to disable logging messages from DLOG, uncomment the next line.
#CXXFLAGS += -DNDEBUG

# Add fuse libraries.
LDFLAGS += $(shell pkg-config --libs fuse)

# Dependencies for the client executable.
MRDFS_CLIENT_LIBS = libMRdfsmain.a libMRdfs.a librpc.a

OBJECTS = $(MRDFS_SERVER_OBJS) $(MRDFS_CLI_OBJS)
DEPENDS = $(OBJECTS:.o=.d)

# targets
.DEFAULT_GOAL = default_goal

default_goal: MRMRdfs.a MRdfs_server

# By default make libMRdfs.a and MRdfs_server.
all: libMRdfs.a MRdfs_server MRdfs_client

# This compiles object files, by default it looks for .c files
# so you may want to change this depending on your file naming scheme.
%.o: %.c
	$(CXX) $(CXXFLAGS) -c $(LDFLAGS) -L. -lrpc $<

# Make the client library.
libMRdfs.a: $(MRDFS_CLI_OBJS)
	ar rc $@ $^

# Make the server executable.
MRdfs_server: $(MRDFS_SERVER_OBJS)
	$(CXX) $(CXXFLAGS) $^ $(LDFLAGS) -L. -lrpc -o $@

# Make the client executable.
MRdfs_client: $(MRDFS_CLIENT_LIBS)
	$(CXX) $(CXXFLAGS) -o MRdfs_client -L. -lMRdfsmain -lMRdfs -lrpc $(LDFLAGS)

# Add dependencies so object files are tracked in the correct order.
depend:
	makedepend -f- -- $(CXXFLAGS) -- $(MRDFS_SERVER_FILES) $(MRDFS_CLI_FILES) > .depend

-include $(DEPENDS)

# Clean up extra dependencies and objects.
clean:
	/bin/rm -f $(DEPENDS) $(OBJECTS) MRdfs_server libMRdfs.a MRdfs_client *.log

zip: clean createzip

# Update as required.
createzip:
	zip -r MRdfs.zip $(MRDFS_SERVER_FILES) $(MRDFS_CLI_FILES) Makefile *.h
