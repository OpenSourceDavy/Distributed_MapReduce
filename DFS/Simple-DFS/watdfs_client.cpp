
#include "watdfs_client.h"
#include "debug.h"
#include <sys/stat.h>
#include <map>

INIT_LOG

#include "rpc.h"

struct Filedata {
    int client_mode;
    int file_descriptor;
    time_t tc;
};

struct Client_information {
    time_t cacheInterval;
    char *cachePath;
    std::map<std::string, struct Filedata > filedatas;
};

int rpc_call_getattr(void *userdata, const char *path, struct stat *statbuf) {
    // SET UP THE RPC CALL
    DLOG("watdfs_cli_getattr called for '%s'", path);

    // getattr has 3 arguments.
    int ARG_COUNT = 3;

    // Allocate space for the output arguments.
    void **args = new void*[ARG_COUNT];

    // Allocate the space for arg types, and one extra space for the null
    // array element.
    int arg_types[ARG_COUNT + 1];

    // The path has string length (strlen) + 1 (for the null character).
    int pathlen = strlen(path) + 1;

    // Fill in the arguments
    // The first argument is the path, it is an input only argument, and a char
    // array. The length of the array is the length of the path.
    arg_types[0] =
            (1u << ARG_INPUT) | (1u << ARG_ARRAY) | (ARG_CHAR << 16u) | (uint) pathlen;
    // For arrays the argument is the array pointer, not a pointer to a pointer.
    args[0] = (void *)path;

    // The second argument is the stat structure. This argument is an output
    // only argument, and we treat it as a char array. The length of the array
    // is the size of the stat structure, which we can determine with sizeof.
    arg_types[1] = (1u << ARG_OUTPUT) | (1u << ARG_ARRAY) | (ARG_CHAR << 16u) |
                   (uint) sizeof(struct stat); // statbuf
    args[1] = (void *)statbuf;

    // The third argument is the return code, an output only argument, which is
    // an integer.
    // TODO: fill in this argument type.
    arg_types[2] = (1u << ARG_OUTPUT)  | (ARG_INT << 16u);// retcode
    // The return code is not an array, so we need to hand args[2] an int*.
    // The int* could be the address of an integer located on the stack, or use
    // a heap allocated integer, in which case it should be freed.
    // TODO: Fill in the argument
    int return_code;
    args[2] = (int *)&return_code;
    // Finally, the last position of the arg types is 0. There is no
    // corresponding arg.
    arg_types[3] = 0;

    // MAKE THE RPC CALL
    int rpc_ret = rpcCall((char *)"getattr", arg_types, args);

    // HANDLE THE RETURN
    // The integer value watdfs_cli_getattr will return.
    int fxn_ret = 0;
    if (rpc_ret < 0) {
        DLOG("getattr rpc failed with error '%d'", rpc_ret);
        // Something went wrong with the rpcCall, return a sensible return
        // value. In this case lets return, -EINVAL
        fxn_ret = -EINVAL;
    } else {
        // Our RPC call succeeded. However, it's possible that the return code
        // from the server is not 0, that is it may be -errno. Therefore, we
        // should set our function return value to the retcode from the server.

        // TODO: set the function return value to the return code from the server.
        fxn_ret = return_code;
    }

    if (fxn_ret < 0) {
        // If the return code of watdfs_cli_getattr is negative (an error), then
        // we need to make sure that the stat structure is filled with 0s. Otherwise,
        // FUSE will be confused by the contradicting return values.
        memset(statbuf, 0, sizeof(struct stat));
    }

    // Clean up the memory we have allocated.
    delete []args;

    // Finally return the value we got from the server.
    return fxn_ret;
}

// CREATE, OPEN AND CLOSE
int rpc_call_mknod(void *userdata, const char *path, mode_t mode, dev_t dev) {

    // Called to create a file.
    // getattr has 4 arguments.
    int ARG_COUNT = 4;

    // Allocate space for the output arguments.
    void **args = new void*[ARG_COUNT];

    // Allocate the space for arg types, and one extra space for the null
    // array element.
    int arg_types[ARG_COUNT + 1];

    // The path has string length (strlen) + 1 (for the null character).
    int pathlen = strlen(path) + 1;

    // Fill in the arguments
    // The first argument is the path, it is an input only argument, and a char
    // array. The length of the array is the length of the path.
    arg_types[0] =
            (1u << ARG_INPUT) | (1u << ARG_ARRAY) | (ARG_CHAR << 16u) | (uint) pathlen;
    // For arrays the argument is the array pointer, not a pointer to a pointer.
    args[0] = (void *)path;

    //Second argument is the mode
    arg_types[1] = (1u << ARG_INPUT)  | (ARG_INT << 16u);
    args[1] = (void *)&mode;

    //Third argument is the dev
    arg_types[2] = (1u << ARG_INPUT)  | (ARG_LONG << 16u);
    args[2] = (void *)&dev;

    //Fourth is the retcode
    arg_types[3] = (1u << ARG_OUTPUT)  | (ARG_INT << 16u);// retcode
    int return_code;
    args[3] = (void *)&return_code;

    arg_types[4] = 0;


    int rpc_ret = rpcCall((char *)"mknod", arg_types, args);

    // HANDLE THE RETURN
    // The integer value watdfs_cli_getattr will return.
    int fxn_ret = 0;
    if (rpc_ret < 0) {
        DLOG("mknod rpc failed with error '%d'", rpc_ret);
        // Something went wrong with the rpcCall, return a sensible return
        // value. In this case lets return, -EINVAL
        fxn_ret = -EINVAL;
    } else {
        // Our RPC call succeeded. However, it's possible that the return code
        // from the server is not 0, that is it may be -errno. Therefore, we
        // should set our function return value to the retcode from the server.

        // TODO: set the function return value to the return code from the server.
        fxn_ret = return_code;
    }

    /*if (fxn_ret < 0) {
        // If the return code of watdfs_cli_getattr is negative (an error), then
        // we need to make sure that the stat structure is filled with 0s. Otherwise,
        // FUSE will be confused by the contradicting return values.
        memset(statbuf, 0, sizeof(struct stat));
    }*/

    // Clean up the memory we have allocated.
    delete []args;

    // Finally return the value we got from the server.
    return fxn_ret;

    return -ENOSYS;
}
int rpc_call_open(void *userdata, const char *path,
                    struct fuse_file_info *fi) {
    // Called during open.
    // You should fill in fi->fh.
    // getattr has 3 arguments.
    int ARG_COUNT = 3;

    // Allocate space for the output arguments.
    void **args = new void*[ARG_COUNT];

    // Allocate the space for arg types, and one extra space for the null
    // array element.
    int arg_types[ARG_COUNT + 1];

    // The path has string length (strlen) + 1 (for the null character).
    int pathlen = strlen(path) + 1;

    // Fill in the arguments
    // The first argument is the path, it is an input only argument, and a char
    // array. The length of the array is the length of the path.
    arg_types[0] =
            (1u << ARG_INPUT) | (1u << ARG_ARRAY) | (ARG_CHAR << 16u) | (uint) pathlen;
    // For arrays the argument is the array pointer, not a pointer to a pointer.
    args[0] = (void *)path;

    //fi
    arg_types[1] =
            (1u << ARG_INPUT) | (1u << ARG_OUTPUT) | (1u << ARG_ARRAY) | (ARG_CHAR << 16u) | sizeof(struct fuse_file_info);
    args[1] = (void *)fi;

    //retcode
    arg_types[2] = (1u << ARG_OUTPUT)  | (ARG_INT << 16u);// retcode
    int return_code;
    args[2] = (int *)&return_code;

    arg_types[3] = 0;

    int rpc_ret = rpcCall((char *)"open", arg_types, args);

    // HANDLE THE RETURN
    // The integer value watdfs_cli_getattr will return.
    int fxn_ret = 0;
    if (rpc_ret < 0) {
        DLOG("open rpc failed with error '%d'", rpc_ret);
        // Something went wrong with the rpcCall, return a sensible return
        // value. In this case lets return, -EINVAL
        fxn_ret = -EINVAL;
    } else {
        // Our RPC call succeeded. However, it's possible that the return code
        // from the server is not 0, that is it may be -errno. Therefore, we
        // should set our function return value to the retcode from the server.

        // TODO: set the function return value to the return code from the server.
        fxn_ret = return_code;
    }

    /*if (fxn_ret < 0) {
        // If the return code of watdfs_cli_getattr is negative (an error), then
        // we need to make sure that the stat structure is filled with 0s. Otherwise,
        // FUSE will be confused by the contradicting return values.
        memset(statbuf, 0, sizeof(struct stat));
    }*/

    // Clean up the memory we have allocated.
    delete []args;

    // Finally return the value we got from the server.
    return fxn_ret;

    return -ENOSYS;
}

int rpc_call_release(void *userdata, const char *path,
                       struct fuse_file_info *fi) {
    // Called during close, but possibly asynchronously.
    int ARG_COUNT = 3;

    // Allocate space for the output arguments.
    void **args = new void*[ARG_COUNT];

    // Allocate the space for arg types, and one extra space for the null
    // array element.
    int arg_types[ARG_COUNT + 1];

    // The path has string length (strlen) + 1 (for the null character).
    int pathlen = strlen(path) + 1;

    // Fill in the arguments
    // The first argument is the path, it is an input only argument, and a char
    // array. The length of the array is the length of the path.
    arg_types[0] =
            (1u << ARG_INPUT) | (1u << ARG_ARRAY) | (ARG_CHAR << 16u) | (uint) pathlen;
    // For arrays the argument is the array pointer, not a pointer to a pointer.
    args[0] = (void *)path;

    //fi
    arg_types[1] =
            (1u << ARG_INPUT) | (1u << ARG_ARRAY) | (ARG_CHAR << 16u) | sizeof(struct fuse_file_info);
    args[1] = (void *)fi;

    //retcode
    arg_types[2] = (1u << ARG_OUTPUT)  | (ARG_INT << 16u);// retcode
    int return_code;
    args[2] = (int *)&return_code;

    arg_types[3] = 0;

    int rpc_ret = rpcCall((char *)"release", arg_types, args);

    // HANDLE THE RETURN
    // The integer value watdfs_cli_getattr will return.
    int fxn_ret = 0;
    if (rpc_ret < 0) {
        DLOG("release rpc failed with error '%d'", rpc_ret);
        // Something went wrong with the rpcCall, return a sensible return
        // value. In this case lets return, -EINVAL
        fxn_ret = -EINVAL;
    } else {
        // Our RPC call succeeded. However, it's possible that the return code
        // from the server is not 0, that is it may be -errno. Therefore, we
        // should set our function return value to the retcode from the server.

        // TODO: set the function return value to the return code from the server.
        fxn_ret = return_code;
    }

    /*if (fxn_ret < 0) {
        // If the return code of watdfs_cli_getattr is negative (an error), then
        // we need to make sure that the stat structure is filled with 0s. Otherwise,
        // FUSE will be confused by the contradicting return values.
        memset(statbuf, 0, sizeof(struct stat));
    }*/

    // Clean up the memory we have allocated.
    delete []args;

    // Finally return the value we got from the server.
    return fxn_ret;
    return -ENOSYS;
}

// READ AND WRITE DATA
int rpc_call_read(void *userdata, const char *path, char *buf, size_t size,
                    off_t offset, struct fuse_file_info *fi) {
    // Read size amount of data at offset of file into buf.

    // Remember that size may be greater then the maximum array size of the RPC
    // library.


    int ARG_COUNT = 6;

    // Allocate space for the output arguments.
    void **args = new void*[ARG_COUNT];

    // Allocate the space for arg types, and one extra space for the null
    // array element.
    int arg_types[ARG_COUNT + 1];

    // The path has string length (strlen) + 1 (for the null character).
    int pathlen = strlen(path) + 1;

    // Fill in the arguments
    // The first argument is the path, it is an input only argument, and a char
    // array. The length of the array is the length of the path.
    arg_types[0] =
            (1u << ARG_INPUT) | (1u << ARG_ARRAY) | (ARG_CHAR << 16u) | (uint) pathlen;
    // For arrays the argument is the array pointer, not a pointer to a pointer.
    args[0] = (void *)path;

    //buf
    //写的有问题
    arg_types[1] =
            (1u << ARG_OUTPUT) | (1u << ARG_ARRAY) | (ARG_CHAR << 16u) | MAX_ARRAY_LEN;
    // For arrays the argument is the array pointer, not a pointer to a pointer.
    args[1] = (void *)buf;

    //size
    arg_types[2] = (1u << ARG_INPUT)  | (ARG_LONG << 16u);
    args[2] = (long *)&size;

    //offset
    arg_types[3] = (1u << ARG_INPUT)  | (ARG_LONG << 16u);
    args[3] = (long *)&offset;

    //fi
    arg_types[4] = (1u << ARG_INPUT) | (1u << ARG_ARRAY) | (ARG_CHAR << 16u) | sizeof(struct fuse_file_info);
    args[4] = (void *)fi;

    //retcode
    arg_types[5] = (1u << ARG_OUTPUT)  | (ARG_INT << 16u);// retcode
    int return_code;
    args[5] = (int *)&return_code;

    arg_types[6] = 0;


    /*long size_copy = size;
    while(size_copy > MAX_ARRAY_LEN){
        size = MAX_ARRAY_LEN;
        int rpc_ret = rpcCall((char *)"read", arg_types, args);
        if (rpc_ret < 0 ) {
            return -EINVAL;
        }
        offset += MAX_ARRAY_LEN;
        size_copy-= MAX_ARRAY_LEN;
    }
    size = size_copy;*/
    //buf = (char *)malloc(size+1);
    //memset(buf,0,sizeof(size+1));
    char *buf_total = (char *)malloc(size+1);
    memset(buf_total,0,sizeof(size+1));
    long offset_copy =offset;
    long size_copy = size;
    long total_read = 0;
    long chunk_size;
    while (total_read < size_copy){
        long remaining_size = size_copy-total_read;
        if(remaining_size>MAX_ARRAY_LEN)
            chunk_size = MAX_ARRAY_LEN;
        else
            chunk_size = remaining_size;
        size = chunk_size;
        memset(buf,0,sizeof(size));
        arg_types[1] =
                (1u << ARG_OUTPUT) | (1u << ARG_ARRAY) | (ARG_CHAR << 16u) | chunk_size;
        int rpc_ret = rpcCall((char *)"read", arg_types, args);
        if (rpc_ret < 0) {
            return -EINVAL;
        }
        if(return_code < 0){
            return return_code;
        }
        if(return_code == 0){
            //DLOG("something wrong");
            return_code = total_read;
            memcpy(buf,buf_total,total_read);
            buf = buf_total;
            DLOG("offset in client: %ld\n", offset);
            DLOG("buf_end in client: %s\n", buf);
            DLOG("total_read in client: %ld\n", total_read);
            return total_read;
        }
        DLOG("read content in client: %s\n", buf);
        DLOG("sys_ret in client: %d\n", return_code);
        memcpy(buf_total+total_read,buf,return_code);
        offset+=return_code;
        total_read+=return_code;
        if(return_code < chunk_size)
            break;
    }
    return_code = total_read;
    offset = offset_copy;
    //buf = buf_total;
    memcpy(buf,buf_total,total_read);
    DLOG("offset in client: %ld\n", offset);
    DLOG("buf_end in client: %s\n", buf);
    DLOG("total_read in client: %ld\n", total_read);
    delete []args;
    return total_read;

    /*int rpc_ret = rpcCall((char *)"read", arg_types, args);

    // HANDLE THE RETURN
    // The integer value watdfs_cli_getattr will return.
    int fxn_ret = 0;
    if (rpc_ret < 0) {
        DLOG("read rpc failed with error '%d'", rpc_ret);
        // Something went wrong with the rpcCall, return a sensible return
        // value. In this case lets return, -EINVAL
        fxn_ret = -EINVAL;
    } else {
        // Our RPC call succeeded. However, it's possible that the return code
        // from the server is not 0, that is it may be -errno. Therefore, we
        // should set our function return value to the retcode from the server.

        // TODO: set the function return value to the return code from the server.
        DLOG("read content in client: %s\n", buf);
        DLOG("sys_ret in client: %d\n", return_code);
        fxn_ret = return_code;
    }

    /*if (fxn_ret < 0) {
        // If the return code of watdfs_cli_getattr is negative (an error), then
        // we need to make sure that the stat structure is filled with 0s. Otherwise,
        // FUSE will be confused by the contradicting return values.
        memset(statbuf, 0, sizeof(struct stat));
    }*/

    // Clean up the memory we have allocated.
    //delete []args;

    // Finally return the value we got from the server.
    //return fxn_ret;

    return -ENOSYS;
}
int rpc_call_write(void *userdata, const char *path, const char *buf,
                     size_t size, off_t offset, struct fuse_file_info *fi) {
    // Write size amount of data at offset of file from buf.

    // Remember that size may be greater then the maximum array size of the RPC
    // library.
    int ARG_COUNT = 6;

    // Allocate space for the output arguments.
    void **args = new void*[ARG_COUNT];

    // Allocate the space for arg types, and one extra space for the null
    // array element.
    int arg_types[ARG_COUNT + 1];

    // The path has string length (strlen) + 1 (for the null character).
    int pathlen = strlen(path) + 1;

    // Fill in the arguments
    // The first argument is the path, it is an input only argument, and a char
    // array. The length of the array is the length of the path.
    arg_types[0] =
            (1u << ARG_INPUT) | (1u << ARG_ARRAY) | (ARG_CHAR << 16u) | (uint) pathlen;
    // For arrays the argument is the array pointer, not a pointer to a pointer.
    args[0] = (void *)path;

    //buf
    //写的有问题
    arg_types[1] =
            (1u << ARG_INPUT) | (1u << ARG_ARRAY) | (ARG_CHAR << 16u) | MAX_ARRAY_LEN;
    // For arrays the argument is the array pointer, not a pointer to a pointer.
    args[1] = (void *)buf;

    //size
    arg_types[2] = (1u << ARG_INPUT)  | (ARG_LONG << 16u);
    args[2] = (long *)&size;

    //offset
    arg_types[3] = (1u << ARG_INPUT)  | (ARG_LONG << 16u);
    args[3] = (long *)&offset;

    //fi
    arg_types[4] = (1u << ARG_INPUT) | (1u << ARG_ARRAY) | (ARG_CHAR << 16u) | sizeof(struct fuse_file_info);
    args[4] = (void *)fi;

    //retcode
    arg_types[5] = (1u << ARG_OUTPUT)  | (ARG_INT << 16u);// retcode
    int return_code;
    args[5] = (int *)&return_code;

    arg_types[6] = 0;

    //char *buf_total = (char *)malloc(size);
    //memset(buf_total,0,sizeof(buf_total));

    long offset_copy =offset;
    long size_copy = size;
    long total_read = 0;
    long chunk_size;
    while (total_read < size_copy){
        long remaining_size = size_copy-total_read;
        if(remaining_size>MAX_ARRAY_LEN)
            chunk_size = MAX_ARRAY_LEN;
        else
            chunk_size = remaining_size;
        size = chunk_size;
        arg_types[1] =
                (1u << ARG_INPUT) | (1u << ARG_ARRAY) | (ARG_CHAR << 16u) | chunk_size;
        int rpc_ret = rpcCall((char *)"write", arg_types, args);
        args[1] = (char *)args[1]+chunk_size;
        if (rpc_ret < 0) {
            return -EINVAL;
        }
        if(return_code < 0){
            return return_code;
        }
        if(return_code == 0){
            //DLOG("something wrong");
            return_code = total_read;
            //offset = offset_copy;
            //buf = buf_total;
            return total_read;
        }
        DLOG("ARGS[1]: %p\n", args[1]);
        DLOG("offset in client: %ld\n", offset);
        DLOG("sys_ret in client: %d\n", return_code);
        //memcpy(buf_total+total_read,buf,return_code);
        offset+=return_code;
        total_read+=return_code;
        if(return_code < chunk_size)
            break;
    }
    return_code = total_read;
    //offset = offset_copy;
    //buf = buf_total;
    DLOG("offset in client: %ld\n", offset);
    DLOG("buf_end in client: %s\n", buf);
    DLOG("total_write in client: %ld\n", total_read);
    delete []args;
    return total_read;

    /*long size_copy = size;
    while(size_copy > MAX_ARRAY_LEN){
        size = MAX_ARRAY_LEN;
        int rpc_ret = rpcCall((char *)"write", arg_types, args);
        if (rpc_ret < 0 ) {
            return -EINVAL;
        }
        offset += MAX_ARRAY_LEN;
        size_copy-= MAX_ARRAY_LEN;
    }
    size = size_copy;*/

    /*long size_copy = size;
    long total_read = 0;
    long chunk_size;
    while (total_read < size_copy){
        long remaining_size = size-total_read;
        if(remaining_size>MAX_ARRAY_LEN)
            chunk_size = MAX_ARRAY_LEN;
        else
            chunk_size = remaining_size;
        size = chunk_size;
        int rpc_ret = rpcCall((char *)"write", arg_types, args);
        if (rpc_ret < 0) {
            return -EINVAL;
        }
        if(return_code < 0){
            return return_code;
        }
        if(return_code == 0){
            return total_read;
        }
        offset+=return_code;
        total_read+=return_code;
        if(return_code < chunk_size)
            break;
    }
    return total_read;*/

    /*int rpc_ret = rpcCall((char *)"write", arg_types, args);

    // HANDLE THE RETURN
    // The integer value watdfs_cli_getattr will return.
    int fxn_ret = 0;
    if (rpc_ret < 0) {
        DLOG("write rpc failed with error '%d'", rpc_ret);
        // Something went wrong with the rpcCall, return a sensible return
        // value. In this case lets return, -EINVAL
        fxn_ret = -EINVAL;
    } else {
        // Our RPC call succeeded. However, it's possible that the return code
        // from the server is not 0, that is it may be -errno. Therefore, we
        // should set our function return value to the retcode from the server.

        // TODO: set the function return value to the return code from the server.
            fxn_ret = return_code;
    }

    /*if (fxn_ret < 0) {
        // If the return code of watdfs_cli_getattr is negative (an error), then
        // we need to make sure that the stat structure is filled with 0s. Otherwise,
        // FUSE will be confused by the contradicting return values.
        memset(statbuf, 0, sizeof(struct stat));
    }*/

    // Clean up the memory we have allocated.
    //delete []args;

    // Finally return the value we got from the server.
    //return fxn_ret;


    return -ENOSYS;
}

int rpc_call_truncate(void *userdata, const char *path, off_t newsize) {
    // Change the file size to newsize.

    int ARG_COUNT = 3;

    // Allocate space for the output arguments.
    void **args = new void*[ARG_COUNT];

    // Allocate the space for arg types, and one extra space for the null
    // array element.
    int arg_types[ARG_COUNT + 1];

    // The path has string length (strlen) + 1 (for the null character).
    int pathlen = strlen(path) + 1;

    // Fill in the arguments
    // The first argument is the path, it is an input only argument, and a char
    // array. The length of the array is the length of the path.
    arg_types[0] =
            (1u << ARG_INPUT) | (1u << ARG_ARRAY) | (ARG_CHAR << 16u) | (uint) pathlen;
    // For arrays the argument is the array pointer, not a pointer to a pointer.
    args[0] = (void *)path;

    //new size
    arg_types[1] =
            (1u << ARG_INPUT)  | (ARG_LONG << 16u) ;
    args[1] = (long *)&newsize;

    //retcode
    arg_types[2] = (1u << ARG_OUTPUT)  | (ARG_INT << 16u);// retcode
    int return_code;
    args[2] = (int *)&return_code;

    arg_types[3] = 0;

    int rpc_ret = rpcCall((char *)"truncate", arg_types, args);

    // HANDLE THE RETURN
    // The integer value watdfs_cli_getattr will return.
    int fxn_ret = 0;
    if (rpc_ret < 0) {
        DLOG("truncate rpc failed with error '%d'", rpc_ret);
        // Something went wrong with the rpcCall, return a sensible return
        // value. In this case lets return, -EINVAL
        fxn_ret = -EINVAL;
    } else {
        // Our RPC call succeeded. However, it's possible that the return code
        // from the server is not 0, that is it may be -errno. Therefore, we
        // should set our function return value to the retcode from the server.

        // TODO: set the function return value to the return code from the server.
        fxn_ret = return_code;
    }

    /*if (fxn_ret < 0) {
        // If the return code of watdfs_cli_getattr is negative (an error), then
        // we need to make sure that the stat structure is filled with 0s. Otherwise,
        // FUSE will be confused by the contradicting return values.
        memset(statbuf, 0, sizeof(struct stat));
    }*/

    // Clean up the memory we have allocated.
    delete []args;

    // Finally return the value we got from the server.
    return fxn_ret;
    return -ENOSYS;

}

int rpc_call_fsync(void *userdata, const char *path,
                     struct fuse_file_info *fi) {
    // Force a flush of file data.

    int ARG_COUNT = 3;

    // Allocate space for the output arguments.
    void **args = new void*[ARG_COUNT];

    // Allocate the space for arg types, and one extra space for the null
    // array element.
    int arg_types[ARG_COUNT + 1];

    // The path has string length (strlen) + 1 (for the null character).
    int pathlen = strlen(path) + 1;

    // Fill in the arguments
    // The first argument is the path, it is an input only argument, and a char
    // array. The length of the array is the length of the path.
    arg_types[0] =
            (1u << ARG_INPUT) | (1u << ARG_ARRAY) | (ARG_CHAR << 16u) | (uint) pathlen;
    // For arrays the argument is the array pointer, not a pointer to a pointer.
    args[0] = (void *)path;

    //fi
    arg_types[1] =
            (1u << ARG_INPUT) | (1u << ARG_ARRAY) | (ARG_CHAR << 16u) | sizeof(struct fuse_file_info);
    args[1] = (void *)fi;

    //retcode
    arg_types[2] = (1u << ARG_OUTPUT)  | (ARG_INT << 16u);// retcode
    int return_code;
    args[2] = (int *)&return_code;

    arg_types[3] = 0;

    int rpc_ret = rpcCall((char *)"fsync", arg_types, args);

    // HANDLE THE RETURN
    // The integer value watdfs_cli_getattr will return.
    int fxn_ret = 0;
    if (rpc_ret < 0) {
        DLOG("fsync rpc failed with error '%d'", rpc_ret);
        // Something went wrong with the rpcCall, return a sensible return
        // value. In this case lets return, -EINVAL
        fxn_ret = -EINVAL;
    } else {
        // Our RPC call succeeded. However, it's possible that the return code
        // from the server is not 0, that is it may be -errno. Therefore, we
        // should set our function return value to the retcode from the server.

        // TODO: set the function return value to the return code from the server.
        fxn_ret = return_code;
    }

    /*if (fxn_ret < 0) {
        // If the return code of watdfs_cli_getattr is negative (an error), then
        // we need to make sure that the stat structure is filled with 0s. Otherwise,
        // FUSE will be confused by the contradicting return values.
        memset(statbuf, 0, sizeof(struct stat));
    }*/

    // Clean up the memory we have allocated.
    delete []args;

    // Finally return the value we got from the server.
    return fxn_ret;
    return -ENOSYS;
}

// CHANGE METADATA
int rpc_call_utimensat(void *userdata, const char *path,
                         const struct timespec ts[2]) {
    // Change file access and modification times.
    int ARG_COUNT = 3;

    // Allocate space for the output arguments.
    void **args = new void*[ARG_COUNT];

    // Allocate the space for arg types, and one extra space for the null
    // array element.
    int arg_types[ARG_COUNT + 1];

    // The path has string length (strlen) + 1 (for the null character).
    int pathlen = strlen(path) + 1;

    // Fill in the arguments
    // The first argument is the path, it is an input only argument, and a char
    // array. The length of the array is the length of the path.
    arg_types[0] =
            (1u << ARG_INPUT) | (1u << ARG_ARRAY) | (ARG_CHAR << 16u) | (uint) pathlen;
    // For arrays the argument is the array pointer, not a pointer to a pointer.
    args[0] = (void *)path;

    //ts
    arg_types[1] =
            (1u << ARG_INPUT) | (1u << ARG_ARRAY) | (ARG_CHAR << 16u) | (uint)(2*sizeof(struct timespec));

    //struct timespec sub = ts[2];
    //const struct timespec *ptr = &ts[2];
    args[1] = (void *) ts;

    //retcode
    arg_types[2] = (1u << ARG_OUTPUT)  | (ARG_INT << 16u);// retcode
    int return_code;
    args[2] = (int *)&return_code;

    arg_types[3] = 0;

    int rpc_ret = rpcCall((char *)"utimensat", arg_types, args);

    // HANDLE THE RETURN
    // The integer value watdfs_cli_getattr will return.
    int fxn_ret = 0;
    if (rpc_ret < 0) {
        DLOG("utimensat rpc failed with error '%d'", rpc_ret);
        // Something went wrong with the rpcCall, return a sensible return
        // value. In this case lets return, -EINVAL
        fxn_ret = -EINVAL;
    } else {
        // Our RPC call succeeded. However, it's possible that the return code
        // from the server is not 0, that is it may be -errno. Therefore, we
        // should set our function return value to the retcode from the server.

        // TODO: set the function return value to the return code from the server.
        fxn_ret = return_code;
    }

    /*if (fxn_ret < 0) {
        // If the return code of watdfs_cli_getattr is negative (an error), then
        // we need to make sure that the stat structure is filled with 0s. Otherwise,
        // FUSE will be confused by the contradicting return values.
        memset(statbuf, 0, sizeof(struct stat));
    }*/

    // Clean up the memory we have allocated.
    delete []args;

    // Finally return the value we got from the server.
    return fxn_ret;
    //return -ENOSYS;
}


char *get_full_path(char *path_to_cache, const char* rela_path) {
    int rela_path_len = strlen(rela_path);
    int dir_len = strlen(path_to_cache);

    int full_path_len = dir_len + rela_path_len + 1;

    char *full_path = (char *) malloc(full_path_len);

    strcpy(full_path, path_to_cache);
    strcat(full_path, rela_path);

    return full_path;
}


int file_freshness_check(void *userdata,const char *path){

    int str_len = strlen(((struct Client_information *)userdata)->cachePath) + 1;

    char *cachepath = (char *)malloc(str_len+1);

    strcpy(cachepath, ((struct Client_information *)userdata)->cachePath);
    char *full_path = get_full_path(cachepath, path);

    std::string p = std::string(full_path);


    time_t current_time = time(0);

    Filedata file = (((struct Client_information*)userdata)->filedatas)[p];

    //struct File_metadata f = get_file_meta(path);

    if((current_time - file.tc) < ((struct Client_information*)userdata)->cacheInterval){
        return 0;
    }

    //time_t s_m_time = get_server_file_m_time(userdata,path);

    struct stat statbuf;
    rpc_call_getattr(userdata, path, &statbuf);
    //return statbuf.st_mtim.tv_sec;

    struct stat buf;
    stat(cachepath,&buf);

    if(buf.st_mtim.tv_sec == statbuf.st_mtim.tv_sec){
        return 0;
    }
    return -1;

}

int download(struct Client_information *userdata, const char *path, const char *full_path){

    int fxn_ret = 0;
    int sys_ret = 0;
    DLOG("download begin");
    //get file attributes from the server

    struct stat *statbuf = new struct stat;
    int rpc_ret = rpc_call_getattr((void *)userdata, path, statbuf);
    if(rpc_ret < 0){
        return rpc_ret;
    }
    size_t size = statbuf->st_size;
    char *buf = (char *) malloc(((off_t) size) * sizeof(char));
    struct fuse_file_info *fi = new struct fuse_file_info;

    //read the file from the server

    fi->flags = O_RDONLY;
    rpc_ret = rpc_call_open((void *)userdata, path, fi);
    DLOG("rpc_call_open fi->fh %d",fi->fh);
    DLOG("rpc_call_open return value %d",rpc_ret);
    if (rpc_ret < 0) fxn_ret = rpc_ret;
    rpc_ret = rpc_call_read((void *)userdata, path, buf, size, 0, fi);
    DLOG("rpc_call_read return value %d",rpc_ret);
    if (rpc_ret < 0) fxn_ret = rpc_ret;

    //write the file to the client

    //First we need to open the client side
    sys_ret = open(full_path, O_RDWR);
    if (sys_ret < 0) {
        DLOG("open local file error 951");
        // trigger sys call for mknod
        mknod(full_path, statbuf->st_mode, statbuf->st_dev);
        sys_ret = open(full_path, O_RDWR);
    }
    DLOG("download: open return value %d",sys_ret);
    //set the file_descriptor of the filedata
    //((((struct Client_information*)userdata)->filedatas)[full_path]).file_descriptor = sys_ret;

    //Second truncate the file at the client
    rpc_ret = truncate(full_path, (off_t)size);
    if (rpc_ret < 0) fxn_ret = -errno;

    //Third is write the file to the client
    //DLOG("Download function: full_path %s",full_path);

    //DLOG("Download: file_descriptor before %d",(((struct Client_information *)userdata)->filedatas)[full_path].file_descriptor);
    if((((struct Client_information *)userdata)->filedatas)[full_path].file_descriptor == 0)
        (((struct Client_information *)userdata)->filedatas)[full_path].file_descriptor = sys_ret;

    DLOG("Download: file_descriptor after %d",(((struct Client_information *)userdata)->filedatas)[full_path].file_descriptor);

    int sys_ret_write = pwrite(sys_ret, buf, size, 0);
    DLOG("pwrite return value %d",sys_ret_write);
    if (sys_ret_write < 0) {
        sys_ret_write = -errno;
        return sys_ret_write;
    }



    // update the file metadata at the client
    struct timespec ts[2];

    ts[0] = (struct timespec)(statbuf->st_atim);
    ts[1] = (struct timespec)(statbuf->st_mtim);

    sys_ret = utimensat(0, full_path, ts, 0);
    rpc_ret = rpc_call_release((void *)userdata, path, fi);
    if (rpc_ret < 0) fxn_ret = rpc_ret;

    // close file locally
    int ret_code = close(sys_ret);
    if(ret_code < 0) fxn_ret = -errno;

    sys_ret = open(full_path, O_RDWR);
    DLOG("download: open return value1 %d",sys_ret);
    ret_code = close(sys_ret);
    sys_ret = open(full_path, O_RDWR);
    DLOG("download: open return value2 %d",sys_ret);
    ret_code = close(sys_ret);
    sys_ret = open(full_path, O_RDWR);
    DLOG("download: open return value3 %d",sys_ret);
    ret_code = close(sys_ret);

    return fxn_ret;
}

int upload(struct Client_information *userdata, const char *path, const char *full_path){
    DLOG("upload begin");
    int fxn_ret = 0;
    int sys_ret = 0;
    struct fuse_file_info *fi = new struct fuse_file_info;
    fi->flags = O_RDWR;
    std::string p = std::string(full_path);
    struct stat *statbuf = new struct stat;

    //get the  local file information
    int ret_code = stat(full_path, statbuf);

    //open the file in the server side
    ret_code = rpc_call_open((void *)userdata, path, fi);
    //judge whether the file exists
    if (ret_code < 0){
        mode_t mt = statbuf->st_mode;
        dev_t dt = statbuf->st_dev;
        ret_code = rpc_call_mknod((void *)userdata, path, mt, dt);
        ret_code = rpc_call_open((void *)userdata, path, fi);
    }

    //read the file in the local side
    size_t size = statbuf->st_size;
    //char * buf = (char *) malloc(((off_t) size) * sizeof(char));
    DLOG("size %d",size);
    char buf[size];
    sys_ret = open(full_path, O_RDONLY);

    if (ret_code < 0){
        fxn_ret = -errno;
    }
    //(((struct Client_information *)userdata)->filedatas)[full_path].file_descriptor = sys_ret;
    //DLOG("Download: file_descriptor after %d",(((struct Client_information *)userdata)->filedatas)[full_path].file_descriptor);
    DLOG("buf1 %s",buf);
    ret_code = pread(sys_ret, buf, size, 0);
    DLOG("buf2 %s",buf);
    if (ret_code < 0){
        fxn_ret = -errno;
    }
    //truncte the size in server side
    ret_code = rpc_call_truncate((void *)userdata, path, (off_t) size);
    if (ret_code < 0){
        fxn_ret = -errno;
    }
    //write the data into the server side
    DLOG("buf3 %s",buf);
    ret_code = rpc_call_write((void*)userdata, path, buf, (off_t) size, 0, fi);
    if(ret_code < 0){
        fxn_ret = -errno;
    }
    DLOG("return rpc_call_write: %d",ret_code);

    //Update metadata to the server side
    struct timespec ts[2];

    ts[0] = (struct timespec)(statbuf->st_atim);
    ts[1] = (struct timespec)(statbuf->st_mtim);

    ret_code = rpc_call_utimensat((void *)userdata, path, ts);
    DLOG("return rpc_call_utimensat: %d",ret_code);
    if (ret_code < 0) {
        fxn_ret = -errno;
    }

    ret_code = close(sys_ret);
    if(ret_code < 0) fxn_ret = -errno;

    int rpc_ret = rpc_call_release((void *)userdata, path, fi);
    DLOG("return rpc_call_release: %d",rpc_ret);
    if (rpc_ret < 0) fxn_ret = rpc_ret;

    return fxn_ret;
}


// SETUP AND TEARDOWN
void *watdfs_cli_init(struct fuse_conn_info *conn, const char *path_to_cache,
                      time_t cache_interval, int *ret_code) {
    // TODO: set up the RPC library by calling `rpcClientInit`.
    int  return_code = rpcClientInit();
    // TODO: check the return code of the `rpcClientInit` call
    // `rpcClientInit` may fail, for example, if an incorrect port was exported.

    // It may be useful to print to stderr or stdout during debugging.
    // Important: Make sure you turn off logging prior to submission!
    // One useful technique is to use pre-processor flags like:
    // # ifdef PRINT_ERR
    // std::cerr << "Failed to initialize RPC Client" << std::endl;
    // #endif
    // Tip: Try using a macro for the above to minimize the debugging code.

    // TODO Initialize any global state that you require for the assignment and return it.
    // The value that you return here will be passed as userdata in other functions.
    // In A1, you might not need it, so you can return `nullptr`.
    //void *userdata = nullptr;
    struct Client_information * userdata = new struct Client_information;
    userdata -> cacheInterval = cache_interval;
    int str_len = strlen(path_to_cache) + 1;
    userdata->cachePath = (char *)malloc(str_len);
    strcpy(userdata->cachePath, path_to_cache);

    // TODO: save `path_to_cache` and `cache_interval` (for A3).

    // TODO: set `ret_code` to 0 if everything above succeeded else some appropriate
    // non-zero value.
    if(return_code == 0)
        *ret_code = 0;
    else
        *ret_code = return_code;
    // Return pointer to global state data.
    return (void *) userdata;
}

void watdfs_cli_destroy(void *userdata) {
    // TODO: clean up your userdata state.
    // TODO: tear down the RPC library by calling `rpcClientDestroy`.
        rpcClientDestroy();
        //free(((struct Client_information *)userdata)->cachePath);
    // delete userdata;
        //userdata = NULL;
}

// GET FILE ATTRIBUTES
int watdfs_cli_getattr(void *userdata, const char *path, struct stat *statbuf) {
    // SET UP THE RPC CALL
    //(char *)malloc(size+1);

    int str_len = strlen(((struct Client_information *)userdata)->cachePath) + 1;

    char *cachepath = (char *)malloc(str_len+1);

    strcpy(cachepath, ((struct Client_information *)userdata)->cachePath);
    char *full_path = get_full_path(cachepath, path);

    std::string p = std::string(full_path);

    DLOG("ogetattr_address_0226: %s", full_path);

    int ret_code = rpc_call_getattr(userdata, path, statbuf);
    if(ret_code < 0){
        return ret_code;
    }

    if((((struct Client_information *)userdata)->filedatas).find(p) != (((struct Client_information *)userdata)->filedatas).end()){
        DLOG("IN watdfs_cli_getattr, the client file is open");
        int local_mode = (((struct Client_information*)userdata)->filedatas)[p].client_mode;
        if((local_mode & O_ACCMODE) == O_RDONLY){
            int fresh_check = file_freshness_check(userdata,path);
            if(fresh_check == 0) {
                DLOG("IN watdfs_cli_getattr: client file is fresh, do local stat, update Tc");
                ret_code = stat(full_path, statbuf);
                if(ret_code < 0) {
                    free(full_path);
                    return -errno;
                }
                (((struct Client_information*)userdata)->filedatas)[p].tc = time(0);
                return ret_code;
            }
            else{
                int ret_code = download((Client_information*)userdata, path, full_path);
                if(ret_code < 0) {
                    free(full_path);
                    return ret_code;
                }
                (((struct Client_information*)userdata)->filedatas)[p].tc = time(0);
                ret_code = stat(full_path, statbuf);
                if(ret_code < 0) {
                    free(full_path);
                    memset(statbuf, 0, sizeof(struct stat));
                    return -errno;
                }
                return ret_code;
            }

        }
        else{
            ret_code = stat(full_path, statbuf);
            if(ret_code < 0) {
                free(full_path);
                memset(statbuf, 0, sizeof(struct stat));
                return -errno;
            }
            return ret_code;
        }
    }
    else{
        DLOG("watdfs_cli_getattr: client file exist but is not open, open and copy file, do local stat, close file");
        struct fuse_file_info *fi = new struct fuse_file_info;
        fi->flags = O_RDONLY;
        watdfs_cli_open(userdata,path,fi);
        ret_code = stat(full_path, statbuf);
        if(ret_code < 0) {
            free(full_path);
            memset(statbuf, 0, sizeof(struct stat));
            return -errno;
        }
        watdfs_cli_release(userdata,path,fi);
        return ret_code;

    }
    return 0;

}

// CREATE, OPEN AND CLOSE
int watdfs_cli_mknod(void *userdata, const char *path, mode_t mode, dev_t dev) {

    int str_len = strlen(((struct Client_information *)userdata)->cachePath) + 1;

    char *cachepath = (char *)malloc(str_len+1);

    strcpy(cachepath, ((struct Client_information *)userdata)->cachePath);
    char *full_path = get_full_path(cachepath, path);

    std::string p = std::string(full_path);

    struct stat *statbuf = new struct stat;



    int ret_code = rpc_call_getattr(userdata, path, statbuf);
    if(ret_code < 0){
        DLOG("watdfs_cli_mknod: No file exists in server side , and it also does not exist in client side");
        int ret_code1 = rpc_call_mknod((void *)userdata, path, mode, dev);
        if(ret_code1 < 0){
            return ret_code1;
        }
        DLOG("watdfs_cli_mknod: ret_code1 for rpc_call_mknod %d", ret_code1);
        ret_code1 = mknod(full_path,mode,dev);
        DLOG("watdfs_cli_mknod: ret_code1 for local_mknod %d", ret_code1);
        if(ret_code1 < 0){
            return -errno;
        }
        return 0;
    }

    if(!((((struct Client_information *)userdata)->filedatas).find(p) != (((struct Client_information *)userdata)->filedatas).end())){
        DLOG("IN watdfs_cli_mknod: server file exist but no client file");
        int ret_code1 = mknod(full_path,mode,dev);
        if(ret_code1 < 0){
            return -errno;
        }
        return 0;
    }

    return 0;

}
int watdfs_cli_open(void *userdata, const char *path,
                    struct fuse_file_info *fi) {
    int str_len = strlen(((struct Client_information *)userdata)->cachePath) + 1;
    char *cachepath = (char *)malloc(str_len+1);

    strcpy(cachepath, ((struct Client_information *)userdata)->cachePath);
    char *full_path = get_full_path(cachepath, path);

    std::string p = std::string(full_path);

    DLOG("open_address_0226: %s", full_path);

    if((((struct Client_information *)userdata)->filedatas).find(p) != (((struct Client_information *)userdata)->filedatas).end()){
        free(full_path);
        return -EMFILE;
    }

    //judge whether the file exists in the server side
    int ret_code = 0;
    int fxn_ret = 0;
    struct stat *statbuf = new struct stat;
    ret_code = rpc_call_getattr(userdata, path, statbuf);
    if(ret_code < 0){
        if(((fi->flags) & O_CREAT) != O_CREAT)
            return ret_code;
        ret_code = rpc_call_open(userdata, path, fi);
        if (ret_code < 0) return  ret_code;
    }


    /*if((((fi->flags) & (O_ACCMODE)) == O_RDWR) || (((fi->flags) & (O_ACCMODE)) == O_WRONLY)){
        DLOG("dddddddddddddddddddddddddddddddddddddddddddddddddd");
        int ret_code_1001 = rpc_call_open(userdata,path,fi);
        if(ret_code_1001 == -EACCES){
            DLOG("eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee");
            return ret_code_1001;
        }
        else{
            rpc_call_release(userdata,path,fi);
        }
    }*/

    //struct Filedata file = {fi->flags, ret_code, time(0)};
    //(((struct Client_information*)userdata)->filedatas)[p] = file;
    ret_code = download((Client_information*)userdata, path, full_path);
    DLOG("watdfs_cli_open: return download value %d",ret_code);
    if (ret_code < 0) {
        return ret_code;
    }
    else{
        ret_code = open(full_path, fi->flags);
        if (ret_code < 0) {
            DLOG("watdfs_cli_open: return open value1266 %d",ret_code);
            free(full_path);
            free(statbuf);
            return -errno;
        }
        //DLOG("open_give_value");
        //DLOG("retcode %d",ret_code);
        //DLOG("fi->flag %d",fi->flags);
        //fi->fh = ret_code;
        struct Filedata file = {fi->flags, ret_code, time(0)};
        (((struct Client_information*)userdata)->filedatas)[p] = file;
        //DLOG("watdfs_cli_open: file %d",file.file_descriptor);
    }

    free(full_path);
    free(statbuf);
    return 0;
}

int watdfs_cli_release(void *userdata, const char *path,
                       struct fuse_file_info *fi) {

    int sys_ret = 0;


    int str_len = strlen(((struct Client_information *)userdata)->cachePath) + 1;

    char *cachepath = (char *)malloc(str_len+1);

    strcpy(cachepath, ((struct Client_information *)userdata)->cachePath);
    char *full_path = get_full_path(cachepath, path);

    std::string p = std::string(full_path);

    int file_flag = (((struct Client_information *)userdata)->filedatas)[p].client_mode;

    if (!(O_RDONLY == (file_flag & O_ACCMODE))) {
        // not read only, push the updates to server
        sys_ret = upload((Client_information*)userdata, path, full_path);
        if (sys_ret < 0) return sys_ret;
    }
    DLOG("watdfs_cli_release: %d",(((struct Client_information *)userdata)->filedatas)[p].file_descriptor);
    sys_ret = close((((struct Client_information *)userdata)->filedatas)[p].file_descriptor);
    if (sys_ret < 0) return -errno;

    //bool whether_empty = (((struct Client_information *)userdata)->filedatas).empty();
    //DLOG("whether the userdata is empty1 %s",whether_empty);
    DLOG("I erase the local file here");
    (((struct Client_information *)userdata)->filedatas).erase(p);
    //whether_empty = (((struct Client_information *)userdata)->filedatas).empty();
    //DLOG("whether the userdata is empty2 %s",whether_empty);
    free(full_path);
    return 0;

}

// READ AND WRITE DATA
int watdfs_cli_read(void *userdata, const char *path, char *buf, size_t size,
                    off_t offset, struct fuse_file_info *fi) {
    int sys_ret = 0;


    int str_len = strlen(((struct Client_information *)userdata)->cachePath) + 1;

    char *cachepath = (char *)malloc(str_len+1);

    strcpy(cachepath, ((struct Client_information *)userdata)->cachePath);
    char *full_path = get_full_path(cachepath, path);

    std::string p = std::string(full_path);

    int file_descriptor = (((struct Client_information *)userdata)->filedatas)[p].file_descriptor;

    int file_flag = (((struct Client_information *)userdata)->filedatas)[p].client_mode;
    if((file_flag & O_ACCMODE)!= O_RDONLY){
        DLOG("watdfs_cli_read: file_flag is not O_RDONLY");
        int sys_ret = pread(file_descriptor, buf, size, offset);
        return sys_ret;
    }
    DLOG("watdfs_cli_read: file_flag is %d", (file_flag & O_ACCMODE));

    int fresh_check = file_freshness_check(userdata,path);
    if(fresh_check != 0) {
        DLOG("IN watdfs_cli_getattr: client file is not fresh, download");
        int ret_code = download((Client_information*)userdata, path, full_path);
        if (ret_code < 0) {
            return -EPERM;
        }
        (((struct Client_information*)userdata)->filedatas)[p].tc = time(0);
    }

    //int ret_code = download((Client_information*)userdata, path, full_path);
    file_descriptor = (((struct Client_information *)userdata)->filedatas)[p].file_descriptor;
    //DLOG("watdfs_cli_read: download return code %d",ret_code);
    //if (ret_code < 0) {
    //    return ret_code;
    //}

    int ret_code = open(full_path, O_RDWR);
    if (ret_code < 0) {
        free(full_path);
        return -errno;
    }
    DLOG("watdfs_cli_read: return open value1373 %d",ret_code);
    int ret_code1 = close(ret_code);
    if (ret_code1 < 0) {
        free(full_path);
        return -errno;
    }
    DLOG("watdfs_cli_read: return close value1380 %d",ret_code1);

    //(((struct Client_information*)userdata)->filedatas)[p].tc = time(0);
    DLOG("watdfs_cli_read: file_descriptor %d",file_descriptor);
    int sys_ret1 = pread(file_descriptor, buf, size, offset);
    DLOG("watdfs_cli_read: pread return code %d",sys_ret1);
    return sys_ret1;

}
int watdfs_cli_write(void *userdata, const char *path, const char *buf,
                     size_t size, off_t offset, struct fuse_file_info *fi) {

    int sys_ret = 0;


    int str_len = strlen(((struct Client_information *)userdata)->cachePath) + 1;

    char *cachepath = (char *)malloc(str_len+1);

    strcpy(cachepath, ((struct Client_information *)userdata)->cachePath);
    char *full_path = get_full_path(cachepath, path);

    std::string p = std::string(full_path);

    int file_descriptor = (((struct Client_information *)userdata)->filedatas)[p].file_descriptor;
    DLOG("i operate the write system function here");
    DLOG("buf sentence %s",buf);
    DLOG("offset %d",offset);
    DLOG("file_descriptor %d",file_descriptor);
    DLOG("fi->fh %d",fi->fh);
    int ret_code = pwrite(file_descriptor, buf, size, offset);
    DLOG("ret_code %d",ret_code);
    if(ret_code < 0)
        return -errno;
    int fxn_ret = upload((Client_information*)userdata, path, full_path);
    if(fxn_ret < 0)
        return fxn_ret;
    (((struct Client_information*)userdata)->filedatas)[p].tc = time(0);
    return ret_code;
}

int watdfs_cli_truncate(void *userdata, const char *path, off_t newsize) {
    int sys_ret = 0;


    int str_len = strlen(((struct Client_information *)userdata)->cachePath) + 1;

    char *cachepath = (char *)malloc(str_len+1);

    strcpy(cachepath, ((struct Client_information *)userdata)->cachePath);
    char *full_path = get_full_path(cachepath, path);

    std::string p = std::string(full_path);


    //judge whether the local file is open
    if(!((((struct Client_information *)userdata)->filedatas).find(p) != (((struct Client_information *)userdata)->filedatas).end())){
        int ret_code = download((Client_information*)userdata, path, full_path);
        if (ret_code < 0) {
            return ret_code;
        }
        int sys_ret1 = open(full_path, O_RDWR);
        if (sys_ret1 < 0) {
            return -errno;
        }
        int sys_ret = truncate(full_path, newsize);
        if (sys_ret < 0) {
            return -errno;
        }
        close(sys_ret1);
        return 0;
    }
    //The file is open in write mode: Read calls should not perform freshness checks, as there
        //would be no updates on the server due to write exclusion and this prevents overwriting
        //local file updates if freshness condition has expired. Write calls should perform the
        //freshness checks at the end of writes, as usual.
    else{
        int file_flag = (((struct Client_information *)userdata)->filedatas)[p].client_mode;
        if ((file_flag & O_ACCMODE) != O_RDONLY) {
            int ret_code = truncate(full_path, newsize);
            if (ret_code < 0) {
                return -errno;
            }
            int fxn_ret = upload((Client_information*)userdata, path, full_path);
            if(fxn_ret < 0)
                return fxn_ret;
            (((struct Client_information*)userdata)->filedatas)[p].tc = time(0);
            return fxn_ret;
        }
        else{
            return -EMFILE;;
        }

    }

    return 0;

}

int watdfs_cli_fsync(void *userdata, const char *path,
                     struct fuse_file_info *fi) {
    int sys_ret = 0;


    int str_len = strlen(((struct Client_information *)userdata)->cachePath) + 1;

    char *cachepath = (char *)malloc(str_len+1);

    strcpy(cachepath, ((struct Client_information *)userdata)->cachePath);
    char *full_path = get_full_path(cachepath, path);

    std::string p = std::string(full_path);


    int local_mode = (((struct Client_information*)userdata)->filedatas)[p].client_mode;
    if((local_mode & O_ACCMODE) == O_RDONLY){
        return -EMFILE;
    }

    int fxn_ret = upload((Client_information*)userdata, path, full_path);
    if(fxn_ret < 0)
        return fxn_ret;
    (((struct Client_information*)userdata)->filedatas)[p].tc = time(0);

    free(full_path);
    return fxn_ret;
}

// CHANGE METADATA
int watdfs_cli_utimensat(void *userdata, const char *path,
                       const struct timespec ts[2]) {
    int sys_ret = 0;


    int str_len = strlen(((struct Client_information *)userdata)->cachePath) + 1;

    char *cachepath = (char *)malloc(str_len+1);

    strcpy(cachepath, ((struct Client_information *)userdata)->cachePath);
    char *full_path = get_full_path(cachepath, path);

    std::string p = std::string(full_path);

    if(!((((struct Client_information *)userdata)->filedatas).find(p) != (((struct Client_information *)userdata)->filedatas).end())){

        int ret_code = download((Client_information*)userdata, path, full_path);
        if (ret_code < 0) {
            return ret_code;
        }
        int sys_ret1 = open(full_path, O_RDWR);
        if (sys_ret1 < 0) {
            return -errno;
        }
        int sys_ret = utimensat(0, full_path, ts, 0);
        if (sys_ret < 0) {
            return -errno;
        }
        close(sys_ret1);
        return 0;

    }
    else{
        int file_flag = (((struct Client_information *)userdata)->filedatas)[p].client_mode;
        if ((file_flag & O_ACCMODE) != O_RDONLY) {
            int ret_code = utimensat(0, full_path, ts, 0);
            if (ret_code < 0) {
                return -errno;
            }
            int fxn_ret = upload((Client_information*)userdata, path, full_path);
            if(fxn_ret < 0)
                return fxn_ret;
            (((struct Client_information*)userdata)->filedatas)[p].tc = time(0);
            return fxn_ret;
        }
        else{
            return -EMFILE;;
        }

    }

    return 0;
}


