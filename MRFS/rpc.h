

#ifndef RPC_H
#define RPC_H

// rpc.h
// This file defines all of the RPC related infomation.
#ifdef __cplusplus
extern "C" {
#endif

// TYPES

// Arg types.
#define ARG_CHAR 1
#define ARG_SHORT 2
#define ARG_INT 3
#define ARG_LONG 4
#define ARG_DOUBLE 5
#define ARG_FLOAT 6

// Types for shifting bits.
#define ARG_INPUT 31
#define ARG_OUTPUT 30
#define ARG_ARRAY 29

// The maximum array size.
#define MAX_ARRAY_LEN 65535

// Error codes that might be returned
#define OK 0
#define ALREADY_EXISTS -200
#define NOT_INIT -201
#define FUNCTION_NOT_FOUND -202
#define FUNCTION_FAILURE -203
#define ARRAY_LENS_TOO_LONG -204
#define BAD_TYPES -205
#define TERMINATED -300
#define UNEXPECTED_MSG -301
#define FAILED_TO_SEND -302


typedef int (*skeleton)(int *, void **);
extern int rpcServerInit();

extern int rpcRegister(char *name, int *argTypes, skeleton f);

extern int rpcExecute();

extern int rpcClientInit();

extern int rpcCall(char *name, int *argTypes, void **args);

extern int rpcClientDestroy();

#ifdef __cplusplus
}
#endif

#endif
