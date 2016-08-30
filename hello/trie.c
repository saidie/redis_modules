#include <redismodule.h>

static RedisModuleType *TrieType;

void *HelloTrieType_Load(RedisModuleIO *rdb, int encver) {
}

void HelloTrieType_Save(RedisModuleIO *rdb, void *value) {
}

void HelloTrieType_Rewrite(RedisModuleIO *aof, RedisModuleString *key, void *value) {
}

void HelloTrieType_Digest(RedisModuleDigest *digest, void *value) {
}

void HelloTrieType_Free(void *value) {
}

int RedisModule_OnLoad(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
  if (RedisModule_Init(ctx, "hello", 1, REDISMODULE_APIVER_1) == REDISMODULE_ERR)
    return REDISMODULE_ERR;

  TrieType = RedisModule_CreateDataType(ctx, "hellotrie", 0, HelloTrieType_Load, HelloTrieType_Save,
    HelloTrieType_Rewrite, HelloTrieType_Digest, HelloTrieType_Free);
  if (TrieType == NULL)
    return REDISMODULE_ERR;

  return REDISMODULE_OK;
}
