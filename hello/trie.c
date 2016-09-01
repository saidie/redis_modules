#include <stdio.h>
#include <redismodule.h>

static RedisModuleType *TrieType;
static char buffer[1024];

struct TrieTypeNode {
  uint8_t terminal;
  struct TrieTypeNode* children[26];
};

void TrieTypeInsert(struct TrieTypeNode *n, const char *word) {
  while(*word) {
    uint8_t i = *word - 'a';
    if (!n->children[i])
      n->children[i] = RedisModule_Calloc(1, sizeof(struct TrieTypeNode));
    n = n->children[i];
    ++word;
  }
  n->terminal = 1;
}

void TrieTypePrettyPrint(struct TrieTypeNode *n) {
  printf("(");
  if (n->terminal) printf("$");
  struct TrieTypeNode** cursor = n->children;
  char ch = 'a';
  while (cursor != n->children + 26) {
    if (*cursor) {
      printf("%c", ch);
      TrieTypePrettyPrint(*cursor);
    }
    ++cursor;
    ++ch;
  }
  printf(")");
}

void recur(RedisModuleIO *rdb, struct TrieTypeNode *n) {
  uint64_t u = RedisModule_LoadUnsigned(rdb);
  n->terminal = u & 1;
  u >>= 1;
  if (u) {
    struct TrieTypeNode** cursor = n->children;
    while (u) {
      if (u & 1) {
        *cursor = RedisModule_Calloc(1, sizeof(**cursor));
        recur(rdb, *cursor);
      }
      ++cursor;
      u >>= 1;
    }
  }
}

void *HelloTrieType_Load(RedisModuleIO *rdb, int encver) {
  if (encver != 0)
    return NULL;

  struct TrieTypeNode *n;
  n = RedisModule_Calloc(1, sizeof(*n));
  recur(rdb, n);
  return n;
}

void HelloTrieType_Save(RedisModuleIO *rdb, void *value) {
  struct TrieTypeNode *n = value;

  uint64_t u;
  struct TrieTypeNode** cursor = n->children + 26;
  while (cursor != n->children) {
    --cursor;
    if (*cursor)
      u |= 1;
    u <<= 1;
  }
  u |= n->terminal;

  RedisModule_SaveUnsigned(rdb, u);

  u >>= 1;
  while(u) {
    if (u & 1)
      HelloTrieType_Save(rdb, *cursor);
    ++cursor;
    u >>= 1;
  }
}

void dfs(RedisModuleIO *aof, RedisModuleString *key, struct TrieTypeNode *n, int depth) {
  if (n->terminal && depth > 0) {
    buffer[depth] = '\0';
    RedisModule_EmitAOF(aof, "hello.trie.add", "sc", key, buffer);
  }

  struct TrieTypeNode** cursor = n->children;
  char ch = 'a';
  while (cursor != n->children + 26) {
    if (*cursor) {
      buffer[depth] = ch;
      dfs(aof, key, *cursor, depth+1);
    }
    ++cursor;
    ++ch;
  }
}

void HelloTrieType_Rewrite(RedisModuleIO *aof, RedisModuleString *key, void *value) {
  dfs(aof, key, value, 0);
}

void HelloTrieType_Digest(RedisModuleDigest *digest, void *value) {
}

void HelloTrieType_Free(void *value) {
  struct TrieTypeNode *n = value;

  struct TrieTypeNode** cursor = n->children;
  while (cursor != n->children + 26) {
    if (*cursor)
      HelloTrieType_Free(*cursor);
    ++cursor;
  }
  RedisModule_Free(n->children);
  RedisModule_Free(n);
}

int HelloTrieAdd_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
  if (argc != 3) return RedisModule_WrongArity(ctx);

  RedisModuleKey *key = RedisModule_OpenKey(ctx, argv[1],
    REDISMODULE_READ | REDISMODULE_WRITE);

  int type = RedisModule_KeyType(key);
  if (type != REDISMODULE_KEYTYPE_EMPTY && RedisModule_ModuleTypeGetType(key) != TrieType)
    return RedisModule_ReplyWithError(ctx, REDISMODULE_ERRORMSG_WRONGTYPE);

  struct TrieTypeNode *n;
  if (type == REDISMODULE_KEYTYPE_EMPTY) {
    n = RedisModule_Calloc(1, sizeof(*n));
    RedisModule_ModuleTypeSetValue(key, TrieType, n);
  } else {
    n = RedisModule_ModuleTypeGetValue(key);
  }

  size_t len;
  const char *word = RedisModule_StringPtrLen(argv[2], &len);

  TrieTypeInsert(n, word);

  RedisModule_ReplyWithNull(ctx);

  RedisModule_CloseKey(key);

  RedisModule_ReplicateVerbatim(ctx);

  return REDISMODULE_OK;
}

int RedisModule_OnLoad(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
  if (RedisModule_Init(ctx, "hello", 1, REDISMODULE_APIVER_1) == REDISMODULE_ERR)
    return REDISMODULE_ERR;

  TrieType = RedisModule_CreateDataType(ctx, "hellotrie", 0, HelloTrieType_Load, HelloTrieType_Save,
    HelloTrieType_Rewrite, HelloTrieType_Digest, HelloTrieType_Free);
  if (TrieType == NULL)
    return REDISMODULE_ERR;

  if (RedisModule_CreateCommand(ctx, "hello.trie.add",
      HelloTrieAdd_RedisCommand, "write deny-oom", 1, 1, 1) == REDISMODULE_ERR)
    return REDISMODULE_ERR;

  return REDISMODULE_OK;
}
