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

struct TrieTypeNode *TrieTypeSearch(struct TrieTypeNode *n, const char *word) {
    while (*word) {
        uint8_t i = *word - 'a';
        if (!n->children[i])
            return NULL;
        n = n->children[i];
        ++word;
    }
    return n;
}

size_t TrieTypeComplete(struct TrieTypeNode *n, const char *prefix, size_t len, char **result) {
    size_t newlen = 0;

    *result = RedisModule_Realloc(*result, sizeof(char) * (len + 1));
    while (*prefix) {
        int i = *prefix - 'a';
        if (!n->children[i])
            return 0;
        (*result)[newlen] = *prefix;

        n = n->children[i];
        ++prefix;
        ++newlen;
    }

    while (!n->terminal) {
        *result = RedisModule_Realloc(*result, sizeof(char) * (newlen + 1));
        (*result)[newlen] = 'a';

        struct TrieTypeNode** cursor = n->children;
        while (!*cursor)
            ++cursor, ++(*result)[newlen];

        n = *cursor;
        ++newlen;
    }
    (*result)[newlen] = '\0';

    return newlen;
}

int TrieTypeExist(struct TrieTypeNode *n, const char *word) {
    n = TrieTypeSearch(n, word);
    return n && n->terminal;
}

void TrieTypePrettyPrint(RedisModuleCtx *ctx, RedisModuleString *str, struct TrieTypeNode *n) {
    RedisModule_StringAppendBuffer(ctx, str, "(", 1);
    if (n->terminal) RedisModule_StringAppendBuffer(ctx, str, "$", 1);
    struct TrieTypeNode** cursor = n->children;
    char s[2] = "a";
    while (cursor != n->children + 26) {
        if (*cursor) {
            RedisModule_StringAppendBuffer(ctx, str, s, 1);
            TrieTypePrettyPrint(ctx, str, *cursor);
        }
        ++cursor;
        ++s[0];
    }
    RedisModule_StringAppendBuffer(ctx, str, ")", 1);
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

int HelloTriePrettyPrint_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    if (argc != 2) return RedisModule_WrongArity(ctx);

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

    RedisModuleString *str = RedisModule_CreateString(ctx, NULL, 0);
    TrieTypePrettyPrint(ctx, str, n);

    RedisModule_ReplyWithString(ctx, str);

    RedisModule_FreeString(ctx, str);

    RedisModule_CloseKey(key);

    return REDISMODULE_OK;
}

int HelloTrieExist_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
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

    int res = TrieTypeExist(n, word);

    RedisModule_ReplyWithLongLong(ctx, res);

    RedisModule_CloseKey(key);

    RedisModule_ReplicateVerbatim(ctx);

    return REDISMODULE_OK;
}

int HelloTrieComplete_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    if (argc != 3) return RedisModule_WrongArity(ctx);

    RedisModuleKey *key = RedisModule_OpenKey(ctx, argv[1], REDISMODULE_READ);

    int type = RedisModule_KeyType(key);
    if (type != REDISMODULE_KEYTYPE_EMPTY && RedisModule_ModuleTypeGetType(key) != TrieType)
        return RedisModule_ReplyWithError(ctx, REDISMODULE_ERRORMSG_WRONGTYPE);

    struct TrieTypeNode *n;
    if (type == REDISMODULE_KEYTYPE_EMPTY) {
        RedisModule_ReplyWithNull(ctx);
    } else {
        n = RedisModule_ModuleTypeGetValue(key);

        size_t len;
        const char *prefix = RedisModule_StringPtrLen(argv[2], &len);
        char *result = RedisModule_Alloc(sizeof(char) * (len + 1));

        if (len = TrieTypeComplete(n, prefix, len, &result)) {
            RedisModuleString *s = RedisModule_CreateString(ctx, result, len);
            RedisModule_ReplyWithString(ctx, s);
            RedisModule_Free(s);
        } else {
            RedisModule_ReplyWithNull(ctx);
        }

        RedisModule_Free(result);
    }

    RedisModule_CloseKey(key);

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

    if (RedisModule_CreateCommand(ctx, "hello.trie.pp",
            HelloTriePrettyPrint_RedisCommand, "write deny-oom", 1, 1, 1) == REDISMODULE_ERR)
        return REDISMODULE_ERR;

    if (RedisModule_CreateCommand(ctx, "hello.trie.exist",
            HelloTrieExist_RedisCommand, "write deny-oom", 1, 1, 1) == REDISMODULE_ERR)
        return REDISMODULE_ERR;

    if (RedisModule_CreateCommand(ctx, "hello.trie.complete",
            HelloTrieComplete_RedisCommand, "write deny-oom", 1, 1, 1) == REDISMODULE_ERR)
        return REDISMODULE_ERR;

    return REDISMODULE_OK;
}
