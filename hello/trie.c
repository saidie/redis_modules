#include <stdio.h>
#include <redismodule.h>

static RedisModuleType *TrieType;

typedef struct TrieTypeNode {
    uint8_t terminal;
    struct TrieTypeNode* children[26];
} TrieTypeNode;

TrieTypeNode *TrieTypeFind(TrieTypeNode *n, const char *word, uint8_t create) {
    while(*word) {
        uint8_t i = *word - 'a';
        if (!n->children[i])
            if (create)
                n->children[i] = RedisModule_Calloc(1, sizeof(TrieTypeNode));
            else
                return NULL;
        n = n->children[i];
        ++word;
    }
    return n;
}

void TrieTypeInsert(TrieTypeNode *n, const char *word) {
    n = TrieTypeFind(n, word, 1);
    n->terminal = 1;
}

char *TrieTypeComplete(TrieTypeNode *n, const char *prefix, size_t len, char *result, size_t *newlen) {
    *newlen = 0;

    result = RedisModule_Realloc(result, sizeof(char) * (len + 1));
    while (*prefix) {
        int i = *prefix - 'a';
        if (!n->children[i])
            return NULL;
        result[(*newlen)++] = *prefix;

        n = n->children[i];
        ++prefix;
    }

    while (!n->terminal) {
        result[*newlen] = 'a';

        TrieTypeNode** cursor = n->children;
        while (!*cursor)
            ++cursor, ++result[*newlen];

        n = *cursor;
        ++*newlen;
        result = RedisModule_Realloc(result, sizeof(char) * (*newlen + 1));
    }
    result[*newlen] = '\0';

    return result;
}

int TrieTypeExist(TrieTypeNode *n, const char *word) {
    n = TrieTypeFind(n, word, 0);
    return n && n->terminal;
}

char *TrieTypePrettyPrint(TrieTypeNode *n, char *buffer, size_t *len) {
    buffer[(*len)++] = '(';
    buffer = RedisModule_Realloc(buffer, sizeof(char) * (*len + 1));

    if (n->terminal) {
        buffer[(*len)++] = '$';
        buffer = RedisModule_Realloc(buffer, sizeof(char) * (*len + 1));
    }

    TrieTypeNode** cursor = n->children;
    char ch = 'a';
    while (cursor != n->children + 26) {
        if (*cursor) {
            buffer[(*len)++] = ch;
            buffer = RedisModule_Realloc(buffer, sizeof(char) * (*len + 1));
            buffer = TrieTypePrettyPrint(*cursor, buffer, len);
        }
        ++cursor;
        ++ch;
    }
    buffer[(*len)++] = ')';
    buffer = RedisModule_Realloc(buffer, sizeof(char) * (*len + 1));

    return buffer;
}

void HelloTrieType_LoadRecursive(RedisModuleIO *rdb, TrieTypeNode *n) {
    uint64_t u = RedisModule_LoadUnsigned(rdb);
    n->terminal = u & 1;

    TrieTypeNode** cursor = n->children;
    while (u >>= 1) {
        if (u & 1) {
            *cursor = RedisModule_Calloc(1, sizeof(**cursor));
            HelloTrieType_LoadRecursive(rdb, *cursor);
        }
        ++cursor;
    }
}

void *HelloTrieType_Load(RedisModuleIO *rdb, int encver) {
    if (encver != 0)
        return NULL;

    TrieTypeNode *n;
    n = RedisModule_Calloc(1, sizeof(*n));
    HelloTrieType_LoadRecursive(rdb, n);
    return n;
}

void HelloTrieType_Save(RedisModuleIO *rdb, void *value) {
    TrieTypeNode *n = value;

    uint64_t u;
    TrieTypeNode** cursor = n->children + 26;
    while (cursor != n->children) {
        --cursor;
        if (*cursor)
            u |= 1;
        u <<= 1;
    }
    u |= n->terminal;

    RedisModule_SaveUnsigned(rdb, u);

    while(u >>= 1) {
        if (u & 1)
            HelloTrieType_Save(rdb, *cursor);
        ++cursor;
    }
}

char *HelloTrieType_RewriteRecursive(RedisModuleIO *aof, RedisModuleString *key, TrieTypeNode *n, char *buffer, int depth) {
    buffer = RedisModule_Realloc(buffer, sizeof(char) * (depth + 1));

    if (n->terminal && depth > 0) {
        buffer[depth] = '\0';
        RedisModule_EmitAOF(aof, "hello.trie.insert", "sc", key, buffer);
    }

    TrieTypeNode** cursor = n->children;
    char ch = 'a';
    while (cursor != n->children + 26) {
        if (*cursor) {
            buffer[depth] = ch;
            buffer = HelloTrieType_RewriteRecursive(aof, key, *cursor, buffer, depth+1);
        }
        ++cursor;
        ++ch;
    }

    return buffer;
}

void HelloTrieType_Rewrite(RedisModuleIO *aof, RedisModuleString *key, void *value) {
    char *buffer = RedisModule_Calloc(1, sizeof(char));
    buffer = HelloTrieType_RewriteRecursive(aof, key, value, buffer, 0);
    RedisModule_Free(buffer);
}

void HelloTrieType_Digest(RedisModuleDigest *digest, void *value) {
}

void HelloTrieType_Free(void *value) {
    TrieTypeNode *n = value;

    TrieTypeNode** cursor = n->children;
    while (cursor != n->children + 26) {
        if (*cursor)
            HelloTrieType_Free(*cursor);
        ++cursor;
    }
    RedisModule_Free(n->children);
    RedisModule_Free(n);
}

int HelloTrieInsert_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    if (argc != 3) return RedisModule_WrongArity(ctx);

    RedisModuleKey *key = RedisModule_OpenKey(ctx, argv[1],
        REDISMODULE_READ | REDISMODULE_WRITE);

    int type = RedisModule_KeyType(key);
    if (type != REDISMODULE_KEYTYPE_EMPTY && RedisModule_ModuleTypeGetType(key) != TrieType)
        return RedisModule_ReplyWithError(ctx, REDISMODULE_ERRORMSG_WRONGTYPE);

    TrieTypeNode *n;
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

    RedisModuleKey *key = RedisModule_OpenKey(ctx, argv[1], REDISMODULE_READ);

    int type = RedisModule_KeyType(key);
    if (type != REDISMODULE_KEYTYPE_EMPTY && RedisModule_ModuleTypeGetType(key) != TrieType)
        return RedisModule_ReplyWithError(ctx, REDISMODULE_ERRORMSG_WRONGTYPE);

    TrieTypeNode *n;
    size_t len = 0;
    char *buffer = RedisModule_Calloc(1, sizeof(char));
    if (type != REDISMODULE_KEYTYPE_EMPTY) {
        n = RedisModule_ModuleTypeGetValue(key);

        buffer = TrieTypePrettyPrint(n, buffer, &len);
    }

    RedisModuleString *str = RedisModule_CreateString(ctx, buffer, len);
    RedisModule_ReplyWithString(ctx, str);
    RedisModule_FreeString(ctx, str);

    RedisModule_Free(buffer);

    RedisModule_CloseKey(key);

    return REDISMODULE_OK;
}

int HelloTrieExist_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    if (argc != 3) return RedisModule_WrongArity(ctx);

    RedisModuleKey *key = RedisModule_OpenKey(ctx, argv[1], REDISMODULE_READ);

    int type = RedisModule_KeyType(key);
    if (type != REDISMODULE_KEYTYPE_EMPTY && RedisModule_ModuleTypeGetType(key) != TrieType)
        return RedisModule_ReplyWithError(ctx, REDISMODULE_ERRORMSG_WRONGTYPE);

    TrieTypeNode *n;
    uint8_t res = 0;
    if (type != REDISMODULE_KEYTYPE_EMPTY) {
        n = RedisModule_ModuleTypeGetValue(key);

        size_t len;
        const char *word = RedisModule_StringPtrLen(argv[2], &len);

        res = TrieTypeExist(n, word);
    }

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

    TrieTypeNode *n;
    if (type == REDISMODULE_KEYTYPE_EMPTY) {
        RedisModule_ReplyWithNull(ctx);
    } else {
        n = RedisModule_ModuleTypeGetValue(key);

        size_t len;
        const char *prefix = RedisModule_StringPtrLen(argv[2], &len);
        char *result = RedisModule_Alloc(sizeof(char) * (len + 1));

        if (result = TrieTypeComplete(n, prefix, len, result, &len)) {
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

    if (RedisModule_CreateCommand(ctx, "hello.trie.insert",
            HelloTrieInsert_RedisCommand, "write deny-oom", 1, 1, 1) == REDISMODULE_ERR)
        return REDISMODULE_ERR;

    if (RedisModule_CreateCommand(ctx, "hello.trie.pp",
            HelloTriePrettyPrint_RedisCommand, "readonly deny-oom", 1, 1, 1) == REDISMODULE_ERR)
        return REDISMODULE_ERR;

    if (RedisModule_CreateCommand(ctx, "hello.trie.exist",
            HelloTrieExist_RedisCommand, "readonly", 1, 1, 1) == REDISMODULE_ERR)
        return REDISMODULE_ERR;

    if (RedisModule_CreateCommand(ctx, "hello.trie.complete",
            HelloTrieComplete_RedisCommand, "write deny-oom", 1, 1, 1) == REDISMODULE_ERR)
        return REDISMODULE_ERR;

    return REDISMODULE_OK;
}
