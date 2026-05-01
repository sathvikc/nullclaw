//! Knowledge Graph memory — entity-relation store backed by SQLite with recursive CTEs.
//!
//! Schema:
//!   kg_entities   (id TEXT PRIMARY KEY, type TEXT NOT NULL, content TEXT NOT NULL, created_at TEXT NOT NULL)
//!   kg_relations  (id TEXT PRIMARY KEY, subject_id TEXT NOT NULL, predicate TEXT NOT NULL, object_id TEXT NOT NULL, created_at TEXT NOT NULL)
//!   kg_entities_fts (FTS5 virtual table on kg_entities.content)
//!
//! Graph traversal via recursive CTE:
//!   WITH RECURSIVE traversal(id, depth) AS (
//!       SELECT id, 0 FROM kg_entities WHERE id = ?1
//!       UNION ALL
//!       SELECT r.object_id, t.depth + 1 FROM kg_relations r, traversal t
//!        WHERE r.subject_id = t.id AND t.depth < ?2
//!   ) SELECT e.* FROM kg_entities e, traversal t WHERE e.id = t.id;
//!
//! Recall query encoding:
//!   "kg:traverse:{entity_id}:{max_depth}"  — BFS graph traversal from entity
//!   "kg:path:{from}:{to}:{max_depth}"     — find path between two entities
//!   "kg:relations:{entity_id}"             — all edges for an entity
//!   plain text                             — FTS5 search on entity content
//!
//! Query arguments and relation-key segments use percent-encoding for reserved
//! bytes; prefer `KgMemory.traverseQuery`, `pathQuery`, `relationsQuery`, and
//! `relationStoreKey` when composing these strings programmatically.

const std = @import("std");
const std_compat = @import("compat");
const build_options = @import("build_options");
const root = @import("../root.zig");
const url_percent = @import("../../url_percent.zig");
const Memory = root.Memory;
const MemoryCategory = root.MemoryCategory;
const MemoryEntry = root.MemoryEntry;
const log = std.log.scoped(.memory_kg);
const sqlite_mod = if (build_options.enable_sqlite) @import("sqlite.zig") else @import("sqlite_disabled.zig");
const c = sqlite_mod.c;
const SQLITE_STATIC = sqlite_mod.SQLITE_STATIC;

const ENTITY_STORE_PREFIX = "__kg:entity:";
const RELATION_STORE_PREFIX = "__kg:rel:";
const TRAVERSE_QUERY_PREFIX = "kg:traverse:";
const PATH_QUERY_PREFIX = "kg:path:";
const RELATIONS_QUERY_PREFIX = "kg:relations:";
const RELATION_CATEGORY = "__kg:relation";

const ParsedRelationKey = struct {
    id: []const u8,
    subject_id: []u8,
    predicate: []u8,
    object_id: []u8,

    fn deinit(self: ParsedRelationKey, allocator: std.mem.Allocator) void {
        allocator.free(self.subject_id);
        allocator.free(self.predicate);
        allocator.free(self.object_id);
    }
};

const BUSY_TIMEOUT_MS: c_int = 5000;

pub const KgMemory = struct {
    db: ?*c.sqlite3,
    allocator: std.mem.Allocator,
    owns_self: bool = false,

    const Self = @This();

    pub fn init(allocator: std.mem.Allocator, db_path: [*:0]const u8) !Self {
        const use_wal = sqlite_mod.shouldUseWal(db_path);

        var db: ?*c.sqlite3 = null;
        const rc = c.sqlite3_open(db_path, &db);
        if (rc != c.SQLITE_OK) {
            if (db) |d| _ = c.sqlite3_close(d);
            return error.SqliteOpenFailed;
        }
        errdefer {
            if (db) |d| _ = c.sqlite3_close(d);
        }
        if (db) |d| {
            _ = c.sqlite3_busy_timeout(d, BUSY_TIMEOUT_MS);
        }

        var self_ = Self{ .db = db, .allocator = allocator };
        self_.configurePragmas(use_wal);
        try self_.migrate();
        return self_;
    }

    pub fn deinit(self: *Self) void {
        if (self.db) |db| {
            _ = c.sqlite3_close(db);
            self.db = null;
        }
    }

    fn configurePragmas(self: *Self, use_wal: bool) void {
        const journal_pragma: [:0]const u8 = if (use_wal)
            "PRAGMA journal_mode = WAL;"
        else
            "PRAGMA journal_mode = DELETE;";
        const pragmas = [_][:0]const u8{
            journal_pragma,
            "PRAGMA synchronous  = NORMAL;",
            "PRAGMA temp_store   = MEMORY;",
            "PRAGMA cache_size   = -2000;",
        };
        for (pragmas) |pragma| {
            var err_msg: [*c]u8 = null;
            const rc = c.sqlite3_exec(self.db, pragma, null, null, &err_msg);
            if (rc != c.SQLITE_OK) {
                log.warn("kg pragma failed: {s}", .{if (err_msg) |m| std.mem.span(m) else "unknown"});
                if (err_msg) |msg| c.sqlite3_free(msg);
            }
        }
    }

    fn migrate(self: *Self) !void {
        const sql =
            \\CREATE TABLE IF NOT EXISTS kg_entities (
            \\  id         TEXT PRIMARY KEY,
            \\  type       TEXT NOT NULL DEFAULT 'entity',
            \\  content    TEXT NOT NULL,
            \\  created_at TEXT NOT NULL
            \\);
            \\
            \\CREATE TABLE IF NOT EXISTS kg_relations (
            \\  id         TEXT PRIMARY KEY,
            \\  subject_id TEXT NOT NULL,
            \\  predicate  TEXT NOT NULL,
            \\  object_id  TEXT NOT NULL,
            \\  created_at TEXT NOT NULL
            \\);
            \\
            \\CREATE INDEX IF NOT EXISTS idx_kg_relations_subject ON kg_relations(subject_id);
            \\CREATE INDEX IF NOT EXISTS idx_kg_relations_object  ON kg_relations(object_id);
            \\CREATE INDEX IF NOT EXISTS idx_kg_relations_predicate ON kg_relations(predicate);
            \\
            \\CREATE VIRTUAL TABLE IF NOT EXISTS kg_entities_fts USING fts5(id, content);
        ;
        var err_msg: [*c]u8 = null;
        const rc = c.sqlite3_exec(self.db, sql, null, null, &err_msg);
        if (rc != c.SQLITE_OK) {
            if (err_msg) |msg| {
                log.err("kg migration failed: {s}", .{std.mem.span(msg)});
                c.sqlite3_free(msg);
            }
            return error.MigrationFailed;
        }
    }

    fn getNowTimestamp(allocator: std.mem.Allocator) ![]u8 {
        const ts = std_compat.time.timestamp();
        return std.fmt.allocPrint(allocator, "{d}", .{ts});
    }

    fn execSql(self: *Self, sql: [:0]const u8) !void {
        var err_msg: [*c]u8 = null;
        defer if (err_msg) |msg| c.sqlite3_free(msg);

        const rc = c.sqlite3_exec(self.db, sql, null, null, &err_msg);
        if (rc != c.SQLITE_OK) {
            log.warn("kg exec failed for '{s}': {s}", .{
                sql,
                if (err_msg) |msg| std.mem.span(msg) else "unknown",
            });
            return error.SqlExecFailed;
        }
    }

    fn beginTransaction(self: *Self) !void {
        try self.execSql("BEGIN IMMEDIATE;");
    }

    fn commitTransaction(self: *Self) !void {
        try self.execSql("COMMIT;");
    }

    fn rollbackTransaction(self: *Self) !void {
        try self.execSql("ROLLBACK;");
    }

    fn rollbackTransactionQuiet(self: *Self) void {
        self.rollbackTransaction() catch |err| {
            log.warn("kg rollback failed: {}", .{err});
        };
    }

    fn decodeHexNibble(byte: u8) !u8 {
        return switch (byte) {
            '0'...'9' => byte - '0',
            'a'...'f' => 10 + (byte - 'a'),
            'A'...'F' => 10 + (byte - 'A'),
            else => error.InvalidPercentEncoding,
        };
    }

    fn percentDecode(allocator: std.mem.Allocator, encoded: []const u8) ![]u8 {
        var out: std.ArrayListUnmanaged(u8) = .empty;
        errdefer out.deinit(allocator);

        var i: usize = 0;
        while (i < encoded.len) {
            if (encoded[i] != '%') {
                try out.append(allocator, encoded[i]);
                i += 1;
                continue;
            }

            if (i + 2 >= encoded.len) return error.InvalidPercentEncoding;
            const hi = try decodeHexNibble(encoded[i + 1]);
            const lo = try decodeHexNibble(encoded[i + 2]);
            try out.append(allocator, (hi << 4) | lo);
            i += 3;
        }

        return out.toOwnedSlice(allocator);
    }

    pub fn relationStoreKey(allocator: std.mem.Allocator, subject_id: []const u8, predicate: []const u8, object_id: []const u8) ![]u8 {
        const subject_id_enc = try url_percent.encode(allocator, subject_id);
        defer allocator.free(subject_id_enc);
        const predicate_enc = try url_percent.encode(allocator, predicate);
        defer allocator.free(predicate_enc);
        const object_id_enc = try url_percent.encode(allocator, object_id);
        defer allocator.free(object_id_enc);

        return std.fmt.allocPrint(allocator, RELATION_STORE_PREFIX ++ "{s}:{s}:{s}", .{
            subject_id_enc,
            predicate_enc,
            object_id_enc,
        });
    }

    pub fn traverseQuery(allocator: std.mem.Allocator, entity_id: []const u8, max_depth: usize) ![]u8 {
        const entity_id_enc = try url_percent.encode(allocator, entity_id);
        defer allocator.free(entity_id_enc);

        return std.fmt.allocPrint(allocator, TRAVERSE_QUERY_PREFIX ++ "{s}:{d}", .{ entity_id_enc, max_depth });
    }

    pub fn pathQuery(allocator: std.mem.Allocator, from_id: []const u8, to_id: []const u8, max_depth: usize) ![]u8 {
        const from_id_enc = try url_percent.encode(allocator, from_id);
        defer allocator.free(from_id_enc);
        const to_id_enc = try url_percent.encode(allocator, to_id);
        defer allocator.free(to_id_enc);

        return std.fmt.allocPrint(allocator, PATH_QUERY_PREFIX ++ "{s}:{s}:{d}", .{
            from_id_enc,
            to_id_enc,
            max_depth,
        });
    }

    pub fn relationsQuery(allocator: std.mem.Allocator, entity_id: []const u8) ![]u8 {
        const entity_id_enc = try url_percent.encode(allocator, entity_id);
        defer allocator.free(entity_id_enc);

        return std.fmt.allocPrint(allocator, RELATIONS_QUERY_PREFIX ++ "{s}", .{entity_id_enc});
    }

    fn entityIdForKey(key: []const u8) []const u8 {
        if (std.mem.startsWith(u8, key, ENTITY_STORE_PREFIX)) return key[ENTITY_STORE_PREFIX.len..];
        return key;
    }

    fn relationIdForKey(key: []const u8) []const u8 {
        if (std.mem.startsWith(u8, key, RELATION_STORE_PREFIX)) return key[RELATION_STORE_PREFIX.len..];
        return key;
    }

    fn parseRelationKey(allocator: std.mem.Allocator, key: []const u8) !ParsedRelationKey {
        if (!std.mem.startsWith(u8, key, RELATION_STORE_PREFIX)) return error.InvalidRelationKey;

        const id = relationIdForKey(key);
        var it = std.mem.splitScalar(u8, id, ':');
        const subject_id_enc = it.next() orelse return error.InvalidRelationKey;
        const predicate_enc = it.next() orelse return error.InvalidRelationKey;
        const object_id_enc = it.next() orelse return error.InvalidRelationKey;
        if (it.next() != null) return error.InvalidRelationKey;

        const subject_id = try percentDecode(allocator, subject_id_enc);
        errdefer allocator.free(subject_id);
        const predicate = try percentDecode(allocator, predicate_enc);
        errdefer allocator.free(predicate);
        const object_id = try percentDecode(allocator, object_id_enc);
        errdefer allocator.free(object_id);

        if (subject_id.len == 0 or predicate.len == 0 or object_id.len == 0) {
            return error.InvalidRelationKey;
        }

        return .{
            .id = id,
            .subject_id = subject_id,
            .predicate = predicate,
            .object_id = object_id,
        };
    }

    fn categoryFromOwnedString(allocator: std.mem.Allocator, raw: []u8) MemoryCategory {
        const parsed = MemoryCategory.fromString(raw);
        switch (parsed) {
            .core, .daily, .conversation => {
                allocator.free(raw);
                return parsed;
            },
            .custom => return .{ .custom = raw },
        }
    }

    fn buildFtsQuery(allocator: std.mem.Allocator, query: []const u8) ![]u8 {
        var fts_query: std.ArrayListUnmanaged(u8) = .empty;
        errdefer fts_query.deinit(allocator);

        var iter = std.mem.tokenizeAny(u8, query, " \t\n\r");
        var first = true;
        while (iter.next()) |word| {
            if (!first) {
                try fts_query.appendSlice(allocator, " OR ");
            }
            try fts_query.append(allocator, '"');
            for (word) |ch_byte| {
                if (ch_byte == '"') {
                    try fts_query.appendSlice(allocator, "\"\"");
                } else {
                    try fts_query.append(allocator, ch_byte);
                }
            }
            try fts_query.append(allocator, '"');
            first = false;
        }

        return fts_query.toOwnedSlice(allocator);
    }

    fn deleteEntityFts(self: *Self, id: []const u8) !void {
        const sql = "DELETE FROM kg_entities_fts WHERE id = ?1";
        var stmt: ?*c.sqlite3_stmt = null;
        var rc = c.sqlite3_prepare_v2(self.db, sql, -1, &stmt, null);
        if (rc != c.SQLITE_OK) return error.PrepareFailed;
        defer _ = c.sqlite3_finalize(stmt);

        _ = c.sqlite3_bind_text(stmt, 1, id.ptr, @intCast(id.len), SQLITE_STATIC);
        rc = c.sqlite3_step(stmt);
        if (rc != c.SQLITE_DONE) return error.StepFailed;
    }

    fn refreshEntityFts(self: *Self, id: []const u8, content: []const u8) !void {
        try self.deleteEntityFts(id);

        const sql = "INSERT INTO kg_entities_fts (id, content) VALUES (?1, ?2)";
        var stmt: ?*c.sqlite3_stmt = null;
        var rc = c.sqlite3_prepare_v2(self.db, sql, -1, &stmt, null);
        if (rc != c.SQLITE_OK) return error.PrepareFailed;
        defer _ = c.sqlite3_finalize(stmt);

        _ = c.sqlite3_bind_text(stmt, 1, id.ptr, @intCast(id.len), SQLITE_STATIC);
        _ = c.sqlite3_bind_text(stmt, 2, content.ptr, @intCast(content.len), SQLITE_STATIC);
        rc = c.sqlite3_step(stmt);
        if (rc != c.SQLITE_DONE) return error.StepFailed;
    }

    // ── Graph operations ──────────────────────────────────────────────

    fn storeEntity(self: *Self, id: []const u8, entity_type: []const u8, content: []const u8) !void {
        const now = try getNowTimestamp(self.allocator);
        defer self.allocator.free(now);

        try self.beginTransaction();
        var committed = false;
        errdefer if (!committed) self.rollbackTransactionQuiet();

        const sql = "INSERT INTO kg_entities (id, type, content, created_at) VALUES (?1, ?2, ?3, ?4) " ++
            "ON CONFLICT(id) DO UPDATE SET content = excluded.content, type = excluded.type";
        var stmt: ?*c.sqlite3_stmt = null;
        var rc = c.sqlite3_prepare_v2(self.db, sql, -1, &stmt, null);
        if (rc != c.SQLITE_OK) return error.PrepareFailed;
        defer _ = c.sqlite3_finalize(stmt);

        _ = c.sqlite3_bind_text(stmt, 1, id.ptr, @intCast(id.len), SQLITE_STATIC);
        _ = c.sqlite3_bind_text(stmt, 2, entity_type.ptr, @intCast(entity_type.len), SQLITE_STATIC);
        _ = c.sqlite3_bind_text(stmt, 3, content.ptr, @intCast(content.len), SQLITE_STATIC);
        _ = c.sqlite3_bind_text(stmt, 4, now.ptr, @intCast(now.len), SQLITE_STATIC);

        rc = c.sqlite3_step(stmt);
        if (rc != c.SQLITE_DONE) return error.StepFailed;

        try self.refreshEntityFts(id, content);
        try self.commitTransaction();
        committed = true;
    }

    fn storeRelation(self: *Self, id: []const u8, subject_id: []const u8, predicate: []const u8, object_id: []const u8) !void {
        const now = try getNowTimestamp(self.allocator);
        defer self.allocator.free(now);

        const sql = "INSERT INTO kg_relations (id, subject_id, predicate, object_id, created_at) VALUES (?1, ?2, ?3, ?4, ?5) " ++
            "ON CONFLICT(id) DO UPDATE SET " ++
            "subject_id = excluded.subject_id, " ++
            "predicate = excluded.predicate, " ++
            "object_id = excluded.object_id";
        var stmt: ?*c.sqlite3_stmt = null;
        var rc = c.sqlite3_prepare_v2(self.db, sql, -1, &stmt, null);
        if (rc != c.SQLITE_OK) return error.PrepareFailed;
        defer _ = c.sqlite3_finalize(stmt);

        _ = c.sqlite3_bind_text(stmt, 1, id.ptr, @intCast(id.len), SQLITE_STATIC);
        _ = c.sqlite3_bind_text(stmt, 2, subject_id.ptr, @intCast(subject_id.len), SQLITE_STATIC);
        _ = c.sqlite3_bind_text(stmt, 3, predicate.ptr, @intCast(predicate.len), SQLITE_STATIC);
        _ = c.sqlite3_bind_text(stmt, 4, object_id.ptr, @intCast(object_id.len), SQLITE_STATIC);
        _ = c.sqlite3_bind_text(stmt, 5, now.ptr, @intCast(now.len), SQLITE_STATIC);

        rc = c.sqlite3_step(stmt);
        if (rc != c.SQLITE_DONE) return error.StepFailed;
    }

    /// BFS traversal from start_id up to max_depth hops, capped by limit.
    fn traverse(self: *Self, allocator: std.mem.Allocator, start_id: []const u8, max_depth: usize, limit: usize) ![]MemoryEntry {
        var entries: std.ArrayListUnmanaged(MemoryEntry) = .empty;
        errdefer {
            for (entries.items) |*entry| entry.deinit(allocator);
            entries.deinit(allocator);
        }

        const sql =
            \\WITH RECURSIVE traversal(path_ids, id, depth) AS (
            \\  SELECT '|' || hex(CAST(?1 AS BLOB)) || '|', ?1, 0
            \\  UNION ALL
            \\  SELECT t.path_ids || hex(CAST(r.object_id AS BLOB)) || '|', r.object_id, t.depth + 1
            \\   FROM kg_relations r
            \\   JOIN traversal t ON r.subject_id = t.id
            \\   WHERE t.depth < ?2
            \\     AND INSTR(t.path_ids, '|' || hex(CAST(r.object_id AS BLOB)) || '|') = 0
            \\)
            \\, ranked(id, depth) AS (
            \\  SELECT id, MIN(depth)
            \\  FROM traversal
            \\  GROUP BY id
            \\  ORDER BY MIN(depth) ASC, id ASC
            \\  LIMIT ?3
            \\)
            \\SELECT e.id, e.type, e.content, e.created_at
            \\FROM ranked t
            \\JOIN kg_entities e ON e.id = t.id
            \\ORDER BY t.depth ASC, e.id ASC
        ;

        var stmt: ?*c.sqlite3_stmt = null;
        if (c.sqlite3_prepare_v2(self.db, sql, -1, &stmt, null) != c.SQLITE_OK) return error.PrepareFailed;
        defer _ = c.sqlite3_finalize(stmt);

        _ = c.sqlite3_bind_text(stmt, 1, start_id.ptr, @intCast(start_id.len), SQLITE_STATIC);
        _ = c.sqlite3_bind_int64(stmt, 2, @intCast(max_depth));
        _ = c.sqlite3_bind_int64(stmt, 3, @intCast(limit));

        while (c.sqlite3_step(stmt) == c.SQLITE_ROW) {
            const entry = try self.readEntityFromRow(stmt.?, allocator);
            try entries.append(allocator, entry);
        }

        return entries.toOwnedSlice(allocator);
    }

    /// BFS path finding from from_id to to_id up to max_depth, capped by limit.
    /// Returns entities along the path.
    fn findPath(self: *Self, allocator: std.mem.Allocator, from_id: []const u8, to_id: []const u8, max_depth: usize, limit: usize) ![]MemoryEntry {
        var entries: std.ArrayListUnmanaged(MemoryEntry) = .empty;
        errdefer {
            for (entries.items) |*entry| entry.deinit(allocator);
            entries.deinit(allocator);
        }

        const sql =
            \\WITH RECURSIVE path(path_ids, id, depth) AS (
            \\  SELECT '|' || hex(CAST(?1 AS BLOB)) || '|', ?1, 0
            \\  UNION ALL
            \\  SELECT p.path_ids || hex(CAST(r.object_id AS BLOB)) || '|', r.object_id, p.depth + 1
            \\   FROM kg_relations r
            \\   JOIN path p ON r.subject_id = p.id
            \\   WHERE r.subject_id = p.id AND p.depth < ?3
            \\     AND INSTR(p.path_ids, '|' || hex(CAST(r.object_id AS BLOB)) || '|') = 0
            \\)
            \\, target(path_ids) AS (
            \\  SELECT path_ids
            \\  FROM path
            \\  WHERE id = ?2
            \\  ORDER BY depth ASC
            \\  LIMIT 1
            \\)
            \\, split(rest, node_hex, ord) AS (
            \\  SELECT substr(path_ids, 2), '', 0
            \\  FROM target
            \\  UNION ALL
            \\  SELECT substr(rest, instr(rest, '|') + 1),
            \\         substr(rest, 1, instr(rest, '|') - 1),
            \\         ord + 1
            \\  FROM split
            \\  WHERE rest <> ''
            \\)
            \\SELECT e.id, e.type, e.content, e.created_at
            \\FROM split s
            \\JOIN kg_entities e ON hex(CAST(e.id AS BLOB)) = s.node_hex
            \\WHERE s.node_hex <> ''
            \\ORDER BY s.ord ASC
            \\LIMIT ?4
        ;

        var stmt: ?*c.sqlite3_stmt = null;
        if (c.sqlite3_prepare_v2(self.db, sql, -1, &stmt, null) != c.SQLITE_OK) return error.PrepareFailed;
        defer _ = c.sqlite3_finalize(stmt);

        _ = c.sqlite3_bind_text(stmt, 1, from_id.ptr, @intCast(from_id.len), SQLITE_STATIC);
        _ = c.sqlite3_bind_text(stmt, 2, to_id.ptr, @intCast(to_id.len), SQLITE_STATIC);
        _ = c.sqlite3_bind_int64(stmt, 3, @intCast(max_depth));
        _ = c.sqlite3_bind_int64(stmt, 4, @intCast(limit));

        while (c.sqlite3_step(stmt) == c.SQLITE_ROW) {
            const entry = try self.readEntityFromRow(stmt.?, allocator);
            try entries.append(allocator, entry);
        }

        return entries.toOwnedSlice(allocator);
    }

    /// All relations (incoming + outgoing) for an entity.
    fn getRelations(self: *Self, allocator: std.mem.Allocator, entity_id: []const u8, limit: usize) ![]MemoryEntry {
        var entries: std.ArrayListUnmanaged(MemoryEntry) = .empty;
        errdefer {
            for (entries.items) |*entry| entry.deinit(allocator);
            entries.deinit(allocator);
        }

        const sql =
            \\SELECT r.id, r.subject_id, r.predicate, r.object_id, r.created_at
            \\FROM kg_relations r
            \\WHERE r.subject_id = ?1 OR r.object_id = ?1
            \\ORDER BY r.created_at DESC
            \\LIMIT ?2
        ;

        var stmt: ?*c.sqlite3_stmt = null;
        if (c.sqlite3_prepare_v2(self.db, sql, -1, &stmt, null) != c.SQLITE_OK) return error.PrepareFailed;
        defer _ = c.sqlite3_finalize(stmt);

        _ = c.sqlite3_bind_text(stmt, 1, entity_id.ptr, @intCast(entity_id.len), SQLITE_STATIC);
        _ = c.sqlite3_bind_int64(stmt, 2, @intCast(limit));

        while (c.sqlite3_step(stmt) == c.SQLITE_ROW) {
            try entries.append(allocator, try self.readRelationFromRow(stmt.?, allocator));
        }

        return entries.toOwnedSlice(allocator);
    }

    fn appendEntityList(self: *Self, entries: *std.ArrayListUnmanaged(MemoryEntry), allocator: std.mem.Allocator, category: ?MemoryCategory) !void {
        const sql = if (category) |_|
            "SELECT id, type, content, created_at FROM kg_entities WHERE type = ?1 ORDER BY created_at DESC, id DESC"
        else
            "SELECT id, type, content, created_at FROM kg_entities ORDER BY created_at DESC, id DESC";

        var stmt: ?*c.sqlite3_stmt = null;
        if (c.sqlite3_prepare_v2(self.db, sql, -1, &stmt, null) != c.SQLITE_OK) return error.PrepareFailed;
        defer _ = c.sqlite3_finalize(stmt);

        if (category) |cat| {
            const cat_str = cat.toString();
            _ = c.sqlite3_bind_text(stmt, 1, cat_str.ptr, @intCast(cat_str.len), SQLITE_STATIC);
        }

        while (c.sqlite3_step(stmt) == c.SQLITE_ROW) {
            try entries.append(allocator, try self.readEntityFromRow(stmt.?, allocator));
        }
    }

    fn appendAllList(self: *Self, entries: *std.ArrayListUnmanaged(MemoryEntry), allocator: std.mem.Allocator) !void {
        const sql =
            \\SELECT kind, id, type, content, subject_id, predicate, object_id, created_at
            \\FROM (
            \\  SELECT 0 AS kind, id, type, content, NULL AS subject_id, NULL AS predicate, NULL AS object_id, created_at
            \\  FROM kg_entities
            \\  UNION ALL
            \\  SELECT 1 AS kind, id, NULL AS type, NULL AS content, subject_id, predicate, object_id, created_at
            \\  FROM kg_relations
            \\)
            \\ORDER BY created_at DESC, id DESC
        ;

        var stmt: ?*c.sqlite3_stmt = null;
        if (c.sqlite3_prepare_v2(self.db, sql, -1, &stmt, null) != c.SQLITE_OK) return error.PrepareFailed;
        defer _ = c.sqlite3_finalize(stmt);

        while (c.sqlite3_step(stmt) == c.SQLITE_ROW) {
            if (c.sqlite3_column_int(stmt, 0) == 0) {
                try entries.append(allocator, try self.readEntityFromMixedRow(stmt.?, allocator));
            } else {
                try entries.append(allocator, try self.readRelationFromMixedRow(stmt.?, allocator));
            }
        }
    }

    fn deleteRelationsForEntity(self: *Self, entity_id: []const u8) !void {
        const sql = "DELETE FROM kg_relations WHERE subject_id = ?1 OR object_id = ?1";
        var stmt: ?*c.sqlite3_stmt = null;
        var rc = c.sqlite3_prepare_v2(self.db, sql, -1, &stmt, null);
        if (rc != c.SQLITE_OK) return error.PrepareFailed;
        defer _ = c.sqlite3_finalize(stmt);

        _ = c.sqlite3_bind_text(stmt, 1, entity_id.ptr, @intCast(entity_id.len), SQLITE_STATIC);
        rc = c.sqlite3_step(stmt);
        if (rc != c.SQLITE_DONE) return error.StepFailed;
    }

    fn getRelationById(self: *Self, allocator: std.mem.Allocator, relation_id: []const u8) !?MemoryEntry {
        const sql = "SELECT id, subject_id, predicate, object_id, created_at FROM kg_relations WHERE id = ?1";
        var stmt: ?*c.sqlite3_stmt = null;
        var rc = c.sqlite3_prepare_v2(self.db, sql, -1, &stmt, null);
        if (rc != c.SQLITE_OK) return error.PrepareFailed;
        defer _ = c.sqlite3_finalize(stmt);

        _ = c.sqlite3_bind_text(stmt, 1, relation_id.ptr, @intCast(relation_id.len), SQLITE_STATIC);

        rc = c.sqlite3_step(stmt);
        if (rc == c.SQLITE_ROW) {
            return try self.readRelationFromRow(stmt.?, allocator);
        }
        return null;
    }

    /// FTS5 search on entity content.
    fn ftsSearch(self: *Self, allocator: std.mem.Allocator, query: []const u8, limit: usize) ![]MemoryEntry {
        const fts_query = try buildFtsQuery(allocator, query);
        defer allocator.free(fts_query);

        if (fts_query.len == 0) return allocator.alloc(MemoryEntry, 0);

        var entries: std.ArrayListUnmanaged(MemoryEntry) = .empty;
        errdefer {
            for (entries.items) |*entry| entry.deinit(allocator);
            entries.deinit(allocator);
        }

        const sql =
            \\SELECT e.id, e.type, e.content, e.created_at
            \\FROM kg_entities e
            \\JOIN kg_entities_fts f ON e.id = f.id
            \\WHERE kg_entities_fts MATCH ?1
            \\ORDER BY bm25(kg_entities_fts) ASC
            \\LIMIT ?2
        ;

        var stmt: ?*c.sqlite3_stmt = null;
        if (c.sqlite3_prepare_v2(self.db, sql, -1, &stmt, null) != c.SQLITE_OK) return error.PrepareFailed;
        defer _ = c.sqlite3_finalize(stmt);

        _ = c.sqlite3_bind_text(stmt, 1, fts_query.ptr, @intCast(fts_query.len), SQLITE_STATIC);
        _ = c.sqlite3_bind_int64(stmt, 2, @intCast(limit));

        while (c.sqlite3_step(stmt) == c.SQLITE_ROW) {
            const entry = try self.readEntityFromRow(stmt.?, allocator);
            try entries.append(allocator, entry);
        }

        return entries.toOwnedSlice(allocator);
    }

    fn readEntityFromRow(_: *Self, stmt: *c.sqlite3_stmt, allocator: std.mem.Allocator) !MemoryEntry {
        const id_ptr = c.sqlite3_column_text(stmt, 0);
        const type_ptr = c.sqlite3_column_text(stmt, 1);
        const content_ptr = c.sqlite3_column_text(stmt, 2);
        const created_ptr = c.sqlite3_column_text(stmt, 3);

        if (id_ptr == null or type_ptr == null or content_ptr == null or created_ptr == null) {
            return error.StepFailed;
        }

        const id = try allocator.dupe(u8, std.mem.span(id_ptr));
        errdefer allocator.free(id);

        const type_str = try allocator.dupe(u8, std.mem.span(type_ptr));
        errdefer allocator.free(type_str);

        const content = try allocator.dupe(u8, std.mem.span(content_ptr));
        errdefer allocator.free(content);

        const created_at = try allocator.dupe(u8, std.mem.span(created_ptr));
        errdefer allocator.free(created_at);

        const key = try allocator.dupe(u8, id);
        errdefer allocator.free(key);

        return MemoryEntry{
            .id = id,
            .key = key,
            .content = content,
            .category = categoryFromOwnedString(allocator, type_str),
            .timestamp = created_at,
        };
    }

    fn readRelationFromRow(_: *Self, stmt: *c.sqlite3_stmt, allocator: std.mem.Allocator) !MemoryEntry {
        const id_ptr = c.sqlite3_column_text(stmt, 0);
        const subject_ptr = c.sqlite3_column_text(stmt, 1);
        const predicate_ptr = c.sqlite3_column_text(stmt, 2);
        const object_ptr = c.sqlite3_column_text(stmt, 3);
        const created_ptr = c.sqlite3_column_text(stmt, 4);

        if (id_ptr == null or subject_ptr == null or predicate_ptr == null or object_ptr == null or created_ptr == null) {
            return error.StepFailed;
        }

        const id = try allocator.dupe(u8, std.mem.span(id_ptr));
        errdefer allocator.free(id);

        const subject_id = try allocator.dupe(u8, std.mem.span(subject_ptr));
        defer allocator.free(subject_id);

        const predicate = try allocator.dupe(u8, std.mem.span(predicate_ptr));
        defer allocator.free(predicate);

        const object_id = try allocator.dupe(u8, std.mem.span(object_ptr));
        defer allocator.free(object_id);

        const created_at = try allocator.dupe(u8, std.mem.span(created_ptr));
        errdefer allocator.free(created_at);

        const key = try std.fmt.allocPrint(allocator, RELATION_STORE_PREFIX ++ "{s}", .{id});
        errdefer allocator.free(key);

        const content = try std.fmt.allocPrint(allocator, "{s} --{s}--> {s}", .{
            subject_id,
            predicate,
            object_id,
        });
        errdefer allocator.free(content);

        const category_name = try allocator.dupe(u8, RELATION_CATEGORY);
        errdefer allocator.free(category_name);

        return MemoryEntry{
            .id = id,
            .key = key,
            .content = content,
            .category = .{ .custom = category_name },
            .timestamp = created_at,
        };
    }

    fn readEntityFromMixedRow(_: *Self, stmt: *c.sqlite3_stmt, allocator: std.mem.Allocator) !MemoryEntry {
        const id_ptr = c.sqlite3_column_text(stmt, 1);
        const type_ptr = c.sqlite3_column_text(stmt, 2);
        const content_ptr = c.sqlite3_column_text(stmt, 3);
        const created_ptr = c.sqlite3_column_text(stmt, 7);

        if (id_ptr == null or type_ptr == null or content_ptr == null or created_ptr == null) {
            return error.StepFailed;
        }

        const id = try allocator.dupe(u8, std.mem.span(id_ptr));
        errdefer allocator.free(id);
        const type_str = try allocator.dupe(u8, std.mem.span(type_ptr));
        errdefer allocator.free(type_str);
        const content = try allocator.dupe(u8, std.mem.span(content_ptr));
        errdefer allocator.free(content);
        const created_at = try allocator.dupe(u8, std.mem.span(created_ptr));
        errdefer allocator.free(created_at);
        const key = try allocator.dupe(u8, id);
        errdefer allocator.free(key);

        return MemoryEntry{
            .id = id,
            .key = key,
            .content = content,
            .category = categoryFromOwnedString(allocator, type_str),
            .timestamp = created_at,
        };
    }

    fn readRelationFromMixedRow(_: *Self, stmt: *c.sqlite3_stmt, allocator: std.mem.Allocator) !MemoryEntry {
        const id_ptr = c.sqlite3_column_text(stmt, 1);
        const subject_ptr = c.sqlite3_column_text(stmt, 4);
        const predicate_ptr = c.sqlite3_column_text(stmt, 5);
        const object_ptr = c.sqlite3_column_text(stmt, 6);
        const created_ptr = c.sqlite3_column_text(stmt, 7);

        if (id_ptr == null or subject_ptr == null or predicate_ptr == null or object_ptr == null or created_ptr == null) {
            return error.StepFailed;
        }

        const id = try allocator.dupe(u8, std.mem.span(id_ptr));
        errdefer allocator.free(id);

        const subject_id = try allocator.dupe(u8, std.mem.span(subject_ptr));
        defer allocator.free(subject_id);
        const predicate = try allocator.dupe(u8, std.mem.span(predicate_ptr));
        defer allocator.free(predicate);
        const object_id = try allocator.dupe(u8, std.mem.span(object_ptr));
        defer allocator.free(object_id);

        const created_at = try allocator.dupe(u8, std.mem.span(created_ptr));
        errdefer allocator.free(created_at);
        const key = try std.fmt.allocPrint(allocator, RELATION_STORE_PREFIX ++ "{s}", .{id});
        errdefer allocator.free(key);
        const content = try std.fmt.allocPrint(allocator, "{s} --{s}--> {s}", .{
            subject_id,
            predicate,
            object_id,
        });
        errdefer allocator.free(content);
        const category_name = try allocator.dupe(u8, RELATION_CATEGORY);
        errdefer allocator.free(category_name);

        return MemoryEntry{
            .id = id,
            .key = key,
            .content = content,
            .category = .{ .custom = category_name },
            .timestamp = created_at,
        };
    }

    // ── VTable implementations ────────────────────────────────────────

    fn implName(_: *anyopaque) []const u8 {
        return "kg";
    }

    fn implStore(ptr: *anyopaque, key: []const u8, content: []const u8, category: MemoryCategory, _: ?[]const u8) anyerror!void {
        const self_: *Self = @ptrCast(@alignCast(ptr));
        const cat_str = category.toString();

        if (std.mem.startsWith(u8, key, ENTITY_STORE_PREFIX)) {
            const entity_id = entityIdForKey(key);
            try self_.storeEntity(entity_id, cat_str, content);
        } else if (std.mem.startsWith(u8, key, RELATION_STORE_PREFIX)) {
            const parsed = try parseRelationKey(self_.allocator, key);
            defer parsed.deinit(self_.allocator);
            try self_.storeRelation(parsed.id, parsed.subject_id, parsed.predicate, parsed.object_id);
        } else {
            // Generic key — treat as entity
            try self_.storeEntity(key, cat_str, content);
        }
    }

    fn implRecall(ptr: *anyopaque, allocator: std.mem.Allocator, query: []const u8, limit: usize, _: ?[]const u8) anyerror![]MemoryEntry {
        const self_: *Self = @ptrCast(@alignCast(ptr));

        const trimmed = std.mem.trim(u8, query, " \t\n\r");
        if (trimmed.len == 0) return allocator.alloc(MemoryEntry, 0);

        if (std.mem.startsWith(u8, trimmed, TRAVERSE_QUERY_PREFIX)) {
            const args = trimmed[TRAVERSE_QUERY_PREFIX.len..];
            var it = std.mem.splitScalar(u8, args, ':');
            const entity_id_enc = it.next() orelse return allocator.alloc(MemoryEntry, 0);
            const depth_str = it.next() orelse "3";
            if (it.next() != null) return error.InvalidKgQuery;
            const entity_id = try percentDecode(allocator, entity_id_enc);
            defer allocator.free(entity_id);
            const max_depth = std.fmt.parseInt(usize, depth_str, 10) catch 3;
            return self_.traverse(allocator, entity_id, max_depth, limit);
        }

        if (std.mem.startsWith(u8, trimmed, PATH_QUERY_PREFIX)) {
            const args = trimmed[PATH_QUERY_PREFIX.len..];
            var it = std.mem.splitScalar(u8, args, ':');
            const from_id_enc = it.next() orelse return allocator.alloc(MemoryEntry, 0);
            const to_id_enc = it.next() orelse return allocator.alloc(MemoryEntry, 0);
            const depth_str = it.next() orelse "5";
            if (it.next() != null) return error.InvalidKgQuery;
            const from_id = try percentDecode(allocator, from_id_enc);
            defer allocator.free(from_id);
            const to_id = try percentDecode(allocator, to_id_enc);
            defer allocator.free(to_id);
            const max_depth = std.fmt.parseInt(usize, depth_str, 10) catch 5;
            return self_.findPath(allocator, from_id, to_id, max_depth, limit);
        }

        if (std.mem.startsWith(u8, trimmed, RELATIONS_QUERY_PREFIX)) {
            const entity_id_enc = trimmed[RELATIONS_QUERY_PREFIX.len..];
            if (entity_id_enc.len == 0) return allocator.alloc(MemoryEntry, 0);
            const entity_id = try percentDecode(allocator, entity_id_enc);
            defer allocator.free(entity_id);
            return self_.getRelations(allocator, entity_id, limit);
        }

        // Fall back to FTS5 content search
        return self_.ftsSearch(allocator, trimmed, limit);
    }

    fn implGet(ptr: *anyopaque, allocator: std.mem.Allocator, key: []const u8) anyerror!?MemoryEntry {
        const self_: *Self = @ptrCast(@alignCast(ptr));
        const entity_id = entityIdForKey(key);

        const sql = "SELECT id, type, content, created_at FROM kg_entities WHERE id = ?1";
        var stmt: ?*c.sqlite3_stmt = null;
        var rc = c.sqlite3_prepare_v2(self_.db, sql, -1, &stmt, null);
        if (rc != c.SQLITE_OK) return error.PrepareFailed;
        defer _ = c.sqlite3_finalize(stmt);

        _ = c.sqlite3_bind_text(stmt, 1, entity_id.ptr, @intCast(entity_id.len), SQLITE_STATIC);

        rc = c.sqlite3_step(stmt);
        if (rc == c.SQLITE_ROW) {
            return try self_.readEntityFromRow(stmt.?, allocator);
        }
        return try self_.getRelationById(allocator, relationIdForKey(key));
    }

    fn implList(ptr: *anyopaque, allocator: std.mem.Allocator, category: ?MemoryCategory, _: ?[]const u8) anyerror![]MemoryEntry {
        const self_: *Self = @ptrCast(@alignCast(ptr));

        var entries: std.ArrayListUnmanaged(MemoryEntry) = .empty;
        errdefer {
            for (entries.items) |*entry| entry.deinit(allocator);
            entries.deinit(allocator);
        }

        if (category) |cat| {
            try self_.appendEntityList(&entries, allocator, cat);
        } else {
            try self_.appendAllList(&entries, allocator);
        }

        return entries.toOwnedSlice(allocator);
    }

    fn implForget(ptr: *anyopaque, key: []const u8) anyerror!bool {
        const self_: *Self = @ptrCast(@alignCast(ptr));
        const entity_id = entityIdForKey(key);

        // Try to delete as entity first
        {
            try self_.beginTransaction();
            var committed = false;
            errdefer if (!committed) self_.rollbackTransactionQuiet();

            const sql = "DELETE FROM kg_entities WHERE id = ?1";
            var stmt: ?*c.sqlite3_stmt = null;
            var rc = c.sqlite3_prepare_v2(self_.db, sql, -1, &stmt, null);
            if (rc != c.SQLITE_OK) return error.PrepareFailed;
            defer _ = c.sqlite3_finalize(stmt);
            _ = c.sqlite3_bind_text(stmt, 1, entity_id.ptr, @intCast(entity_id.len), SQLITE_STATIC);
            rc = c.sqlite3_step(stmt);
            if (rc != c.SQLITE_DONE) return error.StepFailed;
            if (c.sqlite3_changes(self_.db) > 0) {
                try self_.deleteEntityFts(entity_id);
                try self_.deleteRelationsForEntity(entity_id);
                try self_.commitTransaction();
                committed = true;
                return true;
            }
            try self_.commitTransaction();
            committed = true;
        }

        // Try as relation id
        {
            const relation_id = relationIdForKey(key);
            const sql = "DELETE FROM kg_relations WHERE id = ?1";
            var stmt: ?*c.sqlite3_stmt = null;
            var rc = c.sqlite3_prepare_v2(self_.db, sql, -1, &stmt, null);
            if (rc != c.SQLITE_OK) return error.PrepareFailed;
            defer _ = c.sqlite3_finalize(stmt);
            _ = c.sqlite3_bind_text(stmt, 1, relation_id.ptr, @intCast(relation_id.len), SQLITE_STATIC);
            rc = c.sqlite3_step(stmt);
            if (rc != c.SQLITE_DONE) return error.StepFailed;
            return c.sqlite3_changes(self_.db) > 0;
        }
    }

    fn implCount(ptr: *anyopaque) anyerror!usize {
        const self_: *Self = @ptrCast(@alignCast(ptr));

        const sql = "SELECT (SELECT COUNT(*) FROM kg_entities) + (SELECT COUNT(*) FROM kg_relations)";
        var stmt: ?*c.sqlite3_stmt = null;
        var rc = c.sqlite3_prepare_v2(self_.db, sql, -1, &stmt, null);
        if (rc != c.SQLITE_OK) return error.PrepareFailed;
        defer _ = c.sqlite3_finalize(stmt);

        rc = c.sqlite3_step(stmt);
        if (rc == c.SQLITE_ROW) {
            return @intCast(c.sqlite3_column_int64(stmt, 0));
        }
        return 0;
    }

    fn implHealthCheck(ptr: *anyopaque) bool {
        const self_: *Self = @ptrCast(@alignCast(ptr));
        var err_msg: [*c]u8 = null;
        const rc = c.sqlite3_exec(self_.db, "SELECT 1", null, null, &err_msg);
        if (err_msg) |msg| c.sqlite3_free(msg);
        return rc == c.SQLITE_OK;
    }

    fn implDeinit(ptr: *anyopaque) void {
        const self_: *Self = @ptrCast(@alignCast(ptr));
        self_.deinit();
        if (self_.owns_self) {
            self_.allocator.destroy(self_);
        }
    }

    pub const vtable = Memory.VTable{
        .name = &implName,
        .store = &implStore,
        .recall = &implRecall,
        .get = &implGet,
        .getScoped = null,
        .list = &implList,
        .forget = &implForget,
        .forgetScoped = null,
        .count = &implCount,
        .healthCheck = &implHealthCheck,
        .deinit = &implDeinit,
    };

    pub fn memory(self: *Self) Memory {
        return .{
            .ptr = @ptrCast(self),
            .vtable = &vtable,
        };
    }
};

// ── Tests ──────────────────────────────────────────────────────────

test "kg memory init with in-memory db" {
    var kg = try KgMemory.init(std.testing.allocator, ":memory:");
    defer kg.deinit();
    const m = kg.memory();
    try std.testing.expect(m.healthCheck());
}

test "kg name" {
    var kg = try KgMemory.init(std.testing.allocator, ":memory:");
    defer kg.deinit();
    const m = kg.memory();
    try std.testing.expectEqualStrings("kg", m.name());
}

test "kg health check" {
    var kg = try KgMemory.init(std.testing.allocator, ":memory:");
    defer kg.deinit();
    const m = kg.memory();
    try std.testing.expect(m.healthCheck());
}

test "kg store and count" {
    var kg = try KgMemory.init(std.testing.allocator, ":memory:");
    defer kg.deinit();
    const m = kg.memory();

    try m.store("__kg:entity:test1", "Alice knows Bob", .core, null);
    try m.store("__kg:entity:test2", "Bob lives in NYC", .core, null);
    try m.store("__kg:rel:test1:knows:test2", "", .core, null);

    const count = try m.count();
    try std.testing.expect(count >= 2);
}

test "kg get entity" {
    var kg = try KgMemory.init(std.testing.allocator, ":memory:");
    defer kg.deinit();
    const m = kg.memory();

    try m.store("__kg:entity:e1", "Test entity content", .core, null);

    const entry = try m.get(std.testing.allocator, "e1");
    try std.testing.expect(entry != null);
    defer entry.?.deinit(std.testing.allocator);
    try std.testing.expectEqualStrings("e1", entry.?.key);
    try std.testing.expectEqualStrings("Test entity content", entry.?.content);
}

test "kg built-in categories round-trip" {
    var kg = try KgMemory.init(std.testing.allocator, ":memory:");
    defer kg.deinit();
    const m = kg.memory();

    // Regression: built-in categories must not come back as `.custom`.
    try m.store("__kg:entity:e1", "Core entity", .core, null);

    const entry = try m.get(std.testing.allocator, "e1");
    try std.testing.expect(entry != null);
    defer entry.?.deinit(std.testing.allocator);
    try std.testing.expect(entry.?.category.eql(.core));
}

test "kg path recall stops at requested destination" {
    var kg = try KgMemory.init(std.testing.allocator, ":memory:");
    defer kg.deinit();
    const m = kg.memory();

    // Regression: `kg:path` must use the destination instead of returning a generic traversal.
    try m.store("__kg:entity:a", "Alice", .core, null);
    try m.store("__kg:entity:b", "Bob", .core, null);
    try m.store("__kg:entity:c", "Carol", .core, null);
    try m.store("__kg:entity:d", "Dora", .core, null);
    try m.store("__kg:rel:a:knows:b", "", .core, null);
    try m.store("__kg:rel:b:knows:c", "", .core, null);
    try m.store("__kg:rel:a:knows:d", "", .core, null);

    const results = try m.recall(std.testing.allocator, "kg:path:a:c:3", 10, null);
    defer root.freeEntries(std.testing.allocator, results);

    try std.testing.expectEqual(@as(usize, 3), results.len);
    try std.testing.expectEqualStrings("a", results[0].key);
    try std.testing.expectEqualStrings("b", results[1].key);
    try std.testing.expectEqualStrings("c", results[2].key);
}

test "kg relations recall preserves entity id prefix parsing" {
    var kg = try KgMemory.init(std.testing.allocator, ":memory:");
    defer kg.deinit();
    const m = kg.memory();

    // Regression: `kg:relations:` must not drop the first byte of the entity id.
    try m.store("__kg:entity:abc", "Alpha", .core, null);
    try m.store("__kg:entity:def", "Delta", .core, null);
    try m.store("__kg:rel:abc:links:def", "", .core, null);

    const results = try m.recall(std.testing.allocator, "kg:relations:abc", 10, null);
    defer root.freeEntries(std.testing.allocator, results);

    try std.testing.expectEqual(@as(usize, 1), results.len);
    try std.testing.expectEqualStrings("__kg:rel:abc:links:def", results[0].key);
}

test "kg relations round-trip through get and forget" {
    var kg = try KgMemory.init(std.testing.allocator, ":memory:");
    defer kg.deinit();
    const m = kg.memory();

    // Regression: relation keys must remain retrievable and forgettable through the Memory vtable.
    const relation_key = "__kg:rel:test1:knows:test2";
    try m.store(relation_key, "", .core, null);

    const entry = try m.get(std.testing.allocator, relation_key);
    try std.testing.expect(entry != null);
    defer entry.?.deinit(std.testing.allocator);
    try std.testing.expectEqualStrings(relation_key, entry.?.key);
    try std.testing.expectEqualStrings("test1 --knows--> test2", entry.?.content);

    try std.testing.expect(try m.forget(relation_key));
    const missing = try m.get(std.testing.allocator, relation_key);
    try std.testing.expect(missing == null);
}

test "kg updating entity refreshes FTS rows" {
    var kg = try KgMemory.init(std.testing.allocator, ":memory:");
    defer kg.deinit();
    const m = kg.memory();

    // Regression: FTS rows must be replaced on update instead of accumulating stale matches.
    try m.store("__kg:entity:e1", "oldterm content", .core, null);
    try m.store("__kg:entity:e1", "newterm content", .core, null);

    const old_results = try m.recall(std.testing.allocator, "oldterm", 10, null);
    defer root.freeEntries(std.testing.allocator, old_results);
    try std.testing.expectEqual(@as(usize, 0), old_results.len);

    const new_results = try m.recall(std.testing.allocator, "newterm", 10, null);
    defer root.freeEntries(std.testing.allocator, new_results);
    try std.testing.expectEqual(@as(usize, 1), new_results.len);
    try std.testing.expectEqualStrings("e1", new_results[0].key);
}

test "kg relations recall respects requested limit" {
    var kg = try KgMemory.init(std.testing.allocator, ":memory:");
    defer kg.deinit();
    const m = kg.memory();

    // Regression: `kg:relations:` must honor recall limit instead of returning the full edge set.
    try m.store("__kg:entity:hub", "Hub", .core, null);
    try m.store("__kg:entity:n1", "Node 1", .core, null);
    try m.store("__kg:entity:n2", "Node 2", .core, null);
    try m.store("__kg:rel:hub:links:n1", "", .core, null);
    try m.store("__kg:rel:hub:links:n2", "", .core, null);

    const results = try m.recall(std.testing.allocator, "kg:relations:hub", 1, null);
    defer root.freeEntries(std.testing.allocator, results);
    try std.testing.expectEqual(@as(usize, 1), results.len);
}

test "kg recall treats zero limit as zero results" {
    var kg = try KgMemory.init(std.testing.allocator, ":memory:");
    defer kg.deinit();
    const m = kg.memory();

    // Regression: Memory.recall limit is a hard maximum; zero must not expand to an internal default.
    try m.store("__kg:entity:a", "Alice searchable", .core, null);
    try m.store("__kg:entity:b", "Bob searchable", .core, null);
    try m.store("__kg:rel:a:links:b", "", .core, null);

    const fts_results = try m.recall(std.testing.allocator, "searchable", 0, null);
    defer root.freeEntries(std.testing.allocator, fts_results);
    try std.testing.expectEqual(@as(usize, 0), fts_results.len);

    const relation_results = try m.recall(std.testing.allocator, "kg:relations:a", 0, null);
    defer root.freeEntries(std.testing.allocator, relation_results);
    try std.testing.expectEqual(@as(usize, 0), relation_results.len);

    const traverse_results = try m.recall(std.testing.allocator, "kg:traverse:a:2", 0, null);
    defer root.freeEntries(std.testing.allocator, traverse_results);
    try std.testing.expectEqual(@as(usize, 0), traverse_results.len);
}

test "kg traverse deduplicates cyclic paths" {
    var kg = try KgMemory.init(std.testing.allocator, ":memory:");
    defer kg.deinit();
    const m = kg.memory();

    // Regression: cyclic traversals must not return repeated entities.
    try m.store("__kg:entity:a", "Alice", .core, null);
    try m.store("__kg:entity:b", "Bob", .core, null);
    try m.store("__kg:rel:a:links:b", "", .core, null);
    try m.store("__kg:rel:b:links:a", "", .core, null);

    const results = try m.recall(std.testing.allocator, "kg:traverse:a:4", 10, null);
    defer root.freeEntries(std.testing.allocator, results);

    try std.testing.expectEqual(@as(usize, 2), results.len);
    try std.testing.expectEqualStrings("a", results[0].key);
    try std.testing.expectEqualStrings("b", results[1].key);
}

test "kg count includes relations" {
    var kg = try KgMemory.init(std.testing.allocator, ":memory:");
    defer kg.deinit();
    const m = kg.memory();

    // Regression: relation entries stored through the vtable must contribute to count().
    try m.store("__kg:entity:a", "Alice", .core, null);
    try m.store("__kg:entity:b", "Bob", .core, null);
    try m.store("__kg:rel:a:knows:b", "", .core, null);

    try std.testing.expectEqual(@as(usize, 3), try m.count());
}

test "kg list can return relation entries" {
    var kg = try KgMemory.init(std.testing.allocator, ":memory:");
    defer kg.deinit();
    const m = kg.memory();

    // Regression: list() must surface relation entries instead of hiding half the store.
    try m.store("__kg:entity:a", "Alice", .core, null);
    try m.store("__kg:entity:b", "Bob", .core, null);
    try m.store("__kg:rel:a:knows:b", "", .core, null);

    const all_entries = try m.list(std.testing.allocator, null, null);
    defer root.freeEntries(std.testing.allocator, all_entries);
    try std.testing.expectEqual(@as(usize, 3), all_entries.len);
    var saw_relation = false;
    for (all_entries) |entry| {
        if (std.mem.eql(u8, entry.key, "__kg:rel:a:knows:b")) saw_relation = true;
    }
    try std.testing.expect(saw_relation);
}

test "kg list keeps global recency order across entities and relations" {
    var kg = try KgMemory.init(std.testing.allocator, ":memory:");
    defer kg.deinit();
    const m = kg.memory();

    // Regression: list(null) must not append all relations after all entities.
    try m.store("__kg:entity:a", "Alice", .core, null);
    try m.store("__kg:entity:b", "Bob", .core, null);
    try m.store("__kg:rel:a:links:b", "", .core, null);

    try kg.execSql(
        \\UPDATE kg_entities
        \\SET created_at = CASE id
        \\  WHEN 'a' THEN '1'
        \\  WHEN 'b' THEN '2'
        \\  ELSE created_at
        \\END;
        \\UPDATE kg_relations
        \\SET created_at = '3'
        \\WHERE id = 'a:links:b';
    );

    const all_entries = try m.list(std.testing.allocator, null, null);
    defer root.freeEntries(std.testing.allocator, all_entries);

    try std.testing.expectEqual(@as(usize, 3), all_entries.len);
    try std.testing.expectEqualStrings("__kg:rel:a:links:b", all_entries[0].key);
    try std.testing.expectEqualStrings("b", all_entries[1].key);
    try std.testing.expectEqualStrings("a", all_entries[2].key);
}

test "kg list preserves user custom relation category" {
    var kg = try KgMemory.init(std.testing.allocator, ":memory:");
    defer kg.deinit();
    const m = kg.memory();

    // Regression: a user custom category named "relation" must not be hijacked by edge listing.
    try m.store("__kg:entity:user_relation", "User-defined relation category", .{ .custom = "relation" }, null);
    try m.store("__kg:entity:b", "Bob", .core, null);
    try m.store("__kg:rel:user_relation:links:b", "", .core, null);

    const filtered = try m.list(std.testing.allocator, .{ .custom = "relation" }, null);
    defer root.freeEntries(std.testing.allocator, filtered);

    try std.testing.expectEqual(@as(usize, 1), filtered.len);
    try std.testing.expectEqualStrings("user_relation", filtered[0].key);
    try std.testing.expect(filtered[0].category.eql(.{ .custom = "relation" }));
}

test "kg forgetting entity removes incident relations" {
    var kg = try KgMemory.init(std.testing.allocator, ":memory:");
    defer kg.deinit();
    const m = kg.memory();

    // Regression: forgetting an entity must not leave orphaned edges behind.
    try m.store("__kg:entity:a", "Alice", .core, null);
    try m.store("__kg:entity:b", "Bob", .core, null);
    try m.store("__kg:rel:a:knows:b", "", .core, null);

    try std.testing.expect(try m.forget("__kg:entity:a"));
    try std.testing.expectEqual(@as(usize, 1), try m.count());
    const relation = try m.get(std.testing.allocator, "__kg:rel:a:knows:b");
    defer if (relation) |entry| entry.deinit(std.testing.allocator);
    try std.testing.expect(relation == null);
}

test "kg rejects malformed relation keys" {
    var kg = try KgMemory.init(std.testing.allocator, ":memory:");
    defer kg.deinit();
    const m = kg.memory();

    // Regression: malformed relation keys must fail fast instead of creating partial rows.
    try std.testing.expectError(error.InvalidRelationKey, m.store("__kg:rel:a:knows:", "", .core, null));
}

test "kg relation helpers support reserved characters" {
    var kg = try KgMemory.init(std.testing.allocator, ":memory:");
    defer kg.deinit();
    const m = kg.memory();

    // Regression: graph ids containing ':' or '<' must round-trip through key/query encoding.
    try m.store("__kg:entity:alpha:1", "Alpha", .core, null);
    try m.store("__kg:entity:beta<2", "Beta", .core, null);

    const relation_key = try KgMemory.relationStoreKey(std.testing.allocator, "alpha:1", "links:<to>", "beta<2");
    defer std.testing.allocator.free(relation_key);
    try m.store(relation_key, "", .core, null);

    const relation_entry = try m.get(std.testing.allocator, relation_key);
    try std.testing.expect(relation_entry != null);
    defer relation_entry.?.deinit(std.testing.allocator);
    try std.testing.expectEqualStrings(relation_key, relation_entry.?.key);
    try std.testing.expectEqualStrings("alpha:1 --links:<to>--> beta<2", relation_entry.?.content);

    const relations_query = try KgMemory.relationsQuery(std.testing.allocator, "alpha:1");
    defer std.testing.allocator.free(relations_query);
    const relation_results = try m.recall(std.testing.allocator, relations_query, 10, null);
    defer root.freeEntries(std.testing.allocator, relation_results);
    try std.testing.expectEqual(@as(usize, 1), relation_results.len);
    try std.testing.expectEqualStrings(relation_key, relation_results[0].key);

    const path_query = try KgMemory.pathQuery(std.testing.allocator, "alpha:1", "beta<2", 3);
    defer std.testing.allocator.free(path_query);
    const path_results = try m.recall(std.testing.allocator, path_query, 10, null);
    defer root.freeEntries(std.testing.allocator, path_results);
    try std.testing.expectEqual(@as(usize, 2), path_results.len);
    try std.testing.expectEqualStrings("alpha:1", path_results[0].key);
    try std.testing.expectEqualStrings("beta<2", path_results[1].key);

    const traverse_query = try KgMemory.traverseQuery(std.testing.allocator, "alpha:1", 3);
    defer std.testing.allocator.free(traverse_query);
    const traverse_results = try m.recall(std.testing.allocator, traverse_query, 10, null);
    defer root.freeEntries(std.testing.allocator, traverse_results);
    try std.testing.expectEqual(@as(usize, 2), traverse_results.len);
    try std.testing.expectEqualStrings("alpha:1", traverse_results[0].key);
    try std.testing.expectEqualStrings("beta<2", traverse_results[1].key);
}

test "kg store rolls back entity when fts maintenance fails" {
    var kg = try KgMemory.init(std.testing.allocator, ":memory:");
    defer kg.deinit();
    const m = kg.memory();

    // Regression: entity writes must not partially commit when FTS refresh fails.
    try kg.execSql("DROP TABLE kg_entities_fts;");
    try std.testing.expectError(error.PrepareFailed, m.store("__kg:entity:e1", "should rollback", .core, null));
    try std.testing.expectEqual(@as(usize, 0), try m.count());
    try std.testing.expect((try m.get(std.testing.allocator, "e1")) == null);
}

test "kg forget rolls back entity delete when cleanup fails" {
    var kg = try KgMemory.init(std.testing.allocator, ":memory:");
    defer kg.deinit();
    const m = kg.memory();

    // Regression: entity deletes must roll back when FTS cleanup fails.
    try m.store("__kg:entity:a", "Alice", .core, null);
    try m.store("__kg:entity:b", "Bob", .core, null);
    try m.store("__kg:rel:a:knows:b", "", .core, null);

    try kg.execSql("DROP TABLE kg_entities_fts;");
    try std.testing.expectError(error.PrepareFailed, m.forget("__kg:entity:a"));

    try std.testing.expectEqual(@as(usize, 3), try m.count());
    const entity = try m.get(std.testing.allocator, "a");
    try std.testing.expect(entity != null);
    defer entity.?.deinit(std.testing.allocator);
    const relation = try m.get(std.testing.allocator, "__kg:rel:a:knows:b");
    try std.testing.expect(relation != null);
    defer relation.?.deinit(std.testing.allocator);
}
