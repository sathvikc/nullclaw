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

const std = @import("std");
const root = @import("../root.zig");
const Memory = root.Memory;
const MemoryCategory = root.MemoryCategory;
const MemoryEntry = root.MemoryEntry;
const log = std.log.scoped(.memory_kg);

const ENTITY_STORE_PREFIX = "__kg:entity:";
const RELATION_STORE_PREFIX = "__kg:rel:";
const TRAVERSE_QUERY_PREFIX = "kg:traverse:";
const PATH_QUERY_PREFIX = "kg:path:";
const RELATIONS_QUERY_PREFIX = "kg:relations:";
const RELATION_CATEGORY = "relation";
const DEFAULT_QUERY_LIMIT: usize = 100;

pub const c = @cImport({
    @cInclude("sqlite3.h");
});

pub const SQLITE_STATIC: c.sqlite3_destructor_type = null;
const BUSY_TIMEOUT_MS: c_int = 5000;

pub const KgMemory = struct {
    db: ?*c.sqlite3,
    allocator: std.mem.Allocator,
    owns_self: bool = false,

    const Self = @This();

    pub fn init(allocator: std.mem.Allocator, db_path: [*:0]const u8) !Self {
        var db: ?*c.sqlite3 = null;
        const rc = c.sqlite3_open(db_path, &db);
        if (rc != c.SQLITE_OK) {
            if (db) |d| _ = c.sqlite3_close(d);
            return error.SqliteOpenFailed;
        }
        if (db) |d| {
            _ = c.sqlite3_busy_timeout(d, BUSY_TIMEOUT_MS);
        }

        var self_ = Self{ .db = db, .allocator = allocator };
        try self_.configurePragmas();
        try self_.migrate();
        return self_;
    }

    pub fn deinit(self: *Self) void {
        if (self.db) |db| {
            _ = c.sqlite3_close(db);
            self.db = null;
        }
    }

    fn configurePragmas(self: *Self) !void {
        const pragmas = [_][:0]const u8{
            "PRAGMA journal_mode = DELETE;",
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
        const ts = std.time.timestamp();
        return std.fmt.allocPrint(allocator, "{d}", .{ts});
    }

    fn effectiveLimit(limit: usize) usize {
        return if (limit > 0) limit else DEFAULT_QUERY_LIMIT;
    }

    fn entityIdForKey(key: []const u8) []const u8 {
        if (std.mem.startsWith(u8, key, ENTITY_STORE_PREFIX)) return key[ENTITY_STORE_PREFIX.len..];
        return key;
    }

    fn relationIdForKey(key: []const u8) []const u8 {
        if (std.mem.startsWith(u8, key, RELATION_STORE_PREFIX)) return key[RELATION_STORE_PREFIX.len..];
        return key;
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

    // ── Graph operations ──────────────────────────────────────────────

    fn storeEntity(self: *Self, id: []const u8, entity_type: []const u8, content: []const u8) !void {
        const now = try getNowTimestamp(self.allocator);
        defer self.allocator.free(now);

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

        // Insert into FTS table directly (app is sole writer)
        {
            const fts_sql = "INSERT OR REPLACE INTO kg_entities_fts (id, content) VALUES (?1, ?2)";
            var fts_stmt: ?*c.sqlite3_stmt = null;
            if (c.sqlite3_prepare_v2(self.db, fts_sql, -1, &fts_stmt, null) == c.SQLITE_OK) {
                defer _ = c.sqlite3_finalize(fts_stmt);
                _ = c.sqlite3_bind_text(fts_stmt, 1, id.ptr, @intCast(id.len), SQLITE_STATIC);
                _ = c.sqlite3_bind_text(fts_stmt, 2, content.ptr, @intCast(content.len), SQLITE_STATIC);
                _ = c.sqlite3_step(fts_stmt);
            }
        }
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
            \\WITH RECURSIVE traversal(id, depth) AS (
            \\  SELECT id, 0 FROM kg_entities WHERE id = ?1
            \\  UNION ALL
            \\  SELECT r.object_id, t.depth + 1 FROM kg_relations r, traversal t
            \\   WHERE r.subject_id = t.id AND t.depth < ?2
            \\)
            \\SELECT e.id, e.type, e.content, e.created_at FROM kg_entities e, traversal t WHERE e.id = t.id LIMIT ?3
        ;

        var stmt: ?*c.sqlite3_stmt = null;
        if (c.sqlite3_prepare_v2(self.db, sql, -1, &stmt, null) != c.SQLITE_OK) return error.PrepareFailed;
        defer _ = c.sqlite3_finalize(stmt);

        _ = c.sqlite3_bind_text(stmt, 1, start_id.ptr, @intCast(start_id.len), SQLITE_STATIC);
        _ = c.sqlite3_bind_int64(stmt, 2, @intCast(max_depth));
        _ = c.sqlite3_bind_int64(stmt, 3, @intCast(effectiveLimit(limit)));

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
            \\  SELECT '<' || ?1, ?1, 0
            \\  UNION ALL
            \\  SELECT p.path_ids || '<' || r.object_id, r.object_id, p.depth + 1
            \\   FROM kg_relations r
            \\   JOIN path p ON r.subject_id = p.id
            \\   WHERE r.subject_id = p.id AND p.depth < ?3
            \\     AND INSTR(p.path_ids || '<', '<' || r.object_id || '<') = 0
            \\)
            \\, target(path_ids) AS (
            \\  SELECT path_ids
            \\  FROM path
            \\  WHERE id = ?2
            \\  ORDER BY depth ASC
            \\  LIMIT 1
            \\)
            \\, split(rest, node_id, ord) AS (
            \\  SELECT substr(path_ids, 2) || '<', '', 0
            \\  FROM target
            \\  UNION ALL
            \\  SELECT substr(rest, instr(rest, '<') + 1),
            \\         substr(rest, 1, instr(rest, '<') - 1),
            \\         ord + 1
            \\  FROM split
            \\  WHERE rest <> ''
            \\)
            \\SELECT e.id, e.type, e.content, e.created_at
            \\FROM split s
            \\JOIN kg_entities e ON e.id = s.node_id
            \\WHERE s.node_id <> ''
            \\ORDER BY s.ord ASC
            \\LIMIT ?4
        ;

        var stmt: ?*c.sqlite3_stmt = null;
        if (c.sqlite3_prepare_v2(self.db, sql, -1, &stmt, null) != c.SQLITE_OK) return error.PrepareFailed;
        defer _ = c.sqlite3_finalize(stmt);

        _ = c.sqlite3_bind_text(stmt, 1, from_id.ptr, @intCast(from_id.len), SQLITE_STATIC);
        _ = c.sqlite3_bind_text(stmt, 2, to_id.ptr, @intCast(to_id.len), SQLITE_STATIC);
        _ = c.sqlite3_bind_int64(stmt, 3, @intCast(max_depth));
        _ = c.sqlite3_bind_int64(stmt, 4, @intCast(effectiveLimit(limit)));

        while (c.sqlite3_step(stmt) == c.SQLITE_ROW) {
            const entry = try self.readEntityFromRow(stmt.?, allocator);
            try entries.append(allocator, entry);
        }

        return entries.toOwnedSlice(allocator);
    }

    /// All relations (incoming + outgoing) for an entity.
    fn getRelations(self: *Self, allocator: std.mem.Allocator, entity_id: []const u8) ![]MemoryEntry {
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
        ;

        var stmt: ?*c.sqlite3_stmt = null;
        if (c.sqlite3_prepare_v2(self.db, sql, -1, &stmt, null) != c.SQLITE_OK) return error.PrepareFailed;
        defer _ = c.sqlite3_finalize(stmt);

        _ = c.sqlite3_bind_text(stmt, 1, entity_id.ptr, @intCast(entity_id.len), SQLITE_STATIC);

        while (c.sqlite3_step(stmt) == c.SQLITE_ROW) {
            try entries.append(allocator, try self.readRelationFromRow(stmt.?, allocator));
        }

        return entries.toOwnedSlice(allocator);
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

        _ = c.sqlite3_bind_text(stmt, 1, query.ptr, @intCast(query.len), SQLITE_STATIC);
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
            // Format: __kg:rel:{subject_id}:{predicate}:{object_id}
            const rel_id = relationIdForKey(key);
            const rel_part = rel_id;
            var it = std.mem.splitScalar(u8, rel_part, ':');
            const subject_id = it.next() orelse return error.StepFailed;
            const predicate = it.next() orelse return error.StepFailed;
            const object_id = it.rest();

            try self_.storeRelation(rel_id, subject_id, predicate, object_id);
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
            const entity_id = it.next() orelse return allocator.alloc(MemoryEntry, 0);
            const depth_str = it.next() orelse "3";
            const max_depth = std.fmt.parseInt(usize, depth_str, 10) catch 3;
            return self_.traverse(allocator, entity_id, max_depth, limit);
        }

        if (std.mem.startsWith(u8, trimmed, PATH_QUERY_PREFIX)) {
            const args = trimmed[PATH_QUERY_PREFIX.len..];
            var it = std.mem.splitScalar(u8, args, ':');
            const from_id = it.next() orelse return allocator.alloc(MemoryEntry, 0);
            const to_id = it.next() orelse return allocator.alloc(MemoryEntry, 0);
            const depth_str = it.next() orelse "5";
            const max_depth = std.fmt.parseInt(usize, depth_str, 10) catch 5;
            return self_.findPath(allocator, from_id, to_id, max_depth, limit);
        }

        if (std.mem.startsWith(u8, trimmed, RELATIONS_QUERY_PREFIX)) {
            const entity_id = trimmed[RELATIONS_QUERY_PREFIX.len..];
            if (entity_id.len == 0) return allocator.alloc(MemoryEntry, 0);
            return self_.getRelations(allocator, entity_id);
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

        const sql = if (category) |_|
            "SELECT id, type, content, created_at FROM kg_entities WHERE type = ?1 ORDER BY created_at DESC"
        else
            "SELECT id, type, content, created_at FROM kg_entities ORDER BY created_at DESC";

        var stmt: ?*c.sqlite3_stmt = null;
        if (c.sqlite3_prepare_v2(self_.db, sql, -1, &stmt, null) != c.SQLITE_OK) return error.PrepareFailed;
        defer _ = c.sqlite3_finalize(stmt);

        if (category) |cat| {
            const cat_str = cat.toString();
            _ = c.sqlite3_bind_text(stmt, 1, cat_str.ptr, @intCast(cat_str.len), SQLITE_STATIC);
        }

        while (c.sqlite3_step(stmt) == c.SQLITE_ROW) {
            const entry = try self_.readEntityFromRow(stmt.?, allocator);
            try entries.append(allocator, entry);
        }

        return entries.toOwnedSlice(allocator);
    }

    fn implForget(ptr: *anyopaque, key: []const u8) anyerror!bool {
        const self_: *Self = @ptrCast(@alignCast(ptr));
        const entity_id = entityIdForKey(key);

        // Try to delete as entity first
        {
            const sql = "DELETE FROM kg_entities WHERE id = ?1";
            var stmt: ?*c.sqlite3_stmt = null;
            var rc = c.sqlite3_prepare_v2(self_.db, sql, -1, &stmt, null);
            if (rc != c.SQLITE_OK) return error.PrepareFailed;
            defer _ = c.sqlite3_finalize(stmt);
            _ = c.sqlite3_bind_text(stmt, 1, entity_id.ptr, @intCast(entity_id.len), SQLITE_STATIC);
            rc = c.sqlite3_step(stmt);
            if (rc != c.SQLITE_DONE) return error.StepFailed;
            if (c.sqlite3_changes(self_.db) > 0) {
                // Also delete from FTS
                var fts_stmt: ?*c.sqlite3_stmt = null;
                if (c.sqlite3_prepare_v2(self_.db, "DELETE FROM kg_entities_fts WHERE id = ?1", -1, &fts_stmt, null) == c.SQLITE_OK) {
                    defer _ = c.sqlite3_finalize(fts_stmt);
                    _ = c.sqlite3_bind_text(fts_stmt, 1, entity_id.ptr, @intCast(entity_id.len), SQLITE_STATIC);
                    _ = c.sqlite3_step(fts_stmt);
                }
                return true;
            }
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

        const sql = "SELECT COUNT(*) FROM kg_entities";
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
