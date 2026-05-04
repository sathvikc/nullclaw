//! Cross-backend contract tests for the Memory vtable.
//!
//! Every backend that implements Memory must satisfy these invariants.
//! Each test creates its own backend instance, runs the contract, and deinits.

const std = @import("std");
const build_options = @import("build_options");
const root = @import("../root.zig");
const Memory = root.Memory;
const MemoryCategory = root.MemoryCategory;
const MemoryEntry = root.MemoryEntry;

const SqliteMemory = if (build_options.enable_sqlite) @import("sqlite.zig").SqliteMemory else @import("sqlite_disabled.zig").SqliteMemory;
const NoneMemory = @import("none.zig").NoneMemory;
const MarkdownMemory = @import("markdown.zig").MarkdownMemory;
const InMemoryLruMemory = @import("memory_lru.zig").InMemoryLruMemory;
const KgMemory = if (build_options.enable_memory_kg) @import("kg.zig").KgMemory else struct {};

// ── Contract: common invariants ─────────────────────────────────────

/// Validates that a Memory backend satisfies the basic vtable contract:
/// name() returns a non-empty string, healthCheck() is true, and the
/// vtable methods do not crash on an empty store.
fn contractBasics(m: Memory) !void {
    const allocator = std.testing.allocator;

    // name() returns non-empty
    const n = m.name();
    try std.testing.expect(n.len > 0);

    // healthCheck is true after init
    try std.testing.expect(m.healthCheck());

    // Empty store: count is 0
    try std.testing.expectEqual(@as(usize, 0), try m.count());

    // Empty store: get returns null
    const got = try m.get(allocator, "nonexistent");
    try std.testing.expect(got == null);

    // Empty store: recall returns empty
    const recalled = try m.recall(allocator, "query", 10, null);
    defer allocator.free(recalled);
    try std.testing.expectEqual(@as(usize, 0), recalled.len);

    // Empty store: list returns empty
    const listed = try m.list(allocator, null, null);
    defer allocator.free(listed);
    try std.testing.expectEqual(@as(usize, 0), listed.len);
}

/// Full CRUD contract for backends that truly persist (sqlite, lucid, postgres).
/// After store(), the entry is retrievable via get(), recall(), list(), count().
/// After forget(), the entry is gone.
fn contractCrud(m: Memory) !void {
    const allocator = std.testing.allocator;

    // 1. store a memory entry
    try m.store("test_key", "test content", .core, null);

    // 2. get the entry back, verify content matches
    {
        const entry = try m.get(allocator, "test_key");
        try std.testing.expect(entry != null);
        defer entry.?.deinit(allocator);
        try std.testing.expectEqualStrings("test_key", entry.?.key);
        try std.testing.expectEqualStrings("test content", entry.?.content);
        try std.testing.expect(entry.?.category.eql(.core));
    }

    // 3. recall with a query, verify the entry appears
    {
        const results = try m.recall(allocator, "test", 10, null);
        defer root.freeEntries(allocator, results);
        try std.testing.expect(results.len >= 1);
        // At least one result should contain our content
        var found = false;
        for (results) |e| {
            if (std.mem.indexOf(u8, e.content, "test content") != null) {
                found = true;
                break;
            }
        }
        try std.testing.expect(found);
    }

    // 4. list all core entries, verify count
    {
        const core_list = try m.list(allocator, .core, null);
        defer root.freeEntries(allocator, core_list);
        try std.testing.expect(core_list.len >= 1);
    }

    // 5. count total entries
    try std.testing.expectEqual(@as(usize, 1), try m.count());

    // 6. store a second entry, verify count=2
    try m.store("second_key", "second content", .core, null);
    try std.testing.expectEqual(@as(usize, 2), try m.count());

    // 7. forget the first entry, verify count=1
    const forgotten = try m.forget("test_key");
    try std.testing.expect(forgotten);
    try std.testing.expectEqual(@as(usize, 1), try m.count());

    // 8. get the forgotten entry returns null
    {
        const entry = try m.get(allocator, "test_key");
        try std.testing.expect(entry == null);
    }
}

/// Contract for NoneMemory: every write is a no-op, every read returns empty.
fn contractNone(m: Memory) !void {
    const allocator = std.testing.allocator;

    try std.testing.expectEqualStrings("none", m.name());

    // store does not crash
    try m.store("test_key", "test content", .core, null);

    // get returns null
    const got = try m.get(allocator, "test_key");
    try std.testing.expect(got == null);

    // recall returns empty
    const recalled = try m.recall(allocator, "test", 10, null);
    defer allocator.free(recalled);
    try std.testing.expectEqual(@as(usize, 0), recalled.len);

    // list returns empty
    const listed = try m.list(allocator, .core, null);
    defer allocator.free(listed);
    try std.testing.expectEqual(@as(usize, 0), listed.len);

    // count is always 0
    try std.testing.expectEqual(@as(usize, 0), try m.count());

    // forget returns false
    try std.testing.expect(!(try m.forget("test_key")));

    // Store a second entry, count is still 0
    try m.store("second_key", "second content", .core, null);
    try std.testing.expectEqual(@as(usize, 0), try m.count());
}

/// Contract for MarkdownMemory: append-only, forget returns false,
/// content is stored with markdown formatting.
fn contractMarkdown(m: Memory) !void {
    const allocator = std.testing.allocator;

    try std.testing.expectEqualStrings("markdown", m.name());
    try std.testing.expect(m.healthCheck());

    // Empty at start
    try std.testing.expectEqual(@as(usize, 0), try m.count());

    // Store an entry
    try m.store("test_key", "test content", .core, null);

    // count should be 1
    try std.testing.expectEqual(@as(usize, 1), try m.count());

    // get by key — markdown stores as "**key**: content", get matches by substring
    {
        const entry = try m.get(allocator, "test_key");
        try std.testing.expect(entry != null);
        defer entry.?.deinit(allocator);
        // Content contains both key and value due to markdown formatting
        try std.testing.expect(std.mem.indexOf(u8, entry.?.content, "test_key") != null);
        try std.testing.expect(std.mem.indexOf(u8, entry.?.content, "test content") != null);
    }

    // recall with query
    {
        const results = try m.recall(allocator, "test", 10, null);
        defer root.freeEntries(allocator, results);
        try std.testing.expect(results.len >= 1);
    }

    // list core entries
    {
        const core_list = try m.list(allocator, .core, null);
        defer root.freeEntries(allocator, core_list);
        try std.testing.expect(core_list.len >= 1);
    }

    // Store a second entry
    try m.store("second_key", "second content", .core, null);
    try std.testing.expectEqual(@as(usize, 2), try m.count());

    // forget always returns false (append-only)
    try std.testing.expect(!(try m.forget("test_key")));
    // count unchanged after forget
    try std.testing.expectEqual(@as(usize, 2), try m.count());
}

/// Contract: session_id parameter is accepted by all backends.
fn contractSessionId(m: Memory) !void {
    const allocator = std.testing.allocator;

    // store with session_id does not crash
    try m.store("sess_key", "session data", .core, "session-42");

    // recall with session_id
    const recalled = try m.recall(allocator, "session", 10, "session-42");
    defer root.freeEntries(allocator, recalled);

    // list with session_id
    const listed = try m.list(allocator, null, "session-42");
    defer root.freeEntries(allocator, listed);
}

// ── SQLite tests ─────────────────────────────────────────────────────

test "contract: sqlite basics" {
    if (!build_options.enable_sqlite) return;
    var mem = try SqliteMemory.init(std.testing.allocator, ":memory:");
    defer mem.deinit();
    try contractBasics(mem.memory());
}

test "contract: sqlite crud" {
    if (!build_options.enable_sqlite) return;
    var mem = try SqliteMemory.init(std.testing.allocator, ":memory:");
    defer mem.deinit();
    try contractCrud(mem.memory());
}

test "contract: sqlite session_id" {
    if (!build_options.enable_sqlite) return;
    var mem = try SqliteMemory.init(std.testing.allocator, ":memory:");
    defer mem.deinit();
    try contractSessionId(mem.memory());
}

// ── NoneMemory tests ─────────────────────────────────────────────────

test "contract: none basics" {
    var mem = NoneMemory.init();
    defer mem.deinit();
    try contractBasics(mem.memory());
}

test "contract: none noop" {
    var mem = NoneMemory.init();
    defer mem.deinit();
    try contractNone(mem.memory());
}

test "contract: none session_id" {
    var mem = NoneMemory.init();
    defer mem.deinit();
    try contractSessionId(mem.memory());
}

// ── MarkdownMemory tests ─────────────────────────────────────────────

test "contract: markdown basics" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const base = try @import("compat").fs.Dir.wrap(tmp.dir).realpathAlloc(std.testing.allocator, ".");
    defer std.testing.allocator.free(base);

    var mem = try MarkdownMemory.init(std.testing.allocator, base);
    defer mem.deinit();
    try contractBasics(mem.memory());
}

test "contract: markdown append-only" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const base = try @import("compat").fs.Dir.wrap(tmp.dir).realpathAlloc(std.testing.allocator, ".");
    defer std.testing.allocator.free(base);

    var mem = try MarkdownMemory.init(std.testing.allocator, base);
    defer mem.deinit();
    try contractMarkdown(mem.memory());
}

test "contract: markdown session_id" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const base = try @import("compat").fs.Dir.wrap(tmp.dir).realpathAlloc(std.testing.allocator, ".");
    defer std.testing.allocator.free(base);

    var mem = try MarkdownMemory.init(std.testing.allocator, base);
    defer mem.deinit();
    try contractSessionId(mem.memory());
}

// ── InMemoryLruMemory tests ──────────────────────────────────────────

test "contract: memory_lru basics" {
    var mem = InMemoryLruMemory.init(std.testing.allocator, 100);
    defer mem.deinit();
    try contractBasics(mem.memory());
}

test "contract: memory_lru crud" {
    var mem = InMemoryLruMemory.init(std.testing.allocator, 100);
    defer mem.deinit();
    try contractCrud(mem.memory());
}

test "contract: memory_lru session_id" {
    var mem = InMemoryLruMemory.init(std.testing.allocator, 100);
    defer mem.deinit();
    try contractSessionId(mem.memory());
}

// ── KgMemory tests ──────────────────────────────────────────────────

test "contract: kg basics" {
    if (!build_options.enable_memory_kg) return;
    var mem = try KgMemory.init(std.testing.allocator, ":memory:");
    defer mem.deinit();
    try contractBasics(mem.memory());
}

test "contract: kg crud" {
    if (!build_options.enable_memory_kg) return;
    var mem = try KgMemory.init(std.testing.allocator, ":memory:");
    defer mem.deinit();
    try contractCrud(mem.memory());
}

test "contract: kg session_id" {
    if (!build_options.enable_memory_kg) return;
    var mem = try KgMemory.init(std.testing.allocator, ":memory:");
    defer mem.deinit();
    try contractSessionId(mem.memory());
}

// ── Recall-after-store contract ──────────────────────────────────────

fn recallAfterStoreContract(m: Memory) !void {
    const allocator = std.testing.allocator;
    const unique_key = "recall_probe";
    // The query must be a single FTS5 token — sqlite's FTS5 unicode61
    // tokenizer treats `_` as a separator, so a query like "recall_probe"
    // would split into the phrase ["recall", "probe"] and miss the
    // sqlite/kg FTS path entirely. KG has no LIKE fallback, so a
    // multi-token query would silently fail there. `xyz123uniquecontent`
    // is a single alphanumeric token that all three search paths
    // (FTS5 phrase, markdown substring, LRU substring) match identically.
    const unique_content = "xyz123uniquecontent recall_probe_content";
    try m.store(unique_key, unique_content, .core, null);

    const query = "xyz123uniquecontent";
    const results = try m.recall(allocator, query, 10, null);
    defer root.freeEntries(allocator, results);

    try std.testing.expect(results.len >= 1);
    var found = false;
    for (results) |e| {
        if (std.mem.indexOf(u8, e.content, query) != null) {
            found = true;
            break;
        }
    }
    try std.testing.expect(found);
}

test "stateful memory backends recall stored entries" {
    // NoneMemory is intentionally covered by "contract: none noop"; it must
    // not recall stored entries because writes are explicit no-ops.
    if (build_options.enable_sqlite) {
        var sql = try SqliteMemory.init(std.testing.allocator, ":memory:");
        defer sql.deinit();
        try recallAfterStoreContract(sql.memory());
    }
    {
        var dir = std.testing.tmpDir(.{});
        defer dir.cleanup();
        const path = try @import("compat").fs.Dir.wrap(dir.dir).realpathAlloc(std.testing.allocator, ".");
        defer std.testing.allocator.free(path);
        var md = try MarkdownMemory.init(std.testing.allocator, path);
        defer md.deinit();
        try recallAfterStoreContract(md.memory());
    }
    {
        var lru = InMemoryLruMemory.init(std.testing.allocator, 100);
        defer lru.deinit();
        try recallAfterStoreContract(lru.memory());
    }
    if (build_options.enable_memory_kg) {
        var kg = try KgMemory.init(std.testing.allocator, ":memory:");
        defer kg.deinit();
        try recallAfterStoreContract(kg.memory());
    }
}

// ── Deinit-after-store contract ──────────────────────────────────────

/// Validates that deinit is safe after a successful store call —
/// post-store internal state must not require any extra "close" /
/// "flush" call before deinit. AGENTS.md §3.4 fail-fast: no UB, no
/// double-free, no leak.
///
/// Note: the Memory vtable has no `close()` method. An earlier draft of
/// this comment referenced one ("close not called") — that was wrong.
/// The contract being checked is simply that deinit alone is sufficient
/// teardown after a mutation.
fn contractDeinitAfterStore(m: Memory) !void {
    try m.store("ownership_probe", "content", .core, null);
}

test "every engine survives store-then-deinit" {
    // sqlite
    if (build_options.enable_sqlite) {
        var mem = try SqliteMemory.init(std.testing.allocator, ":memory:");
        defer mem.deinit();
        try contractDeinitAfterStore(mem.memory());
    }

    // markdown
    {
        var tmp = std.testing.tmpDir(.{});
        defer tmp.cleanup();
        const base = try @import("compat").fs.Dir.wrap(tmp.dir).realpathAlloc(std.testing.allocator, ".");
        defer std.testing.allocator.free(base);
        var mem = try MarkdownMemory.init(std.testing.allocator, base);
        defer mem.deinit();
        try contractDeinitAfterStore(mem.memory());
    }

    // lru
    {
        var mem = InMemoryLruMemory.init(std.testing.allocator, 100);
        defer mem.deinit();
        try contractDeinitAfterStore(mem.memory());
    }

    // none — no-op store, but contract still applies
    {
        var mem = NoneMemory.init();
        defer mem.deinit();
        try contractDeinitAfterStore(mem.memory());
    }

    // kg
    if (build_options.enable_memory_kg) {
        var mem = try KgMemory.init(std.testing.allocator, ":memory:");
        defer mem.deinit();
        try contractDeinitAfterStore(mem.memory());
    }
}
