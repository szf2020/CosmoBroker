using System;
using System.Collections.Generic;
using System.Data;
using System.Threading;
using System.Threading.Tasks;
using CosmoSQLClient.Core;
using CosmoSQLClient.Sqlite;

namespace CosmoBroker.Persistence;

public class MessageRepository
{
    private readonly ISqlDatabase _db;

    public MessageRepository(string connectionString)
    {
        _db = new SqliteConnectionPool(SqliteConfiguration.Parse(connectionString), maxConnections: 5);
    }

    public async Task InitializeAsync(CancellationToken ct = default)
    {
        // Simple schema application for SQLite
        var schema = @"
            CREATE TABLE IF NOT EXISTS mq_messages (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                subject     TEXT NOT NULL,
                payload     BLOB NOT NULL,
                created_at  DATETIME DEFAULT CURRENT_TIMESTAMP
            );
            CREATE TABLE IF NOT EXISTS mq_consumers (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                name        TEXT NOT NULL UNIQUE,
                subject     TEXT NOT NULL,
                last_msg_id INTEGER DEFAULT 0
            );
            CREATE TABLE IF NOT EXISTS mq_users (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                username    TEXT NOT NULL UNIQUE,
                password    TEXT NOT NULL,
                token       TEXT
            );
            CREATE INDEX IF NOT EXISTS idx_mq_messages_subject ON mq_messages(subject);";

        var statements = schema.Split(';', StringSplitOptions.RemoveEmptyEntries);
        foreach (var stmt in statements)
        {
            await _db.ExecuteAsync(stmt, ct: ct);
        }
    }

    public async Task AddUserAsync(string username, string password, string? token = null, CancellationToken ct = default)
    {
        await _db.ExecuteAsync(
            "INSERT OR IGNORE INTO mq_users (username, password, token) VALUES (@u, @p, @t)",
            new[] {
                SqlParameter.Named("u", SqlValue.From(username)),
                SqlParameter.Named("p", SqlValue.From(password)),
                SqlParameter.Named("t", SqlValue.From(token))
            }, ct: ct);
    }

    public async Task<bool> ValidateUserAsync(string username, string password, CancellationToken ct = default)
    {
        var rows = await _db.QueryAsync(
            "SELECT 1 FROM mq_users WHERE username = @u AND password = @p",
            new[] {
                SqlParameter.Named("u", SqlValue.From(username)),
                SqlParameter.Named("p", SqlValue.From(password))
            }, ct: ct);
        return rows.Count > 0;
    }

    public async Task<bool> ValidateTokenAsync(string token, CancellationToken ct = default)
    {
        var rows = await _db.QueryAsync(
            "SELECT 1 FROM mq_users WHERE token = @t",
            new[] { SqlParameter.Named("t", SqlValue.From(token)) }, ct: ct);
        return rows.Count > 0;
    }

    public async Task<long> SaveMessageAsync(string subject, byte[] payload, CancellationToken ct = default)
    {
        await _db.ExecuteAsync(
            "INSERT INTO mq_messages (subject, payload) VALUES (@subject, @payload)",
            new[] { 
                SqlParameter.Named("subject", SqlValue.From(subject)),
                SqlParameter.Named("payload", SqlValue.From(payload))
            }, ct: ct);
        
        var rows = await _db.QueryAsync("SELECT last_insert_rowid() as id", ct: ct);
        return rows[0]["id"].AsInt() ?? 0;
    }

    public async Task<List<PersistedMessage>> GetMessagesAsync(string subject, long startAfterId, int limit = 100, CancellationToken ct = default)
    {
        var query = "SELECT id, subject, payload FROM mq_messages WHERE subject = @subject AND id > @id ORDER BY id ASC LIMIT @limit";
        var rows = await _db.QueryAsync(query, new[] {
            SqlParameter.Named("subject", SqlValue.From(subject)),
            SqlParameter.Named("id", SqlValue.From(startAfterId)),
            SqlParameter.Named("limit", SqlValue.From(limit))
        }, ct: ct);

        var list = new List<PersistedMessage>();
        foreach (var row in rows)
        {
            list.Add(new PersistedMessage {
                Id = row["id"].AsInt() ?? 0,
                Subject = row["subject"].AsString() ?? string.Empty,
                Payload = row["payload"].AsBytes() ?? Array.Empty<byte>()
            });
        }
        return list;
    }

    public async Task<long> GetConsumerOffsetAsync(string name, CancellationToken ct = default)
    {
        var rows = await _db.QueryAsync("SELECT last_msg_id FROM mq_consumers WHERE name = @name",
            new[] { SqlParameter.Named("name", SqlValue.From(name)) }, ct: ct);
        
        if (rows.Count == 0) return 0;
        return rows[0]["last_msg_id"].AsInt() ?? 0;
    }

    public async Task UpdateConsumerOffsetAsync(string name, string subject, long lastId, CancellationToken ct = default)
    {
        // Upsert logic for SQLite
        await _db.ExecuteAsync(
            "INSERT INTO mq_consumers (name, subject, last_msg_id) VALUES (@name, @subject, @id) " +
            "ON CONFLICT(name) DO UPDATE SET last_msg_id = excluded.last_msg_id",
            new[] {
                SqlParameter.Named("name", SqlValue.From(name)),
                SqlParameter.Named("subject", SqlValue.From(subject)),
                SqlParameter.Named("id", SqlValue.From(lastId))
            }, ct: ct);
    }
}

public class PersistedMessage
{
    public long Id { get; set; }
    public required string Subject { get; set; }
    public required byte[] Payload { get; set; }
}
