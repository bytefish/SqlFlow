using Npgsql;

Console.WriteLine("=== SqlFlow DbDeployer started ===");

// Get the connection string from environment variable or use default
string connectionString = Environment.GetEnvironmentVariable("ConnectionStrings__SqlFlowDb")
    ?? "Host=127.0.0.1;Port=5432;Database=sqlflow;Username=postgres;Password=SuperStrongPassword123;";

await WaitForDatabaseAsync(connectionString);

string baseSqlPath = Path.Combine(AppContext.BaseDirectory, "sql", "ssf-postgres-minimal.sql");
string citusSqlPath = Path.Combine(AppContext.BaseDirectory, "sql", "citus-db-sharding.sql");

string baseSql = await File.ReadAllTextAsync(baseSqlPath);
string citusSql = await File.ReadAllTextAsync(citusSqlPath);

await ExecuteSqlAsync(connectionString, "SqlFlow-Schema (ssf-postgres-minimal.sql)", baseSql);
await ExecuteSqlAsync(connectionString, "CitusDB Sharding (citus-sharding.sql)", citusSql);

Console.WriteLine("=== SqlFlow DbDeployer completed successfully ===");
return 0;

static async Task WaitForDatabaseAsync(string connStr)
{
    Console.WriteLine("Waiting for Citus and Postgres Database...");
    int maxRetries = 30;

    for (int i = 1; i <= maxRetries; i++)
    {
        try
        {
            await using var conn = new NpgsqlConnection(connStr);
            await conn.OpenAsync();
            Console.WriteLine("--> Datenbank-Connection established");
            return;
        }
        catch (Exception)
        {
            if (i == maxRetries) throw;
            Console.WriteLine($"[Attempt {i}/{maxRetries}] Database is not ready yet. Retrying in 2 seconds...");
            await Task.Delay(2000);
        }
    }
}

static async Task ExecuteSqlAsync(string connStr, string description, string sql)
{
    Console.WriteLine($"Executing: {description}...");

    await using var conn = new NpgsqlConnection(connStr);
    await conn.OpenAsync();

    await using var cmd = conn.CreateCommand();
    cmd.CommandText = sql;
    await cmd.ExecuteNonQueryAsync();

    Console.WriteLine($"--> {description} executed successfully");
}