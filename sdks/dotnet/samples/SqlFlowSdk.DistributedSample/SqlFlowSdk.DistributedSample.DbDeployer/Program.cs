using Npgsql;

Console.WriteLine("=== SqlFlow DbDeployer started ===");

string connectionString = Environment.GetEnvironmentVariable("ConnectionStrings__SqlFlowDb")
    ?? "Host=127.0.0.1;Port=5432;Database=sqlflow;Username=postgres;Password=SuperStrongPassword123;";

await WaitForDatabaseAsync(connectionString);

string baseSqlPath = Path.Combine(AppContext.BaseDirectory, "sql", "ssf-postgres-minimal.sql");
string citusSqlPath = Path.Combine(AppContext.BaseDirectory, "sql", "citus-db-sharding.sql");

string baseSql = await File.ReadAllTextAsync(baseSqlPath);
string citusSql = await File.ReadAllTextAsync(citusSqlPath);

// Create Schema, Tables, Functions, etc. 
await ExecuteSqlAsync(connectionString, "SqlFlow-Schema (ssf-postgres-minimal.sql)", baseSql);

// CitusDB Sharding: Create Distributed Tables, Shards, etc.
await ExecuteCitusShardingAsync(connectionString, "CitusDB Sharding (citus-db-sharding.sql)", citusSql);

// Verify that the deployment was successful and that the schema and workers are correctly set up
await VerifyDeploymentAsync(connectionString);

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
            Console.WriteLine("--> Database connection established");
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

static async Task ExecuteCitusShardingAsync(string connStr, string description, string sql)
{
    Console.WriteLine($"Executing: {description} (statement by statement)...");

    await using var conn = new NpgsqlConnection(connStr);
    await conn.OpenAsync();

    // Trennt DO $$ ... END $$; Blöcke und einzelne SELECTs sauber auf, 
    // damit Npgsql sie nicht in eine gemeinsame Transaktion verpackt.
    var statements = SplitSqlStatements(sql);

    foreach (var stmt in statements)
    {
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = stmt;
        await cmd.ExecuteNonQueryAsync();
    }

    Console.WriteLine($"--> {description} executed successfully");
}

static async Task VerifyDeploymentAsync(string connStr)
{
    await using var conn = new NpgsqlConnection(connStr);
    await conn.OpenAsync();

    await using var schemaCmd = conn.CreateCommand();
    schemaCmd.CommandText = "SELECT schema_name FROM information_schema.schemata WHERE schema_name = 'ssf';";
    var schema = await schemaCmd.ExecuteScalarAsync();

    if (schema is null)
        throw new InvalidOperationException("Verification failed: Schema 'ssf' was not found after deployment!");

    await using var workerCmd = conn.CreateCommand();
    workerCmd.CommandText = "SELECT count(*) FROM citus_get_active_worker_nodes();";
    var workerCount = Convert.ToInt32(await workerCmd.ExecuteScalarAsync());

    Console.WriteLine($"--> Verified: Schema '{schema}' exists, active Citus workers: {workerCount}");
}

static IEnumerable<string> SplitSqlStatements(string sql)
{
    // Zerlegt das Skript anhand von "END $$;" oder ";" außerhalb von Dollar-Quotes
    var results = new List<string>();
    var current = new System.Text.StringBuilder();
    bool inDollarQuote = false;

    var lines = sql.Split('\n');
    foreach (var rawLine in lines)
    {
        var line = rawLine.Trim();
        if (string.IsNullOrWhiteSpace(line) || line.StartsWith("--"))
            continue;

        current.AppendLine(rawLine);

        // Zählt Vorkommen von $$ in der Zeile, um DO $$ ... END $$; Blöcke zusammenzuhalten
        int dollarCount = (line.Length - line.Replace("$$", "").Length) / 2;
        if (dollarCount % 2 != 0)
            inDollarQuote = !inDollarQuote;

        if (!inDollarQuote && line.EndsWith(";"))
        {
            results.Add(current.ToString().Trim());
            current.Clear();
        }
    }

    if (current.Length > 0)
        results.Add(current.ToString().Trim());

    return results;
}