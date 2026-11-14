var builder = DistributedApplication.CreateBuilder(args);

string postgresConnectionString = "Host=127.0.0.1;Port=5432;Database=sqlflow;Username=postgres;Password=SuperStrongPassword123;";
string natsConnectionString = "nats://127.0.0.1:4222";

// 1. NATS JetStream
var nats = builder.AddContainer("nats-broker", "nats", "2.10")
    .WithArgs("-js")
    .WithEndpoint(port: 4222, targetPort: 4222, name: "client", isProxied: false);

// 2. CitusDB Coordinator Node
var coordinator = builder.AddContainer("coordinator", "citusdata/citus", "12.1")
    .WithEnvironment("POSTGRES_USER", "postgres")
    .WithEnvironment("POSTGRES_PASSWORD", "SuperStrongPassword123")
    .WithEnvironment("POSTGRES_DB", "sqlflow")
    .WithEnvironment("POSTGRES_HOST_AUTH_METHOD", "trust")
    .WithEndpoint(port: 5432, targetPort: 5432, name: "tcp");

// 3. Database Deployer (vorab deklarieren, damit die Worker direkt angehängt werden können)
var dbDeployer = builder.AddProject<Projects.SqlFlowSdk_DistributedSample_DbDeployer>("db-deployer")
    .WithEnvironment("ConnectionStrings__SqlFlowDb", postgresConnectionString)
    .WaitFor(coordinator);

// 4. CitusDB Worker Nodes (10 Replicas) & direkte Kopplung an den Deployer
for (int i = 1; i <= 10; i++)
{
    var worker = builder.AddContainer($"worker-{i}", "citusdata/citus", "12.1")
        .WithEnvironment("POSTGRES_USER", "postgres")
        .WithEnvironment("POSTGRES_PASSWORD", "SuperStrongPassword123")
        .WithEnvironment("POSTGRES_DB", "sqlflow")
        .WithEnvironment("POSTGRES_HOST_AUTH_METHOD", "trust")
        .WithArgs("/wait-for-manager.sh")
        .WaitFor(coordinator);

    dbDeployer.WaitFor(worker);
}

// 5. Worker App (Wartet auf NATS und den vollständigen Abschluss des Deployers)
builder.AddProject<Projects.SqlFlowSdk_DistributedSample_WorkerApp>("sqlflow-workers")
    .WithEnvironment("ConnectionStrings__SqlFlowDb", postgresConnectionString)
    .WithEnvironment("ConnectionStrings__nats-broker", natsConnectionString)
    .WaitFor(nats)
    .WaitForCompletion(dbDeployer);

builder.Build().Run();