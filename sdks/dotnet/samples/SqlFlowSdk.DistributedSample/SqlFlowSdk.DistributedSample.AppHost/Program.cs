var builder = DistributedApplication.CreateBuilder(args);

string postgresConnectionString = "Host=127.0.0.1;Port=5432;Database=sqlflow;Username=postgres;Password=SuperStrongPassword123;";
string natsConnectionString = "nats://127.0.0.1:4222";

// NATS JetStream
var nats = builder.AddContainer("nats-broker", "nats", "2.10")
    .WithArgs("-js")
    .WithEndpoint(port: 4222, targetPort: 4222, name: "client", isProxied: false);

// CitusDB Coordinator Node
var coordinator = builder.AddContainer("coordinator", "citusdata/citus", "12.1")
    .WithEnvironment("POSTGRES_USER", "postgres")
    .WithEnvironment("POSTGRES_PASSWORD", "SuperStrongPassword123")
    .WithEnvironment("POSTGRES_DB", "sqlflow")
    .WithEnvironment("POSTGRES_HOST_AUTH_METHOD", "trust")
    .WithEndpoint(port: 5432, targetPort: 5432, name: "tcp");

// Database Deployer (declared upfront so workers can be attached directly)
var dbDeployer = builder.AddProject<Projects.SqlFlowSdk_DistributedSample_DbDeployer>("db-deployer")
    .WithEnvironment("ConnectionStrings__SqlFlowDb", postgresConnectionString)
    .WaitFor(coordinator);

// CitusDB Worker Nodes (10 Replicas) & direct attachment to the deployer
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

// Worker App (waits for NATS and full completion of the deployer)
builder.AddProject<Projects.SqlFlowSdk_DistributedSample_WorkerApp>("sqlflow-workers")
    .WithEnvironment("ConnectionStrings__SqlFlowDb", postgresConnectionString)
    .WithEnvironment("ConnectionStrings__nats-broker", natsConnectionString)
    .WaitFor(nats)
    .WaitForCompletion(dbDeployer);

builder.Build().Run();