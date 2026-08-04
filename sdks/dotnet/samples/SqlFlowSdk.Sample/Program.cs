// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using Microsoft.AspNetCore.Mvc;
using SqlFlowSdk.Extensions;
using SqlFlowSdk.Core;
using SqlFlowSdk.Sample.Models;
using SqlFlowSdk.Sample.Services;
using SqlFlowSdk.Sample.Docker;
using SqlFlowSdk.Sample.Jobs;

var builder = WebApplication.CreateBuilder(args);

// Start Docker Containers for dependencies
await DockerContainers.StartAllContainersAsync();

string connectionString = DockerContainers.PostgresContainer.GetConnectionString();

// Add Logging
builder.Services.AddLogging();

// Register Services
builder.Services.AddSingleton<PaymentService>();
builder.Services.AddSingleton<ShippingService>();
builder.Services.AddSingleton<OrderService>();

// Register the SqlServerFlow SDK
builder.Services.AddSqlFlowSdk(connectionString);

// Configure Workers and Jobs. In this example, we have two different queues
// for standard and VIP orders, each with its own processing configuration.
builder.Services.AddSqlFlowWorker("standard-orders-queue", worker =>
{
    worker
        .SetConcurrency(1)
        .SetPollInterval(1);

    worker.AddJob<FulfillOrderJob, OrderData, FulfillOrderResult>("standard-fulfill", options =>
    {
        options.WithMaxAttempts(3);
    });
});

builder.Services.AddSqlFlowWorker("vip-orders-queue", worker =>
{
    worker
        .SetConcurrency(5)
        .SetPollInterval(0.5);

    worker.AddJob<FulfillOrderJob, OrderData, FulfillOrderResult>("vip-fulfill", options =>
    {
        options.WithMaxAttempts(5);
    });
});

var app = builder.Build();

// A User places an order through this endpoint. Depending on whether it's a VIP order or not, it gets published
// to a different queue with different processing configurations.
app.MapPost("/order", async (IJobPublisher publisher, [FromBody] OrderData request, CancellationToken ct) =>
{
    // VIP Orders go to the VIP Queue with a different Job configuration (e.g. more retries, faster processing, etc.)
    string queueName = request.IsPremium ? "vip-orders-queue" : "standard-orders-queue";

    SpawnResult result = await publisher.PublishAsync<FulfillOrderJob, OrderData>("vip-fulfill", request, ct);

    return Results.Ok(new { RunId = result.RunId });
});

app.MapPost("/order/{orderId}/picked", async (OrderService orderService, IEventPublisher publisher, string orderId, [FromBody] PickingData data, CancellationToken ct) =>
{
    // We fetch the OrderData to determine which queue to publish the event to. In a real application, you might have this
    // information cached or included in the request to avoid an extra database call.
    OrderData orderData = await orderService.GetOrderByIdAsync(orderId);

    // Premium Customers Events go to the VIP Queue with a different Job configuration (e.g. more retries, faster processing, etc.)
    string queueName = orderData.IsPremium ? "vip-orders-queue" : "standard-orders-queue";

    // This wakes up the suspended task waiting for "order-picked:{orderId}"
    await publisher.EmitEventAsync(
        queue: queueName,
        eventName: $"order-picked:{orderId}",
        payload: data,
        ct
    );

    return Results.Ok(new { Message = "Pick signal sent. Workflow will resume." });
});

app.Run();