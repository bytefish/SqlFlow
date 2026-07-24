# SqlFlow.NET #

SqlFlow is a simple durable execution workflow system, for PostgreSQL and SQL Server. It handles scheduling and retries, without needing any other services to run in addition to PostgreSQL or SQL Server.

The SQL Script for creating the SqlFlow Database Schema is available here:

* [https://github.com/bytefish/SqlFlow/blob/main/sql/](https://github.com/bytefish/SqlFlow/blob/main/sql)

## 1. Setup ##

To include SqlFlowSdk in your project, install the NuGet package using the .NET CLI:

```
dotnet add package SqlFlowSdk
```

Alternatively, you can use the NuGet Package Manager in Visual Studio:

```
Install-Package SqlFlowSdk
```

## 2. Quick Start ##

We start by defining a `IJob`, which is going to model an Order Fulfillment Task:

```csharp
public class FulfillOrderJob : IJob<OrderData, FulfillOrderResult>
{
    private readonly ILogger<FulfillOrderJob> _logger;

    private readonly PaymentService _paymentService;
    private readonly ShippingService _shippingService;
    
    public FulfillOrderJob(
        PaymentService paymentService,
        ShippingService shippingService,
        ILogger<FulfillOrderJob> logger)
    {
        _paymentService = paymentService;
        _shippingService = shippingService;
        _logger = logger;
    }

    public async Task<FulfillOrderResult> ExecuteAsync(TaskContext ctx, OrderData order)
    {
        _logger.LogInformation("Processing Order {OrderId}", order.OrderId);

        // Process the Payment
        PaymentResult payment = await ctx.Step("charge-payment", async () =>
        {
            return await _paymentService.ChargeAsync(order.OrderId, order.Amount);
        });

        if (!payment.Success)
        {
            throw new Exception($"Payment failed: {payment.ErrorMessage}");
        }

        // Wait for Warehouse
        _logger.LogInformation("Waiting for pick signal...");

        JsonNode pickPayload = await ctx.AwaitEvent(
            eventName: $"order-picked:{order.OrderId}",
            stepName: "wait-for-picking"
        );

        // Ship the items
        ShippingResult shipment = await ctx.Step("ship-items", async () =>
        {
            return await _shippingService.ShipAsync(order.OrderId, order.Items);
        });

        return new FulfillOrderResult { Status = "Fulfilled", Tracking = shipment.TrackingNumber };
    }
}
```

We then register it in the `Program.cs` like this:

```csharp
// Your custom connection string
string connectionString = "Server=localhost;Database=DeinDatenbankName;Integrated Security=True;TrustServerCertificate=True";

// Add Logging
builder.Services.AddLogging();

// Register Services
builder.Services.AddSingleton<PaymentService>();
builder.Services.AddSingleton<ShippingService>();
builder.Services.AddSingleton<OrderService>();

// Register the SqlFlow SDK
builder.Services.AddSqlFlowSdk(connectionString);
```

We can then create a Background Workers for polling and executing tasks:

```csharp
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
```

And finally we define two endpoints to create an order and emit events to it:

```csharp
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
```

To kick off an Order send the JSON Payload to the endpoints:

```http
### Creates Order "ORD-123"
POST https://localhost:5000/order
Content-Type: application/json
Accept-Language: en-US,en;q=0.5
{
  "orderId": "ORD-123",
  "isPremium": true,
  "amount": 99.50,
  "items": ["Item A", "Item B"]
}

### Continues running Order "ORD-123"
POST https://localhost:5000/order/ORD-123/picked
Content-Type: application/json
{ 
  "picker": "Philipp",
  "pickedAt": "2024-06-01T10:15:30Z"
}
```
