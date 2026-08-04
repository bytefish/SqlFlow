// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using SqlFlowSdk.Sample.Models;
using SqlFlowSdk.Sample.Services;
using SqlFlowSdk.Core;
using System.Text.Json.Nodes;

namespace SqlFlowSdk.Sample.Jobs;

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