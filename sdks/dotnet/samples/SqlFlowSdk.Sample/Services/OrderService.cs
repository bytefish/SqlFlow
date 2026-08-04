// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using SqlFlowSdk.Sample.Models;
using System.Collections.Concurrent;

namespace SqlFlowSdk.Sample.Services;

public class OrderService
{
    private readonly ConcurrentDictionary<string, OrderData> _orders = new();

    public OrderService()
    {
        _orders["ORD-123"] = new OrderData { OrderId = "ORD-123", IsPremium = true };
    }

    public async Task<OrderData> GetOrderByIdAsync(string orderId, CancellationToken cancellationToken = default)
    {
        await Task.Yield();

        return _orders.TryGetValue(orderId, out var order) ? order : throw new KeyNotFoundException();
    }
}