// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using SqlFlowSdk.Sample.Models;

namespace SqlFlowSdk.Sample.Services;

public class ShippingService
{
    public async Task<ShippingResult> ShipAsync(string orderId, List<string> items)
    {
        await Task.Delay(500);

        return new ShippingResult
        {
            TrackingNumber = $"TRK-{Guid.NewGuid().ToString().Substring(0, 8).ToUpper()}",
            EstimatedDelivery = DateTime.UtcNow.AddDays(2)
        };
    }
}